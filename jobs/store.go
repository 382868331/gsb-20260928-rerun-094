package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var ErrNotFound = errors.New("job not found")

type snapshot struct {
	Seq       int                   `json:"seq"`
	NextSeq   int                   `json:"next_seq"`
	Jobs      map[string]*Job       `json:"jobs"`
	Attempts  map[string][]*Attempt `json:"attempts"`
	Revisions map[string]*Revision  `json:"revisions,omitempty"`
}

type Store struct {
	path string
	mu   sync.Mutex
}

func OpenStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &Store{path: path}
	return s, s.withLock(func(*snapshot) error { return nil })
}

func (s *Store) Close() error { return nil }

func (s *Store) Create(job *Job, attempts ...*Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withLock(func(snap *snapshot) error {
		if job.ID != "" {
			if _, exists := snap.Jobs[job.ID]; exists {
				return fmt.Errorf("job %s already exists", job.ID)
			}
		}
		if job.ID == "" {
			snap.NextSeq++
			job.ID = fmt.Sprintf("job-%d-%d", snap.NextSeq, job.CreatedAt.UnixNano())
		}
		jobCopy := *job
		snap.Jobs[job.ID] = &jobCopy
		list := make([]*Attempt, len(attempts))
		for i := range attempts {
			a := *attempts[i]
			list[i] = &a
		}
		snap.Attempts[job.ID] = list
		return nil
	})
}

func (s *Store) Get(id string) (*Job, []*Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result *Job
	var resultAttempts []*Attempt
	err := s.withLock(func(snap *snapshot) error {
		job, ok := snap.Jobs[id]
		if !ok {
			return ErrNotFound
		}
		jobCopy := *job
		result = &jobCopy
		resultAttempts = copyAttempts(snap.Attempts[id])
		return nil
	})
	return result, resultAttempts, err
}

func (s *Store) List() ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var jobs []*Job
	err := s.withLock(func(snap *snapshot) error {
		jobs = make([]*Job, 0, len(snap.Jobs))
		for _, job := range snap.Jobs {
			jobCopy := *job
			jobs = append(jobs, &jobCopy)
		}
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.Before(jobs[j].CreatedAt) })
		return nil
	})
	return jobs, err
}

func (s *Store) Mutate(id string, fn func(*Job, *[]*Attempt) error) (*Job, []*Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result *Job
	var resultAttempts []*Attempt
	err := s.withLock(func(snap *snapshot) error {
		job, ok := snap.Jobs[id]
		if !ok {
			return ErrNotFound
		}
		attempts := copyAttempts(snap.Attempts[id])
		if err := fn(job, &attempts); err != nil {
			return err
		}
		snap.Attempts[id] = attempts
		jobCopy := *job
		result = &jobCopy
		resultAttempts = copyAttempts(attempts)
		return nil
	})
	return result, resultAttempts, err
}

// MutateGraph works like Mutate but also exposes the full job map so the
// callback can resolve dependencies atomically inside the same transaction.
func (s *Store) MutateGraph(id string, fn func(job *Job, attempts *[]*Attempt, all map[string]*Job) error) (*Job, []*Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result *Job
	var resultAttempts []*Attempt
	err := s.withLock(func(snap *snapshot) error {
		job, ok := snap.Jobs[id]
		if !ok {
			return ErrNotFound
		}
		attempts := copyAttempts(snap.Attempts[id])
		if err := fn(job, &attempts, snap.Jobs); err != nil {
			return err
		}
		snap.Attempts[id] = attempts
		jobCopy := *job
		result = &jobCopy
		resultAttempts = copyAttempts(attempts)
		return nil
	})
	return result, resultAttempts, err
}

func (s *Store) withLock(fn func(*snapshot) error) error {
	lock, err := os.OpenFile(s.path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := flock(lock); err != nil {
		return err
	}
	defer funlock(lock)
	snap, err := loadSnapshot(s.path)
	if err != nil {
		return err
	}
	if err := fn(snap); err != nil {
		return err
	}
	return saveSnapshot(s.path, snap)
}

func loadSnapshot(path string) (*snapshot, error) {
	snap := &snapshot{Jobs: map[string]*Job{}, Attempts: map[string][]*Attempt{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return snap, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, snap); err != nil {
		return nil, fmt.Errorf("load job store: %w", err)
	}
	if snap.Jobs == nil {
		snap.Jobs = map[string]*Job{}
	}
	if snap.Attempts == nil {
		snap.Attempts = map[string][]*Attempt{}
	}
	if snap.Revisions == nil {
		snap.Revisions = map[string]*Revision{}
	}
	if snap.NextSeq == 0 {
		snap.NextSeq = len(snap.Jobs)
	}
	return snap, nil
}

func saveSnapshot(path string, snap *snapshot) error {
	snap.Seq++
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp-%d", path, snap.Seq)
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func copyAttempts(in []*Attempt) []*Attempt {
	out := make([]*Attempt, len(in))
	for i := range in {
		a := *in[i]
		out[i] = &a
	}
	return out
}

// MutateMany runs fn inside one locked transaction over the whole graph:
// jobs, attempts and revisions are all committed or rolled back together.
func (s *Store) MutateMany(fn func(jobs map[string]*Job, attempts map[string][]*Attempt, revisions map[string]*Revision) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withLock(func(snap *snapshot) error {
		return fn(snap.Jobs, snap.Attempts, snap.Revisions)
	})
}

// SaveRevision inserts or replaces a revision, assigning an ID on insert.
func (s *Store) SaveRevision(rev *Revision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withLock(func(snap *snapshot) error {
		if rev.ID == "" {
			snap.NextSeq++
			rev.ID = fmt.Sprintf("rev-%d-%d", snap.NextSeq, rev.CreatedAt.UnixNano())
		}
		revCopy := *rev
		snap.Revisions[rev.ID] = &revCopy
		return nil
	})
}

func (s *Store) GetRevision(id string) (*Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result *Revision
	err := s.withLock(func(snap *snapshot) error {
		rev, ok := snap.Revisions[id]
		if !ok {
			return ErrRevisionNotFound
		}
		revCopy := *rev
		result = &revCopy
		return nil
	})
	return result, err
}

func (s *Store) ListRevisions() ([]*Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var revisions []*Revision
	err := s.withLock(func(snap *snapshot) error {
		revisions = make([]*Revision, 0, len(snap.Revisions))
		for _, rev := range snap.Revisions {
			revCopy := *rev
			revisions = append(revisions, &revCopy)
		}
		sort.Slice(revisions, func(i, j int) bool { return revisions[i].CreatedAt.Before(revisions[j].CreatedAt) })
		return nil
	})
	return revisions, err
}

func nowTime() time.Time { return time.Now().UTC() }
