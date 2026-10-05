package jobs

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrUnknownType  = errors.New("unknown job type")
	ErrStaleAttempt = errors.New("stale attempt")
	ErrTerminal     = errors.New("job is terminal")
)

var (
	ErrUnknownDependency = errors.New("unknown dependency")
	ErrDependencyFailed  = errors.New("dependency did not succeed")
	ErrArtifactRevoked   = errors.New("dependency artifact revoked")
	ErrExternalPending   = errors.New("dependency has unverified external effects")
	ErrNotSucceeded      = errors.New("job has no confirmed artifact")
	ErrStaleGeneration   = errors.New("revision targets an outdated generation")
	ErrRevisionClosed    = errors.New("revision is no longer open")
	ErrChangeNotOpen     = errors.New("change is not open")
	ErrReviewerMismatch  = errors.New("reviewer is not assigned to this change")
	ErrReviewerRequired  = errors.New("assigned change requires a named reviewer")
	ErrReviewConflict    = errors.New("change already carries an effective different decision")
)

type Manager struct {
	store    *Store
	registry *Registry
	now      func() time.Time
}

type RetryConfig struct {
	Delay time.Duration
}

func NewManager(store *Store, registry *Registry) *Manager {
	return &Manager{store: store, registry: registry, now: nowTime}
}

type EnqueueOption func(*Job)

func WithDependencies(ids ...string) EnqueueOption {
	return func(job *Job) {
		seen := map[string]bool{}
		for _, id := range ids {
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			job.DependsOn = append(job.DependsOn, id)
		}
	}
}

func WithGroup(groupID string) EnqueueOption {
	return func(job *Job) { job.GroupID = groupID }
}

func (m *Manager) Enqueue(typ, payload string, maxAttempts int, opts ...EnqueueOption) (*Job, error) {
	def, ok := m.registry.Get(typ)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownType, typ)
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	now := m.now()
	job := &Job{
		Type:        typ,
		Payload:     payload,
		Status:      StatusPending,
		SafeRetry:   def.SafeToRetry,
		MaxAttempts: maxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	for _, opt := range opts {
		opt(job)
	}
	for _, depID := range job.DependsOn {
		dep, _, err := m.store.Get(depID)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrUnknownDependency, depID)
		}
		if dep.Status == StatusFailed || dep.Status == StatusCanceled || dep.ArtifactState == ArtifactRevoked {
			return nil, fmt.Errorf("%w: %s is %s", ErrDependencyFailed, depID, dep.Status)
		}
	}
	return job, m.store.Create(job)
}

func (m *Manager) Get(id string) (*Job, []*Attempt, error) {
	return m.store.Get(id)
}

func (m *Manager) List() ([]*Job, error) { return m.store.List() }

func (m *Manager) Claim() (*Job, *Attempt, error) {
	jobs, err := m.store.List()
	if err != nil {
		return nil, nil, err
	}
	now := m.now()
	for _, candidate := range jobs {
		if candidate.Status != StatusPending || (candidate.NextRunAt != nil && candidate.NextRunAt.After(now)) {
			continue
		}
		claimed, attempts, err := m.store.MutateGraph(candidate.ID, func(job *Job, list *[]*Attempt, all map[string]*Job) error {
			if job.Status != StatusPending || (job.NextRunAt != nil && job.NextRunAt.After(now)) {
				return nil
			}
			depResults, depGenerations, ready, blocked := resolveDependencies(job, all)
			if blocked != "" {
				job.Status = StatusCanceled
				job.Error = blocked
				job.NextRunAt = nil
				job.UpdatedAt = now
				return nil
			}
			if !ready {
				return nil
			}
			job.AttemptCount++
			job.CurrentAttempt = job.AttemptCount
			job.Status = StatusRunning
			job.NextRunAt = nil
			job.Error = ""
			job.UpdatedAt = now
			revisionIDs := uniqueStrings(job.PendingRevisionIDs)
			revisionID := job.PendingRevisionID
			if revisionID == "" && len(revisionIDs) > 0 {
				revisionID = revisionIDs[0]
			}
			if revisionID != "" && !containsString(revisionIDs, revisionID) {
				revisionIDs = append([]string{revisionID}, revisionIDs...)
			}
			job.PendingRevisionID = ""
			job.PendingRevisionIDs = nil
			*list = append(*list, &Attempt{Number: job.AttemptCount, Status: AttemptRunning, StartedAt: now, DepResults: depResults, DepGenerations: depGenerations, RevisionID: revisionID, RevisionIDs: revisionIDs})
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
		if claimed.Status == StatusRunning {
			return claimed, attempts[len(attempts)-1], nil
		}
	}
	return nil, nil, nil
}

// resolveDependencies inspects a pending job's dependencies. It returns the
// confirmed results of succeeded predecessors once every dependency has
// succeeded. A dependency that failed or was canceled permanently blocks the
// job, reported as a non-empty blocked reason.
// resolveDependencies returns the confirmed artifacts a pending job may
// consume, together with the exact generation of each one. Revoked artifacts
// permanently block the job; open external-effect verifications make it wait
// without canceling it, so no branch proceeds against an unverified result.
func resolveDependencies(job *Job, all map[string]*Job) (map[string]string, map[string]int, bool, string) {
	if len(job.DependsOn) == 0 {
		return nil, nil, true, ""
	}
	results := make(map[string]string, len(job.DependsOn))
	generations := make(map[string]int, len(job.DependsOn))
	for _, depID := range job.DependsOn {
		dep, ok := all[depID]
		if !ok {
			return nil, nil, false, fmt.Sprintf("%s: %s is missing", ErrDependencyFailed, depID)
		}
		if dep.ArtifactState == ArtifactRevoked {
			return nil, nil, false, fmt.Sprintf("%s: %s artifact was revoked", ErrArtifactRevoked, depID)
		}
		switch dep.Status {
		case StatusSucceeded:
			if dep.ExternalPending || len(dep.ExternalPendingIDs) > 0 {
				return nil, nil, false, ""
			}
			active := activeArtifact(dep)
			if active == nil {
				return nil, nil, false, ""
			}
			results[depID] = active.Result
			generations[depID] = active.Generation
		case StatusFailed, StatusCanceled:
			return nil, nil, false, fmt.Sprintf("%s: %s is %s", ErrDependencyFailed, depID, dep.Status)
		default:
			return nil, nil, false, ""
		}
	}
	return results, generations, true, ""
}

func (m *Manager) Complete(jobID string, attempt int, result string, execErr error) (*Job, error) {
	now := m.now()
	job, _, err := m.store.Mutate(jobID, func(job *Job, attempts *[]*Attempt) error {
		if job.Status != StatusRunning || job.CurrentAttempt != attempt {
			return ErrStaleAttempt
		}
		record := (*attempts)[len(*attempts)-1]
		if record.Number != attempt || record.Status != AttemptRunning {
			return ErrStaleAttempt
		}
		record.FinishedAt = &now
		if execErr != nil {
			record.Status = AttemptFailed
			record.Error = execErr.Error()
			job.Error = execErr.Error()
			if job.SafeRetry && attempt < job.MaxAttempts {
				next := now.Add(100 * time.Millisecond)
				job.Status = StatusPending
				job.NextRunAt = &next
				job.CurrentAttempt = 0
			} else {
				job.Status = StatusFailed
				job.CurrentAttempt = 0
				if !job.SafeRetry {
					job.Error = "non-idempotent job is not safe to retry: " + job.Error
				}
			}
		} else {
			record.Status = AttemptSucceeded
			record.Result = result
			job.Status = StatusSucceeded
			job.Result = result
			job.Error = ""
			job.CurrentAttempt = 0
			job.NextRunAt = nil
			revisionIDs := uniqueStrings(append(append([]string{}, record.RevisionIDs...), record.RevisionID))
			commitArtifact(job, result, attempt, record.DepGenerations, revisionIDs, now)
		}
		job.UpdatedAt = now
		return nil
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (m *Manager) Cancel(jobID string) (*Job, error) {
	now := m.now()
	job, _, err := m.store.Mutate(jobID, func(job *Job, attempts *[]*Attempt) error {
		switch job.Status {
		case StatusSucceeded, StatusFailed, StatusCanceled:
			return ErrTerminal
		case StatusRunning:
			for i := len(*attempts) - 1; i >= 0; i-- {
				record := (*attempts)[i]
				if record.Number == job.CurrentAttempt && record.Status == AttemptRunning {
					record.Status = AttemptCanceled
					record.FinishedAt = &now
					record.Error = "canceled"
					break
				}
			}
		}
		job.Status = StatusCanceled
		job.Error = "canceled"
		job.CurrentAttempt = 0
		job.NextRunAt = nil
		job.UpdatedAt = now
		return nil
	})
	return job, err
}

// CancelGroup cancels every non-terminal job in a group. Jobs that already
// reached a terminal state keep their records untouched; running jobs get an
// independent CANCELED terminal state for their in-flight attempt.
func (m *Manager) CancelGroup(groupID string) ([]*Job, error) {
	all, err := m.store.List()
	if err != nil {
		return nil, err
	}
	var canceled []*Job
	for _, job := range all {
		if job.GroupID != groupID {
			continue
		}
		updated, err := m.Cancel(job.ID)
		if errors.Is(err, ErrTerminal) {
			continue
		}
		if err != nil {
			return canceled, err
		}
		canceled = append(canceled, updated)
	}
	return canceled, nil
}

func (m *Manager) RecoverInterrupted() ([]string, error) {
	now := m.now()
	var recovered []string
	jobs, err := m.store.List()
	if err != nil {
		return nil, err
	}
	for _, candidate := range jobs {
		if candidate.Status != StatusRunning {
			continue
		}
		_, _, err := m.store.Mutate(candidate.ID, func(job *Job, attempts *[]*Attempt) error {
			if job.Status != StatusRunning {
				return nil
			}
			oldNumber := job.CurrentAttempt
			for i := len(*attempts) - 1; i >= 0; i-- {
				record := (*attempts)[i]
				if record.Number == oldNumber && record.Status == AttemptRunning {
					record.Status = AttemptInterrupted
					record.FinishedAt = &now
					record.Error = "worker stopped before result was confirmed"
					break
				}
			}
			if job.SafeRetry && oldNumber < job.MaxAttempts {
				job.Status = StatusPending
				job.CurrentAttempt = 0
				job.Error = "recovering interrupted attempt"
				job.NextRunAt = nil
				recovered = append(recovered, job.ID)
			} else {
				job.Status = StatusFailed
				job.CurrentAttempt = 0
				job.Error = "interrupted before confirmation; result is ambiguous and cannot safely be retried"
			}
			job.UpdatedAt = now
			return nil
		})
		if err != nil {
			return recovered, err
		}
	}
	return recovered, nil
}
