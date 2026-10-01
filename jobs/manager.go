package jobs

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrUnknownType    = errors.New("unknown job type")
	ErrStaleAttempt   = errors.New("stale attempt")
	ErrTerminal       = errors.New("job is terminal")
	ErrNoVerification = errors.New("job is not waiting for verification")
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

func (m *Manager) Enqueue(typ, payload string, maxAttempts int) (*Job, error) {
	return m.EnqueueWithDeps(typ, payload, maxAttempts, nil, "")
}

// EnqueueWithDeps registers a job that becomes dispatchable only after all
// jobs in dependsOn have succeeded. groupID links jobs so they can be
// canceled together with CancelGroup.
func (m *Manager) EnqueueWithDeps(typ, payload string, maxAttempts int, dependsOn []string, groupID string) (*Job, error) {
	def, ok := m.registry.Get(typ)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownType, typ)
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	seen := map[string]bool{}
	for _, depID := range dependsOn {
		if depID == "" || seen[depID] {
			return nil, fmt.Errorf("invalid dependency %q", depID)
		}
		seen[depID] = true
		if _, _, err := m.store.Get(depID); err != nil {
			return nil, fmt.Errorf("dependency %s: %w", depID, err)
		}
	}
	now := m.now()
	job := &Job{
		Type:        typ,
		Payload:     payload,
		Status:      StatusPending,
		SafeRetry:   def.SafeToRetry,
		MaxAttempts: maxAttempts,
		DependsOn:   append([]string(nil), dependsOn...),
		GroupID:     groupID,
		CreatedAt:   now,
		UpdatedAt:   now,
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
		depResults, ready, skipReason, err := m.evaluateDeps(candidate)
		if err != nil {
			return nil, nil, err
		}
		if skipReason != "" {
			if _, _, err := m.store.Mutate(candidate.ID, func(job *Job, attempts *[]*Attempt) error {
				if job.Status != StatusPending {
					return nil
				}
				job.Status = StatusSkipped
				job.Error = skipReason
				job.NextRunAt = nil
				job.UpdatedAt = m.now()
				return nil
			}); err != nil {
				return nil, nil, err
			}
			continue
		}
		if !ready {
			continue
		}
		claimed, attempts, err := m.store.Mutate(candidate.ID, func(job *Job, list *[]*Attempt) error {
			if job.Status != StatusPending || (job.NextRunAt != nil && job.NextRunAt.After(now)) {
				return nil
			}
			job.AttemptCount++
			job.CurrentAttempt = job.AttemptCount
			job.Status = StatusRunning
			job.NextRunAt = nil
			job.Error = ""
			job.UpdatedAt = now
			*list = append(*list, &Attempt{Number: job.AttemptCount, Status: AttemptRunning, StartedAt: now})
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
		if claimed.Status == StatusRunning {
			claimed.DepResults = depResults
			return claimed, attempts[len(attempts)-1], nil
		}
	}
	return nil, nil, nil
}

// evaluateDeps reports whether every dependency of the job has reached a
// confirmed success. ready=false means at least one dependency is still
// running, pending, or waiting for verification of an ambiguous result.
// A non-empty skipReason means a dependency can never succeed and the job
// must be skipped instead of dispatched.
func (m *Manager) evaluateDeps(job *Job) (map[string]string, bool, string, error) {
	if len(job.DependsOn) == 0 {
		return nil, true, "", nil
	}
	results := make(map[string]string, len(job.DependsOn))
	for _, depID := range job.DependsOn {
		dep, _, err := m.store.Get(depID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, false, "dependency " + depID + " no longer exists", nil
			}
			return nil, false, "", err
		}
		switch dep.Status {
		case StatusSucceeded:
			results[depID] = dep.Result
		case StatusFailed:
			if dep.NeedsVerification {
				// Ambiguous external result: the successor must wait for
				// an operator to confirm what actually happened instead of
				// treating the step as failed or redoing it blindly.
				return nil, false, "", nil
			}
			return nil, false, "dependency " + depID + " failed: " + dep.Error, nil
		case StatusCanceled, StatusSkipped:
			return nil, false, "dependency " + depID + " ended as " + string(dep.Status), nil
		default:
			return nil, false, "", nil
		}
	}
	return results, true, "", nil
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
					job.Error = "non-idempotent job is not safe to retry; verify the external result before rerunning: " + job.Error
					job.NeedsVerification = true
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
		case StatusSucceeded, StatusFailed, StatusCanceled, StatusSkipped:
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

// Verify resolves a job whose non-idempotent attempt ended with an
// ambiguous external result. succeeded=true promotes the job to SUCCEEDED
// with the confirmed note as its result, unblocking dependents;
// succeeded=false confirms the failure so dependents are skipped. The
// original attempt history is preserved either way.
func (m *Manager) Verify(jobID string, succeeded bool, note string) (*Job, error) {
	now := m.now()
	job, _, err := m.store.Mutate(jobID, func(job *Job, attempts *[]*Attempt) error {
		if !job.NeedsVerification {
			return ErrNoVerification
		}
		job.NeedsVerification = false
		if succeeded {
			job.Status = StatusSucceeded
			if note == "" {
				note = "verified externally"
			}
			job.Result = note
			job.Error = ""
		} else {
			job.Status = StatusFailed
			if note != "" {
				job.Error = "verified failed: " + note
			}
		}
		job.UpdatedAt = now
		return nil
	})
	return job, err
}

// CancelGroup cancels every non-terminal job in the group. Jobs that
// already reached a terminal state keep their records untouched; running
// attempts receive their own CANCELED terminal state.
func (m *Manager) CancelGroup(groupID string) ([]string, error) {
	jobs, err := m.store.List()
	if err != nil {
		return nil, err
	}
	var canceled []string
	for _, candidate := range jobs {
		if candidate.GroupID != groupID {
			continue
		}
		if _, err := m.Cancel(candidate.ID); err != nil {
			if err == ErrTerminal {
				continue
			}
			return canceled, err
		}
		canceled = append(canceled, candidate.ID)
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
				if !job.SafeRetry {
					job.NeedsVerification = true
				}
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
