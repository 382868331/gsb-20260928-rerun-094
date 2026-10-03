package jobs

import (
	"errors"
	"fmt"
	"sort"
)

var (
	ErrRevisionNotFound  = errors.New("revision not found")
	ErrRevisionClosed    = errors.New("revision is already fully confirmed or canceled")
	ErrJobNotSucceeded   = errors.New("job has no confirmed result to revise")
	ErrNoExternalPending = errors.New("job has no unverified external effect")
)

// ChangeSpec declares one artifact change inside a revision candidate.
type ChangeSpec struct {
	JobID     string
	Action    ChangeAction
	NewResult string
}

// ProposeRevision registers a revision candidate over already-completed jobs
// and computes its impact: which downstream jobs would be re-executed, which
// confirmed results stay reusable, and which branches are blocked on
// unverified external effects. Nothing is applied until ConfirmRevision.
func (m *Manager) ProposeRevision(specs []ChangeSpec) (*Revision, error) {
	if len(specs) == 0 {
		return nil, errors.New("revision must declare at least one artifact change")
	}
	seen := map[string]bool{}
	now := m.now()
	rev := &Revision{Status: RevisionProposed, CreatedAt: now, UpdatedAt: now}
	for _, spec := range specs {
		if spec.JobID == "" || seen[spec.JobID] {
			continue
		}
		seen[spec.JobID] = true
		if spec.Action != ActionReplace && spec.Action != ActionWithdraw {
			return nil, fmt.Errorf("unknown change action %q for %s", spec.Action, spec.JobID)
		}
		job, _, err := m.store.Get(spec.JobID)
		if err != nil {
			return nil, err
		}
		if job.Status != StatusSucceeded || job.Withdrawn {
			return nil, fmt.Errorf("%w: %s is %s", ErrJobNotSucceeded, spec.JobID, job.Status)
		}
		rev.Changes = append(rev.Changes, &ArtifactChange{
			JobID:     spec.JobID,
			Action:    spec.Action,
			NewResult: spec.NewResult,
			Status:    ChangePending,
		})
	}
	if len(rev.Changes) == 0 {
		return nil, errors.New("revision must declare at least one artifact change")
	}
	all, err := m.store.List()
	if err != nil {
		return nil, err
	}
	rev.Impact = computeImpact(all, seen)
	if err := m.store.SaveRevision(rev); err != nil {
		return nil, err
	}
	return rev, nil
}

func (m *Manager) GetRevision(id string) (*Revision, error) { return m.store.GetRevision(id) }

func (m *Manager) ListRevisions() ([]*Revision, error) { return m.store.ListRevisions() }

// ConfirmRevision applies the pending changes of a revision. With only empty
// every pending change is applied; otherwise only the listed job IDs are
// applied and the rest stay pending for a later confirmation. Applied
// changes publish a new artifact generation (history is append-only) and
// reset affected downstream jobs so they re-run against the new generations.
func (m *Manager) ConfirmRevision(id string, only []string) (*Revision, error) {
	now := m.now()
	onlySet := map[string]bool{}
	for _, jobID := range only {
		onlySet[jobID] = true
	}
	var result *Revision
	err := m.store.MutateMany(func(jobs map[string]*Job, attempts map[string][]*Attempt, revisions map[string]*Revision) error {
		rev, ok := revisions[id]
		if !ok {
			return ErrRevisionNotFound
		}
		if rev.Status != RevisionProposed && rev.Status != RevisionPartial {
			return ErrRevisionClosed
		}
		appliedSources := map[string]bool{}
		for _, change := range rev.Changes {
			if change.Status != ChangePending {
				if change.Status == ChangeApplied {
					appliedSources[change.JobID] = true
				}
				continue
			}
			if len(onlySet) > 0 && !onlySet[change.JobID] {
				continue
			}
			job, ok := jobs[change.JobID]
			if !ok {
				change.Status = ChangeSkipped
				change.Note = "job no longer exists"
				continue
			}
			if job.Status != StatusSucceeded || job.Withdrawn {
				change.Status = ChangeSkipped
				change.Note = fmt.Sprintf("job is %s and no longer has a confirmed result", job.Status)
				continue
			}
			change.OldResult = job.Result
			change.OldGeneration = job.Generation
			job.Generation++
			publication := ResultPublication{
				Generation:  job.Generation,
				Source:      "revision:" + rev.ID,
				PublishedAt: now,
			}
			if change.Action == ActionReplace {
				job.Result = change.NewResult
				publication.Result = change.NewResult
			} else {
				job.Result = ""
				job.Withdrawn = true
				publication.Withdrawn = true
			}
			job.ResultHistory = append(job.ResultHistory, publication)
			job.UpdatedAt = now
			change.NewGeneration = job.Generation
			change.Status = ChangeApplied
			change.AppliedAt = &now
			appliedSources[change.JobID] = true
		}
		if len(appliedSources) == 0 {
			return errors.New("no pending changes matched the confirmation")
		}
		// Reset downstream jobs that consumed the superseded generations so
		// they re-run against the newly confirmed ones. Jobs on branches
		// blocked by unverified external effects are left untouched until
		// ResolveExternal unblocks the branch.
		blocked := externalPendingSet(jobs)
		for _, affectedID := range downstreamClosure(jobs, appliedSources) {
			job := jobs[affectedID]
			if job.ExternalPending {
				continue
			}
			if blockedByPending(jobs, job, blocked) {
				continue
			}
			switch job.Status {
			case StatusSucceeded:
				if !job.SafeRetry {
					// Never auto-redo a non-idempotent step: its external
					// effect may already have happened. Park it as a
					// pending item for the user to resolve instead.
					job.ExternalPending = true
					job.UpdatedAt = now
					rev.Impact.PendingExternal = append(rev.Impact.PendingExternal, PendingExternalEntry{
						JobID:  job.ID,
						Reason: "non-idempotent job consumed superseded artifacts; confirm whether its external effect still stands",
					})
					continue
				}
				job.Status = StatusPending
				job.Result = ""
				job.Error = "reset by revision " + rev.ID + ": upstream artifacts changed"
				job.CurrentAttempt = 0
				job.NextRunAt = nil
				job.RevisionID = rev.ID
				job.UpdatedAt = now
				if !contains(rev.ResetJobs, job.ID) {
					rev.ResetJobs = append(rev.ResetJobs, job.ID)
				}
			case StatusPending:
				job.RevisionID = rev.ID
				job.UpdatedAt = now
			}
		}
		sort.Strings(rev.ResetJobs)
		open := 0
		for _, change := range rev.Changes {
			if change.Status == ChangePending {
				open++
			}
		}
		if open == 0 {
			rev.Status = RevisionConfirmed
		} else {
			rev.Status = RevisionPartial
		}
		rev.UpdatedAt = now
		revCopy := *rev
		result = &revCopy
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CancelRevision abandons the still-pending changes of a revision. Changes
// that were already confirmed stay applied; nothing is rolled back silently.
func (m *Manager) CancelRevision(id string) (*Revision, error) {
	now := m.now()
	var result *Revision
	err := m.store.MutateMany(func(jobs map[string]*Job, attempts map[string][]*Attempt, revisions map[string]*Revision) error {
		rev, ok := revisions[id]
		if !ok {
			return ErrRevisionNotFound
		}
		if rev.Status != RevisionProposed && rev.Status != RevisionPartial {
			return ErrRevisionClosed
		}
		for _, change := range rev.Changes {
			if change.Status == ChangePending {
				change.Status = ChangeSkipped
				change.Note = "revision canceled before confirmation"
			}
		}
		rev.Status = RevisionCanceled
		rev.UpdatedAt = now
		revCopy := *rev
		result = &revCopy
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ResolveExternal records the user's verdict for a job whose external side
// effect could not be verified. With confirmed=true the effect is treated as
// completed: the job publishes the given result as a new manual generation
// and canceled direct dependents are revived so the branch continues. With
// confirmed=false the job stays FAILED and its branch remains canceled.
func (m *Manager) ResolveExternal(jobID string, confirmed bool, result string) (*Job, error) {
	now := m.now()
	var updated *Job
	err := m.store.MutateMany(func(jobs map[string]*Job, attempts map[string][]*Attempt, revisions map[string]*Revision) error {
		job, ok := jobs[jobID]
		if !ok {
			return ErrNotFound
		}
		if !job.ExternalPending {
			return fmt.Errorf("%w: %s", ErrNoExternalPending, jobID)
		}
		job.ExternalPending = false
		if confirmed {
			job.Status = StatusSucceeded
			job.Result = result
			job.Error = ""
			job.Generation++
			job.ResultHistory = append(job.ResultHistory, ResultPublication{
				Generation:  job.Generation,
				Result:      result,
				Source:      "manual-resolve",
				PublishedAt: now,
			})
			// Revive direct dependents that were canceled solely because
			// this job was blocked, so the branch can continue.
			for _, other := range jobs {
				if other.Status != StatusCanceled || !contains(other.DependsOn, jobID) {
					continue
				}
				other.Status = StatusPending
				other.Error = ""
				other.NextRunAt = nil
				other.UpdatedAt = now
			}
		} else {
			job.Error += " (external effect verified by user: not completed)"
		}
		job.UpdatedAt = now
		jobCopy := *job
		updated = &jobCopy
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// computeImpact builds the proposal-time report: the transitive downstream
// closure of the changed jobs, the confirmed results outside that closure
// that stay reusable, and the unverified external effects inside it.
func computeImpact(all []*Job, changed map[string]bool) *ImpactReport {
	byID := map[string]*Job{}
	for _, job := range all {
		byID[job.ID] = job
	}
	report := &ImpactReport{}
	for _, id := range downstreamClosure(byID, changed) {
		job := byID[id]
		entry := ImpactEntry{JobID: id}
		switch {
		case job.ExternalPending:
			report.PendingExternal = append(report.PendingExternal, PendingExternalEntry{
				JobID:  id,
				Reason: "external effect unverified; resolve before this branch continues",
			})
			continue
		case job.Status == StatusSucceeded:
			entry.Reason = "confirmed result consumes revised artifacts; will be re-executed"
		case job.Status == StatusPending || job.Status == StatusRunning:
			entry.Reason = "not yet finished; will consume the revised artifacts on dispatch"
		default:
			entry.Reason = fmt.Sprintf("terminal (%s); recorded for traceability", job.Status)
		}
		report.Affected = append(report.Affected, entry)
	}
	for _, job := range all {
		if job.Status == StatusSucceeded && !changed[job.ID] && !inClosure(byID, changed, job.ID) {
			report.Reusable = append(report.Reusable, job.ID)
		}
	}
	sort.Strings(report.Reusable)
	return report
}

// downstreamClosure returns the transitive dependents of the given jobs.
func downstreamClosure(jobs map[string]*Job, sources map[string]bool) []string {
	dependents := map[string][]string{}
	for _, job := range jobs {
		for _, depID := range job.DependsOn {
			dependents[depID] = append(dependents[depID], job.ID)
		}
	}
	visited := map[string]bool{}
	queue := make([]string, 0, len(sources))
	for id := range sources {
		queue = append(queue, id)
	}
	var closure []string
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, next := range dependents[id] {
			if visited[next] {
				continue
			}
			visited[next] = true
			closure = append(closure, next)
			queue = append(queue, next)
		}
	}
	sort.Strings(closure)
	return closure
}

func inClosure(jobs map[string]*Job, sources map[string]bool, id string) bool {
	for _, member := range downstreamClosure(jobs, sources) {
		if member == id {
			return true
		}
	}
	return false
}

func externalPendingSet(jobs map[string]*Job) map[string]bool {
	set := map[string]bool{}
	for _, job := range jobs {
		if job.ExternalPending {
			set[job.ID] = true
		}
	}
	return set
}

// blockedByPending reports whether any transitive dependency of the job has
// an unverified external effect, meaning its branch must wait for a user
// verdict before being re-planned.
func blockedByPending(jobs map[string]*Job, job *Job, pending map[string]bool) bool {
	visited := map[string]bool{}
	queue := append([]string{}, job.DependsOn...)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if visited[id] {
			continue
		}
		visited[id] = true
		if pending[id] {
			return true
		}
		if dep, ok := jobs[id]; ok {
			queue = append(queue, dep.DependsOn...)
		}
	}
	return false
}

func contains(list []string, id string) bool {
	for _, item := range list {
		if item == id {
			return true
		}
	}
	return false
}
