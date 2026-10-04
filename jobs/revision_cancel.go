package jobs

import (
	"fmt"
	"time"
)

// CancelRevision withdraws not-yet-confirmed changes (all of them when
// cancelIndexes is nil). Confirmed changes, completed jobs and every artifact
// generation stay exactly as they are; only the open proposals are closed.
func (m *Manager) CancelRevision(revisionID string, cancelIndexes []int) (*RevisionCandidate, error) {
	want := map[int]bool{}
	for _, idx := range cancelIndexes {
		want[idx] = true
	}
	var out *RevisionCandidate
	err := m.store.MutateAll(func(view *SnapshotView) error {
		candidate, ok := view.Revisions()[revisionID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, revisionID)
		}
		if candidate.Status == RevisionCanceled {
			return fmt.Errorf("%w: revision %s was canceled", ErrRevisionClosed, revisionID)
		}
		now := m.now()
		canceledAny := false
		for i, change := range candidate.Changes {
			if cancelIndexes != nil && !want[i] {
				continue
			}
			if change.Status != ChangeProposed {
				return fmt.Errorf("%w: change %d is %s", ErrChangeNotOpen, i, change.Status)
			}
			change.Status = ChangeCanceled
			canceledAny = true
		}
		if !canceledAny {
			return fmt.Errorf("%w: no open change selected", ErrChangeNotOpen)
		}
		for _, item := range candidate.Pending {
			if item.Status == PendingOpen && (cancelIndexes == nil || want[item.ChangeKey]) {
				item.Status = PendingDismissed
				item.Resolution = "revision canceled"
				item.ResolvedAt = &now
			}
		}
		closeRevisionIfSettled(candidate)
		candidate.UpdatedAt = now
		m.refreshExternalPending(view, candidate, now)
		out = cloneRevision(candidate)
		return nil
	})
	return out, err
}

// ResolvePending records the human verdict on one unverified external effect.
// Once every pending item for a job is resolved, the job's ExternalPending
// block is lifted and waiting downstream branches may dispatch again.
func (m *Manager) ResolvePending(pendingID string, status PendingStatus, resolution string) (*RevisionCandidate, error) {
	if status != PendingConfirmed && status != PendingDismissed {
		return nil, fmt.Errorf("pending resolution must be %s or %s", PendingConfirmed, PendingDismissed)
	}
	var out *RevisionCandidate
	err := m.store.MutateAll(func(view *SnapshotView) error {
		var candidate *RevisionCandidate
		var item *PendingItem
		for _, rev := range view.Revisions() {
			for _, p := range rev.Pending {
				if p.ID == pendingID {
					candidate = rev
					item = p
				}
			}
		}
		if item == nil {
			return fmt.Errorf("%w: pending %s", ErrNotFound, pendingID)
		}
		if item.Status != PendingOpen {
			return fmt.Errorf("%w: pending %s is %s", ErrChangeNotOpen, pendingID, item.Status)
		}
		now := m.now()
		item.Status = status
		item.Resolution = resolution
		item.ResolvedAt = &now
		candidate.UpdatedAt = now
		m.refreshExternalPending(view, candidate, now)
		out = cloneRevision(candidate)
		return nil
	})
	return out, err
}

// refreshExternalPending clears a job's external-effect block when no OPEN
// pending item in any revision still references it.
func (m *Manager) refreshExternalPending(view *SnapshotView, touched *RevisionCandidate, now time.Time) {
	all := view.Jobs()
	jobIDs := map[string]bool{}
	for _, item := range touched.Pending {
		jobIDs[item.JobID] = true
	}
	for jobID := range jobIDs {
		job, ok := all[jobID]
		if !ok || !job.ExternalPending {
			continue
		}
		stillOpen := false
		for _, rev := range view.Revisions() {
			for _, item := range rev.Pending {
				if item.JobID == jobID && item.Status == PendingOpen {
					stillOpen = true
				}
			}
		}
		if !stillOpen {
			job.ExternalPending = false
			job.UpdatedAt = now
		}
	}
}

func closeRevisionIfSettled(candidate *RevisionCandidate) {
	open := false
	settled := false
	for _, change := range candidate.Changes {
		if change.Status == ChangeProposed {
			open = true
		} else if change.Status == ChangeAccepted || change.Status == ChangeNoChange {
			settled = true
		}
	}
	if open {
		return
	}
	if settled {
		candidate.Status = RevisionAccepted
	} else {
		candidate.Status = RevisionCanceled
	}
}

// RevisionDiff compares the artifact state before and after a revision using
// the preserved generation history. It works after a restart because nothing
// is recomputed from mutable job fields alone.
func (m *Manager) RevisionDiff(revisionID string) (*RevisionDiff, error) {
	diff := &RevisionDiff{RevisionID: revisionID}
	err := m.store.MutateAll(func(view *SnapshotView) error {
		candidate, ok := view.Revisions()[revisionID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, revisionID)
		}
		diff.Status = candidate.Status
		all := view.Jobs()
		for _, job := range all {
			ensureArtifacts(job, m.now())
		}
		for i, change := range candidate.Changes {
			entry := DiffEntry{
				ChangeIndex:    i,
				JobID:          change.JobID,
				Kind:           change.Kind,
				Status:         change.Status,
				FromGeneration: change.FromGeneration,
				ToGeneration:   change.ToGeneration,
			}
			if job, ok := all[change.JobID]; ok {
				for _, artifact := range job.Artifacts {
					if artifact.Generation == change.FromGeneration {
						entry.FromResult = artifact.Result
					}
					if change.ToGeneration != 0 && artifact.Generation == change.ToGeneration {
						entry.ToResult = artifact.Result
					}
				}
			}
			if change.Status == ChangeProposed && change.Kind == ChangeReplace {
				entry.ToResult = change.NewResult
			}
			diff.Entries = append(diff.Entries, entry)
		}
		for _, item := range candidate.Pending {
			p := *item
			diff.Pending = append(diff.Pending, &p)
		}
		return nil
	})
	return diff, err
}
