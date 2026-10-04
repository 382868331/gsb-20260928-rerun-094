package jobs

import (
	"fmt"
	"sort"
	"time"
)

func (m *Manager) CancelRevision(revisionID string, cancelIndexes []int) (*RevisionCandidate, error) {
	now := m.now()
	err := m.store.MutateAll(func(view *SnapshotView) error {
		candidate, ok := view.Revisions()[revisionID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, revisionID)
		}
		if candidate.Status == RevisionCanceled {
			return fmt.Errorf("%w: revision %s was canceled", ErrRevisionClosed, revisionID)
		}
		selected, err := selectedIndexes(cancelIndexes, len(candidate.Changes))
		if err != nil {
			return err
		}
		for idx := range selected {
			change := candidate.Changes[idx]
			if change.Status != ChangeProposed {
				return fmt.Errorf("%w: change %d is %s", ErrChangeNotOpen, idx, change.Status)
			}
			change.Status = ChangeCanceled
			for _, pending := range candidate.Pending {
				if pending.ChangeKey == idx && pending.Status == PendingOpen {
					pending.Status = PendingDismissed
					pending.Resolution = "change canceled before confirmation"
					pending.ResolvedAt = &now
				}
			}
		}
		closeRevisionIfComplete(candidate, now)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m.GetRevision(revisionID)
}

func (m *Manager) ResolvePending(pendingID string, status PendingStatus, resolution string) (*PendingItem, error) {
	if status != PendingConfirmed && status != PendingDismissed {
		return nil, fmt.Errorf("pending resolution must be %s or %s", PendingConfirmed, PendingDismissed)
	}
	now := m.now()
	var resolved *PendingItem
	err := m.store.MutateAll(func(view *SnapshotView) error {
		for _, candidate := range view.Revisions() {
			for _, item := range candidate.Pending {
				if item.ID != pendingID {
					continue
				}
				if item.Status != PendingOpen {
					return fmt.Errorf("%w: pending item %s is %s", ErrChangeNotOpen, pendingID, item.Status)
				}
				item.Status = status
				item.Resolution = resolution
				item.ResolvedAt = &now
				resolvedItem := *item
				resolved = &resolvedItem

				job := view.Jobs()[item.JobID]
				if job != nil {
					job.ExternalPendingIDs = removeString(job.ExternalPendingIDs, item.ID)
					if len(job.ExternalPendingIDs) == 0 {
						job.ExternalPending = false
					}
					job.UpdatedAt = now
				}
				return nil
			}
		}
		return fmt.Errorf("%w: pending item %s", ErrNotFound, pendingID)
	})
	if err != nil {
		return nil, err
	}
	return clonePending(resolved), nil
}

func (m *Manager) CompareRevision(revisionID string) (*RevisionComparison, error) {
	comparison := &RevisionComparison{RevisionID: revisionID}
	err := m.store.MutateAll(func(view *SnapshotView) error {
		candidate, ok := view.Revisions()[revisionID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, revisionID)
		}
		comparison.Status = candidate.Status
		all := view.Jobs()
		for i, change := range candidate.Changes {
			diff := ArtifactDiff{
				ChangeIndex:    i,
				JobID:          change.JobID,
				Kind:           change.Kind,
				Status:         change.Status,
				FromGeneration: change.FromGeneration,
				ToGeneration:   change.ToGeneration,
				ProposedResult: change.NewResult,
			}
			if job := all[change.JobID]; job != nil {
				if before := artifactAtGeneration(job, change.FromGeneration); before != nil {
					diff.BeforeResult = before.Result
				}
				if change.ToGeneration != 0 {
					if after := artifactAtGeneration(job, change.ToGeneration); after != nil {
						diff.AfterResult = after.Result
					}
				}
				diff.CurrentGeneration = job.Generation
				diff.CurrentState = job.ArtifactState
				if active := activeArtifact(job); active != nil {
					diff.CurrentResult = active.Result
				}
			}
			diff.ActualChange = change.Status == ChangeAccepted
			if diff.ActualChange && change.Kind == ChangeReplace {
				diff.ActualChange = diff.BeforeResult != diff.AfterResult
			}
			comparison.Changes = append(comparison.Changes, diff)
		}
		return nil
	})
	return comparison, err
}

func selectedIndexes(indexes []int, length int) (map[int]bool, error) {
	if indexes == nil {
		selected := make(map[int]bool, length)
		for i := 0; i < length; i++ {
			selected[i] = true
		}
		return selected, nil
	}
	selected := map[int]bool{}
	for _, idx := range indexes {
		if idx < 0 || idx >= length {
			return nil, fmt.Errorf("change index %d out of range", idx)
		}
		if selected[idx] {
			return nil, fmt.Errorf("duplicate change index %d", idx)
		}
		selected[idx] = true
	}
	return selected, nil
}

func closeRevisionIfComplete(candidate *RevisionCandidate, now time.Time) {
	hasProposed := false
	hasAccepted := false
	for _, change := range candidate.Changes {
		switch change.Status {
		case ChangeProposed:
			hasProposed = true
		case ChangeAccepted, ChangeNoChange:
			hasAccepted = true
		}
	}
	if !hasProposed {
		if hasAccepted {
			candidate.Status = RevisionAccepted
		} else {
			candidate.Status = RevisionCanceled
		}
	}
	candidate.UpdatedAt = now
}

func sortedIntSet(values map[int]bool) []int {
	out := make([]int, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Ints(out)
	return out
}
