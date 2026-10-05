package jobs

import (
	"fmt"
	"sort"
	"time"
)

type changeDecision struct {
	reviewer string
	kind     DecisionKind
	reason   string
}

// ReviewRevision records reviewer decisions for the selected changes. Only
// the reviewer assigned to each change may decide it; denied submissions are
// recorded but never take effect. Resubmitting the same already-effective
// decision is idempotent: it is logged as a duplicate and does not bump any
// artifact generation.
func (m *Manager) ReviewRevision(revisionID, reviewer string, decisions []ReviewDecision) (*ConfirmationReport, error) {
	if reviewer == "" {
		return nil, fmt.Errorf("%w: review requires a named reviewer", ErrReviewerRequired)
	}
	if len(decisions) == 0 {
		return nil, fmt.Errorf("review must contain at least one decision")
	}
	selected := map[int]changeDecision{}
	for _, decision := range decisions {
		switch decision.Kind {
		case DecisionAccept, DecisionReturn:
		default:
			return nil, fmt.Errorf("unknown review decision %q", decision.Kind)
		}
		if _, dup := selected[decision.Index]; dup {
			return nil, fmt.Errorf("duplicate change index %d", decision.Index)
		}
		selected[decision.Index] = changeDecision{reviewer: reviewer, kind: decision.Kind, reason: decision.Reason}
	}
	return m.applyRevisionDecisions(revisionID, selected, DecisionAccept)
}

// reviewRevisionUnchecked is the shared engine used by the legacy
// ConfirmRevision/CancelRevision wrappers (empty reviewer) as well as
// ReviewRevision. It runs every selected decision in one atomic transaction.
func (m *Manager) applyRevisionDecisions(revisionID string, decisions map[int]changeDecision, defaultKind DecisionKind) (*ConfirmationReport, error) {
	report := &ConfirmationReport{RevisionID: revisionID, NewGenerations: map[string]int{}}
	err := m.store.MutateAll(func(view *SnapshotView) error {
		candidate, ok := view.Revisions()[revisionID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, revisionID)
		}
		if candidate.Status == RevisionCanceled {
			return fmt.Errorf("%w: revision %s was canceled", ErrRevisionClosed, revisionID)
		}
		all := view.Jobs()
		now := m.now()
		for _, job := range all {
			ensureArtifacts(job, now)
		}
		dependents := buildDependents(all)
		invalidatedSet := map[string]bool{}
		reusableSet := map[string]bool{}
		indexes := make([]int, 0, len(candidate.Changes))
		if decisions == nil {
			for idx := range candidate.Changes {
				indexes = append(indexes, idx)
			}
		} else {
			for idx := range decisions {
				if idx < 0 || idx >= len(candidate.Changes) {
					return fmt.Errorf("change index %d out of range", idx)
				}
				indexes = append(indexes, idx)
			}
		}
		sort.Ints(indexes)
		for _, i := range indexes {
			change := candidate.Changes[i]
			decision := changeDecision{kind: defaultKind}
			if decisions != nil {
				decision = decisions[i]
			}
			record := ReviewRecord{Reviewer: decision.reviewer, Kind: decision.kind, At: now, Reason: decision.reason}

			// Permission gate. Assigned changes are decided only by their named
			// reviewer; the unassigned legacy wrappers cannot touch them.
			if change.AssignedReviewer != "" && decision.reviewer != change.AssignedReviewer {
				if decision.reviewer == "" {
					return fmt.Errorf("%w: change %d is assigned to %q", ErrReviewerRequired, i, change.AssignedReviewer)
				}
				change.Reviews = append(change.Reviews, record)
				candidate.UpdatedAt = now
				report.Denied = append(report.Denied, i)
				continue
			}
			if decision.kind == DecisionCancel && change.AssignedReviewer != "" {
				return fmt.Errorf("%w: change %d is assigned; reviewers return changes instead of canceling", ErrReviewerRequired, i)
			}
			record.Authorized = true

			// Idempotency / conflict gate. Repeating the decision already in
			// effect is recorded as a duplicate and changes nothing.
			if change.Status != ChangeProposed {
				if effectiveDecision(change.Status) == decision.kind {
					change.Reviews = append(change.Reviews, record)
					candidate.UpdatedAt = now
					report.Duplicate = append(report.Duplicate, i)
					continue
				}
				return fmt.Errorf("%w: change %d is %s", ErrReviewConflict, i, change.Status)
			}

			record.Applied = true
			change.DecidedBy = decision.reviewer
			change.DecidedAt = &now
			if decision.reason != "" {
				change.Reason = decision.reason
			}

			switch decision.kind {
			case DecisionAccept:
				job := all[change.JobID]
				if job.Status != StatusSucceeded || job.ArtifactState != ArtifactActive {
					return fmt.Errorf("%w: %s is %s/%s", ErrNotSucceeded, change.JobID, job.Status, job.ArtifactState)
				}
				if job.Generation != change.FromGeneration {
					return fmt.Errorf("%w: change %d targets gen %d but %s is now gen %d", ErrStaleGeneration, i, change.FromGeneration, change.JobID, job.Generation)
				}
				change.Status = ChangeAccepted
				change.ToGeneration = job.Generation
				unchanged := change.Kind == ChangeReplace && change.NewResult == job.Result
				if unchanged {
					change.Status = ChangeNoChange
					change.Unchanged = true
					report.Unchanged = append(report.Unchanged, i)
					break
				}
				report.Accepted = append(report.Accepted, i)
				if change.Kind == ChangeReplace {
					newGen := applyReplace(job, change, candidate.ID, now)
					change.ToGeneration = newGen
					report.NewGenerations[job.ID] = newGen
				} else {
					applyRevoke(job, candidate.ID, now)
				}
				hasPending := false
				for _, pending := range candidate.Pending {
					if pending.ChangeKey == i && pending.Status == PendingOpen {
						hasPending = true
						report.PendingOpen = append(report.PendingOpen, pending.ID)
						job.ExternalPendingIDs = append(job.ExternalPendingIDs, pending.ID)
					}
				}
				job.ExternalPendingIDs = uniqueStrings(job.ExternalPendingIDs)
				if len(job.ExternalPendingIDs) > 0 && change.Kind == ChangeReplace {
					job.ExternalPending = true
				} else if !hasPending {
					job.ExternalPending = false
				}
				propagateInvalidation(change, candidate, job, dependents, all, view, invalidatedSet, reusableSet, now)
				if hasPending {
					collectPendingBranches(job.ID, dependents, all, report)
				}
			case DecisionReturn:
				change.Status = ChangeRejected
				dismissChangePending(candidate, i, "returned by reviewer "+decision.reviewer, now)
				report.Returned = append(report.Returned, i)
			case DecisionCancel:
				change.Status = ChangeCanceled
				dismissChangePending(candidate, i, "change canceled before confirmation", now)
				report.Canceled = append(report.Canceled, i)
			}
			change.Reviews = append(change.Reviews, record)
		}
		candidate.UpdatedAt = now
		closeRevisionIfComplete(candidate, now)
		report.Revision = cloneRevision(candidate)
		for id := range invalidatedSet {
			report.InvalidatedJobs = append(report.InvalidatedJobs, id)
		}
		for id := range reusableSet {
			if !invalidatedSet[id] {
				report.ReusableJobs = append(report.ReusableJobs, id)
			}
		}
		sortIDs(report.InvalidatedJobs)
		sortIDs(report.ReusableJobs)
		sortIDs(report.BlockedByPending)
		report.BlockedByPending = uniqueStrings(report.BlockedByPending)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}

func effectiveDecision(status ChangeStatus) DecisionKind {
	switch status {
	case ChangeAccepted, ChangeNoChange:
		return DecisionAccept
	case ChangeRejected:
		return DecisionReturn
	default:
		return DecisionCancel
	}
}

func dismissChangePending(candidate *RevisionCandidate, idx int, resolution string, now time.Time) {
	for _, pending := range candidate.Pending {
		if pending.ChangeKey == idx && pending.Status == PendingOpen {
			pending.Status = PendingDismissed
			pending.Resolution = resolution
			pending.ResolvedAt = &now
		}
	}
}
