package jobs

import "fmt"

// ReviewReport summarizes what one review submission actually did. When the
// same change was already decided, the submission is recorded for audit with
// Effective=false and no artifact generation is promoted again.
type ReviewReport struct {
	RevisionID  string         `json:"revision_id"`
	ChangeIndex int            `json:"change_index"`
	Reviewer    string         `json:"reviewer"`
	Decision    ReviewDecision `json:"decision"`
	// Effective is true when this submission is the one that decided the
	// change. Duplicate or cross submissions keep Effective=false.
	Effective bool `json:"effective"`
	// Confirmation is set for an effective accept and mirrors ConfirmRevision.
	Confirmation *ConfirmationReport `json:"confirmation,omitempty"`
	Revision     *RevisionCandidate  `json:"revision"`
}

// ReviewChange lets the assigned reviewer accept or reject one proposed
// change. Accepting promotes a new artifact generation and invalidates
// downstream exactly like ConfirmRevision; rejecting marks the change
// REJECTED and dismisses its open pending items, so an unapproved result
// never reaches downstream inputs. Re-submitting a review for an already
// decided change is idempotent: the action is appended to the audit trail as
// ineffective and no state changes beyond that record.
func (m *Manager) ReviewChange(revisionID string, index int, reviewer string, decision ReviewDecision, note string) (*ReviewReport, error) {
	if reviewer == "" {
		return nil, fmt.Errorf("reviewer is required")
	}
	if decision != ReviewAccept && decision != ReviewReject {
		return nil, fmt.Errorf("decision must be %s or %s", ReviewAccept, ReviewReject)
	}
	report := &ReviewReport{RevisionID: revisionID, ChangeIndex: index, Reviewer: reviewer, Decision: decision}
	now := m.now()
	err := m.store.MutateAll(func(view *SnapshotView) error {
		candidate, ok := view.Revisions()[revisionID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, revisionID)
		}
		if candidate.Status == RevisionCanceled {
			return fmt.Errorf("%w: revision %s was canceled", ErrRevisionClosed, revisionID)
		}
		if index < 0 || index >= len(candidate.Changes) {
			return fmt.Errorf("change index %d out of range", index)
		}
		change := candidate.Changes[index]
		if change.Assignee == "" || change.Assignee != reviewer {
			return fmt.Errorf("%w: change %d is assigned to %q, not %q", ErrReviewerNotAssigned, index, change.Assignee, reviewer)
		}
		record := ReviewRecord{Reviewer: reviewer, Decision: decision, Note: note, RecordedAt: now}
		if change.Status != ChangeProposed {
			// Duplicate or cross submission: keep the audit record but never
			// promote another generation or reopen a decided change.
			change.Reviews = append(change.Reviews, record)
			candidate.UpdatedAt = now
			report.Revision = cloneRevision(candidate)
			return nil
		}
		record.Effective = true
		change.Reviews = append(change.Reviews, record)
		report.Effective = true
		all := view.Jobs()
		for _, job := range all {
			ensureArtifacts(job, now)
		}
		if decision == ReviewAccept {
			confirmation := &ConfirmationReport{RevisionID: revisionID, NewGenerations: map[string]int{}}
			dependents := buildDependents(all)
			invalidatedSet := map[string]bool{}
			reusableSet := map[string]bool{}
			if err := confirmChange(view, candidate, change, index, all, dependents, confirmation, invalidatedSet, reusableSet, now); err != nil {
				return err
			}
			for id := range invalidatedSet {
				confirmation.InvalidatedJobs = append(confirmation.InvalidatedJobs, id)
			}
			for id := range reusableSet {
				if !invalidatedSet[id] {
					confirmation.ReusableJobs = append(confirmation.ReusableJobs, id)
				}
			}
			sortIDs(confirmation.InvalidatedJobs)
			sortIDs(confirmation.ReusableJobs)
			sortIDs(confirmation.BlockedByPending)
			confirmation.BlockedByPending = uniqueStrings(confirmation.BlockedByPending)
			report.Confirmation = confirmation
		} else {
			change.Status = ChangeRejected
			for _, pending := range candidate.Pending {
				if pending.ChangeKey == index && pending.Status == PendingOpen {
					pending.Status = PendingDismissed
					pending.Resolution = "change rejected by reviewer " + reviewer
					pending.ResolvedAt = &now
				}
			}
		}
		closeRevisionIfComplete(candidate, now)
		report.Revision = cloneRevision(candidate)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}

