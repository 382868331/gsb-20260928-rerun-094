package jobs

import (
	"fmt"
	"sort"
	"time"
)

// ConfirmRevision applies the selected changes on the unassigned legacy path
// (all open changes when acceptIndexes is nil). Assigned changes must go
// through ReviewRevision and are refused here. Replacement of each artifact
// generation and downstream invalidation happens in one atomic transaction,
// so redone jobs can only consume confirmed generations.
func (m *Manager) ConfirmRevision(revisionID string, acceptIndexes []int) (*ConfirmationReport, error) {
	var decisions map[int]changeDecision
	if acceptIndexes != nil {
		if len(acceptIndexes) == 0 {
			return nil, fmt.Errorf("at least one change index is required")
		}
		seen := map[int]bool{}
		decisions = make(map[int]changeDecision, len(acceptIndexes))
		for _, idx := range acceptIndexes {
			if seen[idx] {
				return nil, fmt.Errorf("duplicate change index %d", idx)
			}
			seen[idx] = true
			decisions[idx] = changeDecision{kind: DecisionAccept}
		}
	}
	return m.applyRevisionDecisions(revisionID, decisions, DecisionAccept)
}

func collectPendingBranches(rootID string, dependents map[string][]string, all map[string]*Job, report *ConfirmationReport) {
	queue := append([]string(nil), dependents[rootID]...)
	visited := map[string]bool{rootID: true}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if visited[id] {
			continue
		}
		visited[id] = true
		if job := all[id]; job != nil {
			report.BlockedByPending = append(report.BlockedByPending, id)
		}
		queue = append(queue, dependents[id]...)
	}
}

func applyReplace(job *Job, change *ArtifactChange, revisionID string, now time.Time) int {
	for i := range job.Artifacts {
		if job.Artifacts[i].State == ArtifactActive {
			job.Artifacts[i].State = ArtifactSuperseded
		}
	}
	newGen := job.Generation + 1
	job.Artifacts = append(job.Artifacts, Artifact{
		Generation: newGen,
		Result:     change.NewResult,
		State:      ArtifactActive,
		Source:     revisionID,
		CreatedAt:  now,
	})
	job.Generation = newGen
	job.ArtifactState = ArtifactActive
	job.Result = change.NewResult
	job.Error = ""
	job.RevisionNote = "replaced by confirmed " + revisionID
	return newGen
}

func applyRevoke(job *Job, revisionID string, now time.Time) {
	for i := range job.Artifacts {
		if job.Artifacts[i].State == ArtifactActive {
			job.Artifacts[i].State = ArtifactRevoked
		}
	}
	job.ArtifactState = ArtifactRevoked
	job.Result = ""
	job.Error = "artifact revoked by " + revisionID
	job.RevisionNote = "revoked by confirmed " + revisionID
	job.ExternalPending = false
	_ = now
}

// propagateInvalidation carries a confirmed root change through the downstream
// closure. Replacements reset stale consumers back to PENDING for a redo that
// pins the confirmed generations; revocations cancel them permanently. Jobs
// whose surviving artifact was built from other generations are reported as
// reusable and are never executed again.
func propagateInvalidation(change *ArtifactChange, candidate *RevisionCandidate, root *Job, dependents map[string][]string, all map[string]*Job, view *SnapshotView, invalidated, reusable map[string]bool, now time.Time) {
	taint := map[string]int{root.ID: change.FromGeneration}
	queue := []string{root.ID}
	visited := map[string]bool{root.ID: true}
	for len(queue) > 0 {
		up := queue[0]
		queue = queue[1:]
		for _, downID := range dependents[up] {
			down := all[downID]
			active := activeArtifact(down)
			consumedGen := 0
			if active != nil && active.BasedOnGenerations != nil {
				consumedGen = active.BasedOnGenerations[up]
			}
			_, tainted := taint[up]
			materiallyStale := tainted && ((active != nil && consumedGen == taint[up] && (down.Status == StatusSucceeded || down.Status == StatusRunning)) || down.Status == StatusPending)
			if materiallyStale {
				invalidateJob(down, view, change, candidate, now)
				invalidated[downID] = true
				if down.ArtifactState == ArtifactRevoked || active == nil {
					taint[downID] = down.Generation
				} else {
					// Redo consumers the confirmed generations; the old generation
					// is the one that further successors must have built upon to be
					// considered stale.
					taint[downID] = active.Generation
				}
			} else if down.Status == StatusSucceeded {
				reusable[downID] = true
			}
			if !visited[downID] {
				visited[downID] = true
				queue = append(queue, downID)
			}
		}
	}
}

func invalidateJob(job *Job, view *SnapshotView, change *ArtifactChange, candidate *RevisionCandidate, now time.Time) {
	attempts := view.Attempts(job.ID)
	if change.Kind == ChangeRevoke {
		for i := range job.Artifacts {
			if job.Artifacts[i].State == ArtifactActive {
				job.Artifacts[i].State = ArtifactRevoked
			}
		}
		job.ArtifactState = ArtifactRevoked
		job.Status = StatusCanceled
		job.Result = ""
		job.Error = "canceled after upstream revocation in " + candidate.ID
		job.CurrentAttempt = 0
		job.NextRunAt = nil
		job.ExternalPending = false
	} else {
		for i := range job.Artifacts {
			if job.Artifacts[i].State == ArtifactActive {
				job.Artifacts[i].State = ArtifactSuperseded
			}
		}
		job.ArtifactState = ArtifactSuperseded
		job.Status = StatusPending
		job.Result = ""
		job.Error = "invalidated by " + candidate.ID + "; redo must reference confirmed artifact generations"
		job.NextRunAt = nil
		job.PendingRevisionID = candidate.ID
		job.PendingRevisionIDs = uniqueStrings(append(job.PendingRevisionIDs, candidate.ID))
	}
	for i := len(attempts) - 1; i >= 0; i-- {
		if attempts[i].Number == job.CurrentAttempt && attempts[i].Status == AttemptRunning {
			attempts[i].Status = AttemptCanceled
			attempts[i].FinishedAt = &now
			attempts[i].Error = "superseded by revision " + candidate.ID
			job.CurrentAttempt = 0
		}
	}
	job.UpdatedAt = now
	view.SetAttempts(job.ID, attempts)
}

func sortIDs(ids []string) {
	sort.Strings(ids)
}
