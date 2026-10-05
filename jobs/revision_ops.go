package jobs

import (
	"fmt"
	"sort"
)

// ChangeSpec is a user-requested replacement or revocation.
type ChangeSpec struct {
	JobID     string
	Kind      ChangeKind
	NewResult string
	// Reviewer assigns the change to a named reviewer. Assigned changes can
	// only be decided through ReviewChange by that reviewer.
	Reviewer string
	// ExternalEffects lists unverified external effects that the change may
	// trigger. Each entry becomes an OPEN pending item on confirmation.
	ExternalEffects []string
}

// ProposeRevision records a revision candidate without touching any completed
// job. Historical artifacts stay in place; only the candidate is stored.
func (m *Manager) ProposeRevision(changes []ChangeSpec, note string) (*RevisionCandidate, error) {
	if len(changes) == 0 {
		return nil, fmt.Errorf("revision must contain at least one change")
	}
	now := m.now()
	var created *RevisionCandidate
	err := m.store.MutateAll(func(view *SnapshotView) error {
		all := cloneJobMap(view.Jobs())
		candidate := &RevisionCandidate{
			ID:        view.NextRevisionID(now),
			Status:    RevisionProposed,
			Note:      note,
			CreatedAt: now,
			UpdatedAt: now,
		}
		for i, spec := range changes {
			job, ok := all[spec.JobID]
			if !ok {
				return fmt.Errorf("%w: %s", ErrNotFound, spec.JobID)
			}
			ensureArtifacts(job, now)
			if job.Status != StatusSucceeded || job.ArtifactState != ArtifactActive {
				return fmt.Errorf("%w: %s is %s/%s", ErrNotSucceeded, spec.JobID, job.Status, job.ArtifactState)
			}
			kind := spec.Kind
			if kind != ChangeReplace && kind != ChangeRevoke {
				return fmt.Errorf("unknown change kind %q", kind)
			}
			if kind == ChangeReplace && spec.NewResult == "" {
				return fmt.Errorf("replace change for %s needs a new result", spec.JobID)
			}
			change := &ArtifactChange{
				JobID:          spec.JobID,
				Kind:           kind,
				NewResult:      spec.NewResult,
				Status:         ChangeProposed,
				FromGeneration: job.Generation,
				Assignee:       spec.Reviewer,
			}
			candidate.Changes = append(candidate.Changes, change)
			for _, summary := range spec.ExternalEffects {
				candidate.Pending = append(candidate.Pending, &PendingItem{
					ID:         fmt.Sprintf("%s-c%d-p%d", candidate.ID, i, len(candidate.Pending)+1),
					RevisionID: candidate.ID,
					ChangeKey:  i,
					JobID:      spec.JobID,
					Summary:    summary,
					Status:     PendingOpen,
					CreatedAt:  now,
				})
			}
		}
		view.PutRevision(candidate)
		created = candidate
		return nil
	})
	return cloneRevision(created), err
}

// GetRevision returns a stored revision candidate.
func (m *Manager) GetRevision(id string) (*RevisionCandidate, error) {
	var rev *RevisionCandidate
	err := m.store.MutateAll(func(view *SnapshotView) error {
		candidate, ok := view.Revisions()[id]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		rev = cloneRevision(candidate)
		return nil
	})
	return rev, err
}

// ListRevisions returns all revision candidates oldest first.
func (m *Manager) ListRevisions() ([]*RevisionCandidate, error) {
	var out []*RevisionCandidate
	err := m.store.MutateAll(func(view *SnapshotView) error {
		for _, candidate := range view.Revisions() {
			out = append(out, cloneRevision(candidate))
		}
		sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
		return nil
	})
	return out, err
}

// PreviewRevision computes which downstream jobs would be redone or canceled
// and which results remain reusable. It never mutates state.
func (m *Manager) PreviewRevision(id string) (*RevisionPreview, error) {
	preview := &RevisionPreview{RevisionID: id}
	err := m.store.MutateAll(func(view *SnapshotView) error {
		candidate, ok := view.Revisions()[id]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		preview.Status = candidate.Status
		all := view.Jobs()
		for _, job := range all {
			ensureArtifacts(job, m.now())
		}
		for i, change := range candidate.Changes {
			impact := ChangeImpact{
				ChangeIndex: i,
				JobID:       change.JobID,
				Kind:        change.Kind,
			}
			if root, ok := all[change.JobID]; ok {
				if active := activeArtifact(root); active != nil {
					impact.FromResult = active.Result
				}
			}
			impact.ToResult = change.NewResult
			if change.Kind == ChangeReplace && change.NewResult == impact.FromResult {
				impact.Unchanged = true
			}
			stale := !impact.Unchanged
			if change.Status != ChangeProposed {
				stale = false
			}
			rerun, reusable := computeImpact(change.JobID, change.FromGeneration, change.Kind, stale, all)
			impact.WouldRerun = rerun
			impact.Reusable = reusable
			for _, pending := range candidate.Pending {
				if pending.ChangeKey == i && pending.Status == PendingOpen {
					impact.ExternalPending = append(impact.ExternalPending, pending.Summary)
				}
			}
			preview.Impacts = append(preview.Impacts, impact)
		}
		return nil
	})
	return preview, err
}

// computeImpact walks the downstream closure. taintGens maps a job ID to the
// generation whose lineage is outdated; a successor whose active artifact was
// built on that exact generation is invalidated, while successors that used a
// different (older/newer) generation stay reusable. Pending successors have no
// artifact yet and simply pick up the confirmed generation when dispatched.
func computeImpact(rootID string, rootGen int, kind ChangeKind, stale bool, all map[string]*Job) ([]ImpactedJob, []ImpactedJob) {
	dependents := buildDependents(all)
	taint := map[string]int{}
	if stale {
		taint[rootID] = rootGen
	}
	var invalidated []ImpactedJob
	reusableSet := map[string]ImpactedJob{}
	queue := []string{rootID}
	visited := map[string]bool{rootID: true}
	for len(queue) > 0 {
		up := queue[0]
		queue = queue[1:]
		for _, downID := range dependents[up] {
			down := all[downID]
			consumed := map[string]int{}
			if active := activeArtifact(down); active != nil && active.BasedOnGenerations != nil {
				consumed = active.BasedOnGenerations
			}
			if taintedGen, ok := taint[up]; ok && consumed[up] == taintedGen && down.Status == StatusSucceeded {
				entry := ImpactedJob{JobID: downID, CurrentGeneration: down.Generation, ConsumedGenerations: cloneGenMap(consumed)}
				invalidated = append(invalidated, entry)
				taint[downID] = down.Generation
			} else if down.Status == StatusSucceeded {
				reusableSet[downID] = ImpactedJob{JobID: downID, CurrentGeneration: down.Generation, ConsumedGenerations: cloneGenMap(consumed)}
			}
			if !visited[downID] {
				visited[downID] = true
				queue = append(queue, downID)
			}
		}
	}
	// Any not-yet-executed descendant gets no repeated work either.
	for id := range visited {
		if id == rootID {
			continue
		}
		if all[id].Status != StatusSucceeded {
			reusableSet[id] = ImpactedJob{JobID: id, CurrentGeneration: all[id].Generation}
		}
	}
	_ = kind
	reusable := make([]ImpactedJob, 0, len(reusableSet))
	ids := make([]string, 0, len(reusableSet))
	for id := range reusableSet {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		reusable = append(reusable, reusableSet[id])
	}
	sort.Slice(invalidated, func(i, j int) bool { return invalidated[i].JobID < invalidated[j].JobID })
	return invalidated, reusable
}

func buildDependents(all map[string]*Job) map[string][]string {
	out := map[string][]string{}
	ids := make([]string, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		for _, dep := range all[id].DependsOn {
			out[dep] = append(out[dep], id)
		}
	}
	return out
}
