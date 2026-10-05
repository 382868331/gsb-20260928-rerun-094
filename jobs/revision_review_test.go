package jobs_test

import (
	"context"
	"errors"
	"testing"

	"background-jobs/jobs"
)

func reviewStore(t *testing.T) (*jobs.Manager, *jobs.Store, string, func()) {
	t.Helper()
	path := t.TempDir() + "/state/jobs.json"
	store, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	registry := jobs.NewRegistry(jobs.Definition{
		Type:        "echo",
		SafeToRetry: true,
		Execute: func(_ context.Context, in jobs.Input) (string, error) {
			if len(in.DepResults) == 0 {
				return "echo:" + in.Payload, nil
			}
			out := "join:"
			for _, value := range in.DepResults {
				out += value + "|"
			}
			return out, nil
		},
	})
	return jobs.NewManager(store, registry), store, path, func() { store.Close() }
}

func reopenReviewStore(t *testing.T, path string) (*jobs.Manager, *jobs.Store) {
	t.Helper()
	store, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	registry := jobs.NewRegistry(jobs.Definition{
		Type:        "echo",
		SafeToRetry: true,
		Execute: func(_ context.Context, in jobs.Input) (string, error) {
			if len(in.DepResults) == 0 {
				return "echo:" + in.Payload, nil
			}
			out := "join:"
			for _, value := range in.DepResults {
				out += value + "|"
			}
			return out, nil
		},
	})
	return jobs.NewManager(store, registry), store
}

// Two reviewers interleave on the same candidate: each handles only the
// changes assigned to them, and the effective decision records the actual
// reviewer. Cross-handling attempts are denied without taking effect.
func TestReviewAssignmentInterleaveDenyAndIdempotent(t *testing.T) {
	manager, _, path, closeStore := reviewStore(t)
	defer closeStore()
	a, err := manager.Enqueue("echo", "a", 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := manager.Enqueue("echo", "b", 1, jobs.WithDependencies(a.ID))
	if err != nil {
		t.Fatal(err)
	}
	completeJob(t, manager, "echo:a")
	completeJob(t, manager, "join:echo:a|")

	rev, err := manager.ProposeRevision([]jobs.ChangeSpec{
		{JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:a2", Reviewer: "alice"},
		{JobID: b.ID, Kind: jobs.ChangeReplace, NewResult: "echo:b-fix", Reviewer: "bob"},
	}, "split review")
	if err != nil {
		t.Fatal(err)
	}

	// Bob is not assigned change 0: the decision is denied and recorded but
	// changes nothing.
	denied, err := manager.ReviewRevision(rev.ID, "bob", []jobs.ReviewDecision{
		{Index: 0, Kind: jobs.DecisionAccept, Reason: "trying to sneak through"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(denied.Denied) != 1 || denied.Denied[0] != 0 {
		t.Fatalf("denied report = %+v", denied)
	}
	jobA, _, _ := manager.Get(a.ID)
	if jobA.Generation != 1 || jobA.Result != "echo:a" {
		t.Fatalf("unauthorized accept took effect: %+v", jobA)
	}
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("denied decision entered downstream: %+v", claimed)
	}

	// The legacy confirm/cancel path cannot decide assigned changes either.
	if _, err := manager.ConfirmRevision(rev.ID, []int{0}); !errors.Is(err, jobs.ErrReviewerRequired) {
		t.Fatalf("legacy confirm on assigned change = %v", err)
	}
	if _, err := manager.CancelRevision(rev.ID, []int{1}); !errors.Is(err, jobs.ErrReviewerRequired) {
		t.Fatalf("legacy cancel on assigned change = %v", err)
	}

	// Alice accepts her change first.
	accepted, err := manager.ReviewRevision(rev.ID, "alice", []jobs.ReviewDecision{
		{Index: 0, Kind: jobs.DecisionAccept, Reason: "verified corrected output"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted.Accepted) != 1 || accepted.NewGenerations[a.ID] != 2 {
		t.Fatalf("alice report = %+v", accepted)
	}
	if rev := accepted.Revision; rev.Changes[0].DecidedBy != "alice" || rev.Changes[0].ToGeneration != 2 {
		t.Fatalf("effective decision not recorded: %+v", rev.Changes[0])
	}
	if len(accepted.Revision.Changes[0].Reviews) != 2 ||
		accepted.Revision.Changes[0].Reviews[0].Authorized || accepted.Revision.Changes[0].Reviews[0].Applied ||
		!accepted.Revision.Changes[0].Reviews[1].Authorized || !accepted.Revision.Changes[0].Reviews[1].Applied {
		t.Fatalf("review history = %+v", accepted.Revision.Changes[0].Reviews)
	}

	// Alice repeating the accept is a no-op duplicate: no new generation.
	dup, err := manager.ReviewRevision(rev.ID, "alice", []jobs.ReviewDecision{{Index: 0, Kind: jobs.DecisionAccept}})
	if err != nil {
		t.Fatal(err)
	}
	if len(dup.Duplicate) != 1 {
		t.Fatalf("duplicate report = %+v", dup)
	}
	jobA, _, _ = manager.Get(a.ID)
	if jobA.Generation != 2 || len(dup.NewGenerations) != 0 {
		t.Fatalf("duplicate review bumped generation: job=%+v report=%+v", jobA, dup)
	}

	// Bob cross-confirming Alice's already-accepted change is denied first
	// (assignment), and even the assigned reviewer cannot record a different
	// effective decision afterwards.
	cross, err := manager.ReviewRevision(rev.ID, "bob", []jobs.ReviewDecision{{Index: 0, Kind: jobs.DecisionReturn}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cross.Denied) != 1 {
		t.Fatalf("bob cross-return report = %+v", cross)
	}
	if _, err := manager.ReviewRevision(rev.ID, "alice", []jobs.ReviewDecision{{Index: 0, Kind: jobs.DecisionReturn}}); !errors.Is(err, jobs.ErrReviewConflict) {
		t.Fatalf("conflicting effective decision error = %v", err)
	}

	// Bob interleave: returns his own change before persisting.
	returned, err := manager.ReviewRevision(rev.ID, "bob", []jobs.ReviewDecision{
		{Index: 1, Kind: jobs.DecisionReturn, Reason: "candidate incorrect"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(returned.Returned) != 1 || returned.Revision.Changes[1].Status != jobs.ChangeRejected || returned.Revision.Changes[1].DecidedBy != "bob" {
		t.Fatalf("return report = %+v", returned)
	}
	jobB, _, _ := manager.Get(b.ID)
	if jobB.Generation != 1 || len(jobB.PendingRevisionIDs) != 1 || jobB.PendingRevisionIDs[0] != rev.ID || jobB.Error == "" {
		t.Fatalf("returned change must not add rerun history beyond change 0: %+v", jobB)
	}

	// Save and reopen: assignment, effective decisions, review history and the
	// still-pending downstream rerun all survive.
	manager2, store2 := reopenReviewStore(t, path)
	defer store2.Close()
	reloaded, err := manager2.GetRevision(rev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Changes[0].AssignedReviewer != "alice" || reloaded.Changes[0].DecidedBy != "alice" ||
		reloaded.Changes[1].Status != jobs.ChangeRejected || len(reloaded.Changes[0].Reviews) != 4 {
		t.Fatalf("review state did not persist: %+v", reloaded)
	}

	// Downstream B was invalidated by Alice's accepted change and resumes after
	// reopen, consuming only confirmed generation 2.
	claimed, attempt, err := manager2.Claim()
	if err != nil || claimed == nil || claimed.ID != b.ID {
		t.Fatalf("downstream did not resume: attempt=%+v err=%v", attempt, err)
	}
	if attempt.DepGenerations[a.ID] != 2 || attempt.DepResults[a.ID] != "echo:a2" {
		t.Fatalf("rerun did not pin confirmed artifact: %+v", attempt)
	}
	if _, err := manager2.Complete(b.ID, attempt.Number, "join:echo:a2|", nil); err != nil {
		t.Fatal(err)
	}

	// Final comparison keeps history and shows the effective decisions.
	comparison, err := manager2.CompareRevision(rev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if comparison.Changes[0].Status != jobs.ChangeAccepted || !comparison.Changes[0].ActualChange ||
		comparison.Changes[1].Status != jobs.ChangeRejected || comparison.Changes[1].ActualChange {
		t.Fatalf("comparison = %+v", comparison)
	}
}

// A returned change dismisses its open external-effect items and never feeds
// downstream; the accepted sibling still proceeds normally.
func TestReviewReturnDismissesPendingAndUnassignedStaysLegacy(t *testing.T) {
	manager, _, _, closeStore := reviewStore(t)
	defer closeStore()
	a, _ := manager.Enqueue("echo", "a", 1)
	b, _ := manager.Enqueue("echo", "b", 1, jobs.WithDependencies(a.ID))
	completeJob(t, manager, "echo:a")
	completeJob(t, manager, "join:echo:a|")
	rev, err := manager.ProposeRevision([]jobs.ChangeSpec{
		{JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:a2", Reviewer: "alice", ExternalEffects: []string{"verify webhook"}},
		{JobID: b.ID, Kind: jobs.ChangeRevoke},
	}, "mixed")
	if err != nil {
		t.Fatal(err)
	}
	// An anonymous review is rejected up front.
	if _, err := manager.ReviewRevision(rev.ID, "", []jobs.ReviewDecision{{Index: 0, Kind: jobs.DecisionAccept}}); !errors.Is(err, jobs.ErrReviewerRequired) {
		t.Fatalf("anonymous review error = %v", err)
	}
	report, err := manager.ReviewRevision(rev.ID, "alice", []jobs.ReviewDecision{{Index: 0, Kind: jobs.DecisionReturn, Reason: "bad webhook claim"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Returned) != 1 {
		t.Fatalf("return report = %+v", report)
	}
	stored, _ := manager.GetRevision(rev.ID)
	if stored.Pending[0].Status != jobs.PendingDismissed {
		t.Fatalf("pending item not dismissed: %+v", stored.Pending[0])
	}
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("returned change dispatched work: %+v", claimed)
	}
	// Unassigned change 1 still uses the legacy path.
	if _, err := manager.CancelRevision(rev.ID, []int{1}); err != nil {
		t.Fatalf("legacy cancel for unassigned change: %v", err)
	}
	stored, _ = manager.GetRevision(rev.ID)
	if stored.Changes[1].Status != jobs.ChangeCanceled {
		t.Fatalf("unassigned change = %+v", stored.Changes[1])
	}
}
