package jobs_test

import (
	"errors"
	"testing"

	"background-jobs/jobs"
)

// Two reviewers interleave decisions on one candidate; the audit trail shows
// which decision took effect, duplicates never re-promote a generation, and a
// reopened store continues where the previous process left off.
func TestReviewAssignmentInterleavedDuplicateAndReopen(t *testing.T) {
	path := t.TempDir() + "/state/jobs.json"
	store, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	manager := jobs.NewManager(store, revisionRegistry())
	a, b, _ := succeedRevisionChain(t, manager)
	x, _ := manager.Enqueue("echo", "x", 1)
	completeJob(t, manager, "echo:x")

	rev, err := manager.ProposeRevision([]jobs.ChangeSpec{
		{JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:a2", Reviewer: "alice"},
		{JobID: x.ID, Kind: jobs.ChangeReplace, NewResult: "echo:y", Reviewer: "bob"},
	}, "reviewed")
	if err != nil {
		t.Fatal(err)
	}

	// Permission: bob cannot decide alice's change, and the old confirm path
	// cannot touch assigned changes either.
	if _, err := manager.ReviewChange(rev.ID, 0, "bob", jobs.ReviewAccept, ""); !errors.Is(err, jobs.ErrReviewerNotAssigned) {
		t.Fatalf("expected ErrReviewerNotAssigned, got %v", err)
	}
	if _, err := manager.ConfirmRevision(rev.ID, []int{0}); !errors.Is(err, jobs.ErrReviewerNotAssigned) {
		t.Fatalf("expected ErrReviewerNotAssigned from confirm, got %v", err)
	}

	// Bob rejects his own change first.
	bobReport, err := manager.ReviewChange(rev.ID, 1, "bob", jobs.ReviewReject, "not needed")
	if err != nil {
		t.Fatal(err)
	}
	if !bobReport.Effective || bobReport.Confirmation != nil {
		t.Fatalf("bob report: %+v", bobReport)
	}
	if got := bobReport.Revision.Changes[1].Status; got != jobs.ChangeRejected {
		t.Fatalf("change 1 status = %s", got)
	}

	// Alice accepts her change; a2 promotes generation 2 and invalidates b.
	aliceReport, err := manager.ReviewChange(rev.ID, 0, "alice", jobs.ReviewAccept, "looks right")
	if err != nil {
		t.Fatal(err)
	}
	if !aliceReport.Effective || aliceReport.Confirmation.NewGenerations[a.ID] != 2 {
		t.Fatalf("alice report: %+v", aliceReport.Confirmation)
	}

	// Duplicate accept by alice: recorded but ineffective, no new generation.
	dup, err := manager.ReviewChange(rev.ID, 0, "alice", jobs.ReviewAccept, "again")
	if err != nil {
		t.Fatal(err)
	}
	if dup.Effective {
		t.Fatal("duplicate review must not be effective")
	}
	// Cross verdict by alice on her decided change is also inert.
	cross, err := manager.ReviewChange(rev.ID, 0, "alice", jobs.ReviewReject, "changed my mind")
	if err != nil {
		t.Fatal(err)
	}
	if cross.Effective {
		t.Fatal("cross review must not be effective")
	}
	jobA, _, err := manager.Get(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if jobA.Generation != 2 || jobA.Result != "echo:a2" {
		t.Fatalf("duplicate review mutated artifact: gen=%d result=%q", jobA.Generation, jobA.Result)
	}
	if got := jobA.Artifacts; len(got) != 2 {
		t.Fatalf("expected 2 retained generations, got %d", len(got))
	}

	// Audit trail: exactly one effective record per decided change.
	stored, err := manager.GetRevision(rev.ID)
	if err != nil {
		t.Fatal(err)
	}
	reviews := stored.Changes[0].Reviews
	if len(reviews) != 3 || !reviews[0].Effective || reviews[1].Effective || reviews[2].Effective {
		t.Fatalf("audit trail: %+v", reviews)
	}
	if stored.Changes[1].Reviews[0].Decision != jobs.ReviewReject || !stored.Changes[1].Reviews[0].Effective {
		t.Fatalf("bob review: %+v", stored.Changes[1].Reviews)
	}
	if stored.Status != jobs.RevisionAccepted {
		t.Fatalf("revision status = %s", stored.Status)
	}

	// Save/reopen: a fresh manager on the same file keeps revisions, artifacts,
	// and review records, and the invalidated downstream can be redone.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	manager2 := jobs.NewManager(reopened, revisionRegistry())
	revAgain, err := manager2.GetRevision(rev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(revAgain.Changes[0].Reviews) != 3 || revAgain.Changes[1].Status != jobs.ChangeRejected {
		t.Fatalf("review state lost after reopen: %+v", revAgain.Changes)
	}
	// Rejected change result never entered downstream inputs: x still has its
	// original artifact and no new generation.
	jobX, _, err := manager2.Get(x.ID)
	if err != nil {
		t.Fatal(err)
	}
	if jobX.Generation != 1 || jobX.Result != "echo:x" {
		t.Fatalf("rejected change leaked: gen=%d result=%q", jobX.Generation, jobX.Result)
	}
	// The downstream invalidated by alice's accept reruns against gen 2.
	jobB, _, err := manager2.Get(b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if jobB.Status != jobs.StatusPending {
		t.Fatalf("downstream status = %s", jobB.Status)
	}
	redone, attempt, err := manager2.Claim()
	if err != nil || redone == nil {
		t.Fatalf("claim after reopen: %+v %v", redone, err)
	}
	if redone.ID != b.ID || attempt.DepGenerations[a.ID] != 2 || attempt.DepResults[a.ID] != "echo:a2" {
		t.Fatalf("redo consumed wrong input: %+v", attempt)
	}
}

