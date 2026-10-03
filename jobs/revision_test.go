package jobs_test

import (
	"context"
	"testing"

	"background-jobs/jobs"
)

func stepRegistry() *jobs.Registry {
	return jobs.NewRegistry(jobs.Definition{
		Type:        "step",
		SafeToRetry: true,
		Execute: func(_ context.Context, in jobs.Input) (string, error) {
			return "out:" + in.Payload, nil
		},
	})
}

func runToSuccess(t *testing.T, manager *jobs.Manager, id string) *jobs.Job {
	t.Helper()
	job, attempt, err := manager.Claim()
	if err != nil || job == nil {
		t.Fatalf("claim for %s: job=%v err=%v", id, job, err)
	}
	if job.ID != id {
		t.Fatalf("claimed %s, want %s", job.ID, id)
	}
	finished, err := manager.Complete(id, attempt.Number, "v1:"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	return finished
}

func TestRevisionReplaceRerunsDownstream(t *testing.T) {
	manager := jobs.NewManager(testStore(t), stepRegistry())
	a, _ := manager.Enqueue("step", "A", 1)
	b, _ := manager.Enqueue("step", "B", 1, jobs.WithDependencies(a.ID))
	c, _ := manager.Enqueue("step", "C", 1, jobs.WithDependencies(b.ID))
	u, _ := manager.Enqueue("step", "U", 1)
	runToSuccess(t, manager, a.ID)
	runToSuccess(t, manager, b.ID)
	runToSuccess(t, manager, c.ID)
	runToSuccess(t, manager, u.ID)

	rev, err := manager.ProposeRevision([]jobs.ChangeSpec{
		{JobID: a.ID, Action: jobs.ActionReplace, NewResult: "revised:A"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rev.Impact.Affected) != 2 {
		t.Fatalf("affected = %+v, want B and C", rev.Impact.Affected)
	}
	if len(rev.Impact.Reusable) != 1 || rev.Impact.Reusable[0] != u.ID {
		t.Fatalf("reusable = %v, want [%s]", rev.Impact.Reusable, u.ID)
	}

	confirmed, err := manager.ConfirmRevision(rev.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.Status != jobs.RevisionConfirmed {
		t.Fatalf("revision status = %s", confirmed.Status)
	}
	change := confirmed.Changes[0]
	if change.OldResult != "v1:"+a.ID || change.NewResult != "revised:A" || change.OldGeneration != 1 || change.NewGeneration != 2 {
		t.Fatalf("change diff = %+v", change)
	}

	// Original completion record is preserved; the replacement is a new generation.
	jobA, attemptsA, _ := manager.Get(a.ID)
	if len(attemptsA) != 1 || attemptsA[0].Status != jobs.AttemptSucceeded || attemptsA[0].Result != "v1:"+a.ID {
		t.Fatalf("A attempts rewritten: %+v", attemptsA)
	}
	if jobA.Result != "revised:A" || jobA.Generation != 2 || len(jobA.ResultHistory) != 2 {
		t.Fatalf("A after confirm = %+v", jobA)
	}

	// Downstream jobs are reset and re-run against the confirmed generation.
	for _, id := range []string{b.ID, c.ID} {
		job, _, _ := manager.Get(id)
		if job.Status != jobs.StatusPending || job.RevisionID != rev.ID {
			t.Fatalf("%s not reset by revision: %+v", id, job)
		}
	}
	jobU, _, _ := manager.Get(u.ID)
	if jobU.Status != jobs.StatusSucceeded || len(jobU.ResultHistory) != 1 {
		t.Fatalf("unaffected job U was touched: %+v", jobU)
	}

	claimed, attempt, err := manager.Claim()
	if err != nil || claimed.ID != b.ID {
		t.Fatalf("re-claim B: %v %v", claimed, err)
	}
	if attempt.Number != 2 || attempt.DepGenerations[a.ID] != 2 || attempt.RevisionID != rev.ID {
		t.Fatalf("B re-run attempt = %+v", attempt)
	}
	if _, err := manager.Complete(b.ID, attempt.Number, "v2:B", nil); err != nil {
		t.Fatal(err)
	}
	jobB, attemptsB, _ := manager.Get(b.ID)
	if len(attemptsB) != 2 || jobB.Generation != 2 {
		t.Fatalf("B after re-run: gen=%d attempts=%+v", jobB.Generation, attemptsB)
	}
}

func TestRevisionPartialConfirm(t *testing.T) {
	manager := jobs.NewManager(testStore(t), stepRegistry())
	a1, _ := manager.Enqueue("step", "A1", 1)
	a2, _ := manager.Enqueue("step", "A2", 1)
	d, _ := manager.Enqueue("step", "D", 1, jobs.WithDependencies(a1.ID, a2.ID))
	runToSuccess(t, manager, a1.ID)
	runToSuccess(t, manager, a2.ID)
	runToSuccess(t, manager, d.ID)

	rev, err := manager.ProposeRevision([]jobs.ChangeSpec{
		{JobID: a1.ID, Action: jobs.ActionReplace, NewResult: "r:A1"},
		{JobID: a2.ID, Action: jobs.ActionReplace, NewResult: "r:A2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	partial, err := manager.ConfirmRevision(rev.ID, []string{a1.ID})
	if err != nil {
		t.Fatal(err)
	}
	if partial.Status != jobs.RevisionPartial {
		t.Fatalf("status = %s, want PARTIALLY_CONFIRMED", partial.Status)
	}
	jobA2, _, _ := manager.Get(a2.ID)
	if jobA2.Result != "v1:"+a2.ID || jobA2.Generation != 1 {
		t.Fatalf("A2 changed before its change was accepted: %+v", jobA2)
	}
	jobD, _, _ := manager.Get(d.ID)
	if jobD.Status != jobs.StatusPending {
		t.Fatalf("D not reset after partial confirm: %+v", jobD)
	}

	full, err := manager.ConfirmRevision(rev.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if full.Status != jobs.RevisionConfirmed {
		t.Fatalf("status = %s", full.Status)
	}
	jobA2, _, _ = manager.Get(a2.ID)
	if jobA2.Result != "r:A2" || jobA2.Generation != 2 {
		t.Fatalf("A2 after full confirm: %+v", jobA2)
	}
	if _, err := manager.ConfirmRevision(rev.ID, nil); err == nil {
		t.Fatal("confirming a closed revision should fail")
	}
}

func TestRevisionWithdrawBlocksDependents(t *testing.T) {
	manager := jobs.NewManager(testStore(t), stepRegistry())
	a, _ := manager.Enqueue("step", "A", 1)
	b, _ := manager.Enqueue("step", "B", 1, jobs.WithDependencies(a.ID))
	runToSuccess(t, manager, a.ID)

	rev, err := manager.ProposeRevision([]jobs.ChangeSpec{{JobID: a.ID, Action: jobs.ActionWithdraw}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ConfirmRevision(rev.ID, nil); err != nil {
		t.Fatal(err)
	}
	jobA, _, _ := manager.Get(a.ID)
	if !jobA.Withdrawn || jobA.Result != "" || jobA.Generation != 2 {
		t.Fatalf("A after withdraw: %+v", jobA)
	}
	if _, _, err := manager.Claim(); err != nil {
		t.Fatal(err)
	}
	jobB, _, _ := manager.Get(b.ID)
	if jobB.Status != jobs.StatusCanceled {
		t.Fatalf("B should be canceled after upstream withdrawal: %+v", jobB)
	}
	if _, err := manager.Enqueue("step", "late", 1, jobs.WithDependencies(a.ID)); err == nil {
		t.Fatal("enqueue depending on a withdrawn artifact should fail")
	}
}

func TestRevisionCancelKeepsOriginals(t *testing.T) {
	manager := jobs.NewManager(testStore(t), stepRegistry())
	a1, _ := manager.Enqueue("step", "A1", 1)
	a2, _ := manager.Enqueue("step", "A2", 1)
	runToSuccess(t, manager, a1.ID)
	runToSuccess(t, manager, a2.ID)

	rev, _ := manager.ProposeRevision([]jobs.ChangeSpec{
		{JobID: a1.ID, Action: jobs.ActionReplace, NewResult: "r:A1"},
		{JobID: a2.ID, Action: jobs.ActionReplace, NewResult: "r:A2"},
	})
	if _, err := manager.ConfirmRevision(rev.ID, []string{a1.ID}); err != nil {
		t.Fatal(err)
	}
	canceled, err := manager.CancelRevision(rev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != jobs.RevisionCanceled {
		t.Fatalf("status = %s", canceled.Status)
	}
	// The already-confirmed change stays applied; the pending one is dropped.
	jobA1, _, _ := manager.Get(a1.ID)
	if jobA1.Result != "r:A1" {
		t.Fatalf("confirmed change lost on cancel: %+v", jobA1)
	}
	jobA2, _, _ := manager.Get(a2.ID)
	if jobA2.Result != "v1:"+a2.ID || jobA2.Generation != 1 {
		t.Fatalf("unconfirmed change leaked on cancel: %+v", jobA2)
	}
}

func TestInterleavedRevisionsRunDownstreamOnce(t *testing.T) {
	manager := jobs.NewManager(testStore(t), stepRegistry())
	a, _ := manager.Enqueue("step", "A", 1)
	b, _ := manager.Enqueue("step", "B", 1, jobs.WithDependencies(a.ID))
	runToSuccess(t, manager, a.ID)
	runToSuccess(t, manager, b.ID)

	rev1, _ := manager.ProposeRevision([]jobs.ChangeSpec{{JobID: a.ID, Action: jobs.ActionReplace, NewResult: "r1:A"}})
	if _, err := manager.ConfirmRevision(rev1.ID, nil); err != nil {
		t.Fatal(err)
	}
	// A second revision lands before the downstream re-run happened.
	rev2, _ := manager.ProposeRevision([]jobs.ChangeSpec{{JobID: a.ID, Action: jobs.ActionReplace, NewResult: "r2:A"}})
	if _, err := manager.ConfirmRevision(rev2.ID, nil); err != nil {
		t.Fatal(err)
	}

	jobB, _, _ := manager.Get(b.ID)
	if jobB.Status != jobs.StatusPending || jobB.RevisionID != rev2.ID {
		t.Fatalf("B should track the latest revision: %+v", jobB)
	}
	claimed, attempt, _ := manager.Claim()
	if claimed.ID != b.ID || attempt.Number != 2 {
		t.Fatalf("B re-run = %v attempt %+v", claimed, attempt)
	}
	if attempt.DepGenerations[a.ID] != 3 || attempt.RevisionID != rev2.ID {
		t.Fatalf("B consumed generations = %v revision %s, want gen 3 of %s", attempt.DepGenerations, attempt.RevisionID, rev2.ID)
	}
	if _, err := manager.Complete(b.ID, attempt.Number, "v3:B", nil); err != nil {
		t.Fatal(err)
	}
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("unexpected extra re-dispatch: %+v", claimed)
	}
	jobA, _, _ := manager.Get(a.ID)
	if jobA.Generation != 3 || len(jobA.ResultHistory) != 3 {
		t.Fatalf("A generations = %+v", jobA.ResultHistory)
	}
}

func TestExternalPendingResolution(t *testing.T) {
	registry := jobs.NewRegistry(
		jobs.Definition{Type: "step", SafeToRetry: true, Execute: func(_ context.Context, in jobs.Input) (string, error) {
			return "out:" + in.Payload, nil
		}},
		jobs.Definition{Type: "risky", SafeToRetry: false, Execute: func(context.Context, jobs.Input) (string, error) {
			return "charged", nil
		}},
	)
	manager := jobs.NewManager(testStore(t), registry)
	a, _ := manager.Enqueue("step", "A", 1)
	risky, _ := manager.Enqueue("risky", "pay", 1, jobs.WithDependencies(a.ID))
	d, _ := manager.Enqueue("step", "D", 1, jobs.WithDependencies(risky.ID))
	runToSuccess(t, manager, a.ID)
	if _, _, err := manager.Claim(); err != nil {
		t.Fatal(err)
	}
	// Worker crashes before confirming the risky job's external effect.
	if _, err := manager.RecoverInterrupted(); err != nil {
		t.Fatal(err)
	}
	jobRisky, _, _ := manager.Get(risky.ID)
	if jobRisky.Status != jobs.StatusFailed || !jobRisky.ExternalPending {
		t.Fatalf("risky job after recover: %+v", jobRisky)
	}

	// A revision over the branch must surface the unverified effect as pending.
	rev, err := manager.ProposeRevision([]jobs.ChangeSpec{{JobID: a.ID, Action: jobs.ActionReplace, NewResult: "r:A"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rev.Impact.PendingExternal) != 1 || rev.Impact.PendingExternal[0].JobID != risky.ID {
		t.Fatalf("pending external = %+v", rev.Impact.PendingExternal)
	}

	// User verifies the effect did happen: the branch continues with the
	// manually confirmed result instead of re-executing the risky step.
	if _, err := manager.ResolveExternal(risky.ID, true, "charged (verified)"); err != nil {
		t.Fatal(err)
	}
	jobRisky, _, _ = manager.Get(risky.ID)
	if jobRisky.Status != jobs.StatusSucceeded || jobRisky.Result != "charged (verified)" || jobRisky.Generation != 1 {
		t.Fatalf("risky job after resolve: %+v", jobRisky)
	}
	confirmed, err := manager.ConfirmRevision(rev.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Confirming the revision must not silently re-run the non-idempotent
	// job; it is parked as a pending item for a fresh user verdict.
	jobRisky, _, _ = manager.Get(risky.ID)
	if !jobRisky.ExternalPending || jobRisky.Status != jobs.StatusSucceeded {
		t.Fatalf("risky job should be parked, not reset: %+v", jobRisky)
	}
	if len(confirmed.Impact.PendingExternal) == 0 {
		t.Fatalf("confirmed revision should list the pending item: %+v", confirmed.Impact)
	}
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("branch must wait for the pending verdict, dispatched %+v", claimed)
	}
	if _, err := manager.ResolveExternal(risky.ID, true, "charged (verified)"); err != nil {
		t.Fatal(err)
	}
	claimed, attempt, _ := manager.Claim()
	if claimed.ID != d.ID {
		t.Fatalf("claimed %v, want D", claimed)
	}
	if attempt.DepResults[risky.ID] != "charged (verified)" {
		t.Fatalf("D consumed %v", attempt.DepResults)
	}
}

func TestExternalPendingResolvedAsFailed(t *testing.T) {
	registry := jobs.NewRegistry(jobs.Definition{Type: "risky", SafeToRetry: false, Execute: func(context.Context, jobs.Input) (string, error) {
		return "", nil
	}})
	manager := jobs.NewManager(testStore(t), registry)
	risky, _ := manager.Enqueue("risky", "pay", 1)
	d, _ := manager.Enqueue("risky", "next", 1, jobs.WithDependencies(risky.ID))
	if _, _, err := manager.Claim(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RecoverInterrupted(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ResolveExternal(risky.ID, false, ""); err != nil {
		t.Fatal(err)
	}
	jobRisky, _, _ := manager.Get(risky.ID)
	if jobRisky.Status != jobs.StatusFailed || jobRisky.ExternalPending {
		t.Fatalf("risky job after negative resolve: %+v", jobRisky)
	}
	if _, _, err := manager.Claim(); err != nil {
		t.Fatal(err)
	}
	jobD, _, _ := manager.Get(d.ID)
	if jobD.Status != jobs.StatusCanceled {
		t.Fatalf("dependent of verified-failed job should be canceled: %+v", jobD)
	}
}
