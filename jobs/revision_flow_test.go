package jobs_test

import (
	"context"
	"errors"
	"testing"

	"background-jobs/jobs"
)

func revisionRegistry() *jobs.Registry {
	return jobs.NewRegistry(jobs.Definition{
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
}

func succeedRevisionChain(t *testing.T, manager *jobs.Manager) (*jobs.Job, *jobs.Job, *jobs.Job) {
	t.Helper()
	a, err := manager.Enqueue("echo", "a", 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := manager.Enqueue("echo", "b", 1, jobs.WithDependencies(a.ID))
	if err != nil {
		t.Fatal(err)
	}
	c, err := manager.Enqueue("echo", "c", 1, jobs.WithDependencies(b.ID))
	if err != nil {
		t.Fatal(err)
	}
	completeNext := func(result string) {
		t.Helper()
		job, attempt, claimErr := manager.Claim()
		if claimErr != nil || job == nil {
			t.Fatalf("claim failed: job=%+v attempt=%+v err=%v", job, attempt, claimErr)
		}
		if _, err := manager.Complete(job.ID, attempt.Number, result, nil); err != nil {
			t.Fatal(err)
		}
	}
	completeNext("echo:a")
	completeNext("join:echo:a|")
	completeNext("join:join:echo:a||")
	return a, b, c
}

func completeJob(t *testing.T, manager *jobs.Manager, result string) (*jobs.Job, *jobs.Attempt) {
	t.Helper()
	job, attempt, err := manager.Claim()
	if err != nil || job == nil {
		t.Fatalf("claim: job=%+v attempt=%+v err=%v", job, attempt, err)
	}
	if _, err := manager.Complete(job.ID, attempt.Number, result, nil); err != nil {
		t.Fatal(err)
	}
	return job, attempt
}

func TestRevisionPreviewPartialConfirmCancelAndVersionedRerun(t *testing.T) {
	manager := jobs.NewManager(testStore(t), revisionRegistry())
	a, b, c := succeedRevisionChain(t, manager)
	x, _ := manager.Enqueue("echo", "x", 1)
	completeJob(t, manager, "echo:x")

	rev, err := manager.ProposeRevision([]jobs.ChangeSpec{
		{JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:a2"},
		{JobID: x.ID, Kind: jobs.ChangeReplace, NewResult: "echo:y"},
	}, "partial")
	if err != nil {
		t.Fatal(err)
	}
	preview, err := manager.PreviewRevision(rev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Impacts) != 2 || len(preview.Impacts[0].WouldRerun) != 2 {
		t.Fatalf("preview = %+v", preview)
	}
	if got := preview.Impacts[0].WouldRerun[0].JobID; got != b.ID && got != c.ID {
		t.Fatalf("unexpected impacted job %s", got)
	}

	report, err := manager.ConfirmRevision(rev.ID, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Accepted) != 1 || len(report.InvalidatedJobs) != 2 || report.Revision.Status != jobs.RevisionProposed {
		t.Fatalf("partial report = %+v", report)
	}
	jobB, _, _ := manager.Get(b.ID)
	if jobB.Status != jobs.StatusPending || jobB.Generation != 1 || len(jobB.Artifacts) != 1 || jobB.Artifacts[0].State != jobs.ArtifactSuperseded {
		t.Fatalf("B history/state = %+v", jobB)
	}
	_, attemptB, _ := manager.Claim()
	if attemptB == nil || attemptB.DepGenerations[a.ID] != 2 || attemptB.DepResults[a.ID] != "echo:a2" {
		t.Fatalf("rerun did not pin confirmed A generation: %+v", attemptB)
	}

	canceled, err := manager.CancelRevision(rev.ID, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != jobs.RevisionAccepted || canceled.Changes[1].Status != jobs.ChangeCanceled {
		t.Fatalf("candidate after partial cancel = %+v", canceled)
	}
	jobX, _, _ := manager.Get(x.ID)
	if jobX.Result != "echo:x" || jobX.Generation != 1 || len(jobX.Artifacts) != 1 {
		t.Fatalf("canceled change leaked into artifact: %+v", jobX)
	}

	if _, err := manager.Complete(b.ID, attemptB.Number, "join:echo:a2|", nil); err != nil {
		t.Fatal(err)
	}
	jobC, attemptC, err := manager.Claim()
	if err != nil || jobC.ID != c.ID || attemptC.DepGenerations[b.ID] != 2 {
		t.Fatalf("C did not consume versioned B: job=%+v attempt=%+v err=%v", jobC, attemptC, err)
	}
	if _, err := manager.Complete(c.ID, attemptC.Number, "join:join:echo:a2||", nil); err != nil {
		t.Fatal(err)
	}
	comparison, err := manager.CompareRevision(rev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !comparison.Changes[0].ActualChange || comparison.Changes[1].ActualChange || comparison.Changes[1].AfterResult != "" {
		t.Fatalf("comparison = %+v", comparison)
	}
}

func TestInterleavedRevisionsRunOnlyOnceWithBothRevisionIDs(t *testing.T) {
	manager := jobs.NewManager(testStore(t), revisionRegistry())
	a, b, _ := succeedRevisionChain(t, manager)
	r1, err := manager.ProposeRevision([]jobs.ChangeSpec{{JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:r1"}}, "r1")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := manager.ProposeRevision([]jobs.ChangeSpec{{JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:r2"}}, "r2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ConfirmRevision(r1.ID, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ConfirmRevision(r2.ID, nil); !errors.Is(err, jobs.ErrStaleGeneration) {
		t.Fatalf("stale interleaved revision error = %v", err)
	}
	r3, err := manager.ProposeRevision([]jobs.ChangeSpec{{JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:r3"}}, "r3")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ConfirmRevision(r3.ID, nil); err != nil {
		t.Fatal(err)
	}
	jobB, attemptsB, _ := manager.Get(b.ID)
	if jobB.AttemptCount != 1 || jobB.Status != jobs.StatusPending || len(jobB.PendingRevisionIDs) != 2 {
		t.Fatalf("B before rerun = %+v attempts=%+v", jobB, attemptsB)
	}
	_, attempt, err := manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if attempt.DepGenerations[a.ID] != 3 || len(attempt.RevisionIDs) != 2 {
		t.Fatalf("rerun attempt = %+v", attempt)
	}
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("second independent rerun was dispatched: %+v", claimed)
	}
	if _, err := manager.Complete(b.ID, attempt.Number, "join:echo:r3|", nil); err != nil {
		t.Fatal(err)
	}
	jobB, attemptsB, _ = manager.Get(b.ID)
	if len(attemptsB) != 2 || jobB.Generation != 2 || len(jobB.Artifacts) != 2 || jobB.Artifacts[1].BasedOnGenerations[a.ID] != 3 {
		t.Fatalf("B after single rerun = %+v attempts=%+v", jobB, attemptsB)
	}
}

func TestExternalPendingBlocksBranchUntilResolvedAndPersists(t *testing.T) {
	path := t.TempDir() + "/state/jobs.json"
	registry := revisionRegistry()
	store, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	manager := jobs.NewManager(store, registry)
	a, _ := manager.Enqueue("echo", "a", 1)
	b, _ := manager.Enqueue("echo", "b", 1, jobs.WithDependencies(a.ID))
	completeJob(t, manager, "echo:a")
	completeJob(t, manager, "join:echo:a|")
	rev, err := manager.ProposeRevision([]jobs.ChangeSpec{{
		JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:a2", ExternalEffects: []string{"verify webhook"},
	}}, "external")
	if err != nil {
		t.Fatal(err)
	}
	report, err := manager.ConfirmRevision(rev.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.PendingOpen) != 1 {
		t.Fatalf("report = %+v", report)
	}
	if len(report.BlockedByPending) != 1 || report.BlockedByPending[0] != b.ID {
		t.Fatalf("blocked branches = %+v", report.BlockedByPending)
	}
	if claimed, attempt, _ := manager.Claim(); claimed != nil {
		t.Fatalf("branch dispatched with pending external effect: %+v %+v", claimed, attempt)
	}
	store.Close()

	store2, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	manager2 := jobs.NewManager(store2, registry)
	reloaded, err := manager2.GetRevision(rev.ID)
	if err != nil || len(reloaded.Pending) != 1 || reloaded.Pending[0].Status != jobs.PendingOpen {
		t.Fatalf("pending did not persist: rev=%+v err=%v", reloaded, err)
	}
	if claimed, _, _ := manager2.Claim(); claimed != nil {
		t.Fatalf("branch dispatched after restart")
	}
	item, err := manager2.ResolvePending(report.PendingOpen[0], jobs.PendingConfirmed, "human verified")
	if err != nil {
		t.Fatal(err)
	}
	if item.Status != jobs.PendingConfirmed || item.Resolution != "human verified" {
		t.Fatalf("resolved item = %+v", item)
	}
	if _, rerun, err := manager2.Claim(); err != nil || rerun == nil {
		t.Fatalf("branch did not resume: attempt=%+v err=%v", rerun, err)
	}
}

func TestFullRevisionCancelPreservesCompletedHistory(t *testing.T) {
	manager := jobs.NewManager(testStore(t), revisionRegistry())
	a, _, _ := succeedRevisionChain(t, manager)
	rev, _ := manager.ProposeRevision([]jobs.ChangeSpec{{JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:a2"}}, "cancel")
	canceled, err := manager.CancelRevision(rev.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != jobs.RevisionCanceled || canceled.Changes[0].Status != jobs.ChangeCanceled {
		t.Fatalf("canceled revision = %+v", canceled)
	}
	jobA, attemptsA, _ := manager.Get(a.ID)
	if jobA.Status != jobs.StatusSucceeded || jobA.Result != "echo:a" || jobA.Generation != 1 || len(attemptsA) != 1 {
		t.Fatalf("history rewritten after cancel: job=%+v attempts=%+v", jobA, attemptsA)
	}
}

func TestClaimedRevisionSourceStaysImmutableAfterStoreRead(t *testing.T) {
	manager := jobs.NewManager(testStore(t), revisionRegistry())
	a, _ := manager.Enqueue("echo", "a", 1)
	_, attempt, _ := manager.Claim()
	manager.Complete(a.ID, attempt.Number, "echo:a", nil)
	rev, _ := manager.ProposeRevision([]jobs.ChangeSpec{{JobID: a.ID, Kind: jobs.ChangeReplace, NewResult: "echo:a2"}}, "x")
	returned, _ := manager.GetRevision(rev.ID)
	returned.Status = jobs.RevisionCanceled
	returned.Changes[0].NewResult = "mutated"
	stored, _ := manager.GetRevision(rev.ID)
	if stored.Status != jobs.RevisionProposed || stored.Changes[0].NewResult != "echo:a2" {
		t.Fatalf("returned revision aliases storage: %+v", stored)
	}
}
