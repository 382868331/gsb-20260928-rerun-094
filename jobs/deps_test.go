package jobs_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"background-jobs/jobs"
)

func waitForStatus(t *testing.T, manager *jobs.Manager, id string, want jobs.Status) *jobs.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, _, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == want {
			return job
		}
		time.Sleep(5 * time.Millisecond)
	}
	job, _, _ := manager.Get(id)
	t.Fatalf("job %s never reached %s, last status %s", id, want, job.Status)
	return nil
}

func TestWorkerDiamondDependencies(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]jobs.Input{}
	registry := jobs.NewRegistry(jobs.Definition{
		Type:        "step",
		SafeToRetry: true,
		Execute: func(_ context.Context, in jobs.Input) (string, error) {
			mu.Lock()
			seen[in.Payload] = in
			mu.Unlock()
			return "result:" + in.Payload, nil
		},
	})
	manager := jobs.NewManager(testStore(t), registry)
	jobA, _ := manager.Enqueue("step", "A", 1)
	jobB, _ := manager.Enqueue("step", "B", 1, jobs.WithDependencies(jobA.ID))
	jobC, _ := manager.Enqueue("step", "C", 1, jobs.WithDependencies(jobA.ID))
	jobD, _ := manager.Enqueue("step", "D", 1, jobs.WithDependencies(jobB.ID, jobC.ID))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- jobs.NewWorker(manager, registry).Run(ctx) }()

	finished := waitForStatus(t, manager, jobD.ID, jobs.StatusSucceeded)
	cancel()
	<-done

	if finished.Result != "result:D" {
		t.Fatalf("D result = %q", finished.Result)
	}
	mu.Lock()
	inputD := seen["D"]
	mu.Unlock()
	want := map[string]string{jobB.ID: "result:B", jobC.ID: "result:C"}
	if len(inputD.DepResults) != 2 || inputD.DepResults[jobB.ID] != "result:B" || inputD.DepResults[jobC.ID] != "result:C" {
		t.Fatalf("D dep results = %v, want %v", inputD.DepResults, want)
	}
	for _, id := range []string{jobA.ID, jobB.ID, jobC.ID, jobD.ID} {
		job, attempts, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status != jobs.StatusSucceeded {
			t.Fatalf("job %s status = %s", id, job.Status)
		}
		if len(attempts) != 1 || attempts[0].Status != jobs.AttemptSucceeded || attempts[0].FinishedAt == nil {
			t.Fatalf("job %s attempts = %+v", id, attempts)
		}
	}
	_, attemptsD, _ := manager.Get(jobD.ID)
	if len(attemptsD[0].DepResults) != 2 {
		t.Fatalf("D attempt dep results not persisted: %+v", attemptsD[0])
	}
	all, _ := manager.List()
	if len(all) != 4 {
		t.Fatalf("job count = %d, want 4", len(all))
	}
}

func TestDependentWaitsForPredecessorRetry(t *testing.T) {
	registry := jobs.NewRegistry(jobs.Definition{
		Type:        "flaky",
		SafeToRetry: true,
		Execute:     func(context.Context, jobs.Input) (string, error) { return "", nil },
	})
	manager := jobs.NewManager(testStore(t), registry)
	pred, _ := manager.Enqueue("flaky", "", 3)
	succ, _ := manager.Enqueue("flaky", "", 1, jobs.WithDependencies(pred.ID))

	manager.Claim()
	if _, err := manager.Complete(pred.ID, 1, "", errors.New("temporary")); err != nil {
		t.Fatal(err)
	}
	// Predecessor is backing off; nothing may be dispatched for the successor.
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("dispatched while predecessor not confirmed: %+v", claimed)
	}
	time.Sleep(120 * time.Millisecond)
	claimed, attempt, _ := manager.Claim()
	if claimed.ID != pred.ID || attempt.Number != 2 {
		t.Fatalf("expected predecessor retry attempt 2, got job %s attempt %d", claimed.ID, attempt.Number)
	}
	if _, err := manager.Complete(pred.ID, 2, "confirmed output", nil); err != nil {
		t.Fatal(err)
	}

	claimed, attempt, _ = manager.Claim()
	if claimed.ID != succ.ID {
		t.Fatalf("claimed %s, want successor %s", claimed.ID, succ.ID)
	}
	if attempt.Number != 1 || attempt.DepResults[pred.ID] != "confirmed output" {
		t.Fatalf("successor attempt = %+v", attempt)
	}
	if _, err := manager.Complete(succ.ID, 1, "joined", nil); err != nil {
		t.Fatal(err)
	}

	// The retry produced a second attempt on the same job, not a new business job.
	all, _ := manager.List()
	if len(all) != 2 {
		t.Fatalf("job count = %d, want 2", len(all))
	}
	predJob, predAttempts, _ := manager.Get(pred.ID)
	if predJob.AttemptCount != 2 || len(predAttempts) != 2 {
		t.Fatalf("predecessor attempts = %+v", predAttempts)
	}
	if predAttempts[0].Status != jobs.AttemptFailed || predAttempts[1].Status != jobs.AttemptSucceeded {
		t.Fatalf("predecessor attempt history = %+v", predAttempts)
	}
}

func TestInterruptedUnsafePredecessorBlocksDependent(t *testing.T) {
	registry := jobs.NewRegistry(
		jobs.Definition{
			Type:        "charge",
			SafeToRetry: false,
			Execute:     func(context.Context, jobs.Input) (string, error) { return "", nil },
		},
		jobs.Definition{
			Type:        "notify",
			SafeToRetry: true,
			Execute:     func(context.Context, jobs.Input) (string, error) { return "", nil },
		},
	)
	manager := jobs.NewManager(testStore(t), registry)
	unsafe, _ := manager.Enqueue("charge", "", 3)
	dependent, _ := manager.Enqueue("notify", "", 1, jobs.WithDependencies(unsafe.ID))

	// Worker claims the unsafe job, then dies before confirming the result.
	manager.Claim()
	if _, err := manager.RecoverInterrupted(); err != nil {
		t.Fatal(err)
	}
	restored, attempts, _ := manager.Get(unsafe.ID)
	if restored.Status != jobs.StatusFailed || attempts[0].Status != jobs.AttemptInterrupted {
		t.Fatalf("unsafe job after recovery = %s, attempt = %+v", restored.Status, attempts[0])
	}

	// The ambiguous result must not be treated as a failed-and-redoable step,
	// and the dependent must never be dispatched against an unconfirmed input.
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("dispatched job with unconfirmed dependency: %+v", claimed)
	}
	blocked, blockedAttempts, _ := manager.Get(dependent.ID)
	if blocked.Status != jobs.StatusCanceled {
		t.Fatalf("dependent status = %s", blocked.Status)
	}
	if !strings.Contains(blocked.Error, jobs.ErrDependencyFailed.Error()) {
		t.Fatalf("dependent error = %q", blocked.Error)
	}
	if len(blockedAttempts) != 0 {
		t.Fatalf("dependent executed attempts: %+v", blockedAttempts)
	}
}

func TestCancelGroupKeepsCompletedAndStopsRunning(t *testing.T) {
	registry := jobs.BuiltinRegistry()
	manager := jobs.NewManager(testStore(t), registry)
	done, _ := manager.Enqueue("echo", "finished", 1, jobs.WithGroup("batch"))
	running, _ := manager.Enqueue("sleep", "60000", 1, jobs.WithGroup("batch"))
	queued, _ := manager.Enqueue("echo", "queued", 1, jobs.WithGroup("batch"))
	other, _ := manager.Enqueue("echo", "other", 1)

	manager.Claim()
	manager.Complete(done.ID, 1, "echo: finished", nil)
	manager.Claim() // running job is now in-flight

	canceled, err := manager.CancelGroup("batch")
	if err != nil {
		t.Fatal(err)
	}
	if len(canceled) != 2 {
		t.Fatalf("canceled %d jobs, want 2", len(canceled))
	}

	doneJob, doneAttempts, _ := manager.Get(done.ID)
	if doneJob.Status != jobs.StatusSucceeded || doneJob.Result != "echo: finished" {
		t.Fatalf("completed job changed: %+v", doneJob)
	}
	if doneAttempts[0].Status != jobs.AttemptSucceeded {
		t.Fatalf("completed attempt changed: %+v", doneAttempts[0])
	}

	runningJob, runningAttempts, _ := manager.Get(running.ID)
	if runningJob.Status != jobs.StatusCanceled {
		t.Fatalf("running job status = %s", runningJob.Status)
	}
	if len(runningAttempts) != 1 || runningAttempts[0].Status != jobs.AttemptCanceled || runningAttempts[0].FinishedAt == nil {
		t.Fatalf("running attempt not independently terminated: %+v", runningAttempts)
	}
	if _, err := manager.Complete(running.ID, 1, "late", nil); !errors.Is(err, jobs.ErrStaleAttempt) {
		t.Fatalf("late result after group cancel = %v", err)
	}

	queuedJob, queuedAttempts, _ := manager.Get(queued.ID)
	if queuedJob.Status != jobs.StatusCanceled || len(queuedAttempts) != 0 {
		t.Fatalf("queued job = %+v attempts %+v", queuedJob, queuedAttempts)
	}

	otherJob, _, _ := manager.Get(other.ID)
	if otherJob.Status != jobs.StatusPending {
		t.Fatalf("job outside group = %s", otherJob.Status)
	}
}

func TestRestartRestoresSameGraph(t *testing.T) {
	path := t.TempDir() + "/jobs.json"
	registry := jobs.BuiltinRegistry()
	store, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	manager := jobs.NewManager(store, registry)
	root, _ := manager.Enqueue("echo", "root", 1, jobs.WithGroup("pipeline"))
	mid, _ := manager.Enqueue("echo", "mid", 1, jobs.WithGroup("pipeline"), jobs.WithDependencies(root.ID))
	leaf, _ := manager.Enqueue("echo", "leaf", 1, jobs.WithGroup("pipeline"), jobs.WithDependencies(mid.ID))
	manager.Claim()
	manager.Complete(root.ID, 1, "echo: root", nil)
	store.Close()

	// Reopen as a fresh process would: the same graph must come back.
	store2, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	manager2 := jobs.NewManager(store2, registry)
	restored, _, _ := manager2.Get(mid.ID)
	if len(restored.DependsOn) != 1 || restored.DependsOn[0] != root.ID || restored.GroupID != "pipeline" {
		t.Fatalf("restored mid = %+v", restored)
	}
	restoredLeaf, _, _ := manager2.Get(leaf.ID)
	if len(restoredLeaf.DependsOn) != 1 || restoredLeaf.DependsOn[0] != mid.ID {
		t.Fatalf("restored leaf = %+v", restoredLeaf)
	}

	claimed, attempt, _ := manager2.Claim()
	if claimed.ID != mid.ID || attempt.DepResults[root.ID] != "echo: root" {
		t.Fatalf("claim after restart = %+v attempt %+v", claimed, attempt)
	}
	all, _ := manager2.List()
	if len(all) != 3 {
		t.Fatalf("job count after restart = %d, want 3", len(all))
	}
}

func TestEnqueueRejectsUnknownOrFailedDependency(t *testing.T) {
	registry := jobs.BuiltinRegistry()
	manager := jobs.NewManager(testStore(t), registry)
	if _, err := manager.Enqueue("echo", "", 1, jobs.WithDependencies("job-missing")); !errors.Is(err, jobs.ErrUnknownDependency) {
		t.Fatalf("unknown dependency error = %v", err)
	}
	failed, _ := manager.Enqueue("unsafe_flaky", "", 1)
	manager.Claim()
	manager.Complete(failed.ID, 1, "", errors.New("unknown downstream state"))
	if _, err := manager.Enqueue("echo", "", 1, jobs.WithDependencies(failed.ID)); !errors.Is(err, jobs.ErrDependencyFailed) {
		t.Fatalf("failed dependency error = %v", err)
	}
}
