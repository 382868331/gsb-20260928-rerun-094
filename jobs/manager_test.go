package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"background-jobs/jobs"
)

func testStore(t *testing.T) *jobs.Store {
	t.Helper()
	store, err := jobs.OpenStore(t.TempDir() + "/state/jobs.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestFailedSafeJobRetriesAndSucceeds(t *testing.T) {
	registry := jobs.NewRegistry(jobs.Definition{
		Type:        "retryable",
		SafeToRetry: true,
		Execute: func(_ context.Context, in jobs.Input) (string, error) {
			if in.Attempt == 1 {
				return "", errors.New("temporary")
			}
			return "done", nil
		},
	})
	manager := jobs.NewManager(testStore(t), registry)
	job, err := manager.Enqueue("retryable", "", 2)
	if err != nil {
		t.Fatal(err)
	}

	_, attempt1, err := manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if attempt1.Number != 1 {
		t.Fatalf("attempt = %d", attempt1.Number)
	}
	if _, err := manager.Complete(job.ID, 1, "", errors.New("temporary")); err != nil {
		t.Fatal(err)
	}
	if job, _, _ := manager.Get(job.ID); job.Status != jobs.StatusPending {
		t.Fatalf("status after failure = %s", job.Status)
	}
	time.Sleep(120 * time.Millisecond)
	_, attempt2, err := manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if attempt2.Number != 2 {
		t.Fatalf("second attempt = %d", attempt2.Number)
	}
	finished, err := manager.Complete(job.ID, 2, "done", nil)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != jobs.StatusSucceeded || finished.Result != "done" {
		t.Fatalf("unexpected finished job: %+v", finished)
	}
	_, attempts, _ := manager.Get(job.ID)
	if len(attempts) != 2 || attempts[0].Status != jobs.AttemptFailed || attempts[1].Status != jobs.AttemptSucceeded {
		t.Fatalf("attempt history = %+v", attempts)
	}
}

func TestStaleAttemptSuccessCannotReplaceCurrentResult(t *testing.T) {
	registry := jobs.NewRegistry(jobs.Definition{
		Type:        "retryable",
		SafeToRetry: true,
		Execute:     func(context.Context, jobs.Input) (string, error) { return "", nil },
	})
	manager := jobs.NewManager(testStore(t), registry)
	job, _ := manager.Enqueue("retryable", "", 3)
	manager.Claim()
	manager.Complete(job.ID, 1, "", errors.New("lost"))
	time.Sleep(120 * time.Millisecond)
	manager.Claim()

	if _, err := manager.Complete(job.ID, 1, "late success", nil); !errors.Is(err, jobs.ErrStaleAttempt) {
		t.Fatalf("late attempt error = %v", err)
	}
	finished, err := manager.Complete(job.ID, 2, "current success", nil)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Result != "current success" {
		t.Fatalf("result = %q", finished.Result)
	}
}

func TestUnsafeFailureIsNotRetried(t *testing.T) {
	registry := jobs.NewRegistry(jobs.Definition{
		Type:        "charge",
		SafeToRetry: false,
		Execute:     func(context.Context, jobs.Input) (string, error) { return "", errors.New("unknown downstream state") },
	})
	manager := jobs.NewManager(testStore(t), registry)
	job, _ := manager.Enqueue("charge", "", 3)
	manager.Claim()
	finished, err := manager.Complete(job.ID, 1, "", errors.New("unknown downstream state"))
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != jobs.StatusFailed {
		t.Fatalf("status = %s", finished.Status)
	}
	_, attempts, _ := manager.Get(job.ID)
	if len(attempts) != 1 {
		t.Fatalf("attempts = %d", len(attempts))
	}
}

func TestCanceledJobIsNotDispatchedAndLateResultRejected(t *testing.T) {
	registry := jobs.BuiltinRegistry()
	manager := jobs.NewManager(testStore(t), registry)
	job, _ := manager.Enqueue("sleep", "1000", 3)
	if _, err := manager.Cancel(job.ID); err != nil {
		t.Fatal(err)
	}
	claimed, _, err := manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed != nil {
		t.Fatalf("canceled job was dispatched: %+v", claimed)
	}
	if _, err := manager.Complete(job.ID, 1, "late", nil); !errors.Is(err, jobs.ErrStaleAttempt) {
		t.Fatalf("late complete = %v", err)
	}
}

func TestWorkerStopsRunningCanceledJobWithoutResult(t *testing.T) {
	path := t.TempDir() + "/jobs.json"
	registry := jobs.BuiltinRegistry()
	workerStore, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	manager := jobs.NewManager(workerStore, registry)
	job, err := manager.Enqueue("sleep", "5000", 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- jobs.NewWorker(manager, registry).Run(ctx) }()

	for i := 0; i < 50; i++ {
		current, _, _ := manager.Get(job.ID)
		if current.Status == jobs.StatusRunning {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancelStore, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobs.NewManager(cancelStore, registry).Cancel(job.ID); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done
	_ = workerStore.Close()
	_ = cancelStore.Close()

	checkStore, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer checkStore.Close()
	restored, attempts, err := checkStore.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Status != jobs.StatusCanceled {
		t.Fatalf("status = %s", restored.Status)
	}
	if restored.Result != "" || attempts[0].Result != "" {
		t.Fatalf("canceled task wrote a result: job=%+v attempt=%+v", restored, attempts[0])
	}
}

func TestInterruptedSafeJobRequeuedButUnsafeJobFailsClosed(t *testing.T) {
	for _, safe := range []bool{true, false} {
		registry := jobs.NewRegistry(jobs.Definition{
			Type:        "task",
			SafeToRetry: safe,
			Execute:     func(context.Context, jobs.Input) (string, error) { return "", nil },
		})
		manager := jobs.NewManager(testStore(t), registry)
		job, _ := manager.Enqueue("task", "", 3)
		manager.Claim()
		_, err := manager.RecoverInterrupted()
		if err != nil {
			t.Fatal(err)
		}
		restored, attempts, _ := manager.Get(job.ID)
		if safe {
			if restored.Status != jobs.StatusPending {
				t.Fatalf("safe status = %s", restored.Status)
			}
		} else if restored.Status != jobs.StatusFailed {
			t.Fatalf("unsafe status = %s", restored.Status)
		}
		if attempts[0].Status != jobs.AttemptInterrupted {
			t.Fatalf("attempt status = %s", attempts[0].Status)
		}
	}
}

func TestRestartPreservesCompletedResults(t *testing.T) {
	path := t.TempDir() + "/jobs.json"
	registry := jobs.BuiltinRegistry()
	store, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	manager := jobs.NewManager(store, registry)
	job, _ := manager.Enqueue("echo", "hello", 1)
	store.Close()

	store2, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	manager2 := jobs.NewManager(store2, registry)
	claimed, _, _ := manager2.Claim()
	manager2.Complete(claimed.ID, 1, "echo: hello", nil)
	store2.Close()

	store3, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store3.Close()
	restored, _, err := store3.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Status != jobs.StatusSucceeded || restored.Result != "echo: hello" {
		t.Fatalf("restored job = %+v", restored)
	}
}
func TestDiamondDependencyFlowsConfirmedResults(t *testing.T) {
	registry := jobs.BuiltinRegistry()
	manager := jobs.NewManager(testStore(t), registry)
	a, _ := manager.Enqueue("echo", "a", 1)
	b, _ := manager.EnqueueWithDeps("echo", "b", 1, []string{a.ID}, "g1")
	c, _ := manager.EnqueueWithDeps("echo", "c", 1, []string{a.ID}, "g1")
	d, _ := manager.EnqueueWithDeps("collect", "", 1, []string{b.ID, c.ID}, "g1")

	// Only A is dispatchable at first.
	claimed, _, err := manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != a.ID {
		t.Fatalf("first claim = %+v", claimed)
	}
	manager.Complete(a.ID, 1, "echo: a", nil)

	// B becomes dispatchable; D must wait for C as well.
	claimed, _, err = manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != b.ID || claimed.DepResults[a.ID] != "echo: a" {
		t.Fatalf("B claim = %+v", claimed)
	}
	manager.Complete(b.ID, 1, "echo: b", nil)
	claimed, _, err = manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != c.ID {
		t.Fatalf("claim after only B succeeded = %+v", claimed)
	}
	manager.Complete(c.ID, 1, "echo: c", nil)

	// D receives only confirmed results from both parents.
	claimed, attempt, err := manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != d.ID {
		t.Fatalf("final claim = %+v", claimed)
	}
	if claimed.DepResults[b.ID] != "echo: b" || claimed.DepResults[c.ID] != "echo: c" {
		t.Fatalf("D dep results = %+v", claimed.DepResults)
	}
	finished, err := manager.Complete(d.ID, attempt.Number, "joined", nil)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != jobs.StatusSucceeded {
		t.Fatalf("D status = %s", finished.Status)
	}
}

func TestDependencyRetryContinuesSameJobNotNewTask(t *testing.T) {
	registry := jobs.NewRegistry(jobs.Definition{
		Type:        "flaky_dep",
		SafeToRetry: true,
		Execute:     func(context.Context, jobs.Input) (string, error) { return "", nil },
	})
	manager := jobs.NewManager(testStore(t), registry)
	dep, _ := manager.Enqueue("flaky_dep", "", 3)
	child, _ := manager.EnqueueWithDeps("flaky_dep", "child", 1, []string{dep.ID}, "")

	manager.Claim()
	if _, err := manager.Complete(dep.ID, 1, "", errors.New("temporary")); err != nil {
		t.Fatal(err)
	}
	// Child stays blocked while the dependency is between attempts.
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("child dispatched while dep pending retry: %+v", claimed)
	}
	time.Sleep(120 * time.Millisecond)
	claimed, attempt, err := manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != dep.ID || attempt.Number != 2 {
		t.Fatalf("retry claim = %+v attempt %d", claimed, attempt.Number)
	}
	manager.Complete(dep.ID, 2, "recovered", nil)

	restored, attempts, _ := manager.Get(dep.ID)
	if restored.AttemptCount != 2 || len(attempts) != 2 {
		t.Fatalf("retry created a new task instead of an attempt: %+v", restored)
	}
	claimed, _, err = manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != child.ID || claimed.DepResults[dep.ID] != "recovered" {
		t.Fatalf("child claim = %+v", claimed)
	}
}

func TestAmbiguousUnsafeDependencyBlocksUntilVerified(t *testing.T) {
	registry := jobs.NewRegistry(
		jobs.Definition{Type: "charge", SafeToRetry: false, Execute: func(context.Context, jobs.Input) (string, error) { return "", nil }},
		jobs.Definition{Type: "after", SafeToRetry: true, Execute: func(context.Context, jobs.Input) (string, error) { return "", nil }},
	)
	manager := jobs.NewManager(testStore(t), registry)
	dep, _ := manager.Enqueue("charge", "order-1", 3)
	child, _ := manager.EnqueueWithDeps("after", "", 1, []string{dep.ID}, "")

	manager.Claim()
	if _, err := manager.Complete(dep.ID, 1, "", errors.New("downstream state unknown")); err != nil {
		t.Fatal(err)
	}
	failed, _, _ := manager.Get(dep.ID)
	if failed.Status != jobs.StatusFailed || !failed.NeedsVerification {
		t.Fatalf("unsafe failure = %+v", failed)
	}
	// Neither dispatched nor skipped: the ambiguous result must be verified.
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("child dispatched with unverified dependency: %+v", claimed)
	}
	if current, _, _ := manager.Get(child.ID); current.Status != jobs.StatusPending {
		t.Fatalf("child status = %s", current.Status)
	}

	// Operator confirms the external effect actually happened.
	if _, err := manager.Verify(dep.ID, true, "charged: order-1"); err != nil {
		t.Fatal(err)
	}
	claimed, _, err := manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != child.ID || claimed.DepResults[dep.ID] != "charged: order-1" {
		t.Fatalf("child claim after verify = %+v", claimed)
	}
	if _, err := manager.Verify(dep.ID, true, "again"); !errors.Is(err, jobs.ErrNoVerification) {
		t.Fatalf("double verify = %v", err)
	}
}

func TestVerifiedFailedDependencySkipsSuccessors(t *testing.T) {
	registry := jobs.NewRegistry(
		jobs.Definition{Type: "charge", SafeToRetry: false, Execute: func(context.Context, jobs.Input) (string, error) { return "", nil }},
		jobs.Definition{Type: "after", SafeToRetry: true, Execute: func(context.Context, jobs.Input) (string, error) { return "", nil }},
	)
	manager := jobs.NewManager(testStore(t), registry)
	dep, _ := manager.Enqueue("charge", "order-2", 3)
	child, _ := manager.EnqueueWithDeps("after", "", 1, []string{dep.ID}, "")

	manager.Claim()
	manager.Complete(dep.ID, 1, "", errors.New("downstream state unknown"))
	if _, err := manager.Verify(dep.ID, false, "charge never happened"); err != nil {
		t.Fatal(err)
	}
	if claimed, _, _ := manager.Claim(); claimed != nil {
		t.Fatalf("child dispatched after confirmed failure: %+v", claimed)
	}
	current, _, _ := manager.Get(child.ID)
	if current.Status != jobs.StatusSkipped {
		t.Fatalf("child status = %s", current.Status)
	}
}

func TestCancelGroupPreservesCompletedAndTerminatesRunning(t *testing.T) {
	registry := jobs.BuiltinRegistry()
	manager := jobs.NewManager(testStore(t), registry)
	done, _ := manager.EnqueueWithDeps("echo", "done", 1, nil, "batch")
	running, _ := manager.EnqueueWithDeps("sleep", "5000", 1, nil, "batch")
	waiting, _ := manager.EnqueueWithDeps("echo", "waiting", 1, []string{running.ID}, "batch")
	other, _ := manager.Enqueue("echo", "other", 1)

	manager.Claim()
	manager.Complete(done.ID, 1, "echo: done", nil)
	manager.Claim() // running

	canceled, err := manager.CancelGroup("batch")
	if err != nil {
		t.Fatal(err)
	}
	if len(canceled) != 2 {
		t.Fatalf("canceled = %v", canceled)
	}
	doneJob, doneAttempts, _ := manager.Get(done.ID)
	if doneJob.Status != jobs.StatusSucceeded || doneJob.Result != "echo: done" {
		t.Fatalf("completed record changed: %+v", doneJob)
	}
	runningJob, runningAttempts, _ := manager.Get(running.ID)
	if runningJob.Status != jobs.StatusCanceled {
		t.Fatalf("running job status = %s", runningJob.Status)
	}
	if runningAttempts[0].Status != jobs.AttemptCanceled || runningAttempts[0].FinishedAt == nil {
		t.Fatalf("running attempt has no independent terminal state: %+v", runningAttempts[0])
	}
	waitingJob, _, _ := manager.Get(waiting.ID)
	if waitingJob.Status != jobs.StatusCanceled {
		t.Fatalf("waiting job status = %s", waitingJob.Status)
	}
	// Jobs outside the group are untouched and still dispatchable.
	claimed, _, err := manager.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != other.ID {
		t.Fatalf("claim after group cancel = %+v", claimed)
	}
	_ = doneAttempts
}

func TestRestartRestoresSameGraphAndAttempts(t *testing.T) {
	path := t.TempDir() + "/jobs.json"
	registry := jobs.BuiltinRegistry()
	store, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	manager := jobs.NewManager(store, registry)
	unsafe, _ := manager.EnqueueWithDeps("unsafe_payment", "order-9", 3, nil, "restart")
	downstream, _ := manager.EnqueueWithDeps("echo", "after", 1, []string{unsafe.ID}, "restart")
	safe, _ := manager.EnqueueWithDeps("sleep", "5000", 3, nil, "restart")

	manager.Claim() // unsafe running
	manager.Claim() // safe running
	store.Close()   // simulate worker disappearing mid-execution

	reopened, err := jobs.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	manager2 := jobs.NewManager(reopened, registry)
	recovered, err := manager2.RecoverInterrupted()
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0] != safe.ID {
		t.Fatalf("recovered = %v", recovered)
	}
	unsafeJob, unsafeAttempts, _ := manager2.Get(unsafe.ID)
	if unsafeJob.Status != jobs.StatusFailed || !unsafeJob.NeedsVerification {
		t.Fatalf("unsafe after recovery = %+v", unsafeJob)
	}
	if unsafeAttempts[0].Status != jobs.AttemptInterrupted {
		t.Fatalf("unsafe attempt = %+v", unsafeAttempts[0])
	}
	// The graph survived the restart: the dependent is still attached and blocked.
	downstreamJob, _, _ := manager2.Get(downstream.ID)
	if downstreamJob.Status != jobs.StatusPending || len(downstreamJob.DependsOn) != 1 || downstreamJob.DependsOn[0] != unsafe.ID {
		t.Fatalf("graph not restored: %+v", downstreamJob)
	}
	// Verify the ambiguous payment, then the same graph continues.
	if _, err := manager2.Verify(unsafe.ID, true, "charged: order-9"); err != nil {
		t.Fatal(err)
	}
	claimed, _, err := manager2.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != downstream.ID || claimed.DepResults[unsafe.ID] != "charged: order-9" {
		t.Fatalf("downstream claim = %+v", claimed)
	}
	manager2.Complete(downstream.ID, 1, "echo: after", nil)
	claimed, attempt, err := manager2.Claim()
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != safe.ID || attempt.Number != 2 {
		t.Fatalf("safe retry = %+v attempt %d", claimed, attempt.Number)
	}
	safeJob, safeAttempts, _ := manager2.Get(safe.ID)
	if safeJob.AttemptCount != 2 || len(safeAttempts) != 2 {
		t.Fatalf("retry was mistaken for a new task: %+v", safeJob)
	}
}
