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
