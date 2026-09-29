package jobplatform

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func waitForState(t *testing.T, engine *Engine, id string, want JobState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := engine.Get(id)
		if err == nil && job.State == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	job, err := engine.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("state = %s, want %s", job.State, want)
}

type flakyTask struct {
	calls atomic.Int32
}

func (t *flakyTask) Execute(context.Context, Job) (Result, error) {
	if t.calls.Add(1) == 1 {
		return Result{}, errors.New("temporary failure")
	}
	return Result{Message: "second attempt"}, nil
}

func (*flakyTask) RetrySafety(Job) RetrySafety { return RetrySafe }

func TestFailureRetriesSameJob(t *testing.T) {
	store := openTestStore(t)
	registry := NewRegistry()
	task := &flakyTask{}
	if err := registry.Register("flaky", task); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(store, registry, Options{PollInterval: time.Millisecond})
	job, err := engine.Submit(JobDefinition{Type: "flaky", MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	waitForState(t, engine, job.ID, StateSucceeded)
	stop()
	<-done

	finished, err := engine.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(finished.Attempts) != 2 || finished.Attempts[0].State != AttemptFailed || finished.Attempts[1].State != AttemptSucceeded {
		t.Fatalf("attempts = %#v", finished.Attempts)
	}
	if finished.Result != "second attempt" || finished.LastError != "temporary failure" {
		t.Fatalf("finished job = %#v", finished)
	}
	if task.calls.Load() != 2 {
		t.Fatalf("executions = %d, want 2", task.calls.Load())
	}
}

func TestStaleAttemptSuccessCannotOverwriteCurrentAttempt(t *testing.T) {
	store := openTestStore(t)
	engine := NewEngine(store, NewRegistry(), Options{})
	job, err := engine.Submit(JobDefinition{Type: "noop", MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}

	_, firstAttempt, ok := engine.claim(context.Background())
	if !ok || firstAttempt != 1 {
		t.Fatalf("claim = %d, %v", firstAttempt, ok)
	}
	err = store.Update(func(data *storeData) error {
		current := data.Jobs[job.ID]
		current.Attempts = append(current.Attempts, Attempt{Number: 2, State: AttemptRunning})
		current.ActiveAttempt = 2
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	engine.ackSuccess(job.ID, 1, Result{Message: "old success"})
	current, err := engine.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != StateRunning || current.ActiveAttempt != 2 || current.Result != "" {
		t.Fatalf("stale success changed job: %#v", current)
	}
	if current.Attempts[0].State != AttemptRunning {
		t.Fatalf("old attempt state = %s", current.Attempts[0].State)
	}
}

func TestRecoverQueuedAndCompletedJobs(t *testing.T) {
	store := openTestStore(t)
	storePath := store.path
	seed := map[string]*Job{
		"job-pending": {
			ID: "job-pending", Definition: JobDefinition{Type: "noop"}, State: StateQueued,
		},
		"job-done": {
			ID: "job-done", Definition: JobDefinition{Type: "noop"}, State: StateSucceeded,
			Result: "kept", Attempts: []Attempt{{Number: 1, State: AttemptSucceeded, Result: "kept"}},
		},
	}
	if err := store.Update(func(data *storeData) error {
		data.Jobs = seed
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(reopened, NewRegistry(), Options{PollInterval: time.Millisecond})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	waitForState(t, engine, "job-pending", StateSucceeded)
	stop()
	<-done

	completed, err := engine.Get("job-done")
	if err != nil {
		t.Fatal(err)
	}
	if completed.Result != "kept" || len(completed.Attempts) != 1 {
		t.Fatalf("completed result not retained: %#v", completed)
	}
}

func TestRecoverInterruptedAttemptByRetrySafety(t *testing.T) {
	tests := []struct {
		name   string
		safety RetrySafety
		want   JobState
	}{
		{name: "safe retried", safety: RetrySafe, want: StateQueued},
		{name: "unsafe ambiguous", safety: RetryUnsafe, want: StateAmbiguous},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)
			if err := store.Update(func(data *storeData) error {
				data.Jobs["job-interrupted"] = &Job{
					ID:            "job-interrupted",
					Definition:    JobDefinition{Type: "noop", MaxRetries: 1},
					State:         StateRunning,
					ActiveAttempt: 1,
					Attempts:      []Attempt{{Number: 1, State: AttemptRunning, RetrySafety: tt.safety}},
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			engine := NewEngine(store, NewRegistry(), Options{})
			if err := engine.Recover(); err != nil {
				t.Fatal(err)
			}
			job, err := engine.Get("job-interrupted")
			if err != nil {
				t.Fatal(err)
			}
			if job.State != tt.want {
				t.Fatalf("state = %s, want %s", job.State, tt.want)
			}
			if job.Attempts[0].State != AttemptInterrupted {
				t.Fatalf("attempt = %s", job.Attempts[0].State)
			}
		})
	}
}

type unsafeFailTask struct{}

func (unsafeFailTask) Execute(context.Context, Job) (Result, error) {
	return Result{}, errors.New("unsafe failure")
}

func (unsafeFailTask) RetrySafety(Job) RetrySafety { return RetryUnsafe }

func TestUnsafeFailureDoesNotRetry(t *testing.T) {
	store := openTestStore(t)
	registry := NewRegistry()
	if err := registry.Register("unsafe-fail", unsafeFailTask{}); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(store, registry, Options{PollInterval: time.Millisecond})
	job, err := engine.Submit(JobDefinition{Type: "unsafe-fail", MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	waitForState(t, engine, job.ID, StateFailed)
	stop()
	<-done

	finished, err := engine.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(finished.Attempts) != 1 || finished.Attempts[0].State != AttemptFailed {
		t.Fatalf("unsafe task attempts = %#v", finished.Attempts)
	}
}

type waitCancelTask struct {
	started chan struct{}
}

func (t *waitCancelTask) Execute(ctx context.Context, _ Job) (Result, error) {
	close(t.started)
	<-ctx.Done()
	return Result{}, ctx.Err()
}

func (*waitCancelTask) RetrySafety(Job) RetrySafety { return RetryUnsafe }

func TestCancelQueuedAndRunning(t *testing.T) {
	store := openTestStore(t)
	registry := NewRegistry()
	blocking := &waitCancelTask{started: make(chan struct{})}
	if err := registry.Register("wait-cancel", blocking); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(store, registry, Options{PollInterval: time.Millisecond})
	running, err := engine.Submit(JobDefinition{Type: "wait-cancel"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	<-blocking.started
	queued, err := engine.Submit(JobDefinition{Type: "noop"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := engine.Cancel(queued.ID); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := engine.Cancel(running.ID); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()
	waitForState(t, engine, queued.ID, StateCanceled)
	waitForState(t, engine, running.ID, StateCanceled)
	stop()
	<-done

	finished, err := engine.Get(running.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(finished.Attempts) != 1 || finished.Attempts[0].State != AttemptCanceled {
		t.Fatalf("running cancel attempts = %#v", finished.Attempts)
	}
}
