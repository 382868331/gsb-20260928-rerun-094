package jobplatform

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrNotFound     = errors.New("job not found")
	ErrStaleAttempt = errors.New("stale job attempt")
)

type Options struct {
	Workers      int
	PollInterval time.Duration
	BeforeAck    func(Job, int, Result, error)
}

type Engine struct {
	store    *Store
	registry *Registry
	options  Options

	mu      sync.Mutex
	cancels map[string]map[int]context.CancelFunc
}

func NewEngine(store *Store, registry *Registry, options Options) *Engine {
	if options.Workers <= 0 {
		options.Workers = 1
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 10 * time.Millisecond
	}
	return &Engine{
		store:    store,
		registry: registry,
		options:  options,
		cancels:  make(map[string]map[int]context.CancelFunc),
	}
}

func (e *Engine) Submit(def JobDefinition) (*Job, error) {
	task, err := e.registry.Get(def.Type)
	if err != nil {
		return nil, err
	}
	if def.MaxRetries < 0 {
		return nil, fmt.Errorf("max_retries cannot be negative")
	}
	job := &Job{
		Definition: JobDefinition{
			Type:       def.Type,
			Args:       cloneArgs(def.Args),
			MaxRetries: def.MaxRetries,
		},
		State: StateQueued,
	}
	_ = task
	err = e.store.Update(func(data *storeData) error {
		data.Sequence++
		job.ID = fmt.Sprintf("job-%06d", data.Sequence)
		data.Jobs[job.ID] = job
		return nil
	})
	if err != nil {
		return nil, err
	}
	copy := *job
	return &copy, nil
}

func (e *Engine) Get(id string) (*Job, error) {
	var out Job
	err := e.store.View(func(data *storeData) error {
		job, ok := data.Jobs[id]
		if !ok {
			return ErrNotFound
		}
		out = *job
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (e *Engine) List() ([]Job, error) {
	snapshot, _, err := e.store.Snapshot()
	if err != nil {
		return nil, err
	}
	jobs := make([]Job, 0, len(snapshot))
	for _, job := range snapshot {
		i := job
		if len(i.Attempts) > 0 {
			i.Attempts = append([]Attempt(nil), job.Attempts...)
		}
		jobs = append(jobs, i)
	}
	return jobs, nil
}

func (e *Engine) Cancel(id string) error {
	e.mu.Lock()
	for _, cancel := range e.cancels[id] {
		cancel()
	}
	e.mu.Unlock()
	return e.store.Update(func(data *storeData) error {
		job, ok := data.Jobs[id]
		if !ok {
			return ErrNotFound
		}
		switch job.State {
		case StateSucceeded, StateFailed, StateCanceled, StateAmbiguous:
			return nil
		}
		job.CancelRequested = true
		if job.State == StateQueued {
			job.State = StateCanceled
			job.ActiveAttempt = 0
		}
		return nil
	})
}

func (e *Engine) Recover() error {
	return e.store.Update(func(data *storeData) error {
		for _, job := range data.Jobs {
			switch job.State {
			case StateQueued:
				if job.CancelRequested {
					job.State = StateCanceled
					job.ActiveAttempt = 0
				}
			case StateRunning:
				if job.ActiveAttempt <= 0 || job.ActiveAttempt > len(job.Attempts) {
					return fmt.Errorf("job %s has invalid active attempt", job.ID)
				}
				attempt := &job.Attempts[job.ActiveAttempt-1]
				attempt.State = AttemptInterrupted
				attempt.Error = "worker stopped before the result was confirmed"
				if job.CancelRequested {
					attempt.State = AttemptCanceled
					job.State = StateCanceled
					job.ActiveAttempt = 0
					continue
				}
				if attempt.RetrySafety == RetrySafe && attempt.Number <= job.Definition.MaxRetries {
					job.State = StateQueued
					job.ActiveAttempt = 0
				} else {
					job.State = StateAmbiguous
					job.LastError = "the attempt may have completed before its result was recorded"
				}
			}
		}
		return nil
	})
}

func (e *Engine) Run(ctx context.Context) error {
	if err := e.Recover(); err != nil {
		return err
	}
	var wg sync.WaitGroup
	for i := 0; i < e.options.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.worker(ctx)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

func (e *Engine) WaitForTerminal(ctx context.Context, ids ...string) error {
	ticker := time.NewTicker(e.options.PollInterval)
	defer ticker.Stop()
	for {
		done := true
		for _, id := range ids {
			job, err := e.Get(id)
			if err != nil {
				return err
			}
			if !terminal(job.State) {
				done = false
			}
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Engine) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, attempt, ok := e.claim(ctx)
		if !ok {
			wait(ctx, e.options.PollInterval)
			continue
		}
		e.execute(ctx, job, attempt)
	}
}

func (e *Engine) claim(parent context.Context) (Job, int, bool) {
	var claimed Job
	var attemptNumber int
	ok := false
	_ = e.store.Update(func(data *storeData) error {
		for _, job := range data.Jobs {
			if job.State != StateQueued || job.CancelRequested || job.ActiveAttempt != 0 {
				continue
			}
			task, err := e.registry.Get(job.Definition.Type)
			if err != nil {
				continue
			}
			attemptNumber = len(job.Attempts) + 1
			job.Attempts = append(job.Attempts, Attempt{
				Number:      attemptNumber,
				State:       AttemptRunning,
				RetrySafety: task.RetrySafety(*job),
			})
			job.State = StateRunning
			job.ActiveAttempt = attemptNumber
			claimed = *job
			ok = true
			return nil
		}
		return nil
	})
	if !ok {
		return Job{}, 0, false
	}
	var cancelRequested bool
	_ = e.store.View(func(data *storeData) error {
		cancelRequested = data.Jobs[claimed.ID].CancelRequested
		return nil
	})
	if cancelRequested {
		e.recordCanceled(claimed.ID, attemptNumber)
		return Job{}, 0, false
	}
	return claimed, attemptNumber, true
}

func (e *Engine) execute(parent context.Context, job Job, attemptNumber int) {
	task, err := e.registry.Get(job.Definition.Type)
	if err != nil {
		e.ackFailure(job.ID, attemptNumber, err)
		return
	}
	ctx, cancel := context.WithCancel(parent)
	e.mu.Lock()
	if e.cancels[job.ID] == nil {
		e.cancels[job.ID] = make(map[int]context.CancelFunc)
	}
	e.cancels[job.ID][attemptNumber] = cancel
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.cancels[job.ID], attemptNumber)
		if len(e.cancels[job.ID]) == 0 {
			delete(e.cancels, job.ID)
		}
		e.mu.Unlock()
		cancel()
	}()

	result, execErr := task.Execute(ctx, job)
	if e.options.BeforeAck != nil {
		e.options.BeforeAck(job, attemptNumber, result, execErr)
	}
	if execErr != nil {
		e.ackFailure(job.ID, attemptNumber, execErr)
		return
	}
	e.ackSuccess(job.ID, attemptNumber, result)
}

func (e *Engine) ackSuccess(id string, attemptNumber int, result Result) {
	err := e.store.Update(func(data *storeData) error {
		job, ok := data.Jobs[id]
		if !ok || job.ActiveAttempt != attemptNumber {
			return ErrStaleAttempt
		}
		attempt := &job.Attempts[attemptNumber-1]
		if attempt.State != AttemptRunning {
			return ErrStaleAttempt
		}
		if job.State != StateRunning {
			return ErrStaleAttempt
		}
		attempt.State = AttemptSucceeded
		attempt.Result = result.Message
		job.ActiveAttempt = 0
		if job.CancelRequested {
			attempt.State = AttemptCanceled
			job.State = StateCanceled
		} else {
			job.State = StateSucceeded
			job.Result = result.Message
		}
		return nil
	})
	_ = err
}

func (e *Engine) ackFailure(id string, attemptNumber int, execErr error) {
	message := execErr.Error()
	_ = e.store.Update(func(data *storeData) error {
		job, ok := data.Jobs[id]
		if !ok || job.ActiveAttempt != attemptNumber || job.State != StateRunning {
			return ErrStaleAttempt
		}
		attempt := &job.Attempts[attemptNumber-1]
		if attempt.State != AttemptRunning {
			return ErrStaleAttempt
		}
		attempt.Error = message
		job.ActiveAttempt = 0
		if job.CancelRequested {
			attempt.Error = message
			job.LastError = message
			attempt.State = AttemptCanceled
			job.State = StateCanceled
			return nil
		}
		job.LastError = message
		attempt.State = AttemptFailed
		if attempt.RetrySafety == RetrySafe && attemptNumber <= job.Definition.MaxRetries {
			job.State = StateQueued
		} else {
			job.State = StateFailed
		}
		return nil
	})
}

func (e *Engine) recordCanceled(id string, attemptNumber int) {
	_ = e.store.Update(func(data *storeData) error {
		job := data.Jobs[id]
		if job == nil || job.ActiveAttempt != attemptNumber {
			return ErrStaleAttempt
		}
		job.Attempts[attemptNumber-1].State = AttemptCanceled
		job.ActiveAttempt = 0
		job.State = StateCanceled
		return nil
	})
}

func terminal(state JobState) bool {
	switch state {
	case StateSucceeded, StateFailed, StateCanceled, StateAmbiguous:
		return true
	default:
		return false
	}
}

func wait(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func cloneArgs(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
