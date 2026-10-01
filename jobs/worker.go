package jobs

import (
	"context"
	"time"
)

type Worker struct {
	manager  *Manager
	registry *Registry
	interval time.Duration
}

func NewWorker(manager *Manager, registry *Registry) *Worker {
	return &Worker{manager: manager, registry: registry, interval: 20 * time.Millisecond}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		job, attempt, err := w.manager.Claim()
		if err != nil {
			return err
		}
		if job == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				continue
			}
		}
		w.runAttempt(ctx, job, attempt)
	}
}

func (w *Worker) runAttempt(parent context.Context, job *Job, attempt *Attempt) {
	def, ok := w.registry.Get(job.Type)
	if !ok {
		_, _ = w.manager.Complete(job.ID, attempt.Number, "", ErrUnknownType)
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stopWatching := make(chan struct{})
	defer close(stopWatching)
	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopWatching:
				return
			case <-ticker.C:
				current, _, err := w.manager.Get(job.ID)
				if err == nil && current.Status == StatusCanceled {
					cancel()
					return
				}
			}
		}
	}()
	result, err := def.Execute(ctx, Input{JobID: job.ID, Attempt: attempt.Number, Payload: job.Payload, Deps: job.DepResults})
	if ctx.Err() != nil {
		return
	}
	_, _ = w.manager.Complete(job.ID, attempt.Number, result, err)
}
