package jobplatform

import "context"

type JobState string

const (
	StateQueued    JobState = "queued"
	StateRunning   JobState = "running"
	StateSucceeded JobState = "succeeded"
	StateFailed    JobState = "failed"
	StateCanceled  JobState = "canceled"
	StateAmbiguous JobState = "ambiguous"
)

type AttemptState string

const (
	AttemptRunning     AttemptState = "running"
	AttemptSucceeded   AttemptState = "succeeded"
	AttemptFailed      AttemptState = "failed"
	AttemptCanceled    AttemptState = "canceled"
	AttemptInterrupted AttemptState = "interrupted"
)

type RetrySafety string

const (
	RetrySafe   RetrySafety = "safe"
	RetryUnsafe RetrySafety = "unsafe"
)

type JobDefinition struct {
	Type       string         `json:"type"`
	Args       map[string]any `json:"args,omitempty"`
	MaxRetries int            `json:"max_retries"`
}

type Attempt struct {
	Number      int          `json:"number"`
	State       AttemptState `json:"state"`
	Result      string       `json:"result,omitempty"`
	Error       string       `json:"error,omitempty"`
	RetrySafety RetrySafety  `json:"retry_safety,omitempty"`
}

type Job struct {
	ID              string        `json:"id"`
	Definition      JobDefinition `json:"definition"`
	State           JobState      `json:"state"`
	Attempts        []Attempt     `json:"attempts,omitempty"`
	ActiveAttempt   int           `json:"active_attempt,omitempty"`
	Result          string        `json:"result,omitempty"`
	LastError       string        `json:"last_error,omitempty"`
	CancelRequested bool          `json:"cancel_requested,omitempty"`
}

type Result struct {
	Message string
}

type Task interface {
	Execute(context.Context, Job) (Result, error)
	RetrySafety(Job) RetrySafety
}
