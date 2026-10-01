package jobs

import (
	"context"
	"time"
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCanceled  Status = "CANCELED"
)

type AttemptStatus string

const (
	AttemptRunning     AttemptStatus = "RUNNING"
	AttemptSucceeded   AttemptStatus = "SUCCEEDED"
	AttemptFailed      AttemptStatus = "FAILED"
	AttemptInterrupted AttemptStatus = "INTERRUPTED"
	AttemptCanceled    AttemptStatus = "CANCELED"
)

type Job struct {
	ID             string     `json:"id"`
	Type           string     `json:"type"`
	Payload        string     `json:"payload"`
	Status         Status     `json:"status"`
	SafeRetry      bool       `json:"safe_retry"`
	MaxAttempts    int        `json:"max_attempts"`
	AttemptCount   int        `json:"attempt_count"`
	CurrentAttempt int        `json:"current_attempt,omitempty"`
	DependsOn      []string   `json:"depends_on,omitempty"`
	GroupID        string     `json:"group_id,omitempty"`
	Result         string     `json:"result,omitempty"`
	Error          string     `json:"error,omitempty"`
	NextRunAt      *time.Time `json:"next_run_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

type Attempt struct {
	Number     int               `json:"number"`
	Status     AttemptStatus     `json:"status"`
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt *time.Time        `json:"finished_at,omitempty"`
	Result     string            `json:"result,omitempty"`
	Error      string            `json:"error,omitempty"`
	DepResults map[string]string `json:"dep_results,omitempty"`
}

type Input struct {
	JobID      string
	Attempt    int
	Payload    string
	DepResults map[string]string
}

type Definition struct {
	Type        string
	SafeToRetry bool
	Execute     func(context.Context, Input) (string, error)
}
