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
	ID              string              `json:"id"`
	Type            string              `json:"type"`
	Payload         string              `json:"payload"`
	Status          Status              `json:"status"`
	SafeRetry       bool                `json:"safe_retry"`
	MaxAttempts     int                 `json:"max_attempts"`
	AttemptCount    int                 `json:"attempt_count"`
	CurrentAttempt  int                 `json:"current_attempt,omitempty"`
	DependsOn       []string            `json:"depends_on,omitempty"`
	GroupID         string              `json:"group_id,omitempty"`
	Result          string              `json:"result,omitempty"`
	Error           string              `json:"error,omitempty"`
	Generation      int                 `json:"generation,omitempty"`
	Withdrawn       bool                `json:"withdrawn,omitempty"`
	ExternalPending bool                `json:"external_pending,omitempty"`
	RevisionID      string              `json:"revision_id,omitempty"`
	ResultHistory   []ResultPublication `json:"result_history,omitempty"`
	NextRunAt       *time.Time          `json:"next_run_at,omitempty"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
}

// ResultPublication is one confirmed generation of a job's artifact. The
// full list is append-only: revisions never rewrite history, they publish a
// new generation on top.
type ResultPublication struct {
	Generation  int       `json:"generation"`
	Result      string    `json:"result,omitempty"`
	Withdrawn   bool      `json:"withdrawn,omitempty"`
	Source      string    `json:"source"`
	PublishedAt time.Time `json:"published_at"`
}

type Attempt struct {
	Number     int               `json:"number"`
	Status     AttemptStatus     `json:"status"`
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt *time.Time        `json:"finished_at,omitempty"`
	Result     string            `json:"result,omitempty"`
	Error      string            `json:"error,omitempty"`
	DepResults map[string]string `json:"dep_results,omitempty"`
	// DepGenerations pins which artifact generation of each predecessor
	// this attempt consumed, so every result can be traced to its inputs.
	DepGenerations map[string]int `json:"dep_generations,omitempty"`
	// RevisionID is the confirmed revision that triggered this re-run, if any.
	RevisionID string `json:"revision_id,omitempty"`
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

type RevisionStatus string

const (
	RevisionProposed  RevisionStatus = "PROPOSED"
	RevisionPartial   RevisionStatus = "PARTIALLY_CONFIRMED"
	RevisionConfirmed RevisionStatus = "CONFIRMED"
	RevisionCanceled  RevisionStatus = "CANCELED"
)

type ChangeAction string

const (
	ActionReplace  ChangeAction = "replace"
	ActionWithdraw ChangeAction = "withdraw"
)

type ChangeStatus string

const (
	ChangePending ChangeStatus = "pending"
	ChangeApplied ChangeStatus = "applied"
	ChangeSkipped ChangeStatus = "skipped"
)

// ArtifactChange declares one artifact replacement or withdrawal inside a
// revision candidate. Old/new results and generations are recorded when the
// change is applied so the before/after diff stays explainable.
type ArtifactChange struct {
	JobID         string       `json:"job_id"`
	Action        ChangeAction `json:"action"`
	NewResult     string       `json:"new_result,omitempty"`
	Status        ChangeStatus `json:"status"`
	OldResult     string       `json:"old_result,omitempty"`
	OldGeneration int          `json:"old_generation,omitempty"`
	NewGeneration int          `json:"new_generation,omitempty"`
	Note          string       `json:"note,omitempty"`
	AppliedAt     *time.Time   `json:"applied_at,omitempty"`
}

type ImpactEntry struct {
	JobID  string `json:"job_id"`
	Reason string `json:"reason"`
}

type PendingExternalEntry struct {
	JobID  string `json:"job_id"`
	Reason string `json:"reason"`
}

// ImpactReport describes, at proposal time, which downstream jobs would be
// re-executed, which confirmed results stay reusable, and which branches are
// blocked on unverified external effects.
type ImpactReport struct {
	Affected        []ImpactEntry          `json:"affected,omitempty"`
	Reusable        []string               `json:"reusable,omitempty"`
	PendingExternal []PendingExternalEntry `json:"pending_external,omitempty"`
}

type Revision struct {
	ID        string            `json:"id"`
	Status    RevisionStatus    `json:"status"`
	Changes   []*ArtifactChange `json:"changes"`
	Impact    *ImpactReport     `json:"impact,omitempty"`
	ResetJobs []string          `json:"reset_jobs,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}
