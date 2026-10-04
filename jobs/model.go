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

// ArtifactState describes whether a job's current artifact can still be
// consumed by downstream work. Revisions move artifacts between states;
// historical records are never deleted.
type ArtifactState string

const (
	ArtifactActive     ArtifactState = "ACTIVE"
	ArtifactSuperseded ArtifactState = "SUPERSEDED"
	ArtifactRevoked    ArtifactState = "REVOKED"
)

type Job struct {
	ID             string   `json:"id"`
	Type           string   `json:"type"`
	Payload        string   `json:"payload"`
	Status         Status   `json:"status"`
	SafeRetry      bool     `json:"safe_retry"`
	MaxAttempts    int      `json:"max_attempts"`
	AttemptCount   int      `json:"attempt_count"`
	CurrentAttempt int      `json:"current_attempt,omitempty"`
	DependsOn      []string `json:"depends_on,omitempty"`
	GroupID        string   `json:"group_id,omitempty"`
	Result         string   `json:"result,omitempty"`
	// Generation is the artifact generation currently exposed by the job. It
	// starts at 1 when the job first succeeds and is bumped whenever a
	// confirmed revision replaces the artifact.
	Generation    int           `json:"generation,omitempty"`
	ArtifactState ArtifactState `json:"artifact_state,omitempty"`
	// Artifacts keeps every generation that this job ever produced. A revision
	// never erases history: old generations stay here marked superseded or
	// revoked instead of being rewritten.
	Artifacts []Artifact `json:"artifacts,omitempty"`
	// ExternalPending marks a job whose latest confirmed change may have caused
	// unverified external effects. The job itself and every downstream branch
	// stay blocked until the pending items are resolved by a human.
	ExternalPending    bool     `json:"external_pending,omitempty"`
	ExternalPendingIDs []string `json:"external_pending_ids,omitempty"`
	// PendingRevisionID records which confirmed revision forced this job back
	// to PENDING; it is stamped onto the next attempt.
	PendingRevisionID  string     `json:"pending_revision_id,omitempty"`
	PendingRevisionIDs []string   `json:"pending_revision_ids,omitempty"`
	RevisionNote       string     `json:"revision_note,omitempty"`
	Error              string     `json:"error,omitempty"`
	NextRunAt          *time.Time `json:"next_run_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

// Artifact is one immutable generation of a job's output.
type Artifact struct {
	Generation int           `json:"generation"`
	Result     string        `json:"result"`
	Attempt    int           `json:"attempt"`
	State      ArtifactState `json:"state"`
	// Source is "attempt" for normal execution or a revision ID when the
	// generation was introduced by a confirmed revision.
	Source string `json:"source,omitempty"`
	// Sources records every confirmed revision whose invalidation caused this
	// execution. Source stays set to the first revision for backward
	// compatibility.
	Sources   []string  `json:"sources,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	// BasedOnGenerations records which downstream dependency generations fed
	// this artifact, making mixed candidate/old results detectable.
	BasedOnGenerations map[string]int `json:"based_on_generations,omitempty"`
}

type Attempt struct {
	Number     int               `json:"number"`
	Status     AttemptStatus     `json:"status"`
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt *time.Time        `json:"finished_at,omitempty"`
	Result     string            `json:"result,omitempty"`
	Error      string            `json:"error,omitempty"`
	DepResults map[string]string `json:"dep_results,omitempty"`
	// DepGenerations pins the exact artifact generation of every dependency
	// consumed by this attempt. Redo work references the generations confirmed
	// by the revision that invalidated it, never unconfirmed candidates.
	DepGenerations map[string]int `json:"dep_generations,omitempty"`
	RevisionID     string         `json:"revision_id,omitempty"`
	RevisionIDs    []string       `json:"revision_ids,omitempty"`
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
