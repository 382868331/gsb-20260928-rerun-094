package jobs

import "time"

// ChangeKind says what a proposed change does to an artifact.
type ChangeKind string

const (
	// ChangeReplace substitutes a succeeded job's artifact with a new result.
	ChangeReplace ChangeKind = "REPLACE"
	// ChangeRevoke withdraws the artifact without providing a replacement.
	ChangeRevoke ChangeKind = "REVOKE"
)

// ChangeStatus is the lifecycle state of one change inside a revision.
type ChangeStatus string

const (
	ChangeProposed ChangeStatus = "PROPOSED"
	ChangeAccepted ChangeStatus = "ACCEPTED"
	ChangeRejected ChangeStatus = "REJECTED"
	ChangeCanceled ChangeStatus = "CANCELLED"
	// ChangeNoChange means the proposed result was identical to the active one;
	// the change was accepted but produced no new generation and no reruns.
	ChangeNoChange ChangeStatus = "NO_CHANGE"
)

// RevisionStatus is the lifecycle state of a revision candidate.
type RevisionStatus string

const (
	RevisionProposed RevisionStatus = "PROPOSED"
	RevisionAccepted RevisionStatus = "ACCEPTED"
	RevisionCanceled RevisionStatus = "CANCELLED"
)

// PendingStatus tracks the human verification of a possible external effect.
type PendingStatus string

const (
	PendingOpen      PendingStatus = "OPEN"
	PendingConfirmed PendingStatus = "CONFIRMED"
	PendingDismissed PendingStatus = "DISMISSED"
)

// ArtifactChange is one proposed replacement or revocation.
type ArtifactChange struct {
	JobID     string       `json:"job_id"`
	Kind      ChangeKind   `json:"kind"`
	NewResult string       `json:"new_result,omitempty"`
	Status    ChangeStatus `json:"status"`
	// FromGeneration is the active generation when the change was proposed.
	FromGeneration int `json:"from_generation"`
	// ToGeneration is filled in once a replacement is confirmed.
	ToGeneration int    `json:"to_generation,omitempty"`
	Reason       string `json:"reason,omitempty"`
	// Unchanged is set when acceptance found the proposed result equal to the
	// active artifact.
	Unchanged bool `json:"unchanged,omitempty"`
}

// PendingItem is an external effect that must be verified before affected
// branches may continue.
type PendingItem struct {
	ID         string        `json:"id"`
	RevisionID string        `json:"revision_id"`
	ChangeKey  int           `json:"change_key"`
	JobID      string        `json:"job_id"`
	Summary    string        `json:"summary"`
	Status     PendingStatus `json:"status"`
	Resolution string        `json:"resolution,omitempty"`
	ResolvedAt *time.Time    `json:"resolved_at,omitempty"`
	CreatedAt  time.Time     `json:"created_at"`
}

// RevisionCandidate is a set of changes proposed together. Each change stays
// independently confirmable, so partial acceptance is supported without
// rewriting the rest.
type RevisionCandidate struct {
	ID        string            `json:"id"`
	Status    RevisionStatus    `json:"status"`
	Changes   []*ArtifactChange `json:"changes"`
	Pending   []*PendingItem    `json:"pending,omitempty"`
	Note      string            `json:"note,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// ImpactedJob describes one downstream job seen by a revision preview.
type ImpactedJob struct {
	JobID             string `json:"job_id"`
	CurrentGeneration int    `json:"current_generation"`
	// ConsumedGenerations maps changed upstream job ID -> generation that the
	// downstream's current artifact was built from.
	ConsumedGenerations map[string]int `json:"consumed_generations"`
}

// ChangeImpact is the computed impact of a single proposed change.
type ChangeImpact struct {
	ChangeIndex int        `json:"change_index"`
	JobID       string     `json:"job_id"`
	Kind        ChangeKind `json:"kind"`
	FromResult  string     `json:"from_result,omitempty"`
	ToResult    string     `json:"to_result,omitempty"`
	// WouldRerun lists downstream jobs whose artifacts become stale and must be
	// redone (replace) or canceled (revoke).
	WouldRerun []ImpactedJob `json:"would_rerun,omitempty"`
	// Reusable lists downstream jobs that stay valid: they did not consume the
	// changed generation, so no work is repeated for them.
	Reusable []ImpactedJob `json:"reusable,omitempty"`
	// ExternalPending lists possible external effects requiring verification.
	ExternalPending []string `json:"external_pending,omitempty"`
	Unchanged       bool     `json:"unchanged,omitempty"`
}

// RevisionPreview is shown before confirmation; nothing is mutated to build it.
type RevisionPreview struct {
	RevisionID string         `json:"revision_id"`
	Status     RevisionStatus `json:"status"`
	Impacts    []ChangeImpact `json:"impacts"`
}

// ConfirmationReport summarizes what a confirm call actually did.
type ConfirmationReport struct {
	RevisionID       string             `json:"revision_id"`
	Accepted         []int              `json:"accepted_indexes"`
	Unchanged        []int              `json:"unchanged_indexes,omitempty"`
	NewGenerations   map[string]int     `json:"new_generations,omitempty"`
	InvalidatedJobs  []string           `json:"invalidated_jobs,omitempty"`
	ReusableJobs     []string           `json:"reusable_jobs,omitempty"`
	BlockedByPending []string           `json:"blocked_by_pending,omitempty"`
	PendingOpen      []string           `json:"pending_open,omitempty"`
	Revision         *RevisionCandidate `json:"revision"`
}

// ArtifactDiff compares a revision's proposed or confirmed change with the
// artifact generations retained in the store.
type ArtifactDiff struct {
	ChangeIndex       int           `json:"change_index"`
	JobID             string        `json:"job_id"`
	Kind              ChangeKind    `json:"kind"`
	Status            ChangeStatus  `json:"status"`
	FromGeneration    int           `json:"from_generation"`
	ToGeneration      int           `json:"to_generation,omitempty"`
	BeforeResult      string        `json:"before_result,omitempty"`
	AfterResult       string        `json:"after_result,omitempty"`
	ProposedResult    string        `json:"proposed_result,omitempty"`
	ActualChange      bool          `json:"actual_change"`
	CurrentGeneration int           `json:"current_generation,omitempty"`
	CurrentResult     string        `json:"current_result,omitempty"`
	CurrentState      ArtifactState `json:"current_state,omitempty"`
}

type RevisionComparison struct {
	RevisionID string         `json:"revision_id"`
	Status     RevisionStatus `json:"status"`
	Changes    []ArtifactDiff `json:"changes"`
}
