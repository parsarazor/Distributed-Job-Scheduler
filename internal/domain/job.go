package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// JobStatus represents the state of a job in the state machine.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusSucceeded JobStatus = "succeeded"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
	StatusDead      JobStatus = "dead"
)

// JobPriority specifies the priority queue level.
type JobPriority int

const (
	PriorityLow      JobPriority = 0
	PriorityDefault  JobPriority = 2
	PriorityHigh     JobPriority = 5
	PriorityCritical JobPriority = 10
)

// legalTransitions defines the explicit state machine transition table.
// Any transition not explicitly listed in this table is illegal and rejected.
var legalTransitions = map[JobStatus][]JobStatus{
	StatusPending: {
		StatusRunning,
		StatusCancelled,
	},
	StatusRunning: {
		StatusSucceeded,
		StatusFailed,
	},
	StatusFailed: {
		StatusPending, // retry with backoff
		StatusDead,    // retries exhausted
	},
	StatusSucceeded: {}, // Terminal
	StatusCancelled: {}, // Terminal
	StatusDead:      {}, // Terminal
}

// ValidateTransition verifies if transitioning from `from` to `to` is permitted.
func ValidateTransition(from, to JobStatus) error {
	allowed, exists := legalTransitions[from]
	if !exists {
		return fmt.Errorf("%w: unknown source status %q", ErrInvalidTransition, from)
	}

	for _, valid := range allowed {
		if valid == to {
			return nil
		}
	}

	return fmt.Errorf("%w: cannot transition from %q to %q", ErrInvalidTransition, from, to)
}

// Job represents a unit of work scheduled across the distributed cluster.
type Job struct {
	ID             uuid.UUID       `json:"id"`
	TenantID       uuid.UUID       `json:"tenant_id"`
	Name           string          `json:"name"`
	Status         JobStatus       `json:"status"`
	Payload        json.RawMessage `json:"payload"`
	PayloadExpired bool            `json:"payload_expired,omitempty"`
	Priority       JobPriority     `json:"priority"`
	AttemptCount   int             `json:"attempt_count"`
	MaxAttempts    int             `json:"max_attempts"`
	DependsOn      *uuid.UUID      `json:"depends_on,omitempty"`
	FencingToken   int64           `json:"fencing_token"`
	LeaseOwner     *string         `json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	DispatchedAt   *time.Time      `json:"dispatched_at,omitempty"`
	SubmissionKey  string          `json:"submission_key,omitempty"`
	SubmissionHash string          `json:"-"`
	Timeout        time.Duration   `json:"timeout"`
	TraceParent    string          `json:"traceparent,omitempty"`
	TraceState     string          `json:"tracestate,omitempty"`
	ScheduledAt    time.Time       `json:"scheduled_at"`
	NextRetryAt    *time.Time      `json:"next_retry_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// CanCancel determines if the job is eligible for cancellation.
// Only jobs in PENDING status can be cancelled.
func (j *Job) CanCancel() bool {
	return j.Status == StatusPending
}

// IsTerminal returns true if the job is in a non-executable end state.
func (j *Job) IsTerminal() bool {
	return j.Status == StatusSucceeded || j.Status == StatusCancelled || j.Status == StatusDead
}

// TransitionTo applies a valid state machine transition or returns a hard error.
// It also refreshes UpdatedAt so in-memory copies never carry a stale
// timestamp (the Postgres trigger only fixes the stored row).
func (j *Job) TransitionTo(next JobStatus) error {
	if err := ValidateTransition(j.Status, next); err != nil {
		return err
	}
	if next != StatusRunning {
		j.LeaseOwner = nil
		j.LeaseExpiresAt = nil
	}
	if next == StatusPending {
		j.DispatchedAt = nil
	}
	j.Status = next
	j.UpdatedAt = time.Now().UTC()
	return nil
}

// IdempotencyKey identifies this execution attempt for downstream side effects.
// Retries use a new key; use the job ID alone when the effect must be unique
// across all attempts (for example, sending one notification for a job).
func (j *Job) IdempotencyKey() string {
	return fmt.Sprintf("%s:%d", j.ID, j.AttemptCount+1)
}

// IsValidPriority reports whether p is one of the defined priority weights.
func IsValidPriority(p JobPriority) bool {
	switch p {
	case PriorityLow, PriorityDefault, PriorityHigh, PriorityCritical:
		return true
	default:
		return false
	}
}

// NewJob builds a Job with sane defaults and validates its priority.
// Payload may be nil (stored as {}). DependsOn may be nil.
func NewJob(tenantID uuid.UUID, name string, payload json.RawMessage, priority JobPriority, maxAttempts int, dependsOn *uuid.UUID, scheduledAt time.Time) (*Job, error) {
	if tenantID == uuid.Nil {
		return nil, fmt.Errorf("tenant id must not be nil")
	}
	if name == "" {
		return nil, fmt.Errorf("job name must not be empty")
	}
	if !IsValidPriority(priority) {
		return nil, fmt.Errorf("invalid job priority %d", int(priority))
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	if scheduledAt.IsZero() {
		scheduledAt = time.Now().UTC()
	}
	now := time.Now().UTC()
	return &Job{
		ID:           uuid.New(),
		TenantID:     tenantID,
		Name:         name,
		Status:       StatusPending,
		Payload:      payload,
		Priority:     priority,
		AttemptCount: 0,
		MaxAttempts:  maxAttempts,
		DependsOn:    dependsOn,
		FencingToken: 0,
		Timeout:      5 * time.Minute,
		ScheduledAt:  scheduledAt.UTC(),
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

// DetectDependencyCycle walks the single-parent dependency chain upward from
// childID and returns ErrDependencyCycle if adding parentID as the parent of
// childID would form a cycle (including self-dependency).
//
// lookup must return the parent (DependsOn) of the given job id, or nil if the
// job has no parent. It returns an error if the job cannot be found.
func DetectDependencyCycle(childID, parentID uuid.UUID, lookup func(uuid.UUID) (*uuid.UUID, error)) error {
	if childID == parentID {
		return fmt.Errorf("%w: job cannot depend on itself", ErrDependencyCycle)
	}
	seen := map[uuid.UUID]struct{}{childID: {}}
	current := parentID
	for current != uuid.Nil {
		if _, ok := seen[current]; ok {
			return fmt.Errorf("%w: dependency cycle detected", ErrDependencyCycle)
		}
		if len(seen) > 128 {
			return fmt.Errorf("%w: dependency chain exceeds 128 jobs", ErrInvalidInput)
		}
		seen[current] = struct{}{}
		next, err := lookup(current)
		if err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		current = *next
	}
	return nil
}
