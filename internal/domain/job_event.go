package domain

import (
	"time"

	"github.com/google/uuid"
)

// JobEvent documents an immutable audit record of a job state transition.
type JobEvent struct {
	ID           uuid.UUID `json:"id"`
	JobID        uuid.UUID `json:"job_id"`
	FromStatus   JobStatus `json:"from_status"`
	ToStatus     JobStatus `json:"to_status"`
	OccurredAt   time.Time `json:"occurred_at"`
	WorkerID     *string   `json:"worker_id,omitempty"`
	ErrorMessage *string   `json:"error_message,omitempty"`
}
