package repository

import (
	"context"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
)

// JobEventRepository defines operations for the immutable job state audit trail.
type JobEventRepository interface {
	// Record stores an audit event for a status transition.
	Record(ctx context.Context, event *domain.JobEvent) error

	// ListByJobID returns the chronological audit history of a specific job.
	ListByJobID(ctx context.Context, jobID uuid.UUID) ([]*domain.JobEvent, error)

	// ListByJobIDPage returns an audit page in chronological order with a stable ID tie breaker.
	// Limit may include one lookahead row (up to 1001).
	ListByJobIDPage(ctx context.Context, jobID uuid.UUID, limit, offset int) ([]*domain.JobEvent, error)
}
