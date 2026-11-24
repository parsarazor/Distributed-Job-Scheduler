package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

// OutboxRepository defines data operations for the transactional outbox queue.
type OutboxRepository interface {
	// Store appends an unpublished outbox message, typically within the same transaction as Job creation.
	Store(ctx context.Context, event *domain.OutboxEvent) error

	// FetchUnpublished selects unpublished events older than olderThan threshold
	// for inspection; it takes no locks.
	FetchUnpublished(ctx context.Context, limit int, olderThan time.Duration) ([]*domain.OutboxEvent, error)

	// FetchPublishable claims a single due row by stamping publishing_since.
	// The claim commits with the caller's transaction; the broker publish must
	// happen after that commit. Rows claimed longer than the publishing lease
	// are skipped until ReclaimStalePublishing releases them.
	FetchPublishable(ctx context.Context, olderThan time.Duration, exclude []uuid.UUID) (*domain.OutboxEvent, error)

	// ReclaimStalePublishing releases claims older than the publishing lease.
	ReclaimStalePublishing(ctx context.Context) (int64, error)

	// MarkPublished flags the message as successfully published to RabbitMQ.
	MarkPublished(ctx context.Context, id uuid.UUID) error

	// MarkFailed records an error, increments the retry count and dead-letters
	// the message once the retry ceiling (domain.MaxOutboxRetries) is reached.
	MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error

	// MarkDelivered releases this generation's queued-delivery credit.
	MarkDelivered(ctx context.Context, id uuid.UUID) error

	// RetireUnpublished prevents canceled work from entering the broker.
	RetireUnpublished(ctx context.Context, jobID uuid.UUID) error

	// DeletePublished purges acknowledged outbox rows older than the specified duration.
	DeletePublished(ctx context.Context, olderThan time.Duration) (int64, error)
}
