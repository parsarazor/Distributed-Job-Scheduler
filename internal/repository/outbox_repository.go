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
	// using SELECT ... FOR UPDATE SKIP LOCKED to prevent duplicate processor pickups.
	FetchUnpublished(ctx context.Context, limit int, olderThan time.Duration) ([]*domain.OutboxEvent, error)

	// MarkPublished flags the message as successfully published to RabbitMQ.
	MarkPublished(ctx context.Context, id uuid.UUID) error

	// MarkFailed records an error and increments the retry count for an outbox message.
	MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error

	// DeletePublished purges acknowledged outbox rows older than the specified duration.
	DeletePublished(ctx context.Context, olderThan time.Duration) (int64, error)
}
