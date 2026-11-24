package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
)

// WorkerCredentialRepository stores hashed per-worker bearer tokens.
type WorkerCredentialRepository interface {
	Create(ctx context.Context, cred *domain.WorkerCredential) error
	// GetByTokenHash returns domain.ErrWorkerCredentialNotFound when absent.
	GetByTokenHash(ctx context.Context, tokenHash string) (*domain.WorkerCredential, error)
	// List returns credentials ordered by creation time.
	List(ctx context.Context, limit, offset int) ([]*domain.WorkerCredential, error)
	// Revoke sets revoked_at once; revoking again keeps the first timestamp.
	Revoke(ctx context.Context, id uuid.UUID, at time.Time) (*domain.WorkerCredential, error)
}
