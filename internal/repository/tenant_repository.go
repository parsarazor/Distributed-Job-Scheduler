package repository

import (
	"context"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

// TenantRepository defines data access operations for tenant entities.
type TenantRepository interface {
	// Create persists a new tenant.
	Create(ctx context.Context, tenant *domain.Tenant) error

	// GetByID retrieves a tenant by UUID. Returns domain.ErrTenantNotFound if not found.
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error)

	// GetByAPIKeyHash retrieves a tenant matching a SHA-256 API key hash.
	// Returns domain.ErrTenantNotFound if no match exists.
	GetByAPIKeyHash(ctx context.Context, apiKeyHash string) (*domain.Tenant, error)
}
