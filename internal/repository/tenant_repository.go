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
	// Lock serializes submission decisions for one tenant until transaction commit.
	Lock(ctx context.Context, id uuid.UUID) error

	// GetByAPIKeyHash retrieves a tenant matching a SHA-256 API key hash.
	// Returns domain.ErrTenantNotFound if no match exists.
	GetByAPIKeyHash(ctx context.Context, apiKeyHash string) (*domain.Tenant, error)

	// List returns tenants ordered by creation time.
	List(ctx context.Context, limit, offset int) ([]*domain.Tenant, error)

	// Update writes the tenant's API key hash, in-flight limit and disabled
	// state. Returns domain.ErrTenantNotFound if the tenant does not exist.
	Update(ctx context.Context, tenant *domain.Tenant) error
}
