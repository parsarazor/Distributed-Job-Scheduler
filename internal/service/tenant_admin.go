package service

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
)

const (
	tenantKeyPrefix   = "djs_"
	maxTenantNameLen  = 255
	MaxTenantPageSize = 1000
)

func newTenantKey() (string, error) { return randomToken(tenantKeyPrefix) }

func randomToken(prefix string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(b), nil
}

func validInFlight(n int) bool { return n >= 1 && n <= domain.MaxTenantMaxInFlight }

// CreateTenant stores a new tenant and returns its raw API key, which is not
// recoverable afterwards. maxInFlight 0 selects the default.
func (s *Scheduler) CreateTenant(ctx context.Context, name string, maxInFlight int) (*domain.Tenant, string, error) {
	name = strings.TrimSpace(name)
	if maxInFlight == 0 {
		maxInFlight = domain.DefaultTenantMaxInFlight
	}
	if name == "" || len(name) > maxTenantNameLen || !validInFlight(maxInFlight) {
		return nil, "", domain.ErrInvalidInput
	}
	key, err := newTenantKey()
	if err != nil {
		return nil, "", err
	}
	tenant, err := domain.NewTenant(name, key)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", domain.ErrInvalidInput, err)
	}
	tenant.MaxInFlight = maxInFlight
	if err := s.tenants.Create(ctx, tenant); err != nil {
		return nil, "", err
	}
	return tenant, key, nil
}

func (s *Scheduler) ListTenants(ctx context.Context, limit, offset int) ([]*domain.Tenant, error) {
	if limit < 0 || limit > MaxTenantPageSize || offset < 0 {
		return nil, domain.ErrInvalidInput
	}
	if limit == 0 {
		limit = 100
	}
	return s.tenants.List(ctx, limit, offset)
}

// UpdateTenant applies the non-nil fields. Disabling blocks the tenant's API
// key on every replica at once; jobs already accepted keep running.
func (s *Scheduler) UpdateTenant(ctx context.Context, id uuid.UUID, maxInFlight *int, disabled *bool) (*domain.Tenant, error) {
	if maxInFlight != nil && !validInFlight(*maxInFlight) {
		return nil, domain.ErrInvalidInput
	}
	return s.modifyTenant(ctx, id, func(t *domain.Tenant) error {
		if maxInFlight != nil {
			t.MaxInFlight = *maxInFlight
		}
		if disabled != nil {
			switch {
			case *disabled && t.DisabledAt == nil:
				now := time.Now().UTC()
				t.DisabledAt = &now
			case !*disabled:
				t.DisabledAt = nil
			}
		}
		return nil
	})
}

// RotateTenantKey replaces the tenant's API key and returns the new raw key.
func (s *Scheduler) RotateTenantKey(ctx context.Context, id uuid.UUID) (*domain.Tenant, string, error) {
	key, err := newTenantKey()
	if err != nil {
		return nil, "", err
	}
	tenant, err := s.modifyTenant(ctx, id, func(t *domain.Tenant) error {
		t.APIKeyHash = domain.HashAPIKey(key)
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return tenant, key, nil
}

func (s *Scheduler) modifyTenant(ctx context.Context, id uuid.UUID, apply func(*domain.Tenant) error) (*domain.Tenant, error) {
	var out *domain.Tenant
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		if err := s.tenants.Lock(ctx, id); err != nil {
			return err
		}
		tenant, err := s.tenants.GetByID(ctx, id)
		if err != nil {
			return err
		}
		if err := apply(tenant); err != nil {
			return err
		}
		if err := s.tenants.Update(ctx, tenant); err != nil {
			return err
		}
		out = tenant
		return nil
	})
	return out, err
}
