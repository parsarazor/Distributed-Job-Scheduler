package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

// WorkerCredentialRepo is the in-memory repository.WorkerCredentialRepository.
type WorkerCredentialRepo struct {
	mu     sync.RWMutex
	byID   map[uuid.UUID]*domain.WorkerCredential
	byHash map[string]uuid.UUID
}

func NewWorkerCredentialRepo() *WorkerCredentialRepo {
	return &WorkerCredentialRepo{byID: map[uuid.UUID]*domain.WorkerCredential{}, byHash: map[string]uuid.UUID{}}
}

func (r *WorkerCredentialRepo) Create(ctx context.Context, cred *domain.WorkerCredential) error {
	if err := lockContext(ctx, &r.mu); err != nil {
		return err
	}
	defer r.mu.Unlock()
	cp := *cred
	r.byID[cred.ID] = &cp
	r.byHash[cred.TokenHash] = cred.ID
	return nil
}

func (r *WorkerCredentialRepo) GetByTokenHash(ctx context.Context, tokenHash string) (*domain.WorkerCredential, error) {
	if err := readLockContext(ctx, &r.mu); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	id, ok := r.byHash[tokenHash]
	if !ok {
		return nil, domain.ErrWorkerCredentialNotFound
	}
	cp := *r.byID[id]
	return &cp, nil
}

func (r *WorkerCredentialRepo) List(ctx context.Context, limit, offset int) ([]*domain.WorkerCredential, error) {
	if err := readLockContext(ctx, &r.mu); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	out := make([]*domain.WorkerCredential, 0, len(r.byID))
	for _, c := range r.byID {
		cp := *c
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID.String() < out[j].ID.String()
	})
	if offset >= len(out) {
		return nil, nil
	}
	out = out[offset:]
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out, nil
}

func (r *WorkerCredentialRepo) Revoke(ctx context.Context, id uuid.UUID, at time.Time) (*domain.WorkerCredential, error) {
	if err := lockContext(ctx, &r.mu); err != nil {
		return nil, err
	}
	defer r.mu.Unlock()
	c, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrWorkerCredentialNotFound
	}
	if c.RevokedAt == nil {
		c.RevokedAt = &at
	}
	cp := *c
	return &cp, nil
}
