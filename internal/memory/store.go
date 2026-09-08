package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

// shared holds all backend state behind one lock so multi-repository
// transactions are atomic in memory.
type shared struct {
	mu       sync.RWMutex
	jobs     map[uuid.UUID]*domain.Job
	tenants  map[uuid.UUID]*domain.Tenant
	events   map[uuid.UUID][]*domain.JobEvent
	outbox   map[uuid.UUID]*domain.OutboxEvent
	byAPIKey map[string]uuid.UUID
}

// Bundle wires every repository plus a Transactor over shared state.
// It is the default backend for local runs (no external infra required).
type Bundle struct {
	Jobs    *JobRepo
	Tenants *TenantRepo
	Events  *EventRepo
	Outbox  *OutboxRepo
	Tx      *Transactor
}

// NewBundle creates an empty in-memory backend.
func NewBundle() *Bundle {
	s := &shared{
		jobs:     make(map[uuid.UUID]*domain.Job),
		tenants:  make(map[uuid.UUID]*domain.Tenant),
		events:   make(map[uuid.UUID][]*domain.JobEvent),
		outbox:   make(map[uuid.UUID]*domain.OutboxEvent),
		byAPIKey: make(map[string]uuid.UUID),
	}
	return &Bundle{
		Jobs:    &JobRepo{s: s},
		Tenants: &TenantRepo{s: s},
		Events:  &EventRepo{s: s},
		Outbox:  &OutboxRepo{s: s},
		Tx:      &Transactor{s: s},
	}
}

// Transactor runs multi-repository functions (see WithinTransaction).
type Transactor struct{ s *shared }

// WithinTransaction executes fn. Each repository operation locks
// individually, so in-memory "transactions" are safe but not strictly
// atomic under concurrency (Postgres provides real atomicity via Tx).
// NOTE: must NOT hold s.mu here: repo methods lock themselves and
// sync.RWMutex is not reentrant (holding it causes deadlock).
func (t *Transactor) WithinTransaction(_ context.Context, fn func(ctx context.Context) error) error {
	return fn(context.Background())
}

// JobRepo implements repository.JobRepository.
type JobRepo struct{ s *shared }

// TenantRepo implements repository.TenantRepository.
type TenantRepo struct{ s *shared }

// EventRepo implements repository.JobEventRepository.
type EventRepo struct{ s *shared }

// OutboxRepo implements repository.OutboxRepository.
type OutboxRepo struct{ s *shared }

var (
	_ repository.JobRepository      = (*JobRepo)(nil)
	_ repository.TenantRepository   = (*TenantRepo)(nil)
	_ repository.JobEventRepository = (*EventRepo)(nil)
	_ repository.OutboxRepository   = (*OutboxRepo)(nil)
	_ repository.Transactor         = (*Transactor)(nil)
)

func cloneJob(j *domain.Job) *domain.Job {
	if j == nil {
		return nil
	}
	cp := *j
	if j.Payload != nil {
		p := make([]byte, len(j.Payload))
		copy(p, j.Payload)
		cp.Payload = p
	}
	if j.DependsOn != nil {
		d := *j.DependsOn
		cp.DependsOn = &d
	}
	if j.NextRetryAt != nil {
		t := *j.NextRetryAt
		cp.NextRetryAt = &t
	}
	return &cp
}

// --- Jobs ---

func (r *JobRepo) Create(_ context.Context, job *domain.Job) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if _, exists := r.s.jobs[job.ID]; exists {
		return nil // idempotent re-create
	}
	r.s.jobs[job.ID] = cloneJob(job)
	return nil
}

func (r *JobRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Job, error) {
	r.s.mu.RLock()
	defer r.s.mu.RUnlock()
	j, ok := r.s.jobs[id]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	return cloneJob(j), nil
}

func (r *JobRepo) GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	return r.GetByID(ctx, id)
}

func (r *JobRepo) ListByTenant(_ context.Context, tenantID uuid.UUID, filter repository.JobListFilter) ([]*domain.Job, int64, error) {
	r.s.mu.RLock()
	defer r.s.mu.RUnlock()
	var matched []*domain.Job
	for _, j := range r.s.jobs {
		if j.TenantID != tenantID {
			continue
		}
		if filter.Status != nil && j.Status != *filter.Status {
			continue
		}
		if filter.Priority != nil && j.Priority != *filter.Priority {
			continue
		}
		matched = append(matched, cloneJob(j))
	}
	sort.Slice(matched, func(a, b int) bool {
		return matched[a].CreatedAt.After(matched[b].CreatedAt)
	})
	total := int64(len(matched))
	start := filter.Offset
	if start > len(matched) {
		start = len(matched)
	}
	end := len(matched)
	if filter.Limit > 0 && start+filter.Limit < end {
		end = start + filter.Limit
	}
	return matched[start:end], total, nil
}

func (r *JobRepo) Cancel(_ context.Context, id uuid.UUID) (*domain.Job, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	j, ok := r.s.jobs[id]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	if !j.CanCancel() {
		return nil, domain.ErrJobCannotBeCancelled
	}
	if err := j.TransitionTo(domain.StatusCancelled); err != nil {
		return nil, err
	}
	return cloneJob(j), nil
}

func (r *JobRepo) TransitionStatus(_ context.Context, id uuid.UUID, from, to domain.JobStatus, fencingToken int64) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	j, ok := r.s.jobs[id]
	if !ok {
		return domain.ErrJobNotFound
	}
	if j.FencingToken > fencingToken {
		return domain.ErrFencingTokenStale
	}
	if j.Status != from {
		return domain.ErrInvalidTransition
	}
	return j.TransitionTo(to)
}

func (r *JobRepo) UpdateWithFencingToken(_ context.Context, job *domain.Job, expectedToken int64) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	stored, ok := r.s.jobs[job.ID]
	if !ok {
		return domain.ErrJobNotFound
	}
	if stored.FencingToken > expectedToken {
		return domain.ErrFencingTokenStale
	}
	cp := cloneJob(job)
	cp.UpdatedAt = time.Now().UTC()
	r.s.jobs[job.ID] = cp
	return nil
}

func (r *JobRepo) GetReadyJobs(_ context.Context, limit int) ([]*domain.Job, error) {
	r.s.mu.RLock()
	defer r.s.mu.RUnlock()
	now := time.Now().UTC()
	var out []*domain.Job
	for _, j := range r.s.jobs {
		if j.Status != domain.StatusPending {
			continue
		}
		if j.ScheduledAt.After(now) {
			continue
		}
		// A retried job remains pending while it waits for its backoff window.
		// ScheduledAt is the initial schedule; NextRetryAt gates subsequent attempts.
		if j.NextRetryAt != nil && j.NextRetryAt.After(now) {
			continue
		}
		if j.DependsOn != nil {
			parent, ok := r.s.jobs[*j.DependsOn]
			if !ok || parent.Status != domain.StatusSucceeded {
				continue
			}
		}
		out = append(out, cloneJob(j))
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Priority != out[b].Priority {
			return out[a].Priority > out[b].Priority
		}
		return out[a].ScheduledAt.Before(out[b].ScheduledAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *JobRepo) GetPendingRetryJobs(_ context.Context, limit int) ([]*domain.Job, error) {
	r.s.mu.RLock()
	defer r.s.mu.RUnlock()
	now := time.Now().UTC()
	var out []*domain.Job
	for _, j := range r.s.jobs {
		if j.Status != domain.StatusFailed {
			continue
		}
		if j.NextRetryAt == nil || j.NextRetryAt.After(now) {
			continue
		}
		if j.AttemptCount >= j.MaxAttempts {
			continue
		}
		out = append(out, cloneJob(j))
	}
	sort.Slice(out, func(a, b int) bool {
		return out[a].NextRetryAt.Before(*out[b].NextRetryAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *JobRepo) GetDependentJobs(_ context.Context, parentJobID uuid.UUID) ([]*domain.Job, error) {
	r.s.mu.RLock()
	defer r.s.mu.RUnlock()
	var out []*domain.Job
	for _, j := range r.s.jobs {
		if j.DependsOn != nil && *j.DependsOn == parentJobID {
			out = append(out, cloneJob(j))
		}
	}
	return out, nil
}

func (r *JobRepo) IncrementFencingToken(_ context.Context, id uuid.UUID) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	j, ok := r.s.jobs[id]
	if !ok {
		return 0, domain.ErrJobNotFound
	}
	j.FencingToken++
	j.UpdatedAt = time.Now().UTC()
	return j.FencingToken, nil
}

// --- Tenants ---

func (r *TenantRepo) Create(_ context.Context, tenant *domain.Tenant) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	r.s.tenants[tenant.ID] = tenant
	r.s.byAPIKey[tenant.APIKeyHash] = tenant.ID
	return nil
}

func (r *TenantRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Tenant, error) {
	r.s.mu.RLock()
	defer r.s.mu.RUnlock()
	t, ok := r.s.tenants[id]
	if !ok {
		return nil, domain.ErrTenantNotFound
	}
	cp := *t
	return &cp, nil
}

func (r *TenantRepo) GetByAPIKeyHash(_ context.Context, apiKeyHash string) (*domain.Tenant, error) {
	r.s.mu.RLock()
	defer r.s.mu.RUnlock()
	id, ok := r.s.byAPIKey[apiKeyHash]
	if !ok {
		return nil, domain.ErrTenantNotFound
	}
	t := r.s.tenants[id]
	cp := *t
	return &cp, nil
}

// --- Events ---

func (r *EventRepo) Record(_ context.Context, event *domain.JobEvent) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	r.s.events[event.JobID] = append(r.s.events[event.JobID], event)
	return nil
}

func (r *EventRepo) ListByJobID(_ context.Context, jobID uuid.UUID) ([]*domain.JobEvent, error) {
	r.s.mu.RLock()
	defer r.s.mu.RUnlock()
	evs := r.s.events[jobID]
	out := make([]*domain.JobEvent, len(evs))
	copy(out, evs)
	sort.Slice(out, func(a, b int) bool {
		return out[a].OccurredAt.Before(out[b].OccurredAt)
	})
	return out, nil
}

// --- Outbox ---

func (r *OutboxRepo) Store(_ context.Context, event *domain.OutboxEvent) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	cp := *event
	r.s.outbox[event.ID] = &cp
	return nil
}

func (r *OutboxRepo) FetchUnpublished(_ context.Context, limit int, olderThan time.Duration) ([]*domain.OutboxEvent, error) {
	r.s.mu.RLock()
	defer r.s.mu.RUnlock()
	cutoff := time.Now().UTC().Add(-olderThan)
	var out []*domain.OutboxEvent
	for _, e := range r.s.outbox {
		if e.Published || e.ExceededMaxRetries() {
			continue
		}
		if e.CreatedAt.After(cutoff) {
			continue
		}
		cp := *e
		out = append(out, &cp)
	}
	sort.Slice(out, func(a, b int) bool {
		return out[a].CreatedAt.Before(out[b].CreatedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *OutboxRepo) MarkPublished(_ context.Context, id uuid.UUID) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	e, ok := r.s.outbox[id]
	if !ok {
		return nil
	}
	e.MarkPublished()
	return nil
}

func (r *OutboxRepo) MarkFailed(_ context.Context, id uuid.UUID, lastErr string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	e, ok := r.s.outbox[id]
	if !ok {
		return nil
	}
	e.RecordFailure(lastErr)
	return nil
}

func (r *OutboxRepo) DeletePublished(_ context.Context, olderThan time.Duration) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	cutoff := time.Now().UTC().Add(-olderThan)
	var n int64
	for id, e := range r.s.outbox {
		if !e.Published || e.PublishedAt == nil || e.PublishedAt.After(cutoff) {
			continue
		}
		delete(r.s.outbox, id)
		n++
	}
	return n, nil
}
