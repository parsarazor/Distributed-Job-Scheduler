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

type txKey struct{}
type transaction struct {
	source *shared
	state *shared
}

func stateFromCtx(ctx context.Context, s *shared) *shared {
	if tx, ok := ctx.Value(txKey{}).(*transaction); ok && tx.source == s {
		return tx.state
	}
	return s
}

// WithinTransaction serializes transactions and commits a private snapshot only
// on success. Repository calls use the snapshot through the caller's context.
func (t *Transactor) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil { return err }
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	state := &shared{
		jobs: make(map[uuid.UUID]*domain.Job),
		tenants: make(map[uuid.UUID]*domain.Tenant),
		events: make(map[uuid.UUID][]*domain.JobEvent),
		outbox: make(map[uuid.UUID]*domain.OutboxEvent),
		byAPIKey: make(map[string]uuid.UUID),
	}
	for id, j := range t.s.jobs { state.jobs[id] = cloneJob(j) }
	for id, tenant := range t.s.tenants { cp := *tenant; state.tenants[id] = &cp }
	for id, events := range t.s.events {
		for _, event := range events { state.events[id] = append(state.events[id], cloneEvent(event)) }
	}
	for id, event := range t.s.outbox { state.outbox[id] = cloneOutbox(event) }
	for key, id := range t.s.byAPIKey { state.byAPIKey[key] = id }
	if err := fn(context.WithValue(ctx, txKey{}, &transaction{source: t.s, state: state})); err != nil { return err }
	if err := ctx.Err(); err != nil { return err }
	t.s.jobs, t.s.tenants, t.s.events, t.s.outbox, t.s.byAPIKey = state.jobs, state.tenants, state.events, state.outbox, state.byAPIKey
	return nil
}

func cloneEvent(e *domain.JobEvent) *domain.JobEvent {
	cp := *e
	if e.WorkerID != nil { v := *e.WorkerID; cp.WorkerID = &v }
	if e.ErrorMessage != nil { v := *e.ErrorMessage; cp.ErrorMessage = &v }
	return &cp
}

func cloneOutbox(e *domain.OutboxEvent) *domain.OutboxEvent {
	cp := *e
	cp.Payload = append([]byte(nil), e.Payload...)
	if e.LastError != nil { v := *e.LastError; cp.LastError = &v }
	if e.PublishedAt != nil { v := *e.PublishedAt; cp.PublishedAt = &v }
	return &cp
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
	if j.LeaseOwner != nil { v := *j.LeaseOwner; cp.LeaseOwner = &v }
	if j.LeaseExpiresAt != nil { v := *j.LeaseExpiresAt; cp.LeaseExpiresAt = &v }
	return &cp
}

// --- Jobs ---

func (r *JobRepo) Create(ctx context.Context, job *domain.Job) error {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[job.ID]; exists {
		return nil // idempotent re-create
	}
	s.jobs[job.ID] = cloneJob(job)
	return nil
}

func (r *JobRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	return cloneJob(j), nil
}

func (r *JobRepo) GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	return r.GetByID(ctx, id)
}

func (r *JobRepo) ListByTenant(ctx context.Context, tenantID uuid.UUID, filter repository.JobListFilter) ([]*domain.Job, int64, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	var matched []*domain.Job
	for _, j := range s.jobs {
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

func (r *JobRepo) Cancel(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
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

func (r *JobRepo) TransitionStatus(ctx context.Context, id uuid.UUID, from, to domain.JobStatus, fencingToken int64) error {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return domain.ErrJobNotFound
	}
	if j.FencingToken != fencingToken {
		return domain.ErrFencingTokenStale
	}
	if j.Status != from {
		return domain.ErrInvalidTransition
	}
	return j.TransitionTo(to)
}

func (r *JobRepo) UpdateWithFencingToken(ctx context.Context, job *domain.Job, expectedToken int64) error {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.jobs[job.ID]
	if !ok {
		return domain.ErrJobNotFound
	}
	if stored.FencingToken != expectedToken || job.FencingToken != expectedToken {
		return domain.ErrFencingTokenStale
	}
	if stored.Status != job.Status { return domain.ErrInvalidTransition }
	stored.Payload = append([]byte(nil), job.Payload...)
	stored.AttemptCount = job.AttemptCount
	stored.MaxAttempts = job.MaxAttempts
	stored.NextRetryAt = cloneJob(job).NextRetryAt
	stored.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *JobRepo) GetReadyJobs(ctx context.Context, limit int) ([]*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	var out []*domain.Job
	for _, j := range s.jobs {
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
			parent, ok := s.jobs[*j.DependsOn]
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

func (r *JobRepo) GetPendingRetryJobs(ctx context.Context, limit int) ([]*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	var out []*domain.Job
	for _, j := range s.jobs {
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

func (r *JobRepo) GetDependentJobs(ctx context.Context, parentJobID uuid.UUID) ([]*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*domain.Job
	for _, j := range s.jobs {
		if j.DependsOn != nil && *j.DependsOn == parentJobID {
			out = append(out, cloneJob(j))
		}
	}
	return out, nil
}

func (r *JobRepo) ClaimJob(ctx context.Context, id uuid.UUID, workerID string, ttl time.Duration) (int64, error) {
	if err := ctx.Err(); err != nil { return 0, err }
	if workerID == "" || ttl <= 0 { return 0, domain.ErrInvalidLease }
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	now := time.Now().UTC()
	if !ok || j.Status != domain.StatusPending || j.ScheduledAt.After(now) || (j.NextRetryAt != nil && j.NextRetryAt.After(now)) {
		return 0, domain.ErrJobNotClaimable
	}
	if j.DependsOn != nil {
		parent := s.jobs[*j.DependsOn]
		if parent == nil || parent.Status != domain.StatusSucceeded { return 0, domain.ErrJobNotClaimable }
	}
	j.Status = domain.StatusRunning
	j.FencingToken++
	j.LeaseOwner = &workerID
	expires := now.Add(ttl)
	j.LeaseExpiresAt = &expires
	j.UpdatedAt = now
	return j.FencingToken, nil
}

func (r *JobRepo) RenewLease(ctx context.Context, id uuid.UUID, workerID string, token int64, ttl time.Duration) error {
	if err := ctx.Err(); err != nil { return err }
	if workerID == "" || ttl <= 0 { return domain.ErrInvalidLease }
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	now := time.Now().UTC()
	if j == nil || j.Status != domain.StatusRunning || j.FencingToken != token || j.LeaseOwner == nil || *j.LeaseOwner != workerID || j.LeaseExpiresAt == nil || !j.LeaseExpiresAt.After(now) {
		return domain.ErrLeaseLost
	}
	expires := now.Add(ttl)
	j.LeaseExpiresAt = &expires
	j.UpdatedAt = now
	return nil
}

func (r *JobRepo) GetExpiredLeasesForUpdate(ctx context.Context, limit int) ([]*domain.Job, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 { limit = 100 }
	now := time.Now().UTC()
	var out []*domain.Job
	for _, j := range s.jobs {
		if j.Status == domain.StatusRunning && j.LeaseExpiresAt != nil && j.LeaseExpiresAt.Before(now) { out = append(out, cloneJob(j)) }
	}
	sort.Slice(out, func(a, b int) bool { return out[a].LeaseExpiresAt.Before(*out[b].LeaseExpiresAt) })
	if len(out) > limit { out = out[:limit] }
	return out, nil
}

func (r *JobRepo) ExpireLease(ctx context.Context, id uuid.UUID, token int64) (*domain.Job, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil || j.Status != domain.StatusRunning || j.FencingToken != token || j.LeaseExpiresAt == nil || !j.LeaseExpiresAt.Before(time.Now().UTC()) { return nil, domain.ErrLeaseLost }
	j.FencingToken++
	j.AttemptCount++
	if err := j.TransitionTo(domain.StatusFailed); err != nil { return nil, err }
	return cloneJob(j), nil
}

// --- Tenants ---

func (r *TenantRepo) Create(ctx context.Context, tenant *domain.Tenant) error {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *tenant
	s.tenants[tenant.ID] = &cp
	s.byAPIKey[tenant.APIKeyHash] = tenant.ID
	return nil
}

func (r *TenantRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tenants[id]
	if !ok {
		return nil, domain.ErrTenantNotFound
	}
	cp := *t
	return &cp, nil
}

func (r *TenantRepo) GetByAPIKeyHash(ctx context.Context, apiKeyHash string) (*domain.Tenant, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byAPIKey[apiKeyHash]
	if !ok {
		return nil, domain.ErrTenantNotFound
	}
	t := s.tenants[id]
	cp := *t
	return &cp, nil
}

// --- Events ---

func (r *EventRepo) Record(ctx context.Context, event *domain.JobEvent) error {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[event.JobID] = append(s.events[event.JobID], cloneEvent(event))
	return nil
}

func (r *EventRepo) ListByJobID(ctx context.Context, jobID uuid.UUID) ([]*domain.JobEvent, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	evs := s.events[jobID]
	out := make([]*domain.JobEvent, len(evs))
	for i, event := range evs { out[i] = cloneEvent(event) }
	sort.Slice(out, func(a, b int) bool {
		return out[a].OccurredAt.Before(out[b].OccurredAt)
	})
	return out, nil
}

// --- Outbox ---

func (r *OutboxRepo) Store(ctx context.Context, event *domain.OutboxEvent) error {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.outbox[event.ID]; !exists { s.outbox[event.ID] = cloneOutbox(event) }
	return nil
}

func (r *OutboxRepo) FetchUnpublished(ctx context.Context, limit int, olderThan time.Duration) ([]*domain.OutboxEvent, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.RLock()
	defer s.mu.RUnlock()
	cutoff := time.Now().UTC().Add(-olderThan)
	var out []*domain.OutboxEvent
	for _, e := range s.outbox {
		if e.Published || e.ExceededMaxRetries() {
			continue
		}
		if e.CreatedAt.After(cutoff) {
			continue
		}
		out = append(out, cloneOutbox(e))
	}
	sort.Slice(out, func(a, b int) bool {
		return out[a].CreatedAt.Before(out[b].CreatedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, id uuid.UUID) error {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.outbox[id]
	if !ok {
		return nil
	}
	e.MarkPublished()
	return nil
}

func (r *OutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.outbox[id]
	if !ok {
		return nil
	}
	e.RecordFailure(lastErr)
	return nil
}

func (r *OutboxRepo) DeletePublished(ctx context.Context, olderThan time.Duration) (int64, error) {
	s := stateFromCtx(ctx, r.s)
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().UTC().Add(-olderThan)
	var n int64
	for id, e := range s.outbox {
		if !e.Published || e.PublishedAt == nil || e.PublishedAt.After(cutoff) {
			continue
		}
		delete(s.outbox, id)
		n++
	}
	return n, nil
}
