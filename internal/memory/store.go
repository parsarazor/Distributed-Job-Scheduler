package memory

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

func uuidLess(a, b uuid.UUID) bool { return bytes.Compare(a[:], b[:]) < 0 }

// shared holds all backend state behind one lock so multi-repository
// transactions are atomic in memory.
type shared struct {
	mu       sync.RWMutex
	jobs     map[uuid.UUID]*domain.Job
	tenants  map[uuid.UUID]*domain.Tenant
	events   map[uuid.UUID][]*domain.JobEvent
	outbox   map[uuid.UUID]*domain.OutboxEvent
	byAPIKey map[string]uuid.UUID
	turns    map[uuid.UUID]time.Time
}

// Bundle wires every repository plus a Transactor over shared state.
// It is the explicit STORAGE=memory backend for local development and tests.
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
		turns:    make(map[uuid.UUID]time.Time),
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
	state  *shared
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := lockContext(ctx, &t.s.mu); err != nil {
		return err
	}
	defer t.s.mu.Unlock()
	state := &shared{
		jobs:     make(map[uuid.UUID]*domain.Job),
		tenants:  make(map[uuid.UUID]*domain.Tenant),
		events:   make(map[uuid.UUID][]*domain.JobEvent),
		outbox:   make(map[uuid.UUID]*domain.OutboxEvent),
		byAPIKey: make(map[string]uuid.UUID),
		turns:    make(map[uuid.UUID]time.Time),
	}
	for id, j := range t.s.jobs {
		// Stored job, event and outbox payloads are immutable slices. Mutations
		// replace fields, so the snapshot only needs independent structs/maps.
		cp := *j
		state.jobs[id] = &cp
	}
	for id, tenant := range t.s.tenants {
		cp := *tenant
		state.tenants[id] = &cp
	}
	for id, events := range t.s.events {
		state.events[id] = append([]*domain.JobEvent(nil), events...)
	}
	for id, event := range t.s.outbox {
		cp := *event
		state.outbox[id] = &cp
	}
	for key, id := range t.s.byAPIKey {
		state.byAPIKey[key] = id
	}
	for id, turn := range t.s.turns {
		state.turns[id] = turn
	}
	if err := fn(context.WithValue(ctx, txKey{}, &transaction{source: t.s, state: state})); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.s.jobs, t.s.tenants, t.s.events, t.s.outbox, t.s.byAPIKey = state.jobs, state.tenants, state.events, state.outbox, state.byAPIKey
	t.s.turns = state.turns
	return nil
}

// lockContext lets lease operations and transaction admission honor deadlines
// even while another local transaction holds the shared state lock.
func lockContext(ctx context.Context, mu *sync.RWMutex) error {
	return acquireContext(ctx, mu.TryLock, mu.Unlock)
}

func readLockContext(ctx context.Context, mu *sync.RWMutex) error {
	return acquireContext(ctx, mu.TryRLock, mu.RUnlock)
}

func acquireContext(ctx context.Context, tryLock func() bool, unlock func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tryLock() {
		if err := ctx.Err(); err != nil {
			unlock()
			return err
		}
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if tryLock() {
				if err := ctx.Err(); err != nil {
					unlock()
					return err
				}
				return nil
			}
		}
	}
}

func cloneEvent(e *domain.JobEvent) *domain.JobEvent {
	cp := *e
	if e.WorkerID != nil {
		v := *e.WorkerID
		cp.WorkerID = &v
	}
	if e.ErrorMessage != nil {
		v := *e.ErrorMessage
		cp.ErrorMessage = &v
	}
	return &cp
}

func cloneOutbox(e *domain.OutboxEvent) *domain.OutboxEvent {
	cp := *e
	cp.Payload = append([]byte(nil), e.Payload...)
	if e.LastError != nil {
		v := *e.LastError
		cp.LastError = &v
	}
	if e.PublishedAt != nil {
		v := *e.PublishedAt
		cp.PublishedAt = &v
	}
	if e.NextPublishAt != nil {
		v := *e.NextPublishAt
		cp.NextPublishAt = &v
	}
	if e.DeliveredAt != nil {
		v := *e.DeliveredAt
		cp.DeliveredAt = &v
	}
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
	return projectJob(j, false)
}

func projectJob(j *domain.Job, metadataOnly bool) *domain.Job {
	if j == nil {
		return nil
	}
	cp := *j
	if metadataOnly {
		cp.Payload = nil
	} else if j.Payload != nil {
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
	if j.LeaseOwner != nil {
		v := *j.LeaseOwner
		cp.LeaseOwner = &v
	}
	if j.LeaseExpiresAt != nil {
		v := *j.LeaseExpiresAt
		cp.LeaseExpiresAt = &v
	}
	if j.DispatchedAt != nil {
		v := *j.DispatchedAt
		cp.DispatchedAt = &v
	}
	return &cp
}

// --- Jobs ---

func (r *JobRepo) Create(ctx context.Context, job *domain.Job) error {
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if _, exists := s.jobs[job.ID]; exists {
		return nil // idempotent re-create
	}
	if job.DependsOn != nil {
		parent := s.jobs[*job.DependsOn]
		if parent == nil || parent.TenantID != job.TenantID {
			return domain.ErrDependencyNotMet
		}
	}
	if job.SubmissionKey != "" {
		for _, existing := range s.jobs {
			if existing.TenantID == job.TenantID && existing.SubmissionKey == job.SubmissionKey {
				return domain.ErrIdempotencyConflict
			}
		}
	}
	s.jobs[job.ID] = cloneJob(job)
	return nil
}

func (r *JobRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
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
	if filter.Offset < 0 || filter.Limit < 0 {
		return nil, 0, domain.ErrInvalidInput
	}
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, 0, err
	}
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
		matched = append(matched, j)
	}
	sort.Slice(matched, func(a, b int) bool {
		if !matched[a].CreatedAt.Equal(matched[b].CreatedAt) {
			return matched[a].CreatedAt.After(matched[b].CreatedAt)
		}
		return uuidLess(matched[a].ID, matched[b].ID)
	})
	total := int64(len(matched))
	start := filter.Offset
	if start > len(matched) {
		start = len(matched)
	}
	end := len(matched)
	if filter.Limit > 0 && filter.Limit < end-start {
		end = start + filter.Limit
	}
	for i := start; i < end; i++ {
		matched[i] = projectJob(matched[i], filter.MetadataOnly)
	}
	return matched[start:end], total, nil
}

func (r *JobRepo) Cancel(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
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
	j.NextRetryAt, j.DispatchedAt = nil, nil
	return cloneJob(j), nil
}

func (r *JobRepo) TransitionStatus(ctx context.Context, id uuid.UUID, from, to domain.JobStatus, fencingToken int64) error {
	if to == domain.StatusRunning {
		return domain.ErrInvalidTransition // Running requires an atomic claim and lease.
	}
	if err := domain.ValidateTransition(from, to); err != nil {
		return err
	}
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
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
	if from == domain.StatusRunning {
		return domain.ErrLeaseLost
	}
	if err := j.TransitionTo(to); err != nil {
		return err
	}
	if to == domain.StatusDead || to == domain.StatusCancelled {
		j.NextRetryAt = nil
	}
	return nil
}

func (r *JobRepo) SetNextRetryAt(ctx context.Context, id uuid.UUID, token int64, next time.Time) error {
	s := stateFromCtx(ctx, r.s)
	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil || j.FencingToken != token || j.Status != domain.StatusFailed {
		return domain.ErrInvalidTransition
	}
	j.NextRetryAt = &next
	j.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *JobRepo) UpdateWithFencingToken(ctx context.Context, job *domain.Job, expectedToken int64) error {
	if job.FencingToken != expectedToken {
		return domain.ErrFencingTokenStale
	}
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	stored, ok := s.jobs[job.ID]
	if !ok {
		return domain.ErrJobNotFound
	}
	if stored.FencingToken != expectedToken {
		return domain.ErrFencingTokenStale
	}
	if stored.Status != job.Status {
		return domain.ErrInvalidTransition
	}
	if stored.Status == domain.StatusRunning {
		return domain.ErrLeaseLost
	}
	stored.Payload = append([]byte(nil), job.Payload...)
	stored.AttemptCount = job.AttemptCount
	stored.MaxAttempts = job.MaxAttempts
	stored.NextRetryAt = projectJob(job, true).NextRetryAt
	stored.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *JobRepo) GetReadyJobs(ctx context.Context, limit int) ([]*domain.Job, error) {
	return r.readyJobs(ctx, limit, false)
}

func (r *JobRepo) GetUndispatchedJobsForUpdate(ctx context.Context, limit int) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	limit = min(limit, 1000)
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	groups := make(map[uuid.UUID][]*domain.Job)
	outstanding := make(map[uuid.UUID]int)
	for _, j := range s.jobs {
		if (j.Status == domain.StatusPending || j.Status == domain.StatusRunning) && j.DispatchedAt != nil {
			outstanding[j.TenantID]++
		}
	}
	for _, e := range s.outbox {
		j := s.jobs[e.JobID]
		if j != nil && e.Published && e.DeliveredAt == nil && j.IsTerminal() {
			outstanding[j.TenantID]++
		}
	}
	now := time.Now().UTC()
	for _, j := range s.jobs {
		if outstanding[j.TenantID] >= 10 || j.DispatchedAt != nil || !s.isReady(j, now) {
			continue
		}
		groups[j.TenantID] = append(groups[j.TenantID], j)
	}
	tenants := make([]uuid.UUID, 0, len(groups))
	for tenant, group := range groups {
		tenants = append(tenants, tenant)
		sort.Slice(group, func(i, j int) bool { return readyBefore(group[i], group[j]) })
		groups[tenant] = group[:min(len(group), 10-outstanding[tenant])]
	}
	sort.Slice(tenants, func(i, j int) bool {
		a, b := s.turns[tenants[i]], s.turns[tenants[j]]
		if !a.Equal(b) {
			return a.Before(b)
		}
		return uuidLess(tenants[i], tenants[j])
	})
	var result []*domain.Job
	for round := 0; len(result) < limit; round++ {
		added := false
		for _, tenant := range tenants {
			group := groups[tenant]
			if round < len(group) && len(result) < limit {
				job := group[round]
				result = append(result, &domain.Job{ID: job.ID, AttemptCount: job.AttemptCount,
					FencingToken: job.FencingToken, TraceParent: job.TraceParent, TraceState: job.TraceState})
				added = true
			}
		}
		if !added {
			break
		}
	}
	return result, nil
}

func (s *shared) isReady(j *domain.Job, now time.Time) bool {
	if j.Status != domain.StatusPending || j.AttemptCount >= j.MaxAttempts || j.ScheduledAt.After(now) || (j.NextRetryAt != nil && j.NextRetryAt.After(now)) {
		return false
	}
	if j.DependsOn == nil {
		return true
	}
	parent := s.jobs[*j.DependsOn]
	return parent != nil && parent.TenantID == j.TenantID && parent.Status == domain.StatusSucceeded
}

func readyBefore(a, b *domain.Job) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if !a.ScheduledAt.Equal(b.ScheduledAt) {
		return a.ScheduledAt.Before(b.ScheduledAt)
	}
	return uuidLess(a.ID, b.ID)
}

func (r *JobRepo) readyJobs(ctx context.Context, limit int, undispatched bool) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	var out []*domain.Job
	for _, j := range s.jobs {
		if undispatched && j.DispatchedAt != nil {
			continue
		}
		if s.isReady(j, now) {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(a, b int) bool {
		return readyBefore(out[a], out[b])
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	for i, job := range out {
		out[i] = cloneJob(job)
	}
	return out, nil
}

func (r *JobRepo) MarkDispatched(ctx context.Context, id uuid.UUID, token int64) error {
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil || j.Status != domain.StatusPending || j.FencingToken != token || j.DispatchedAt != nil {
		return domain.ErrJobNotClaimable
	}
	now := time.Now().UTC()
	j.DispatchedAt = &now
	s.turns[j.TenantID] = now
	return nil
}

func (r *JobRepo) GetPendingRetryJobs(ctx context.Context, limit int) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	var out []*domain.Job
	for _, j := range s.jobs {
		if j.Status != domain.StatusFailed {
			continue
		}
		if j.NextRetryAt != nil && j.NextRetryAt.After(now) && j.AttemptCount < j.MaxAttempts {
			continue
		}
		out = append(out, projectJob(j, true))
	}
	sort.Slice(out, func(a, b int) bool {
		x, y := out[a].NextRetryAt, out[b].NextRetryAt
		if x == nil && y != nil {
			return true
		}
		if x != nil && y == nil {
			return false
		}
		if x != nil && !x.Equal(*y) {
			return x.Before(*y)
		}
		return uuidLess(out[a].ID, out[b].ID)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *JobRepo) GetDependentJobs(ctx context.Context, parentJobID uuid.UUID) ([]*domain.Job, error) {
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	var out []*domain.Job
	parent := s.jobs[parentJobID]
	if parent == nil {
		return nil, nil
	}
	for _, j := range s.jobs {
		if j.DependsOn != nil && *j.DependsOn == parentJobID && j.TenantID == parent.TenantID {
			out = append(out, cloneJob(j))
		}
	}
	return out, nil
}

func (r *JobRepo) ClaimJob(ctx context.Context, id uuid.UUID, workerID string, ttl time.Duration) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if workerID == "" || ttl <= 0 {
		return 0, domain.ErrInvalidLease
	}
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return 0, err
	}
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	now := time.Now().UTC()
	if !ok || !s.isReady(j, now) {
		return 0, domain.ErrJobNotClaimable
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if workerID == "" || ttl <= 0 {
		return domain.ErrInvalidLease
	}
	s := stateFromCtx(ctx, r.s)
	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
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

func (r *JobRepo) CompleteLease(ctx context.Context, id uuid.UUID, workerID string, token int64, final domain.JobStatus) (*domain.Job, error) {
	if final != domain.StatusSucceeded && final != domain.StatusFailed {
		return nil, domain.ErrInvalidTransition
	}
	s := stateFromCtx(ctx, r.s)
	if err := lockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil {
		return nil, domain.ErrJobNotFound
	}
	if j.FencingToken != token {
		return nil, domain.ErrFencingTokenStale
	}
	if j.Status != domain.StatusRunning {
		return nil, domain.ErrInvalidTransition
	}
	if j.LeaseOwner == nil || *j.LeaseOwner != workerID || j.LeaseExpiresAt == nil || !j.LeaseExpiresAt.After(time.Now()) {
		return nil, domain.ErrLeaseLost
	}
	if err := j.TransitionTo(final); err != nil {
		return nil, err
	}
	if final == domain.StatusFailed {
		j.AttemptCount++
	}
	return cloneJob(j), nil
}

func (r *JobRepo) GetExpiredLeasesForUpdate(ctx context.Context, limit int) ([]*domain.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 100
	}
	now := time.Now().UTC()
	var out []*domain.Job
	for _, j := range s.jobs {
		if j.Status == domain.StatusRunning && j.LeaseExpiresAt != nil && j.LeaseExpiresAt.Before(now) {
			out = append(out, cloneJob(j))
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].LeaseExpiresAt.Before(*out[b].LeaseExpiresAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *JobRepo) ExpireLease(ctx context.Context, id uuid.UUID, token int64) (*domain.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil || j.Status != domain.StatusRunning || j.FencingToken != token || j.LeaseExpiresAt == nil || !j.LeaseExpiresAt.Before(time.Now().UTC()) {
		return nil, domain.ErrLeaseLost
	}
	j.FencingToken++
	j.AttemptCount++
	if err := j.TransitionTo(domain.StatusFailed); err != nil {
		return nil, err
	}
	return cloneJob(j), nil
}

// --- Tenants ---

func (r *TenantRepo) Create(ctx context.Context, tenant *domain.Tenant) error {
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	cp := *tenant
	s.tenants[tenant.ID] = &cp
	s.byAPIKey[tenant.APIKeyHash] = tenant.ID
	return nil
}

func (r *TenantRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error) {
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
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

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
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

	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	s.events[event.JobID] = append(s.events[event.JobID], cloneEvent(event))
	return nil
}

func (r *EventRepo) ListByJobID(ctx context.Context, jobID uuid.UUID) ([]*domain.JobEvent, error) {
	return r.listEvents(ctx, jobID, 0, 0)
}

func (r *EventRepo) ListByJobIDPage(ctx context.Context, jobID uuid.UUID, limit, offset int) ([]*domain.JobEvent, error) {
	if limit <= 0 || limit > 1001 || offset < 0 {
		return nil, domain.ErrInvalidInput
	}
	return r.listEvents(ctx, jobID, limit, offset)
}

func (r *EventRepo) listEvents(ctx context.Context, jobID uuid.UUID, limit, offset int) ([]*domain.JobEvent, error) {
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	evs := append([]*domain.JobEvent(nil), s.events[jobID]...)
	sort.Slice(evs, func(a, b int) bool {
		if !evs[a].OccurredAt.Equal(evs[b].OccurredAt) {
			return evs[a].OccurredAt.Before(evs[b].OccurredAt)
		}
		return uuidLess(evs[a].ID, evs[b].ID)
	})
	start, end := min(offset, len(evs)), len(evs)
	if limit > 0 {
		end = start + min(limit, end-start)
	}
	out := make([]*domain.JobEvent, 0, end-start)
	for _, event := range evs[start:end] {
		if limit == 0 {
			out = append(out, cloneEvent(event))
			continue
		}
		cp := *event
		cp.WorkerID, cp.ErrorMessage = nil, nil
		if event.WorkerID != nil {
			if id := domain.AuditText(*event.WorkerID, 128); id != "" {
				cp.WorkerID = &id
			}
		}
		if event.ErrorMessage != nil {
			if message := domain.SafeHandlerMessage(*event.ErrorMessage); message != "" {
				cp.ErrorMessage = &message
			}
		}
		out = append(out, &cp)
	}
	return out, nil
}

// --- Outbox ---

func (r *OutboxRepo) Store(ctx context.Context, event *domain.OutboxEvent) error {
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if _, exists := s.outbox[event.ID]; !exists {
		s.outbox[event.ID] = cloneOutbox(event)
	}
	return nil
}

func (r *OutboxRepo) FetchUnpublished(ctx context.Context, limit int, olderThan time.Duration) ([]*domain.OutboxEvent, error) {
	s := stateFromCtx(ctx, r.s)

	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	cutoff := now.Add(-olderThan)
	var out []*domain.OutboxEvent
	for _, e := range s.outbox {
		if e.Published {
			continue
		}
		if e.CreatedAt.After(cutoff) {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(a, b int) bool {
		if !out[a].CreatedAt.Equal(out[b].CreatedAt) {
			return out[a].CreatedAt.Before(out[b].CreatedAt)
		}
		return uuidLess(out[a].ID, out[b].ID)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	for i, event := range out {
		out[i] = cloneOutbox(event)
	}
	return out, nil
}

func (r *OutboxRepo) FetchPublishable(ctx context.Context, olderThan time.Duration, exclude []uuid.UUID) (*domain.OutboxEvent, error) {
	s := stateFromCtx(ctx, r.s)
	if err := readLockContext(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	excluded := make(map[uuid.UUID]bool, len(exclude))
	for _, id := range exclude {
		excluded[id] = true
	}
	now := time.Now().UTC()
	cutoff := now.Add(-olderThan)
	var first *domain.OutboxEvent
	for _, e := range s.outbox {
		if e.Published || e.CreatedAt.After(cutoff) || excluded[e.ID] ||
			(e.NextPublishAt != nil && e.NextPublishAt.After(now)) {
			continue
		}
		if first == nil || e.CreatedAt.Before(first.CreatedAt) ||
			(e.CreatedAt.Equal(first.CreatedAt) && uuidLess(e.ID, first.ID)) {
			first = e
		}
	}
	if first == nil {
		return nil, nil
	}
	return cloneOutbox(first), nil
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, id uuid.UUID) error {
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	e, ok := s.outbox[id]
	if !ok {
		return nil
	}
	e.MarkPublished()
	e.NextPublishAt = nil
	return nil
}

func (r *OutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error {
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	e, ok := s.outbox[id]
	if !ok || e.Published {
		return nil
	}
	e.RecordFailure(lastErr)
	return nil
}

func (r *OutboxRepo) MarkDelivered(ctx context.Context, id uuid.UUID) error {
	s := stateFromCtx(ctx, r.s)
	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if e := s.outbox[id]; e != nil && e.DeliveredAt == nil {
		now := time.Now().UTC()
		e.DeliveredAt = &now
	}
	return nil
}

func (r *OutboxRepo) RetireUnpublished(ctx context.Context, jobID uuid.UUID) error {
	s := stateFromCtx(ctx, r.s)
	if err := lockContext(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	for _, e := range s.outbox {
		if e.JobID == jobID && !e.Published {
			e.MarkPublished()
			// Failed confirmation can still mean the broker accepted a copy.
			if e.RetryCount == 0 {
				e.DeliveredAt = e.PublishedAt
			}
			e.NextPublishAt = nil
		}
	}
	return nil
}

func (r *OutboxRepo) DeletePublished(ctx context.Context, olderThan time.Duration) (int64, error) {
	s := stateFromCtx(ctx, r.s)

	if err := lockContext(ctx, &s.mu); err != nil {
		return 0, err
	}
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
