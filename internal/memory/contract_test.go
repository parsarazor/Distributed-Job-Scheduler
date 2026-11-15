package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

// Principle: repository contract suite. These tests encode the interface
// guarantees every backend (memory today, Postgres tomorrow) must honor:
// fencing, gating, ordering, idempotency. A new backend passes by running
// this same suite against it.

// --- helpers ---

func testJob(tenant uuid.UUID, name string, prio domain.JobPriority, scheduled time.Time) *domain.Job {
	j, err := domain.NewJob(tenant, name, nil, prio, 3, nil, scheduled)
	if err != nil {
		panic(err)
	}
	return j
}

func TestContract_CancelRules(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	tenant := uuid.New()

	j := testJob(tenant, "j", domain.PriorityDefault, time.Now().UTC())
	if err := b.Jobs.Create(ctx, j); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Jobs.Cancel(ctx, j.ID); err != nil {
		t.Fatalf("pending job must be cancellable: %v", err)
	}
	if _, err := b.Jobs.Cancel(ctx, j.ID); err != domain.ErrJobCannotBeCancelled {
		t.Fatalf("double cancel must fail with ErrJobCannotBeCancelled, got %v", err)
	}
	if _, err := b.Jobs.Cancel(ctx, uuid.New()); err != domain.ErrJobNotFound {
		t.Fatalf("cancel of unknown job must be ErrJobNotFound, got %v", err)
	}
}

func TestContract_FencingTokens(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	j := testJob(uuid.New(), "j", domain.PriorityDefault, time.Now().UTC())
	_ = b.Jobs.Create(ctx, j)

	tok, err := b.Jobs.ClaimJob(ctx, j.ID, "w", 30*time.Second)
	if err != nil || tok != 1 {
		t.Fatalf("first token must be 1, got %d (%v)", tok, err)
	}
	// Stale token (0 < 1) must be rejected on both guarded writes.
	if err := b.Jobs.TransitionStatus(ctx, j.ID, domain.StatusRunning, domain.StatusSucceeded, 0); err != domain.ErrFencingTokenStale {
		t.Fatalf("stale TransitionStatus must fail, got %v", err)
	}
	stored, _ := b.Jobs.GetByID(ctx, j.ID)
	if err := b.Jobs.UpdateWithFencingToken(ctx, stored, 0); err != domain.ErrFencingTokenStale {
		t.Fatalf("stale UpdateWithFencingToken must fail, got %v", err)
	}
	// Fresh token proceeds.
	if err := b.Jobs.TransitionStatus(ctx, j.ID, domain.StatusRunning, domain.StatusSucceeded, tok); err != nil {
		t.Fatalf("fresh transition: %v", err)
	}
}

func TestContract_ReadyOrderingAndGating(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	tenant := uuid.New()
	now := time.Now().UTC()

	low := testJob(tenant, "low", domain.PriorityLow, now.Add(-time.Minute))
	high := testJob(tenant, "high", domain.PriorityHigh, now.Add(-time.Minute))
	future := testJob(tenant, "future", domain.PriorityCritical, now.Add(time.Hour))
	for _, j := range []*domain.Job{low, high, future} {
		if err := b.Jobs.Create(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	// Gated child: must not be ready until the parent succeeds.
	parent := testJob(tenant, "parent", domain.PriorityDefault, now.Add(-time.Minute))
	_ = b.Jobs.Create(ctx, parent)
	child, _ := domain.NewJob(tenant, "child", nil, domain.PriorityDefault, 3, &parent.ID, now.Add(-time.Minute))
	_ = b.Jobs.Create(ctx, child)

	ready, err := b.Jobs.GetReadyJobs(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 3 { // low, high, parent (not future, not child)
		t.Fatalf("want 3 ready jobs, got %d", len(ready))
	}
	// Ordering: priority DESC first.
	if ready[0].ID != high.ID || ready[1].ID != parent.ID || ready[2].ID != low.ID {
		t.Fatalf("wrong ORDER BY priority DESC: %v", []string{ready[0].Name, ready[1].Name, ready[2].Name})
	}
}

func TestContract_PaginationAndFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	tenant, other := uuid.New(), uuid.New()
	for i := 0; i < 5; i++ {
		_ = b.Jobs.Create(ctx, testJob(tenant, "j", domain.PriorityDefault, time.Now().UTC()))
	}
	_ = b.Jobs.Create(ctx, testJob(other, "x", domain.PriorityDefault, time.Now().UTC()))

	page, total, err := b.Jobs.ListByTenant(ctx, tenant, repository.JobListFilter{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(page) != 2 {
		t.Fatalf("want total=5 page=2, got total=%d page=%d", total, len(page))
	}
}

func TestContract_OutboxLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	jobID := uuid.New()

	e := domain.NewOutboxEvent(jobID, "jobs.dispatch", nil)
	if err := b.Outbox.Store(ctx, e); err != nil {
		t.Fatal(err)
	}
	// Fresh rows are NOT fetchable until olderThan passes (reconciler cadence).
	if got, _ := b.Outbox.FetchUnpublished(ctx, 10, time.Hour); len(got) != 0 {
		t.Fatalf("fresh row must be withheld, got %d", len(got))
	}
	if got, _ := b.Outbox.FetchUnpublished(ctx, 10, 0); len(got) != 1 {
		t.Fatalf("aged row must be fetched, got %d", len(got))
	}
	// Failure bumps the counter; the ceiling removes the poison row.
	for i := 0; i < domain.MaxOutboxRetries; i++ {
		_ = b.Outbox.MarkFailed(ctx, e.ID, "broker down")
	}
	if got, _ := b.Outbox.FetchUnpublished(ctx, 10, 0); len(got) != 0 {
		t.Fatalf("abandoned row must be skipped, got %d", len(got))
	}
	// Publish + purge path.
	e2 := domain.NewOutboxEvent(uuid.New(), "k", nil)
	_ = b.Outbox.Store(ctx, e2)
	_ = b.Outbox.MarkPublished(ctx, e2.ID)
	if n, _ := b.Outbox.DeletePublished(ctx, 0); n != 1 {
		t.Fatalf("purge must delete 1 row, deleted %d", n)
	}
}

func TestContract_TenantAndEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	tn, err := domain.NewTenant("acme", "secret")
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Tenants.Create(ctx, tn)
	if got, _ := b.Tenants.GetByAPIKeyHash(ctx, tn.APIKeyHash); got.ID != tn.ID {
		t.Fatal("API-key-hash lookup broken")
	}
	if _, err := b.Tenants.GetByAPIKeyHash(ctx, "nope"); err != domain.ErrTenantNotFound {
		t.Fatalf("unknown hash must be ErrTenantNotFound, got %v", err)
	}
	jobID := uuid.New()
	for _, to := range []domain.JobStatus{domain.StatusRunning, domain.StatusSucceeded} {
		_ = b.Events.Record(ctx, &domain.JobEvent{ID: uuid.New(), JobID: jobID, FromStatus: domain.StatusPending, ToStatus: to, OccurredAt: time.Now().UTC()})
	}
	evs, _ := b.Events.ListByJobID(ctx, jobID)
	if len(evs) != 2 {
		t.Fatalf("want 2 audit events, got %d", len(evs))
	}
}

// Principle: safe under concurrent writers (run with -race). 50 parallel
// creates must all land with no lost writes or map panics.
func TestContract_ConcurrentCreates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	tenant := uuid.New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Jobs.Create(ctx, testJob(tenant, "j", domain.PriorityDefault, time.Now().UTC()))
		}()
	}
	wg.Wait()
	_, total, _ := b.Jobs.ListByTenant(ctx, tenant, repository.JobListFilter{})
	if total != 50 {
		t.Fatalf("lost writes under concurrency: total=%d want 50", total)
	}
}
