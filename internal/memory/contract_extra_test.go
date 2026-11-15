package memory

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

// Principle: query-semantics tests. The dispatcher trusts these filters;
// each WHERE clause in the Postgres twin gets a memory-side proof here.

func TestContract_GetByIDForUpdate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	j := testJob(uuid.New(), "j", domain.PriorityDefault, time.Now().UTC())
	_ = b.Jobs.Create(ctx, j)
	got, err := b.Jobs.GetByIDForUpdate(ctx, j.ID)
	if err != nil || got.ID != j.ID {
		t.Fatalf("GetByIDForUpdate: %v %+v", err, got)
	}
	if _, err := b.Jobs.GetByID(ctx, uuid.New()); err != domain.ErrJobNotFound {
		t.Fatalf("unknown id must be ErrJobNotFound, got %v", err)
	}
	if _, err := b.Jobs.GetByIDForUpdate(ctx, uuid.New()); err != domain.ErrJobNotFound {
		t.Fatalf("unknown id (locking read) must be ErrJobNotFound, got %v", err)
	}
}

func TestContract_ListFilters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	tenant := uuid.New()
	mk := func(name string, st domain.JobStatus, p domain.JobPriority) {
		j := testJob(tenant, name, p, time.Now().UTC())
		_ = b.Jobs.Create(ctx, j)
		// Drive to the wanted status through legal edges.
		if st == domain.StatusRunning {
			_, _ = b.Jobs.ClaimJob(ctx, j.ID, "w", 30*time.Second)
		}
	}
	mk("a", domain.StatusPending, domain.PriorityLow)
	mk("b", domain.StatusRunning, domain.PriorityHigh)
	mk("c", domain.StatusPending, domain.PriorityHigh)

	running := domain.StatusRunning
	if _, total, _ := b.Jobs.ListByTenant(ctx, tenant, repository.JobListFilter{Status: &running}); total != 1 {
		t.Fatalf("status filter: total=%d want 1", total)
	}
	high := domain.PriorityHigh
	if _, total, _ := b.Jobs.ListByTenant(ctx, tenant, repository.JobListFilter{Priority: &high}); total != 2 {
		t.Fatalf("priority filter: total=%d want 2", total)
	}
	if _, total, _ := b.Jobs.ListByTenant(ctx, uuid.New(), repository.JobListFilter{}); total != 0 {
		t.Fatalf("foreign tenant must see 0, got %d", total)
	}
	// Offset past the end is an empty page, not an error.
	if page, total, _ := b.Jobs.ListByTenant(ctx, tenant, repository.JobListFilter{Offset: 99}); len(page) != 0 || total != 3 {
		t.Fatalf("oob offset: page=%d total=%d", len(page), total)
	}
}

func TestContract_PendingRetryQuery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	tenant := uuid.New()
	past := time.Now().UTC().Add(-time.Minute)
	future := time.Now().UTC().Add(time.Hour)

	due := testJob(tenant, "due", domain.PriorityDefault, past)
	_ = b.Jobs.Create(ctx, due)
	_, _ = b.Jobs.ClaimJob(ctx, due.ID, "w", 30*time.Second)
	_ = b.Jobs.TransitionStatus(ctx, due.ID, domain.StatusRunning, domain.StatusFailed, 1)
	stored, _ := b.Jobs.GetByID(ctx, due.ID)
	stored.NextRetryAt = &past // attempts 0 < 3
	_ = b.Jobs.UpdateWithFencingToken(ctx, stored, stored.FencingToken)

	waiting := testJob(tenant, "waiting", domain.PriorityDefault, past)
	_ = b.Jobs.Create(ctx, waiting)
	_, _ = b.Jobs.ClaimJob(ctx, waiting.ID, "w", 30*time.Second)
	_ = b.Jobs.TransitionStatus(ctx, waiting.ID, domain.StatusRunning, domain.StatusFailed, 1)
	stored2, _ := b.Jobs.GetByID(ctx, waiting.ID)
	stored2.NextRetryAt = &future
	_ = b.Jobs.UpdateWithFencingToken(ctx, stored2, stored2.FencingToken)

	exhausted := testJob(tenant, "exhausted", domain.PriorityDefault, past)
	_ = b.Jobs.Create(ctx, exhausted)
	_, _ = b.Jobs.ClaimJob(ctx, exhausted.ID, "w", 30*time.Second)
	_ = b.Jobs.TransitionStatus(ctx, exhausted.ID, domain.StatusRunning, domain.StatusFailed, 1)
	stored3, _ := b.Jobs.GetByID(ctx, exhausted.ID)
	stored3.NextRetryAt = &past
	stored3.AttemptCount = stored3.MaxAttempts
	_ = b.Jobs.UpdateWithFencingToken(ctx, stored3, stored3.FencingToken)

	got, err := b.Jobs.GetPendingRetryJobs(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != due.ID {
		t.Fatalf("only the due job qualifies, got %d", len(got))
	}
	if _, err := b.Jobs.GetPendingRetryJobs(ctx, 0); err != nil {
		t.Fatalf("limit 0 must still work: %v", err)
	}
}

func TestContract_UpdateAndTokenErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	j := testJob(uuid.New(), "j", domain.PriorityDefault, time.Now().UTC())
	_ = b.Jobs.Create(ctx, j)

	// Success path persists fields and refreshes UpdatedAt.
	stored, _ := b.Jobs.GetByID(ctx, j.ID)
	before := stored.UpdatedAt
	stored.AttemptCount = 2
	time.Sleep(time.Millisecond)
	if err := b.Jobs.UpdateWithFencingToken(ctx, stored, 0); err != nil {
		t.Fatalf("fresh update: %v", err)
	}
	after, _ := b.Jobs.GetByID(ctx, j.ID)
	if after.AttemptCount != 2 || !after.UpdatedAt.After(before) {
		t.Fatalf("update not persisted: %+v", after)
	}
	// Unknown ids.
	ghost := testJob(uuid.New(), "g", domain.PriorityDefault, time.Now().UTC())
	if err := b.Jobs.UpdateWithFencingToken(ctx, ghost, 0); err != domain.ErrJobNotFound {
		t.Fatalf("update of ghost must be ErrJobNotFound, got %v", err)
	}
	if _, err := b.Jobs.ClaimJob(ctx, uuid.New(), "w", time.Second); err != domain.ErrJobNotClaimable {
		t.Fatalf("claim of ghost must be ErrJobNotClaimable, got %v", err)
	}
	// Wrong-from transition surfaces invalid-transition, not stale.
	if err := b.Jobs.TransitionStatus(ctx, j.ID, domain.StatusRunning, domain.StatusSucceeded, 0); err != domain.ErrInvalidTransition {
		t.Fatalf("wrong-from must be ErrInvalidTransition, got %v", err)
	}
}

func TestContract_GetDependentsEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	deps, err := b.Jobs.GetDependentJobs(ctx, uuid.New())
	if err != nil || len(deps) != 0 {
		t.Fatalf("childless job must yield empty, got %v %v", deps, err)
	}
}

func TestContract_OutboxScoping(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	// Limit + oldest-first ordering.
	for i := 0; i < 5; i++ {
		_ = b.Outbox.Store(ctx, domain.NewOutboxEvent(uuid.New(), "k", nil))
		time.Sleep(time.Millisecond)
	}
	got, _ := b.Outbox.FetchUnpublished(ctx, 2, 0)
	if len(got) != 2 {
		t.Fatalf("limit must bound results, got %d", len(got))
	}
	if !got[0].CreatedAt.Before(got[1].CreatedAt) {
		t.Fatal("outbox must be oldest-first")
	}
	// Unknown ids are safe no-ops; purge only touches old published rows.
	_ = b.Outbox.MarkPublished(ctx, uuid.New())
	_ = b.Outbox.MarkFailed(ctx, uuid.New(), "x")
	fresh := domain.NewOutboxEvent(uuid.New(), "k", nil)
	_ = b.Outbox.Store(ctx, fresh)
	_ = b.Outbox.MarkPublished(ctx, fresh.ID)
	if n, _ := b.Outbox.DeletePublished(ctx, time.Hour); n != 0 {
		t.Fatalf("fresh published row must survive purge, deleted %d", n)
	}
}

func TestContract_TransactorRunsFn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	called := false
	if err := b.Tx.WithinTransaction(ctx, func(ctx context.Context) error {
		called = true
		return b.Jobs.Create(ctx, testJob(uuid.New(), "j", domain.PriorityDefault, time.Now().UTC()))
	}); err != nil || !called {
		t.Fatalf("transactor must run fn: called=%v err=%v", called, err)
	}
	// Errors propagate.
	sentinel := context.DeadlineExceeded
	if err := b.Tx.WithinTransaction(ctx, func(ctx context.Context) error { return sentinel }); err != sentinel {
		t.Fatalf("transactor must propagate fn error, got %v", err)
	}
}

func TestContract_TenantGetByID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewBundle()
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	got, err := b.Tenants.GetByID(ctx, tn.ID)
	if err != nil || got.Name != "acme" {
		t.Fatalf("GetByID: %+v %v", got, err)
	}
	if _, err := b.Tenants.GetByID(ctx, uuid.New()); err != domain.ErrTenantNotFound {
		t.Fatalf("unknown tenant must be ErrTenantNotFound, got %v", err)
	}
	// Re-create is idempotent, not an error.
	if err := b.Jobs.Create(ctx, testJob(uuid.New(), "dup-check", domain.PriorityDefault, time.Now().UTC())); err != nil {
		t.Fatalf("create: %v", err)
	}
}
