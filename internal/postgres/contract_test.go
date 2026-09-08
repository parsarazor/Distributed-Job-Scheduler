package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

// Principle: live-contract suite. These run the same guarantees as the memory
// contract suite against real Postgres. They skip without DATABASE_URL so
// unit runs stay hermetic; CI provides the database (see docker-compose.yml).
//
//	DATABASE_URL=postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable go test ./internal/postgres/ -v
func openTestPool(t *testing.T) *Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL unset: live Postgres contract skipped (runs in CI)")
	}
	ctx := context.Background()
	p, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := p.Ping(ctx); err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestLive_TenantRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn, _ := domain.NewTenant("live-acme", "live-key")
	if err := r.Tenants.Create(ctx, tn); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	got, err := r.Tenants.GetByAPIKeyHash(ctx, tn.APIKeyHash)
	if err != nil || got.ID != tn.ID {
		t.Fatalf("hash lookup: %+v %v", got, err)
	}
}

func TestLive_JobFencingRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn, _ := domain.NewTenant("live-acme", "k2")
	_ = r.Tenants.Create(ctx, tn)
	job, _ := domain.NewJob(tn.ID, "live-job", nil, domain.PriorityHigh, 3, nil, time.Now().UTC())
	if err := r.Jobs.Create(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	tok, err := r.Jobs.IncrementFencingToken(ctx, job.ID)
	if err != nil || tok != 1 {
		t.Fatalf("first token must be 1, got %d %v", tok, err)
	}
	if err := r.Jobs.TransitionStatus(ctx, job.ID, domain.StatusPending, domain.StatusRunning, 0); err != domain.ErrFencingTokenStale {
		t.Fatalf("stale write must fail, got %v", err)
	}
	if err := r.Jobs.TransitionStatus(ctx, job.ID, domain.StatusPending, domain.StatusRunning, tok); err != nil {
		t.Fatalf("fresh write: %v", err)
	}
}

func TestLive_OutboxSkipLocked(t *testing.T) {
	ctx := context.Background()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn, _ := domain.NewTenant("live-acme", "k3")
	_ = r.Tenants.Create(ctx, tn)
	job, _ := domain.NewJob(tn.ID, "live-job", nil, domain.PriorityDefault, 3, nil, time.Now().UTC().Add(-time.Minute))
	_ = r.Jobs.Create(ctx, job)
	_ = r.Outbox.Store(ctx, domain.NewOutboxEvent(job.ID, "jobs.dispatch", nil))
	got, err := r.Outbox.FetchUnpublished(ctx, 10, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	found := false
	for _, e := range got {
		if e.JobID == job.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("stored outbox row not returned by SKIP LOCKED fetch")
	}
}

func TestLive_TransactionAtomicity(t *testing.T) {
	ctx := context.Background()
	p := openTestPool(t)
	r := NewRepositories(p)
	tn, _ := domain.NewTenant("live-acme", "k4")
	_ = r.Tenants.Create(ctx, tn)
	job, _ := domain.NewJob(tn.ID, "live-job", nil, domain.PriorityDefault, 3, nil, time.Now().UTC())
	sentinel := context.DeadlineExceeded
	err := r.Tx.WithinTransaction(ctx, func(ctx context.Context) error {
		if err := r.Jobs.Create(ctx, job); err != nil {
			return err
		}
		return sentinel // force rollback
	})
	if err != sentinel {
		t.Fatalf("want sentinel, got %v", err)
	}
	if _, err := r.Jobs.GetByID(ctx, job.ID); err != domain.ErrJobNotFound {
		t.Fatalf("rolled-back job must not exist, got %v", err)
	}
}
