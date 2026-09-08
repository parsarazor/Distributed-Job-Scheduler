package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/lease"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
)

func newTestScheduler() (*Scheduler, *memory.Bundle) {
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), &domain.ConstantBackoff{Delay: time.Millisecond})
	return s, b
}

func seedTenant(t *testing.T, s *Scheduler, b *memory.Bundle) *domain.Tenant {
	t.Helper()
	tn, err := domain.NewTenant("acme", "k")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Tenants.Create(context.Background(), tn); err != nil {
		t.Fatal(err)
	}
	return tn
}

func TestSubmitDispatchComplete(t *testing.T) {
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)

	parent, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "p", Payload: json.RawMessage(`{}`), Priority: domain.PriorityHigh, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "c", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, DependsOn: &parent.ID, ScheduledAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}

	// Only parent is ready (dependency gate).
	ready, _ := b.Jobs.GetReadyJobs(ctx, 10)
	if len(ready) != 1 || ready[0].ID != parent.ID {
		t.Fatalf("gate broken: %+v", ready)
	}
	if n, _ := s.DispatchReady(ctx, 10); n != 1 {
		t.Fatalf("dispatch=%d want 1", n)
	}
	p, _ := s.GetJob(ctx, parent.ID)
	if p.Status != domain.StatusRunning {
		t.Fatalf("parent=%s want running", p.Status)
	}
	if _, err := s.ReportCompletion(ctx, parent.ID, "w", domain.StatusSucceeded, p.FencingToken, ""); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.DispatchReady(ctx, 10); n != 1 {
		t.Fatalf("child dispatch=%d want 1", n)
	}
}

func TestCascadeAbortOnTerminalFailure(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestScheduler()
	tn, _ := domain.NewTenant("acme", "k")
	_ = s.tenants.Create(ctx, tn)

	parent, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "p", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 1, ScheduledAt: time.Now().UTC()})
	child, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "c", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 1, DependsOn: &parent.ID, ScheduledAt: time.Now().UTC()})

	// Dispatch parent to running, then fail it with attempts exhausted.
	_, _ = s.DispatchReady(ctx, 10)
	p, _ := s.GetJob(ctx, parent.ID)
	if _, err := s.ReportCompletion(ctx, parent.ID, "w", domain.StatusFailed, p.FencingToken, "boom"); err != nil {
		t.Fatal(err)
	}
	p2, _ := s.GetJob(ctx, parent.ID)
	if p2.Status != domain.StatusDead {
		t.Fatalf("parent=%s want dead (maxAttempts=1)", p2.Status)
	}
	c2, _ := s.GetJob(ctx, child.ID)
	if c2.Status != domain.StatusCancelled {
		t.Fatalf("child=%s want cancelled via cascade", c2.Status)
	}
}

func TestFencingTokenRejectsZombieWrite(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestScheduler()
	tn, _ := domain.NewTenant("acme", "k")
	_ = s.tenants.Create(ctx, tn)
	job, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	_, _ = s.DispatchReady(ctx, 10)
	j, _ := s.GetJob(ctx, job.ID)
	// Simulate a newer owner bumping the token, then a stale write.
	if _, err := s.jobs.IncrementFencingToken(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReportCompletion(ctx, job.ID, "zombie", domain.StatusSucceeded, j.FencingToken, ""); err != domain.ErrFencingTokenStale {
		t.Fatalf("want ErrFencingTokenStale, got %v", err)
	}
}

func TestOutboxReconcileSkipsAbandoned(t *testing.T) {
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	// Force-age the outbox row past the 5s threshold, then fail it to the ceiling.
	evs, _ := b.Outbox.FetchUnpublished(ctx, 10, 0)
	if len(evs) == 0 {
		t.Fatal("want outbox row")
	}
	_ = job
	for i := 0; i < domain.MaxOutboxRetries; i++ {
		_ = b.Outbox.MarkFailed(ctx, evs[0].ID, "down")
	}
	s.ReconcileThreshold = 0 // prove the skip comes from the ceiling, not age
	if n, _ := s.ReconcileOutbox(ctx, 10); n != 0 {
		t.Fatalf("abandoned row must be skipped, reconciled=%d", n)
	}
}

func TestWorkerRunOnce(t *testing.T) {
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	w := NewWorker("w1", s, lease.NewMemoryLeaseManager(), 4)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("worker ran %d, want 1", n)
	}
}
