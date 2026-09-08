package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/lease"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

func listAll() repository.JobListFilter { return repository.JobListFilter{} }

var _ = time.Now

func newTestWorker(s *Scheduler) *Worker {
	w := NewWorker("test-worker", s, lease.NewMemoryLeaseManager(), 4)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	return w
}

// Principle: worker-lifecycle tests. The worker is the only component that
// touches leases, semaphores, heartbeats and user code at once — its edges
// (drain, panic, failure, unknown jobs) are exactly where production dies.
func TestWorker_DrainStopsIntake(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	w := newTestWorker(s)
	if w.IsDraining() {
		t.Fatal("fresh worker must not be draining")
	}
	w.Drain()
	if !w.IsDraining() {
		t.Fatal("Drain must stick")
	}
	if n, _ := w.RunOnce(ctx, 10); n != 0 {
		t.Fatalf("drained worker must run 0, ran %d", n)
	}
}

func TestWorker_HandlerErrorFailsJobWithRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	w := NewWorker("w", s, lease.NewMemoryLeaseManager(), 4)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error {
		return errors.New("downstream 500")
	})
	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("ran %d want 1", n)
	}
	// Failed with attempts left → back to pending with a scheduled retry.
	jobs, _, _ := s.ListJobs(ctx, tn.ID, listAll())
	if len(jobs) != 1 || jobs[0].Status != domain.StatusPending {
		t.Fatalf("handler error must recycle to pending, got %+v", jobs)
	}
	if jobs[0].NextRetryAt == nil {
		t.Fatal("recycled job must carry NextRetryAt")
	}
}

func TestWorker_HandlerPanicFailsJobNotWorker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	w := NewWorker("w", s, lease.NewMemoryLeaseManager(), 4)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error {
		panic("nil deref in user code")
	})
	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("panicking job must still count as handled, ran %d", n)
	}
	jobs, _, _ := s.ListJobs(ctx, tn.ID, listAll())
	if len(jobs) != 1 || jobs[0].Status != domain.StatusPending {
		t.Fatalf("panic must recycle to pending, got %+v", jobs)
	}
	if len(w.ActiveJobs()) != 0 {
		t.Fatal("panicking job must leave no active residue")
	}
	// Worker survives: second job executes normally.
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	if n, _ := w.RunOnce(ctx, 10); n < 1 {
		t.Fatalf("worker died after panic, ran %d", n)
	}
}

func TestWorker_HeartbeatAndCancelUnknown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := newTestScheduler()
	w := newTestWorker(s)
	if renewed, abort := w.Heartbeat(ctx, uuid.New(), 1); renewed || !abort {
		t.Fatalf("unknown job heartbeat must be (false, true), got (%v, %v)", renewed, abort)
	}
	if w.CancelExecution(uuid.New()) {
		t.Fatal("cancel of unknown execution must be false")
	}
	if len(w.ActiveJobs()) != 0 {
		t.Fatal("fresh worker must have no active jobs")
	}
}

func TestWorker_LeaseContentionSkipsJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	job, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})

	// Another worker holds the lease: our worker must skip, not steal.
	leases := lease.NewMemoryLeaseManager()
	if _, err := leases.Acquire(ctx, job.ID, "rival", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	w := NewWorker("us", s, leases, 4)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	if n, _ := w.RunOnce(ctx, 10); n != 0 {
		t.Fatalf("leased job must be skipped, ran %d", n)
	}
	stored, _ := s.GetJob(ctx, job.ID)
	if stored.Status != domain.StatusPending {
		t.Fatalf("skipped job must stay pending, got %s", stored.Status)
	}
}

func TestWorker_RegisterHandlerOverwrite(t *testing.T) {
	t.Parallel()
	s, _ := newTestScheduler()
	w := newTestWorker(s)
	called := ""
	w.RegisterHandler("h", func(ctx context.Context, job *domain.Job) error { called = "v1"; return nil })
	w.RegisterHandler("h", func(ctx context.Context, job *domain.Job) error { called = "v2"; return nil })
	w.handlers["h"](context.Background(), &domain.Job{})
	if called != "v2" {
		t.Fatalf("last registration must win, called=%q", called)
	}
}
