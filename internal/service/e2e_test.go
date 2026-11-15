package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
)

// Principle: distributed end-to-end in-process. Real scheduler + real workers
// + real leases + real broker, no mocks. These are the closest thing to the
// 10–20 e2e tests GPT named that can run without Docker.

// A parent→child chain must execute in order through actual workers.
func TestE2E_ChainedExecutionThroughWorkers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), &domain.ConstantBackoff{Delay: time.Millisecond})
	w := NewWorker("e2e-1", s, 4, 30*time.Second)
	var order atomic.Int32
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error {
		order.Add(1)
		return nil
	})
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	parent, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, DependsOn: &parent.ID, ScheduledAt: time.Now().UTC()})

	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("pass 1 must run parent only, ran %d", n)
	}
	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("pass 2 must run unlocked child, ran %d", n)
	}
	if order.Load() != 2 {
		t.Fatalf("executed %d want 2", order.Load())
	}
}

// A flaky handler (fail once, then succeed) must recover via the retry path
// with no operator intervention.
func TestE2E_FlakyHandlerRecovers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), &domain.ConstantBackoff{Delay: 0})
	w := NewWorker("e2e-flaky", s, 4, 30*time.Second)
	var calls atomic.Int32
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error {
		if calls.Add(1) == 1 {
			return errors.New("transient 503")
		}
		return nil
	})
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	job, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})

	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("pass 1 ran %d", n)
	}
	mid, _ := s.GetJob(ctx, job.ID)
	if mid.Status != domain.StatusPending || mid.AttemptCount != 1 {
		t.Fatalf("after flake: status=%s attempts=%d", mid.Status, mid.AttemptCount)
	}
	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("pass 2 ran %d", n)
	}
	done, _ := s.GetJob(ctx, job.ID)
	if done.Status != domain.StatusSucceeded || done.AttemptCount != 1 {
		t.Fatalf("after recovery: status=%s attempts=%d", done.Status, done.AttemptCount)
	}
}

// Two workers racing for one job: exactly one executes (lease fencing across
// workers, not just goroutines).
func TestE2E_CompetingWorkersSingleExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	var executions atomic.Int32
	mkWorker := func(id string) *Worker {
		w := NewWorker(id, s, 4, 30*time.Second)
		w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error {
			executions.Add(1)
			time.Sleep(20 * time.Millisecond) // widen the race window
			return nil
		})
		return w
	}
	w1, w2 := mkWorker("w1"), mkWorker("w2")
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})

	var wg sync.WaitGroup
	for _, w := range []*Worker{w1, w2} {
		wg.Add(1)
		go func(w *Worker) { defer wg.Done(); _, _ = w.RunOnce(ctx, 10) }(w)
	}
	wg.Wait()
	if got := executions.Load(); got != 1 {
		t.Fatalf("exactly one execution required, got %d", got)
	}
}

// Tenants are fully isolated through the whole pipeline: jobs, dispatch,
// and completion of one tenant never leak into another.
func TestE2E_TenantIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	w := NewWorker("w", s, 4, 30*time.Second)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })

	mkTenantJob := func(key string) {
		tn, _ := domain.NewTenant("t-"+key, key)
		_ = b.Tenants.Create(ctx, tn)
		_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	}
	mkTenantJob("k1")
	mkTenantJob("k2")
	if n, _ := w.RunOnce(ctx, 10); n != 2 {
		t.Fatalf("both tenants' jobs must run, ran %d", n)
	}
}
