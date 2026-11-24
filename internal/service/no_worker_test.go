package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/memory"
)

type unroutableBroker struct {
	mu   sync.Mutex
	keys []string
}

func (b *unroutableBroker) Publish(_ context.Context, key string, _ []byte, _ string) error {
	b.mu.Lock()
	b.keys = append(b.keys, key)
	b.mu.Unlock()
	return fmt.Errorf("%w: 312 NO_ROUTE", broker.ErrUnroutable)
}
func (b *unroutableBroker) Close() error { return nil }

func TestNoWorker_DefersWithoutRetryBudgetThenDies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	bus := &unroutableBroker{}
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, bus, nil)
	s.ReconcileThreshold = 0
	s.NoWorkerRetry = 20 * time.Millisecond
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	parent, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "send-emial", Payload: json.RawMessage(`{}`), MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	child, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "receipt", Payload: json.RawMessage(`{}`), MaxAttempts: 3, DependsOn: &parent.ID})
	if err != nil {
		t.Fatal(err)
	}
	cycle := func() {
		t.Helper()
		if _, err := s.DispatchReady(ctx, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReconcileOutbox(ctx, 10); err != nil {
			t.Fatal(err)
		}
	}
	cycle()
	job, _ := b.Jobs.GetByID(ctx, parent.ID)
	if job.Status != domain.StatusPending || job.DispatchedAt != nil || job.NextRetryAt == nil || job.AttemptCount != 0 {
		t.Fatalf("unroutable job not deferred: %+v", job)
	}
	if bus.keys[0] != broker.JobQueue("send-emial") {
		t.Fatalf("published to %q", bus.keys[0])
	}
	if left, _ := b.Outbox.FetchUnpublished(ctx, 10, 0); len(left) != 0 {
		t.Fatalf("unroutable announcement left for publish retries: %+v", left)
	}
	if n, _ := s.DispatchReady(ctx, 10); n != 0 {
		t.Fatal("deferred job dispatched before NoWorkerRetry")
	}
	time.Sleep(30 * time.Millisecond)
	cycle()
	if len(bus.keys) != 2 {
		t.Fatalf("deferred job not retried: %d publishes", len(bus.keys))
	}
	s.NoWorkerTimeout = time.Nanosecond
	time.Sleep(30 * time.Millisecond)
	cycle()
	job, _ = b.Jobs.GetByID(ctx, parent.ID)
	if job.Status != domain.StatusDead {
		t.Fatalf("job not dead after NoWorkerTimeout: %s", job.Status)
	}
	events, _ := b.Events.ListByJobID(ctx, parent.ID)
	if last := events[len(events)-1]; last.ToStatus != domain.StatusDead || last.ErrorMessage == nil || *last.ErrorMessage != domain.ReasonNoWorker {
		t.Fatalf("missing no-worker event: %+v", last)
	}
	if dep, _ := b.Jobs.GetByID(ctx, child.ID); dep.Status != domain.StatusCancelled {
		t.Fatalf("dependent of a no-worker job is %s", dep.Status)
	}
}

// Time a job spent blocked (here: scheduled long ago) must not count toward
// NoWorkerTimeout; only time since its first refused announcement does.
func TestNoWorker_TimeoutStartsAtFirstRefusal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, &unroutableBroker{}, nil)
	s.ReconcileThreshold = 0
	s.NoWorkerTimeout = time.Hour
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	job, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "late", Payload: json.RawMessage(`{}`), MaxAttempts: 3, ScheduledAt: time.Now().Add(-48 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReconcileOutbox(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.Jobs.GetByID(ctx, job.ID); got.Status != domain.StatusPending || got.NextRetryAt == nil {
		t.Fatalf("first refusal must defer, not kill: %+v", got)
	}
}

// Refusals from before an operator requeue must not count, or a requeued job
// would die again on its first refusal.
func TestNoWorker_RequeueRestartsTheWait(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, &unroutableBroker{}, nil)
	s.ReconcileThreshold = 0
	s.NoWorkerRetry = 10 * time.Millisecond
	s.NoWorkerTimeout = 200 * time.Millisecond
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	job, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "orphan", Payload: json.RawMessage(`{}`), MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	cycle := func() {
		t.Helper()
		if _, err := s.DispatchReady(ctx, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReconcileOutbox(ctx, 10); err != nil {
			t.Fatal(err)
		}
	}
	cycle()
	time.Sleep(250 * time.Millisecond)
	cycle()
	if got, _ := b.Jobs.GetByID(ctx, job.ID); got.Status != domain.StatusDead {
		t.Fatalf("job not dead after NoWorkerTimeout: %s", got.Status)
	}
	if _, err := s.RequeueDead(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	cycle()
	if got, _ := b.Jobs.GetByID(ctx, job.ID); got.Status != domain.StatusPending || got.NextRetryAt == nil {
		t.Fatalf("requeued job died on its first refusal: %+v", got)
	}
}
