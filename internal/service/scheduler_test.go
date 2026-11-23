package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
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

func claimTestJob(t *testing.T, s *Scheduler, id uuid.UUID, workerID string) int64 {
	t.Helper()
	token, err := s.jobs.ClaimJob(context.Background(), id, workerID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return token
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
	if p.Status != domain.StatusPending || p.FencingToken != 0 || p.LeaseOwner != nil || p.LeaseExpiresAt != nil {
		t.Fatalf("announcement must not claim parent: %+v", p)
	}
	token := claimTestJob(t, s, parent.ID, "w")
	if _, err := s.ReportCompletion(ctx, parent.ID, "w", domain.StatusSucceeded, token, ""); err != nil {
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

	// Claim parent, then fail it with attempts exhausted.
	token := claimTestJob(t, s, parent.ID, "w")
	if _, err := s.ReportCompletion(ctx, parent.ID, "w", domain.StatusFailed, token, "boom"); err != nil {
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

func TestCancelJobRecordsDistinctEventID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "cancel", Priority: domain.PriorityDefault, MaxAttempts: 1, ScheduledAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelJob(ctx, job.ID, "operator request"); err != nil {
		t.Fatal(err)
	}
	events, err := b.Events.ListByJobID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].ID == job.ID || events[1].ToStatus != domain.StatusCancelled {
		t.Fatalf("cancel must append a uniquely identified audit event: %+v", events)
	}
}

func TestFencingTokenRejectsZombieWrite(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestScheduler()
	tn, _ := domain.NewTenant("acme", "k")
	_ = s.tenants.Create(ctx, tn)
	job, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	token := claimTestJob(t, s, job.ID, "w")
	for _, stale := range []int64{token - 1, token + 1} {
		if _, err := s.ReportCompletion(ctx, job.ID, "w", domain.StatusSucceeded, stale, ""); err != domain.ErrFencingTokenStale {
			t.Fatalf("token %d: want ErrFencingTokenStale, got %v", stale, err)
		}
	}
}

func TestOutboxReconcileRecoversAfterProlongedOutage(t *testing.T) {
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	// A broker outage past the alert threshold must not abandon this attempt.
	event := domain.NewOutboxEvent(job.ID, broker.JobsRoutingKey, json.RawMessage(`{}`))
	past := time.Now().Add(-time.Hour)
	event.CreatedAt, event.NextPublishAt, event.RetryCount = past, &past, domain.MaxOutboxRetries
	if err := b.Outbox.Store(ctx, event); err != nil {
		t.Fatal(err)
	}
	s.ReconcileThreshold = 0
	if n, _ := s.ReconcileOutbox(ctx, 10); n != 1 {
		t.Fatalf("outbox must remain retryable after a prolonged outage, reconciled=%d", n)
	}
}

func TestWorkerRunOnce(t *testing.T) {
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	w := NewWorker("w1", s, 4, 30*time.Second)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("worker ran %d, want 1", n)
	}
}
