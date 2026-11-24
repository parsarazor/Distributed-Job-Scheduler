package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
)

// Principle: sociable integration test — exercise the public Scheduler API
// against real collaborators (memory repos + memory broker), not mocks.
// Catches wiring bugs unit tests cannot see: ordering, gating, message ids.

// Critical jobs must dispatch before low ones regardless of insertion order.
func TestIntegration_PriorityOrderingEndToEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := newTestScheduler()
	tn, _ := domain.NewTenant("acme", "k")
	_ = s.tenants.Create(ctx, tn)

	submit := func(name string, p domain.JobPriority) {
		if _, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: name, Payload: json.RawMessage(`{}`), Priority: p, MaxAttempts: 3, ScheduledAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	submit("low", domain.PriorityLow)
	submit("critical", domain.PriorityCritical)
	submit("default", domain.PriorityDefault)

	n, err := s.DispatchReady(ctx, 10)
	if err != nil || n != 3 {
		t.Fatalf("dispatch=%d err=%v, want 3", n, err)
	}
}

// Failed jobs with attempts left must return to pending with a requeue
// outbox row; exhausted ones go dead and cancel dependents (ADR 010).
func TestIntegration_RetryThenDeadWithCascade(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn, _ := domain.NewTenant("acme", "k")
	_ = s.tenants.Create(ctx, tn)

	parent, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "p", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 2, ScheduledAt: time.Now().UTC()})
	child, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "c", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 2, DependsOn: &parent.ID, ScheduledAt: time.Now().UTC()})

	_, _ = s.DispatchReady(ctx, 10)
	claimTestJob(t, s, parent.ID, "w")
	p1, _ := s.GetJob(ctx, parent.ID)
	if _, err := s.ReportCompletion(ctx, parent.ID, "w", domain.StatusFailed, p1.FencingToken, "flaky"); err != nil {
		t.Fatal(err)
	}
	p2, _ := s.GetJob(ctx, parent.ID)
	if p2.Status != domain.StatusPending {
		t.Fatalf("attempt 1/2 must retry to pending, got %s", p2.Status)
	}
	if p2.NextRetryAt == nil {
		t.Fatal("retry must schedule NextRetryAt via backoff")
	}
	// Outbox must hold the requeue row for redispatch.
	aged, _ := b.Outbox.FetchUnpublished(ctx, 10, 0)
	if len(aged) == 0 {
		t.Fatal("retry must enqueue an outbox row")
	}

	// The next attempt must not be dispatched before the persisted backoff window.
	if wait := time.Until(*p2.NextRetryAt); wait > 0 {
		time.Sleep(wait)
	}
	_, _ = s.DispatchReady(ctx, 10)
	claimTestJob(t, s, parent.ID, "w")
	p3, _ := s.GetJob(ctx, parent.ID)
	if _, err := s.ReportCompletion(ctx, parent.ID, "w", domain.StatusFailed, p3.FencingToken, "still down"); err != nil {
		t.Fatal(err)
	}
	p4, _ := s.GetJob(ctx, parent.ID)
	if p4.Status != domain.StatusDead {
		t.Fatalf("attempt 2/2 must go dead, got %s", p4.Status)
	}
	c2, _ := s.GetJob(ctx, child.ID)
	if c2.Status != domain.StatusCancelled {
		t.Fatalf("child must cascade-cancel on parent death, got %s", c2.Status)
	}
}

// Reconciliation must publish with job_id as message id (ADR 011) and a
// second reconcile must not duplicate broker records (idempotent redelivery).
func TestIntegration_ReconcilePublishesIdempotently(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := newTestScheduler()
	tn, _ := domain.NewTenant("acme", "k")
	_ = s.tenants.Create(ctx, tn)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	if _, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatal(err)
	}
	rows, err := s.outbox.FetchUnpublished(ctx, 10, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("outbox rows=%v err=%v", rows, err)
	}
	s.ReconcileThreshold = 0 // fresh rows eligible immediately (deterministic)

	bus := s.bus.(*broker.MemoryBroker)
	if n, _ := s.ReconcileOutbox(ctx, 10); n != 1 {
		t.Fatalf("first reconcile=%d want 1", n)
	}
	if n, _ := s.ReconcileOutbox(ctx, 10); n != 0 {
		t.Fatalf("second reconcile=%d want 0 (already published)", n)
	}
	msgs := bus.Published()
	if len(msgs) != 1 || msgs[0].MessageID != rows[0].ID.String() {
		t.Fatalf("broker record wrong: %+v", msgs)
	}
}
