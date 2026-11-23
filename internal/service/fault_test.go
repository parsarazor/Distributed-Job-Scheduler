package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
)

// Principle: fault injection. Depend on abstractions (Broker interface) so
// tests can simulate a downed RabbitMQ without any container.

// failingBroker fails the first failsLeft publishes, then recovers.
type failingBroker struct {
	inner     *broker.MemoryBroker
	failsLeft atomic.Int32
}

func (f *failingBroker) Publish(ctx context.Context, k string, p []byte, id string) error {
	if f.failsLeft.Add(-1) >= 0 {
		return errors.New("connection refused")
	}
	return f.inner.Publish(ctx, k, p, id)
}
func (f *failingBroker) Close() error { return nil }

// A broker outage must surface as MarkFailed + retry_count growth, and the
// message must still go out once the broker recovers (no silent loss).
func TestFault_BrokerOutageThenRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	inner := broker.NewMemoryBroker()
	fb := &failingBroker{inner: inner}
	fb.failsLeft.Store(1)
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, fb, nil)
	s.ReconcileThreshold = 0 // fresh rows eligible immediately (deterministic)

	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	if _, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatal(err)
	}

	if n, _ := s.ReconcileOutbox(ctx, 10); n != 0 {
		t.Fatalf("outage reconcile must publish 0, got %d", n)
	}
	aged, _ := b.Outbox.FetchUnpublished(ctx, 10, 0)
	if len(aged) != 1 || aged[0].RetryCount != 1 || aged[0].NextPublishAt == nil {
		t.Fatalf("failure must bump retry_count, got %+v", aged)
	}
	time.Sleep(time.Until(*aged[0].NextPublishAt))
	if n, _ := s.ReconcileOutbox(ctx, 10); n != 1 {
		t.Fatalf("recovered reconcile must publish 1, got %d", n)
	}
	if len(inner.Published()) != 1 {
		t.Fatal("message lost across outage")
	}
}

// A zombie worker whose lease expired must not overwrite the new owner's
// progress (the GC-pause scenario from ADR 002/009).
func TestFault_ZombieWriteAfterLeaseLoss(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := newTestScheduler()
	tn, _ := domain.NewTenant("acme", "k")
	_ = s.tenants.Create(ctx, tn)
	job, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	staleToken, err := s.jobs.ClaimJob(ctx, job.ID, "zombie", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReapExpiredLeases(ctx, 10); err != nil || n != 1 {
		t.Fatalf("reap=%d err=%v", n, err)
	}

	if _, err := s.ReportCompletion(ctx, job.ID, "zombie", domain.StatusSucceeded, staleToken, ""); err != domain.ErrFencingTokenStale {
		t.Fatalf("zombie write must be rejected, got %v", err)
	}
}

func TestFault_ReaperRetriesAndDeadLetters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)

	retryJob, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "retry", Priority: domain.PriorityDefault, MaxAttempts: 2, ScheduledAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	retryToken, err := b.Jobs.ClaimJob(ctx, retryJob.ID, "crashed", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if n, err := s.ReapExpiredLeases(ctx, 10); err != nil || n != 1 {
		t.Fatalf("first reaper pass: n=%d err=%v", n, err)
	}
	retried, err := s.GetJob(ctx, retryJob.ID)
	if err != nil || retried.Status != domain.StatusPending || retried.AttemptCount != 1 || retried.NextRetryAt == nil || retried.FencingToken != retryToken+1 {
		t.Fatalf("expired attempt must be fenced and retried: job=%+v err=%v", retried, err)
	}
	if retried.LeaseOwner != nil || retried.LeaseExpiresAt != nil {
		t.Fatalf("retried job must release its lease: %+v", retried)
	}
	outbox, err := b.Outbox.FetchUnpublished(ctx, 100, 0)
	if err != nil || len(outbox) != 0 {
		t.Fatalf("recovery must leave retry publication to ready dispatch: rows=%d err=%v", len(outbox), err)
	}
	if _, err := s.ReportCompletion(ctx, retryJob.ID, "crashed", domain.StatusSucceeded, retryToken, ""); err != domain.ErrFencingTokenStale {
		t.Fatalf("late completion from expired attempt must fail stale: %v", err)
	}

	deadJob, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "dead", Priority: domain.PriorityDefault, MaxAttempts: 1, ScheduledAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	child, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "child", DependsOn: &deadJob.ID})
	if err != nil {
		t.Fatal(err)
	}
	deadToken, err := b.Jobs.ClaimJob(ctx, deadJob.ID, "crashed", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if n, err := s.ReapExpiredLeases(ctx, 10); err != nil || n != 1 {
		t.Fatalf("second reaper pass: n=%d err=%v", n, err)
	}
	dead, err := s.GetJob(ctx, deadJob.ID)
	if err != nil || dead.Status != domain.StatusDead || dead.AttemptCount != 1 || dead.FencingToken != deadToken+1 {
		t.Fatalf("exhausted expired attempt must dead-letter: job=%+v err=%v", dead, err)
	}
	cancelled, err := s.GetJob(ctx, child.ID)
	if err != nil || cancelled.Status != domain.StatusCancelled {
		t.Fatalf("dead-lettering must cancel dependents: job=%+v err=%v", cancelled, err)
	}
}

// Cancellation must cascade transitively: grandparent -> parent -> child.
func TestFault_CancelCascadesTransitively(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := newTestScheduler()
	tn, _ := domain.NewTenant("acme", "k")
	_ = s.tenants.Create(ctx, tn)
	mk := func(name string, parent *uuid.UUID) uuid.UUID {
		j, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: name, Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, DependsOn: parent, ScheduledAt: time.Now().UTC()})
		if err != nil {
			t.Fatal(err)
		}
		return j.ID
	}
	gp := mk("gp", nil)
	p := mk("p", &gp)
	c := mk("c", &p)

	if _, err := s.CancelJob(ctx, gp, "operator"); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[uuid.UUID]string{p: "parent", c: "child"} {
		j, _ := s.GetJob(ctx, id)
		if j.Status != domain.StatusCancelled {
			t.Fatalf("%s not cascade-cancelled: %s", want, j.Status)
		}
	}
}

// Submission must reject unknown parents and self-dependency up front,
// before anything is persisted.
func TestFault_SubmitValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)

	ghost := uuid.New()
	if _, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "x", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, DependsOn: &ghost, ScheduledAt: time.Now().UTC()}); err != domain.ErrDependencyNotMet {
		t.Fatalf("unknown parent must fail, got %v", err)
	}
}
