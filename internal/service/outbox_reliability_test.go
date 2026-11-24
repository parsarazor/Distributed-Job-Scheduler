package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/memory"
)

// A permanently failing publisher must dead-letter the outbox row at the retry
// ceiling instead of retrying forever, and the dead row must never be
// re-claimed by later reconciliation passes.
func TestOutbox_DeadLettersAtRetryCeiling(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, alwaysFailBroker{}, nil)
	s.ReconcileThreshold = 0

	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	event := domain.NewOutboxEvent(uuid.New(), broker.JobsRoutingKey, json.RawMessage(`{}`))
	if err := b.Outbox.Store(ctx, event); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= domain.MaxOutboxRetries; i++ {
		if err := b.Outbox.MarkFailed(ctx, event.ID, "broker down"); err != nil {
			t.Fatal(err)
		}
		aged, err := b.Outbox.FetchUnpublished(ctx, 10, 0)
		if err != nil {
			t.Fatal(err)
		}
		var row *domain.OutboxEvent
		for _, e := range aged {
			if e.ID == event.ID {
				row = e
			}
		}
		if i < domain.MaxOutboxRetries && (row == nil || row.DeadLettered) {
			t.Fatalf("row must survive until the ceiling (attempt %d): %+v", i, row)
		}
		if i == domain.MaxOutboxRetries && (row == nil || !row.DeadLettered) {
			t.Fatalf("row must be dead-lettered at the ceiling: %+v", row)
		}
	}
	// A dead-lettered row is invisible to the claim path even with a clear
	// backoff window.
	if claimed, err := b.Outbox.FetchPublishable(ctx, 0, []uuid.UUID{}); err != nil || claimed != nil {
		t.Fatalf("dead-lettered row must not be claimable, got %v / %v", claimed, err)
	}
}

type alwaysFailBroker struct{}

func (alwaysFailBroker) Publish(context.Context, string, []byte, string) error {
	return context.DeadlineExceeded
}
func (alwaysFailBroker) Close() error { return nil }

// A claim abandoned by a crashed publisher must be reclaimed after the
// publishing lease and become publishable again.
func TestOutbox_ReclaimsStaleClaims(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	s.ReconcileThreshold = 0

	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	if _, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatal(err)
	}

	// First pass claims the row; the memory repo stamps publishing_since at
	// claim time and returns a clone.
	claimed, err := b.Outbox.FetchPublishable(ctx, 0, nil)
	if err != nil || claimed == nil {
		t.Fatalf("expected claim, got %v / %v", claimed, err)
	}
	// A claim younger than the lease must not be handed out twice.
	again, err := b.Outbox.FetchPublishable(ctx, 0, nil)
	if err != nil || again != nil {
		t.Fatalf("live claim must not be re-claimable, got %v / %v", again, err)
	}
	if n, err := b.Outbox.ReclaimStalePublishing(ctx); err != nil || n != 0 {
		t.Fatalf("fresh claim must survive reclaim: n=%d err=%v", n, err)
	}
	// A recorded failure releases the claim (retry path), so the row becomes
	// claimable again once backoff has passed — the stale-claim SQL path is
	// covered by the postgres live suite.
	if err := b.Outbox.MarkFailed(ctx, claimed.ID, "broker down"); err != nil {
		t.Fatal(err)
	}
	aged, err := b.Outbox.FetchUnpublished(ctx, 10, 0)
	if err != nil || len(aged) != 1 {
		t.Fatalf("failed row must remain inspectable: %v / %v", aged, err)
	}
}

// A job whose only announcement was dead-lettered must be recovered once the
// dead row stops counting as an active dispatch. Before the fix the row stayed
// "unpublished" to the recovery predicate, so the job stayed pending forever,
// invisible to workers while holding one of the tenant's dispatch credits.
func TestDispatchReady_RecoversJobAfterDeadLetteredAnnouncement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	// A tiny window keeps the seeded dispatch stale without sleeping.
	s.RedispatchAfter = time.Nanosecond

	tn, _ := domain.NewTenant("acme", "k")
	if err := b.Tenants.Create(ctx, tn); err != nil {
		t.Fatal(err)
	}
	job, err := domain.NewJob(tn.ID, "stranded", json.RawMessage(`{}`), domain.PriorityDefault, 3, nil, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Jobs.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := b.Jobs.MarkDispatched(ctx, job.ID, job.FencingToken); err != nil {
		t.Fatal(err)
	}
	dead := domain.NewOutboxEvent(job.ID, broker.JobsRoutingKey, json.RawMessage(`{}`))
	dead.DeadLettered = true
	dead.RetryCount = domain.MaxOutboxRetries
	if err := b.Outbox.Store(ctx, dead); err != nil {
		t.Fatal(err)
	}

	if _, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	got, err := b.Jobs.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.FencingToken != job.FencingToken+1 {
		t.Fatalf("fencing token must advance on recovery: got %d want %d", got.FencingToken, job.FencingToken+1)
	}
	if got.DispatchedAt == nil {
		t.Fatal("job must be freshly dispatched after recovery")
	}
	rows, err := b.Outbox.FetchUnpublished(ctx, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	var live, deadRows int
	for _, e := range rows {
		if e.JobID != job.ID {
			continue
		}
		if e.DeadLettered {
			deadRows++
			if e.ID != dead.ID || e.RetryCount != domain.MaxOutboxRetries {
				t.Fatalf("dead row must stay untouched for inspection: %+v", e)
			}
			continue
		}
		live++
	}
	if deadRows != 1 || live != 1 {
		t.Fatalf("want one untouched dead row and one fresh announcement, got dead=%d live=%d", deadRows, live)
	}
	claimed, err := b.Outbox.FetchPublishable(ctx, 0, nil)
	if err != nil || claimed == nil || claimed.JobID != job.ID || claimed.DeadLettered {
		t.Fatalf("fresh announcement must be publishable: %+v / %v", claimed, err)
	}
}

// A job at the dead-letter cap must be terminalized, not re-dispatched: every
// recovery pass otherwise stages another announcement that fails into another
// dead row, and the stranded job keeps consuming a dispatch credit forever.
func TestDispatchReady_TerminalizesPoisonPillAtDeadLetterCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	s.RedispatchAfter = time.Nanosecond

	tn, _ := domain.NewTenant("acme", "k")
	if err := b.Tenants.Create(ctx, tn); err != nil {
		t.Fatal(err)
	}
	job, err := domain.NewJob(tn.ID, "poison", json.RawMessage(`{}`), domain.PriorityDefault, 3, nil, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Jobs.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := b.Jobs.MarkDispatched(ctx, job.ID, job.FencingToken); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < domain.MaxDeadLetteredDispatches; i++ {
		e := domain.NewOutboxEvent(job.ID, broker.JobsRoutingKey, json.RawMessage(`{}`))
		e.DeadLettered = true
		e.RetryCount = domain.MaxOutboxRetries
		if err := b.Outbox.Store(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	got, err := b.Jobs.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusDead {
		t.Fatalf("poison pill must reach the terminal failure state, got %q", got.Status)
	}
	if got.DispatchedAt != nil {
		t.Fatal("terminalized job must not keep its dispatch marker")
	}
	rows, err := b.Outbox.FetchUnpublished(ctx, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range rows {
		if e.JobID == job.ID && !e.DeadLettered {
			t.Fatalf("terminalized job must not stage another announcement: %+v", e)
		}
	}
	events, err := b.Events.ListByJobID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	var explained bool
	for _, e := range events {
		if e.FromStatus == domain.StatusPending && e.ToStatus == domain.StatusDead &&
			e.ErrorMessage != nil && strings.Contains(*e.ErrorMessage, "outbox publish exhausted") {
			explained = true
		}
	}
	if !explained {
		t.Fatalf("terminal failure must be explained by a job event, got %+v", events)
	}
	if undispatched, err := b.Jobs.GetUndispatchedJobsForUpdate(ctx, 10); err != nil || len(undispatched) != 0 {
		t.Fatalf("terminalized job must not consume a dispatch credit: %d / %v", len(undispatched), err)
	}
}

// A job terminalized for outbox exhaustion and then requeued gets a fresh
// dead-letter budget instead of dying again on the next dispatch pass.
func TestRequeueDead_ResetsOutboxDeadLetterBudget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	s.RedispatchAfter = time.Nanosecond
	tn, _ := domain.NewTenant("acme", "k")
	if err := b.Tenants.Create(ctx, tn); err != nil {
		t.Fatal(err)
	}
	job, err := domain.NewJob(tn.ID, "poison", json.RawMessage(`{}`), domain.PriorityDefault, 3, nil, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Jobs.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := b.Jobs.MarkDispatched(ctx, job.ID, job.FencingToken); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < domain.MaxDeadLetteredDispatches; i++ {
		e := domain.NewOutboxEvent(job.ID, broker.JobsRoutingKey, json.RawMessage(`{}`))
		e.DeadLettered = true
		e.RetryCount = domain.MaxOutboxRetries
		if err := b.Outbox.Store(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequeueDead(ctx, job.ID); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if _, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatal(err)
	}
	got, err := b.Jobs.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusPending || got.DispatchedAt == nil {
		t.Fatalf("requeued job must be dispatched again, got status=%q dispatched_at=%v", got.Status, got.DispatchedAt)
	}
	rows, err := b.Outbox.FetchUnpublished(ctx, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	var history int
	for _, e := range rows {
		if e.JobID == job.ID && e.DeadLettered {
			history++
		}
	}
	if history != domain.MaxDeadLetteredDispatches {
		t.Fatalf("dead-letter history must be retained, got %d rows", history)
	}
}
