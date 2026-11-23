package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/postgres"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/testinfra"
)

type fixture struct {
	ctx    context.Context
	dsn    string
	repos  *postgres.Repositories
	tenant *domain.Tenant
	bus    *recordingPublisher
	sched  *service.Scheduler
}

func newFixture(t *testing.T, server *testinfra.Postgres) *fixture {
	t.Helper()
	dsn := server.NewDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)
	pool, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		pool.Close()
	})
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	r := postgres.NewRepositories(pool)
	tenant, err := domain.NewTenant("integration", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Tenants.Create(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	f := &fixture{ctx: ctx, dsn: dsn, repos: r, tenant: tenant, bus: &recordingPublisher{}}
	f.sched = f.scheduler(r.Events, r.Outbox, f.bus)
	return f
}

func (f *fixture) scheduler(events repository.JobEventRepository, outbox repository.OutboxRepository, bus broker.Broker) *service.Scheduler {
	return service.NewScheduler(f.repos.Jobs, f.repos.Tenants, events, outbox, f.repos.Tx, bus, &domain.ConstantBackoff{Delay: time.Hour})
}

func (f *fixture) submit(t *testing.T, name string, attempts int, parent *uuid.UUID, scheduledAt time.Time) *domain.Job {
	t.Helper()
	job, err := f.sched.SubmitJob(f.ctx, service.SubmitInput{
		TenantID: f.tenant.ID, Name: name, MaxAttempts: attempts,
		Priority: domain.PriorityHigh, DependsOn: parent, ScheduledAt: scheduledAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return job
}

// This publisher intentionally records every call, so duplicate publication
// cannot be hidden by an idempotent test double.
type recordingPublisher struct {
	mu       sync.Mutex
	messages []broker.PublishedMessage
	hook     func(context.Context) error
}

func (p *recordingPublisher) Publish(ctx context.Context, key string, body []byte, id string) error {
	p.mu.Lock()
	p.messages = append(p.messages, broker.PublishedMessage{RoutingKey: key, Payload: append([]byte(nil), body...), MessageID: id})
	p.mu.Unlock()
	if p.hook != nil {
		return p.hook(ctx)
	}
	return nil
}

func (*recordingPublisher) Close() error { return nil }

func (p *recordingPublisher) published() []broker.PublishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]broker.PublishedMessage(nil), p.messages...)
}

type operationResult struct {
	n   int
	err error
}

type operation struct {
	done   chan struct{}
	result operationResult
}

func launchOperation(fn func() (int, error)) *operation {
	op := &operation{done: make(chan struct{})}
	go func() {
		n, err := fn()
		op.result = operationResult{n: n, err: err}
		close(op.done)
	}()
	return op
}

func awaitResult(t *testing.T, op *operation) operationResult {
	t.Helper()
	select {
	case <-op.done:
		if op.result.err != nil {
			t.Fatalf("concurrent operation failed: %v", op.result.err)
		}
		return op.result
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent operation blocked; expected SKIP LOCKED progress")
		return operationResult{}
	}
}

// A gate keeps one transaction open until the test has verified a competing
// operation made progress. Cleanup releases it even when an assertion fails.
type transactionGate struct {
	once    sync.Once
	opened  chan struct{}
	release chan struct{}
	stop    func()
}

func newGate(t *testing.T) *transactionGate {
	t.Helper()
	g := &transactionGate{opened: make(chan struct{}), release: make(chan struct{})}
	g.stop = sync.OnceFunc(func() { close(g.release) })
	t.Cleanup(g.stop)
	return g
}

func (g *transactionGate) wait(ctx context.Context) error {
	var err error
	g.once.Do(func() {
		close(g.opened)
		select {
		case <-g.release:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return err
}

func (g *transactionGate) await(t *testing.T) {
	t.Helper()
	select {
	case <-g.opened:
	case <-time.After(10 * time.Second):
		t.Fatal("operation did not reach the transaction gate")
	}
}

type interceptEvents struct {
	repository.JobEventRepository
	after func(context.Context, *domain.JobEvent) error
}

func (r *interceptEvents) Record(ctx context.Context, event *domain.JobEvent) error {
	if err := r.JobEventRepository.Record(ctx, event); err != nil {
		return err
	}
	return r.after(ctx, event)
}

type interceptOutbox struct {
	repository.OutboxRepository
	afterStore func(context.Context, *domain.OutboxEvent) error
	afterMark  func(context.Context, uuid.UUID) error
}

func (r *interceptOutbox) Store(ctx context.Context, event *domain.OutboxEvent) error {
	if err := r.OutboxRepository.Store(ctx, event); err != nil {
		return err
	}
	if r.afterStore != nil {
		return r.afterStore(ctx, event)
	}
	return nil
}

func (r *interceptOutbox) MarkPublished(ctx context.Context, id uuid.UUID) error {
	if err := r.OutboxRepository.MarkPublished(ctx, id); err != nil {
		return err
	}
	if r.afterMark != nil {
		return r.afterMark(ctx, id)
	}
	return nil
}

func TestPostgresScheduler(t *testing.T) {
	server := testinfra.StartPostgres(t)
	t.Run("concurrent dispatch stages each ready attempt once", func(t *testing.T) {
		f := newFixture(t, server)
		ready := make(map[uuid.UUID]bool)
		var parentID uuid.UUID
		for i := range 16 {
			tenant, err := domain.NewTenant(fmt.Sprintf("dispatch-%d", i), uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			if err := f.repos.Tenants.Create(f.ctx, tenant); err != nil {
				t.Fatal(err)
			}
			f.tenant = tenant
			job := f.submit(t, fmt.Sprintf("ready-%d", i), 3, nil, time.Now().Add(-time.Minute))
			ready[job.ID] = true
			parentID = job.ID
		}
		f.submit(t, "future", 3, nil, time.Now().Add(time.Hour))
		f.submit(t, "blocked-child", 3, &parentID, time.Time{})
		retrying := f.submit(t, "retry-not-due", 3, nil, time.Time{})
		next := time.Now().Add(time.Hour)
		retrying.NextRetryAt = &next
		if err := f.repos.Jobs.UpdateWithFencingToken(f.ctx, retrying, retrying.FencingToken); err != nil {
			t.Fatal(err)
		}
		if rows := unpublished(t, f); len(rows) != 0 {
			t.Fatalf("submission must not create an outbox announcement: %d rows", len(rows))
		}
		gate := newGate(t)
		staging := &interceptOutbox{OutboxRepository: f.repos.Outbox, afterStore: func(ctx context.Context, _ *domain.OutboxEvent) error { return gate.wait(ctx) }}
		first := f.scheduler(f.repos.Events, staging, f.bus)
		done := launchOperation(func() (int, error) { return first.DispatchReady(f.ctx, 1) })
		defer func() { gate.stop(); awaitResult(t, done) }()
		gate.await(t)
		competing := launchOperation(func() (int, error) { return f.sched.DispatchReady(f.ctx, 100) })
		if got := awaitResult(t, competing).n; got != len(ready)-1 {
			t.Fatalf("competing dispatcher must skip one held row and stage the others: %d", got)
		}
		gate.stop()
		// Wait for the first commit before inspecting the full outbox.
		result := awaitResult(t, done)
		if result.n != 1 {
			t.Fatalf("held dispatcher staged %d rows, want 1", result.n)
		}
		if n, err := f.sched.DispatchReady(f.ctx, 100); err != nil || n != 0 {
			t.Fatalf("each ready attempt must already be staged: n=%d err=%v", n, err)
		}
		rows := unpublished(t, f)
		seen := make(map[uuid.UUID]int)
		for _, row := range rows {
			var announcement domain.Job
			if err := json.Unmarshal(row.Payload, &announcement); err != nil {
				t.Fatal(err)
			}
			if row.RoutingKey != "jobs.execute" || !ready[row.JobID] || announcement.ID != row.JobID || announcement.FencingToken != 0 || announcement.AttemptCount != 0 {
				t.Fatalf("outbox contains an ineligible or malformed announcement: %+v payload=%s", row, row.Payload)
			}
			seen[row.JobID]++
		}
		if len(rows) != len(ready) || len(seen) != len(ready) {
			t.Fatalf("want exactly one announcement per ready job, rows=%d unique=%d ready=%d", len(rows), len(seen), len(ready))
		}
		for id := range ready {
			stored, err := f.sched.GetJob(f.ctx, id)
			if err != nil || stored.DispatchedAt == nil || stored.Status != domain.StatusPending || seen[id] != 1 {
				t.Fatalf("dispatch must mark the pending attempt once: job=%+v announcements=%d err=%v", stored, seen[id], err)
			}
		}
		if len(f.bus.published()) != 0 {
			t.Fatal("DispatchReady must only stage the outbox")
		}
	})

	t.Run("concurrent reapers invalidate each lease once", func(t *testing.T) {
		f := newFixture(t, server)
		parents, children := make([]*domain.Job, 8), make([]*domain.Job, 8)
		for i := range parents {
			attempts := 3
			if i%2 == 0 {
				attempts = 1
			}
			parents[i] = f.submit(t, fmt.Sprintf("abandoned-%d", i), attempts, nil, time.Time{})
			children[i] = f.submit(t, "child", 3, &parents[i].ID, time.Time{})
		}
		if n, err := f.sched.DispatchReady(f.ctx, 100); err != nil || n != len(parents) {
			t.Fatalf("stage parent attempts: n=%d err=%v", n, err)
		}
		for _, job := range parents {
			if token, err := f.repos.Jobs.ClaimJob(f.ctx, job.ID, "dead-worker", time.Nanosecond); err != nil || token != 1 {
				t.Fatalf("claim abandoned attempt: token=%d err=%v", token, err)
			}
		}
		gate := newGate(t)
		events := &interceptEvents{JobEventRepository: f.repos.Events, after: func(ctx context.Context, event *domain.JobEvent) error {
			if event.ToStatus == domain.StatusFailed {
				return gate.wait(ctx)
			}
			return nil
		}}
		first := f.scheduler(events, f.repos.Outbox, f.bus)
		done := launchOperation(func() (int, error) { return first.ReapExpiredLeases(f.ctx, 1) })
		defer func() { gate.stop(); awaitResult(t, done) }()
		gate.await(t)
		other := launchOperation(func() (int, error) { return f.sched.ReapExpiredLeases(f.ctx, 100) })
		if n := awaitResult(t, other).n; n != len(parents)-1 {
			t.Fatalf("competing reaper must process all unheld leases: %d", n)
		}
		gate.stop()
		result := awaitResult(t, done)
		if result.n != 1 {
			t.Fatalf("held reaper processed %d leases", result.n)
		}
		if n, err := f.sched.ReapExpiredLeases(f.ctx, 100); err != nil || n != 0 {
			t.Fatalf("reaping again must do nothing: n=%d err=%v", n, err)
		}
		for i, parent := range parents {
			job, err := f.sched.GetJob(f.ctx, parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := domain.StatusPending
			if parent.MaxAttempts == 1 {
				want = domain.StatusDead
			}
			if job.Status != want || job.FencingToken != 2 || job.AttemptCount != 1 || job.LeaseOwner != nil || job.LeaseExpiresAt != nil {
				t.Fatalf("reaper must invalidate exactly one attempt: %+v", job)
			}
			if want == domain.StatusPending && (job.NextRetryAt == nil || !job.NextRetryAt.After(time.Now()) || job.DispatchedAt != nil) {
				t.Fatalf("retry must have backoff and clear the old dispatch marker: %+v", job)
			}
			audit, err := f.repos.Events.ListByJobID(f.ctx, job.ID)
			if err != nil || len(audit) != 3 || audit[1].FromStatus != domain.StatusRunning || audit[1].ToStatus != domain.StatusFailed || audit[1].WorkerID == nil || *audit[1].WorkerID != "dead-worker" || audit[2].ToStatus != want {
				t.Fatalf("want one lease failure and one retry/dead audit: %+v err=%v", audit, err)
			}
			child, err := f.sched.GetJob(f.ctx, children[i].ID)
			if err != nil {
				t.Fatal(err)
			}
			if want == domain.StatusDead && child.Status != domain.StatusCancelled || want == domain.StatusPending && child.Status != domain.StatusPending {
				t.Fatalf("dependent must follow its parent's terminal result: parent=%s child=%+v", want, child)
			}
			if _, err := f.sched.ReportCompletion(f.ctx, job.ID, "dead-worker", domain.StatusSucceeded, 1, ""); !errors.Is(err, domain.ErrFencingTokenStale) {
				t.Fatalf("reaped holder must not complete: %v", err)
			}
		}
	})

	t.Run("concurrent outbox publishers hold locks through publish", func(t *testing.T) {
		f := newFixture(t, server)
		for i := range 8 {
			f.submit(t, fmt.Sprintf("publish-%d", i), 3, nil, time.Time{})
		}
		if _, err := f.sched.DispatchReady(f.ctx, 100); err != nil {
			t.Fatal(err)
		}
		before := unpublished(t, f)
		gate := newGate(t)
		firstBus := &recordingPublisher{hook: gate.wait}
		first := f.scheduler(f.repos.Events, f.repos.Outbox, firstBus)
		done := launchOperation(func() (int, error) { return first.ReconcileOutbox(f.ctx, 1) })
		defer func() { gate.stop(); awaitResult(t, done) }()
		gate.await(t)
		other := launchOperation(func() (int, error) { return f.sched.ReconcileOutbox(f.ctx, 100) })
		if n := awaitResult(t, other).n; n != len(before)-1 {
			t.Fatalf("competing publisher must skip the unconfirmed held row: %d", n)
		}
		gate.stop()
		result := awaitResult(t, done)
		if result.n != 1 {
			t.Fatalf("held publisher published %d rows", result.n)
		}
		if rows := unpublished(t, f); len(rows) != 0 {
			t.Fatalf("confirmed rows must all be marked published: %d", len(rows))
		}
		messages := append(firstBus.published(), f.bus.published()...)
		counts := make(map[string]int)
		for _, message := range messages {
			counts[message.MessageID]++
		}
		if len(messages) != len(before) {
			t.Fatalf("non-idempotent publisher recorded %d calls for %d rows", len(messages), len(before))
		}
		for _, event := range before {
			if counts[event.ID.String()] != 1 {
				t.Fatalf("outbox event %s was published %d times", event.ID, counts[event.ID.String()])
			}
		}
		if n, err := f.sched.ReconcileOutbox(f.ctx, 100); err != nil || n != 0 {
			t.Fatalf("published rows must not be sent again: n=%d err=%v", n, err)
		}
	})

	t.Run("failed publish dead-letters at the retry ceiling", func(t *testing.T) {
		f := newFixture(t, server)
		f.submit(t, "publish-retry", 3, nil, time.Time{})
		if _, err := f.sched.DispatchReady(f.ctx, 100); err != nil {
			t.Fatal(err)
		}
		failedBus := &recordingPublisher{hook: func(context.Context) error { return errors.New("confirmation unavailable") }}
		failed := f.scheduler(f.repos.Events, f.repos.Outbox, failedBus)
		if n, err := failed.ReconcileOutbox(f.ctx, 100); err != nil || n != 0 {
			t.Fatalf("failed publish must not count as published: n=%d err=%v", n, err)
		}
		rows := unpublished(t, f)
		if len(rows) != 1 || rows[0].Published || rows[0].DeadLettered || rows[0].RetryCount != 1 || rows[0].NextPublishAt == nil || rows[0].LastError == nil || *rows[0].LastError != "broker publish failed" {
			t.Fatalf("failed publish must retain one retryable row and its error: %+v", rows)
		}
		if n, err := f.sched.ReconcileOutbox(f.ctx, 100); err != nil || n != 0 {
			t.Fatalf("retry must respect its backoff: n=%d err=%v", n, err)
		}
		conn := repairSQL(t, f)
		for range domain.MaxOutboxRetries - 1 {
			// Advance only the disposable row's retry clock, so the test crosses
			// the ceiling without waiting through exponential delays.
			if _, err := conn.Exec(f.ctx, `UPDATE job_outbox SET next_publish_at=clock_timestamp()-interval '1 second' WHERE NOT published AND NOT dead_lettered`); err != nil {
				t.Fatal(err)
			}
			if n, err := failed.ReconcileOutbox(f.ctx, 1); err != nil || n != 0 {
				t.Fatalf("failed retry=%d err=%v", n, err)
			}
		}
		attempts := len(failedBus.published())
		if attempts != domain.MaxOutboxRetries {
			t.Fatalf("publish attempts=%d want the ceiling of %d", attempts, domain.MaxOutboxRetries)
		}
		retained := unpublished(t, f)
		if len(retained) != 1 || !retained[0].DeadLettered || retained[0].Published || retained[0].RetryCount != domain.MaxOutboxRetries || retained[0].ID != rows[0].ID {
			t.Fatalf("retry ceiling must dead-letter the event rather than abandon it: %+v", retained)
		}
		if _, err := conn.Exec(f.ctx, `UPDATE job_outbox SET next_publish_at=clock_timestamp()-interval '1 second'`); err != nil {
			t.Fatal(err)
		}
		if n, err := f.sched.ReconcileOutbox(f.ctx, 100); err != nil || n != 0 {
			t.Fatalf("dead-lettered event must not be published again: n=%d err=%v", n, err)
		}
		if len(f.bus.published()) != 0 || len(failedBus.published()) != attempts {
			t.Fatalf("dead-lettered event was re-published: healthy=%d failed=%d", len(f.bus.published()), len(failedBus.published()))
		}
		if again := unpublished(t, f); len(again) != 1 || !again[0].DeadLettered || again[0].RetryCount != domain.MaxOutboxRetries {
			t.Fatalf("dead-lettered event must remain inspectable and unchanged: %+v", again)
		}
	})

	t.Run("atomic rollback after real writes", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			operation string
			attempts  int
			failAt    domain.JobStatus
		}{
			{"submission audit", "submit", 3, domain.StatusPending},
			{"success audit", "success", 3, domain.StatusSucceeded},
			{"retry audit", "failure", 3, domain.StatusPending},
			{"dead child cascade audit", "failure", 1, domain.StatusCancelled},
			{"cancel child cascade audit", "cancel", 3, domain.StatusCancelled},
			{"reaper retry audit", "reap", 3, domain.StatusPending},
			{"reaper dead cascade audit", "reap", 1, domain.StatusCancelled},
			{"second dispatch outbox write", "dispatch", 3, ""},
			{"published marker", "reconcile", 3, ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := newFixture(t, server)
				parent := f.submit(t, "parent", tc.attempts, nil, time.Time{})
				f.submit(t, "child", 3, &parent.ID, time.Time{})
				var token int64
				if tc.operation == "success" || tc.operation == "failure" || tc.operation == "reap" {
					ttl := time.Minute
					if tc.operation == "reap" {
						ttl = time.Nanosecond
					}
					var err error
					token, err = f.repos.Jobs.ClaimJob(f.ctx, parent.ID, "owner", ttl)
					if err != nil {
						t.Fatal(err)
					}
				}
				if tc.operation == "dispatch" {
					f.submit(t, "second-ready", 3, nil, time.Time{})
				}
				if tc.operation == "reconcile" {
					if _, err := f.sched.DispatchReady(f.ctx, 100); err != nil {
						t.Fatal(err)
					}
				}
				before := snapshot(t, f)
				injected := errors.New("injected persistence failure after write")
				events := &interceptEvents{JobEventRepository: f.repos.Events, after: func(_ context.Context, event *domain.JobEvent) error {
					if event.ToStatus == tc.failAt {
						// Let parent cancellation persist before failing the child audit.
						if tc.operation == "cancel" && event.JobID == parent.ID {
							return nil
						}
						return injected
					}
					return nil
				}}
				stores := 0
				outbox := &interceptOutbox{OutboxRepository: f.repos.Outbox, afterStore: func(context.Context, *domain.OutboxEvent) error {
					stores++
					if tc.operation == "dispatch" && stores == 2 {
						return injected
					}
					return nil
				}, afterMark: func(context.Context, uuid.UUID) error {
					if tc.operation == "reconcile" {
						return injected
					}
					return nil
				}}
				s := f.scheduler(events, outbox, f.bus)
				var err error
				switch tc.operation {
				case "submit":
					_, err = s.SubmitJob(f.ctx, service.SubmitInput{TenantID: f.tenant.ID, Name: "rolled-back"})
				case "success":
					_, err = s.ReportCompletion(f.ctx, parent.ID, "owner", domain.StatusSucceeded, token, "")
				case "failure":
					_, err = s.ReportCompletion(f.ctx, parent.ID, "owner", domain.StatusFailed, token, "handler failed")
				case "cancel":
					_, err = s.CancelJob(f.ctx, parent.ID, "operator")
				case "reap":
					_, err = s.ReapExpiredLeases(f.ctx, 100)
				case "dispatch":
					_, err = s.DispatchReady(f.ctx, 100)
				case "reconcile":
					_, err = s.ReconcileOutbox(f.ctx, 100)
				}
				if !errors.Is(err, injected) {
					t.Fatalf("operation must return injected failure: %v", err)
				}
				after := snapshot(t, f)
				if tc.operation != "reconcile" {
					if !reflect.DeepEqual(before, after) {
						t.Fatalf("failed transaction changed jobs, audits or outbox:\nbefore=%+v\nafter=%+v", before, after)
					}
					return
				}
				// The publish marker is a second committed phase: a failed
				// MarkPublished may leave only the committed publishing_since
				// claim, never published state or a partial outcome.
				if !reflect.DeepEqual(before.jobs, after.jobs) || !reflect.DeepEqual(before.events, after.events) {
					t.Fatalf("failed marker leaked job or audit state:\nbefore=%+v\nafter=%+v", before, after)
				}
				if len(before.outbox) != 1 || len(after.outbox) != 1 {
					t.Fatalf("publish outcome lost its outbox row: before=%d after=%d", len(before.outbox), len(after.outbox))
				}
				claimed, original := after.outbox[0], *before.outbox[0]
				if claimed.ID != original.ID || claimed.Published || claimed.DeadLettered || claimed.PublishingSince == nil {
					t.Fatalf("failed marker must leave the row unpublished under its claim: %+v", claimed)
				}
				expected := original
				expected.PublishingSince = claimed.PublishingSince
				if !reflect.DeepEqual(&expected, claimed) {
					t.Fatalf("failed marker changed state beyond the publishing lease:\nwant=%+v\ngot=%+v", &expected, claimed)
				}
				if msgs := f.bus.published(); len(msgs) != 1 || msgs[0].MessageID != claimed.ID.String() {
					t.Fatalf("failed marker must not roll back the broker publish: %+v", msgs)
				}
				if n, err := f.sched.ReconcileOutbox(f.ctx, 100); err != nil || n != 0 {
					t.Fatalf("active claim was reclaimed prematurely: n=%d err=%v", n, err)
				}
				if _, err := repairSQL(t, f).Exec(f.ctx, `UPDATE job_outbox SET publishing_since=clock_timestamp()-make_interval(secs => $1::double precision) WHERE id=$2`, (domain.PublishingLease + time.Second).Seconds(), claimed.ID); err != nil {
					t.Fatal(err)
				}
				if reclaimed, err := f.repos.Outbox.ReclaimStalePublishing(f.ctx); err != nil || reclaimed != 1 {
					t.Fatalf("expired claim reclaimed=%d err=%v", reclaimed, err)
				}
				if n, err := f.sched.ReconcileOutbox(f.ctx, 100); err != nil || n != 1 {
					t.Fatalf("reclaimed announcement republish=%d err=%v", n, err)
				}
				if left := unpublished(t, f); len(left) != 0 {
					t.Fatalf("reclaimed announcement still unpublished: %+v", left)
				}
				if msgs := f.bus.published(); len(msgs) != 2 || msgs[0].MessageID != msgs[1].MessageID {
					t.Fatalf("redelivery must preserve the message identity: %+v", msgs)
				}
			})
		}
	})

	t.Run("message handling waits for committed completion", func(t *testing.T) {
		f := newFixture(t, server)
		job := f.submit(t, "synchronous", 3, nil, time.Time{})
		if _, err := f.sched.DispatchReady(f.ctx, 100); err != nil {
			t.Fatal(err)
		}
		body := unpublished(t, f)[0].Payload
		completion := newGate(t)
		events := &interceptEvents{JobEventRepository: f.repos.Events, after: func(ctx context.Context, event *domain.JobEvent) error {
			if event.ToStatus == domain.StatusSucceeded {
				return completion.wait(ctx)
			}
			return nil
		}}
		s := f.scheduler(events, f.repos.Outbox, f.bus)
		worker := service.NewWorker("worker", s, 2, 20*time.Second)
		handler := newGate(t)
		var executions atomic.Int32
		worker.RegisterHandler("synchronous", func(ctx context.Context, _ *domain.Job) error {
			executions.Add(1)
			return handler.wait(ctx)
		})
		done := launchOperation(func() (int, error) { return 0, worker.HandleMessage(f.ctx, body) })
		defer func() { handler.stop(); completion.stop(); awaitResult(t, done) }()
		handler.await(t)
		select {
		case <-done.done:
			t.Fatalf("HandleMessage returned before execution completed: %+v", done.result)
		default:
		}
		handler.stop()
		completion.await(t)
		stored, err := s.GetJob(f.ctx, job.ID)
		if err != nil || stored.Status != domain.StatusRunning || stored.FencingToken != 1 {
			t.Fatalf("uncommitted completion must remain invisible: job=%+v err=%v", stored, err)
		}
		select {
		case <-done.done:
			t.Fatalf("HandleMessage returned before the completion transaction committed: %+v", done.result)
		default:
		}
		completion.stop()
		awaitResult(t, done)
		stored, err = s.GetJob(f.ctx, job.ID)
		if err != nil || stored.Status != domain.StatusSucceeded || stored.LeaseOwner != nil || stored.LeaseExpiresAt != nil {
			t.Fatalf("successful HandleMessage must have committed completion: job=%+v err=%v", stored, err)
		}
		if err := worker.HandleMessage(f.ctx, body); err != nil || executions.Load() != 1 {
			t.Fatalf("redelivery must not reexecute a finished attempt: executions=%d err=%v", executions.Load(), err)
		}
		if err := worker.Wait(f.ctx); err != nil || len(worker.ActiveJobs()) != 0 {
			t.Fatalf("worker must drain completed execution: active=%v err=%v", worker.ActiveJobs(), err)
		}
	})
}

func unpublished(t *testing.T, f *fixture) []*domain.OutboxEvent {
	t.Helper()
	rows, err := f.repos.Outbox.FetchUnpublished(f.ctx, 10000, 0)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

type databaseSnapshot struct {
	jobs   []*domain.Job
	events map[uuid.UUID][]*domain.JobEvent
	outbox []*domain.OutboxEvent
}

func snapshot(t *testing.T, f *fixture) databaseSnapshot {
	t.Helper()
	jobs, _, err := f.repos.Jobs.ListByTenant(f.ctx, f.tenant.ID, repository.JobListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID.String() < jobs[j].ID.String() })
	events := make(map[uuid.UUID][]*domain.JobEvent, len(jobs))
	for _, job := range jobs {
		audit, err := f.repos.Events.ListByJobID(f.ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		events[job.ID] = audit
	}
	return databaseSnapshot{jobs: jobs, events: events, outbox: unpublished(t, f)}
}
