package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/testinfra"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Exercise the public submission -> dispatch -> confirmed outbox -> delivery ->
// worker -> Postgres completion path, including a dependency and redelivery.
func TestDurableSchedulerWorkerPipeline(t *testing.T) {
	pg := testinfra.StartPostgres(t)
	rabbit := testinfra.StartRabbitMQ(t)
	f := newFixture(t, pg)
	b, ch := rabbitCase(t, rabbit)
	f.sched = f.scheduler(f.repos.Events, f.repos.Outbox, b)
	parent := f.submit(t, "parent", 3, nil, time.Time{})
	child := f.submit(t, "child", 3, &parent.ID, time.Time{})
	if rows := unpublished(t, f); len(rows) != 0 {
		t.Fatalf("submission must not announce work before dispatch: %+v", rows)
	}
	if n, err := f.sched.DispatchReady(f.ctx, 100); err != nil || n != 1 {
		t.Fatalf("only the parent is ready: n=%d err=%v", n, err)
	}
	rows := unpublished(t, f)
	if len(rows) != 1 || rows[0].JobID != parent.ID || rows[0].RoutingKey != broker.JobsRoutingKey {
		t.Fatalf("dispatch must stage only the parent's canonical announcement: %+v", rows)
	}
	parentAnnouncement := rows[0]
	if n, err := f.sched.ReconcileOutbox(f.ctx, 100); err != nil || n != 1 {
		t.Fatalf("publish parent outbox: n=%d err=%v", n, err)
	}
	if len(unpublished(t, f)) != 0 {
		t.Fatal("confirmed parent outbox row must be marked published")
	}
	awaitQueue(t, ch, "parent announcement persisted before worker startup", func(q amqp.Queue) bool { return q.Messages == 1 && q.Consumers == 0 })

	worker := service.NewWorker("integration-worker", f.sched, 2, 10*time.Second)
	var parentRuns, childRuns atomic.Int32
	for _, job := range []*domain.Job{parent, child} {
		counter := &parentRuns
		if job.ID == child.ID {
			counter = &childRuns
		}
		worker.RegisterHandler(job.Name, func(_ context.Context, claimed *domain.Job) error {
			counter.Add(1)
			if claimed.ID != job.ID || claimed.TenantID != f.tenant.ID || claimed.Status != domain.StatusRunning || claimed.FencingToken != 1 || claimed.LeaseOwner == nil || *claimed.LeaseOwner != worker.ID() || claimed.LeaseExpiresAt == nil {
				return fmt.Errorf("handler received an invalid DB claim: %+v", claimed)
			}
			return nil
		})
	}
	type completion struct {
		id  uuid.UUID
		err error
	}
	completed := make(chan completion, 8)
	run := startConsumer(t, b, int(worker.MaxConcurrency()), func(ctx context.Context, body []byte) error {
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		err := worker.HandleMessage(ctx, body)
		var announcement domain.Job
		if decodeErr := json.Unmarshal(body, &announcement); decodeErr != nil && err == nil {
			err = decodeErr
		}
		if err == nil {
			stored, readErr := f.sched.GetJob(ctx, announcement.ID)
			if readErr != nil {
				err = readErr
			} else if stored.Status != domain.StatusSucceeded {
				err = fmt.Errorf("handler returned before successful DB completion: %+v", stored)
			}
		}
		select {
		case completed <- completion{id: announcement.ID, err: err}:
		default:
			return fmt.Errorf("unexpected extra job delivery")
		}
		return err
	})
	assertCompletion := func(id uuid.UUID) {
		t.Helper()
		got := receive(t, completed)
		if got.id != id || got.err != nil {
			t.Fatalf("want committed completion for %s, got %+v", id, got)
		}
	}
	assertCompletion(parent.ID)
	blocked, err := f.sched.GetJob(f.ctx, child.ID)
	if err != nil || blocked.Status != domain.StatusPending || blocked.DispatchedAt != nil || childRuns.Load() != 0 {
		t.Fatalf("child must wait for a new dispatch after parent completion: job=%+v runs=%d err=%v", blocked, childRuns.Load(), err)
	}
	if n, err := f.sched.DispatchReady(f.ctx, 100); err != nil || n != 1 {
		t.Fatalf("successful parent must unblock one child: n=%d err=%v", n, err)
	}
	if n, err := f.sched.ReconcileOutbox(f.ctx, 100); err != nil || n != 1 {
		t.Fatalf("publish child outbox: n=%d err=%v", n, err)
	}
	assertCompletion(child.ID)
	if err := b.Publish(f.ctx, broker.JobsRoutingKey, parentAnnouncement.Payload, parentAnnouncement.ID.String()); err != nil {
		t.Fatal(err)
	}
	assertCompletion(parent.ID)
	awaitQueue(t, ch, "all job announcements consumed", func(q amqp.Queue) bool { return q.Messages == 0 })
	run.stop(t)
	if err := worker.Wait(f.ctx); err != nil {
		t.Fatalf("drain worker: %v", err)
	}
	if parentRuns.Load() != 1 || childRuns.Load() != 1 || len(worker.ActiveJobs()) != 0 {
		t.Fatalf("each job must execute once across redelivery: parent=%d child=%d active=%v", parentRuns.Load(), childRuns.Load(), worker.ActiveJobs())
	}
	for _, job := range []*domain.Job{parent, child} {
		stored, err := f.sched.GetJob(f.ctx, job.ID)
		if err != nil || stored.Status != domain.StatusSucceeded || stored.FencingToken != 1 || stored.AttemptCount != 0 || stored.LeaseOwner != nil || stored.LeaseExpiresAt != nil {
			t.Fatalf("job completion must be durable and clear its lease: %+v err=%v", stored, err)
		}
		audit, err := f.repos.Events.ListByJobID(f.ctx, job.ID)
		if err != nil || len(audit) != 3 || audit[1].FromStatus != domain.StatusPending || audit[1].ToStatus != domain.StatusRunning || audit[2].FromStatus != domain.StatusRunning || audit[2].ToStatus != domain.StatusSucceeded {
			t.Fatalf("redelivery must not create another execution or completion audit: %+v err=%v", audit, err)
		}
	}
	if n, err := f.sched.ReconcileOutbox(f.ctx, 100); err != nil || n != 0 {
		t.Fatalf("all confirmed outbox messages must remain published: n=%d err=%v", n, err)
	}
}
