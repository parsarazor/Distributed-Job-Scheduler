package integration_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/service"
	"github.com/parsarazor/distributed-job-scheduler/internal/testinfra"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDurableReplacementWorkerFencesResumedOriginal(t *testing.T) {
	pg, rabbit := testinfra.StartPostgres(t), testinfra.StartRabbitMQ(t)
	f := newFixture(t, pg)
	b, ch := rabbitCase(t, rabbit)
	f.sched = service.NewScheduler(f.repos.Jobs, f.repos.Tenants, f.repos.Events, f.repos.Outbox, f.repos.Tx, b, &domain.ConstantBackoff{Delay: 0})
	job := f.submit(t, "failover", 3, nil, time.Time{})
	declareJobQueues(t, ch, job.Name)
	if n, err := f.sched.DispatchReady(f.ctx, 1); n != 1 || err != nil {
		t.Fatalf("dispatch=%d err=%v", n, err)
	}
	original := unpublished(t, f)[0]
	if n, err := f.sched.ReconcileOutbox(f.ctx, 1); n != 1 || err != nil {
		t.Fatalf("publish=%d err=%v", n, err)
	}
	old := service.NewWorker("original", f.sched, 1, 200*time.Millisecond)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	old.RegisterHandler(job.Name, func(context.Context, *domain.Job) error { close(started); <-release; return nil })
	oldDone := make(chan error, 1)
	oldRun := startConsumer(t, b, 1, func(ctx context.Context, payload []byte) error {
		err := old.HandleMessage(ctx, payload)
		oldDone <- err
		return err
	}, job.Name)
	old.SetIntakeCancel(oldRun.cancel)
	receive(t, started)
	old.CancelAll() // Simulate a process that stopped renewing but resumes later.
	select {
	case <-time.After(250 * time.Millisecond):
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	if n, err := f.sched.ReapExpiredLeases(f.ctx, 1); n != 1 || err != nil {
		t.Fatalf("reap=%d err=%v", n, err)
	}
	if n, err := f.sched.DispatchReady(f.ctx, 1); n != 1 || err != nil {
		t.Fatalf("redispatch=%d err=%v", n, err)
	}
	retry := unpublished(t, f)[0]
	if retry.ID == original.ID {
		t.Fatal("retry reused the original announcement identity")
	}
	replacement := service.NewWorker("replacement", f.sched, 1, time.Second)
	var runs atomic.Int32
	replacement.RegisterHandler(job.Name, func(_ context.Context, j *domain.Job) error {
		runs.Add(1)
		if j.AttemptCount != 1 || j.FencingToken != 3 {
			return errors.New("replacement received wrong attempt identity")
		}
		return nil
	})
	// Replay the old announcement while the job is pending again.
	if err := replacement.HandleMessage(f.ctx, original.Payload); err != nil || runs.Load() != 0 {
		t.Fatalf("old announcement ran: %v", err)
	}
	if n, err := f.sched.ReconcileOutbox(f.ctx, 1); n != 1 || err != nil {
		t.Fatalf("publish replacement=%d err=%v", n, err)
	}
	replacementBus, _ := rabbitBroker(t, rabbit)
	completed := make(chan error, 4)
	newRun := startConsumer(t, replacementBus, 1, func(ctx context.Context, payload []byte) error {
		err := replacement.HandleMessage(ctx, payload)
		completed <- err
		return err
	}, job.Name)
	if err := receive(t, completed); err != nil {
		t.Fatal(err)
	}
	stored, err := f.sched.GetJob(f.ctx, job.ID)
	if err != nil || stored.Status != domain.StatusSucceeded || stored.AttemptCount != 1 || stored.FencingToken != 3 {
		t.Fatalf("replacement result=%+v err=%v", stored, err)
	}
	once.Do(func() { close(release) })
	if err := receive(t, oldDone); !errors.Is(err, domain.ErrFencingTokenStale) && !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("resumed original was not fenced: %v", err)
	}
	oldRun.stop(t)
	awaitNamedQueue(t, ch, broker.JobQueue(job.Name), "replacement consumed obsolete redelivery", func(q amqp.Queue) bool { return q.Messages == 0 })
	newRun.stop(t)
	if runs.Load() != 1 {
		t.Fatalf("replacement ran %d times", runs.Load())
	}
	events, err := f.repos.Events.ListByJobID(f.ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	successes := 0
	for _, e := range events {
		if e.ToStatus == domain.StatusSucceeded {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("late completion changed audit: %+v", events)
	}
}

func TestDurableLostQueueReannouncesPendingJob(t *testing.T) {
	pg, rabbit := testinfra.StartPostgres(t), testinfra.StartRabbitMQ(t)
	f := newFixture(t, pg)
	b, ch := rabbitCase(t, rabbit)
	f.sched = f.scheduler(f.repos.Events, f.repos.Outbox, b)
	job := f.submit(t, "queue-loss", 3, nil, time.Time{})
	declareJobQueues(t, ch, job.Name)
	if n, err := f.sched.DispatchReady(f.ctx, 1); n != 1 || err != nil {
		t.Fatalf("dispatch=%d err=%v", n, err)
	}
	old := unpublished(t, f)[0]
	if n, err := f.sched.ReconcileOutbox(f.ctx, 1); n != 1 || err != nil {
		t.Fatalf("publish=%d err=%v", n, err)
	}
	if _, err := ch.QueueDelete(broker.JobQueue(job.Name), false, false, false); err != nil {
		t.Fatal(err)
	}
	recovered, _ := rabbitBroker(t, rabbit)
	declareJobQueues(t, ch, job.Name) // A restarted worker recreates its empty queue.
	f.sched = f.scheduler(f.repos.Events, f.repos.Outbox, recovered)
	f.sched.RedispatchAfter = time.Nanosecond
	if n, err := f.sched.DispatchReady(f.ctx, 1); n != 1 || err != nil {
		t.Fatalf("repair=%d err=%v", n, err)
	}
	if n, err := f.sched.ReconcileOutbox(f.ctx, 1); n != 1 || err != nil {
		t.Fatalf("republish=%d err=%v", n, err)
	}
	worker := service.NewWorker("after-queue-loss", f.sched, 1, time.Minute)
	var runs atomic.Int32
	worker.RegisterHandler(job.Name, func(context.Context, *domain.Job) error { runs.Add(1); return nil })
	done := make(chan error, 2)
	run := startConsumer(t, recovered, 1, func(ctx context.Context, payload []byte) error {
		err := worker.HandleMessage(ctx, payload)
		done <- err
		return err
	}, job.Name)
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	if err := worker.HandleMessage(f.ctx, old.Payload); err != nil {
		t.Fatal(err)
	}
	run.stop(t)
	stored, err := f.sched.GetJob(f.ctx, job.ID)
	if err != nil || stored.Status != domain.StatusSucceeded || stored.AttemptCount != 0 || runs.Load() != 1 {
		t.Fatalf("lost queue recovery=%+v runs=%d err=%v", stored, runs.Load(), err)
	}
}
