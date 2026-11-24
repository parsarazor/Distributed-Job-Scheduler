package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

type failingEventRepo struct {
	repository.JobEventRepository
	to  domain.JobStatus
	err error
}

func (r *failingEventRepo) Record(ctx context.Context, event *domain.JobEvent) error {
	if err := r.JobEventRepository.Record(ctx, event); err != nil {
		return err
	}
	if event.ToStatus == r.to {
		return r.err
	}
	return nil
}

type failingOutboxRepo struct {
	repository.OutboxRepository
	err error
}

func (r *failingOutboxRepo) Store(ctx context.Context, event *domain.OutboxEvent) error {
	if err := r.OutboxRepository.Store(ctx, event); err != nil {
		return err
	}
	return r.err
}

func TestScheduler_TransactionFailuresRollBack(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		operation  string
		attempts   int
		failStatus domain.JobStatus
		failOutbox bool
	}{
		{name: "success audit", operation: "success", attempts: 2, failStatus: domain.StatusSucceeded},
		{name: "retry audit", operation: "failure", attempts: 2, failStatus: domain.StatusPending},
		{name: "dispatch outbox", operation: "dispatch", attempts: 2, failOutbox: true},
		{name: "dead letter cascade audit", operation: "failure", attempts: 1, failStatus: domain.StatusCancelled},
		{name: "cancellation audit", operation: "cancel", attempts: 2, failStatus: domain.StatusCancelled},
		{name: "reaper retry audit", operation: "reap", attempts: 2, failStatus: domain.StatusPending},
		{name: "reaper dead letter cascade audit", operation: "reap", attempts: 1, failStatus: domain.StatusCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			s, b := newTestScheduler()
			tn := seedTenant(t, s, b)
			parent, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "parent", MaxAttempts: tc.attempts})
			if err != nil {
				t.Fatal(err)
			}
			child, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "child", DependsOn: &parent.ID})
			if err != nil {
				t.Fatal(err)
			}
			var token int64
			if tc.operation != "cancel" && tc.operation != "dispatch" {
				ttl := time.Minute
				if tc.operation == "reap" {
					ttl = time.Nanosecond
				}
				token, err = b.Jobs.ClaimJob(ctx, parent.ID, "worker", ttl)
				if err != nil {
					t.Fatal(err)
				}
			}

			beforeParent, _ := b.Jobs.GetByID(ctx, parent.ID)
			beforeChild, _ := b.Jobs.GetByID(ctx, child.ID)
			beforeParentEvents, _ := b.Events.ListByJobID(ctx, parent.ID)
			beforeChildEvents, _ := b.Events.ListByJobID(ctx, child.ID)
			beforeOutbox, _ := b.Outbox.FetchUnpublished(ctx, 100, 0)
			persistErr := errors.New("persistence failed")
			if tc.failOutbox {
				s.outbox = &failingOutboxRepo{OutboxRepository: b.Outbox, err: persistErr}
			} else {
				s.events = &failingEventRepo{JobEventRepository: b.Events, to: tc.failStatus, err: persistErr}
			}
			switch tc.operation {
			case "dispatch":
				_, err = s.DispatchReady(ctx, 100)
			case "success":
				_, err = s.ReportCompletion(ctx, parent.ID, "worker", domain.StatusSucceeded, token, "")
			case "failure":
				_, err = s.ReportCompletion(ctx, parent.ID, "worker", domain.StatusFailed, token, "failed")
			case "cancel":
				_, err = s.CancelJob(ctx, parent.ID, "operator")
			case "reap":
				_, err = s.ReapExpiredLeases(ctx, 100)
			}
			if !errors.Is(err, persistErr) {
				t.Fatalf("operation must return persistence error, got %v", err)
			}
			afterParent, _ := b.Jobs.GetByID(ctx, parent.ID)
			afterChild, _ := b.Jobs.GetByID(ctx, child.ID)
			afterParentEvents, _ := b.Events.ListByJobID(ctx, parent.ID)
			afterChildEvents, _ := b.Events.ListByJobID(ctx, child.ID)
			afterOutbox, _ := b.Outbox.FetchUnpublished(ctx, 100, 0)
			if !reflect.DeepEqual(beforeParent, afterParent) || !reflect.DeepEqual(beforeChild, afterChild) {
				t.Fatalf("failed transaction changed jobs: parent=%+v child=%+v", afterParent, afterChild)
			}
			if !reflect.DeepEqual(beforeParentEvents, afterParentEvents) || !reflect.DeepEqual(beforeChildEvents, afterChildEvents) || !reflect.DeepEqual(beforeOutbox, afterOutbox) {
				t.Fatal("failed transaction changed audit events or outbox rows")
			}
		})
	}
}
