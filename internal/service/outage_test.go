package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// outageControl fails every renewal, lookup and completion while down, as a
// restarting scheduler does.
type outageControl struct {
	JobControl
	down  atomic.Bool
	stall atomic.Bool // renewals hang until their deadline while down
}

func (c *outageControl) unavailable() error {
	return status.Error(codes.Unavailable, "scheduler restarting")
}

func (c *outageControl) RenewLease(ctx context.Context, id uuid.UUID, worker string, token int64, ttl time.Duration) error {
	if c.down.Load() {
		if c.stall.Load() {
			<-ctx.Done()
		}
		return c.unavailable()
	}
	return c.JobControl.RenewLease(ctx, id, worker, token, ttl)
}

func (c *outageControl) ReportCompletion(ctx context.Context, id uuid.UUID, worker string, final domain.JobStatus, token int64, msg string) error {
	if c.down.Load() {
		return c.unavailable()
	}
	return c.JobControl.ReportCompletion(ctx, id, worker, final, token, msg)
}

func (c *outageControl) GetExecutionJob(ctx context.Context, id uuid.UUID, worker string, token int64) (*domain.Job, error) {
	if c.down.Load() {
		return nil, c.unavailable()
	}
	return c.JobControl.GetExecutionJob(ctx, id, worker, token)
}

func TestWorker_TransientRenewalFailureKeepsHandlerRunning(t *testing.T) {
	t.Parallel()
	s, _, _ := newWiredPair()
	ctl := &outageControl{JobControl: NewRemoteJobControl(dialScheduler(t, s, workerTestKey))}
	w := NewRemoteWorker("blip-worker", ctl, 1, 600*time.Millisecond)
	w.RegisterHandler("work", func(ctx context.Context, _ *domain.Job) error {
		ctl.down.Store(true)
		select { // spans a failed renewal at ttl/3 but ends before the local deadline
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(300 * time.Millisecond):
		}
		ctl.down.Store(false)
		return nil
	})
	job, announcement := submitAndAnnounce(t, s, "work", `{}`)
	if err := w.HandleMessage(context.Background(), announcement); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if stored, _ := s.GetJob(context.Background(), job.ID); stored.Status != domain.StatusSucceeded || stored.AttemptCount != 0 {
		t.Fatalf("a renewal blip must not abort the attempt: %+v", stored)
	}
}

func TestWorker_CompletionSurvivesSchedulerRestart(t *testing.T) {
	t.Parallel()
	s, _, _ := newWiredPair()
	ctl := &outageControl{JobControl: NewRemoteJobControl(dialScheduler(t, s, workerTestKey))}
	const ttl = 200 * time.Millisecond
	w := NewRemoteWorker("restart-worker", ctl, 1, ttl)
	w.SetCompletionWindow(5 * time.Second)
	var runs atomic.Int32
	w.RegisterHandler("work", func(context.Context, *domain.Job) error {
		runs.Add(1)
		ctl.down.Store(true)
		return nil
	})
	job, announcement := submitAndAnnounce(t, s, "work", `{}`)
	go func() {
		// The outage outlasts the lease; the restarted scheduler grants grace
		// before its reaper runs, then accepts the buffered result.
		time.Sleep(3 * ttl)
		if n, err := s.GrantLeaseGrace(context.Background(), time.Minute); err != nil || n != 1 {
			t.Errorf("grace: n=%d err=%v", n, err)
		}
		ctl.down.Store(false)
	}()
	if err := w.HandleMessage(context.Background(), announcement); err != nil {
		t.Fatalf("handle: %v", err)
	}
	stored, _ := s.GetJob(context.Background(), job.ID)
	if stored.Status != domain.StatusSucceeded || stored.AttemptCount != 0 || runs.Load() != 1 {
		t.Fatalf("result lost across restart: status=%s attempts=%d runs=%d", stored.Status, stored.AttemptCount, runs.Load())
	}
	if n, _ := s.ReapExpiredLeases(context.Background(), 10); n != 0 {
		t.Fatalf("reaper re-ran a completed job: %d", n)
	}
}

func TestGrantLeaseGrace_DisabledByZero(t *testing.T) {
	t.Parallel()
	s, _, _ := newWiredPair()
	if n, err := s.GrantLeaseGrace(context.Background(), 0); n != 0 || err != nil {
		t.Fatalf("zero grace must be a no-op: %d %v", n, err)
	}
}

func TestWorker_StalledRenewalKeepsFinishedResult(t *testing.T) {
	t.Parallel()
	s, _, _ := newWiredPair()
	ctl := &outageControl{JobControl: NewRemoteJobControl(dialScheduler(t, s, workerTestKey))}
	ctl.stall.Store(true)
	const ttl = 300 * time.Millisecond
	w := NewRemoteWorker("stall-worker", ctl, 1, ttl)
	w.SetCompletionWindow(5 * time.Second)
	var runs atomic.Int32
	w.RegisterHandler("work", func(context.Context, *domain.Job) error {
		runs.Add(1)
		ctl.down.Store(true)
		time.Sleep(ttl / 2) // the renewal at ttl/3 is now blocked until the deadline
		return nil
	})
	job, announcement := submitAndAnnounce(t, s, "work", `{}`)
	go func() {
		time.Sleep(3 * ttl)
		if _, err := s.GrantLeaseGrace(context.Background(), time.Minute); err != nil {
			t.Errorf("grace: %v", err)
		}
		ctl.down.Store(false)
	}()
	if err := w.HandleMessage(context.Background(), announcement); err != nil {
		t.Fatalf("handle: %v", err)
	}
	stored, _ := s.GetJob(context.Background(), job.ID)
	if stored.Status != domain.StatusSucceeded || stored.AttemptCount != 0 || runs.Load() != 1 {
		t.Fatalf("finished result discarded: status=%s attempts=%d runs=%d", stored.Status, stored.AttemptCount, runs.Load())
	}
}
