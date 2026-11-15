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
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

// Principle: error-path table. Happy paths prove the system works; error
// paths prove it fails safely with the documented sentinel each time.
func TestScheduler_ErrorPaths(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("submit unknown tenant", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestScheduler()
		_, err := s.SubmitJob(ctx, SubmitInput{TenantID: uuid.New(), Name: "j", Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
		if err != domain.ErrTenantNotFound {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("submit normalizes priority and attempts", func(t *testing.T) {
		t.Parallel()
		s, b := newTestScheduler()
		tn := seedTenant(t, s, b)
		j, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Priority: domain.JobPriority(99), ScheduledAt: time.Now().UTC()})
		if err != nil {
			t.Fatal(err)
		}
		if j.Priority != domain.PriorityDefault || j.MaxAttempts != 3 {
			t.Fatalf("defaults not applied: %+v", j)
		}
	})

	t.Run("submit empty name rejected", func(t *testing.T) {
		t.Parallel()
		s, b := newTestScheduler()
		tn := seedTenant(t, s, b)
		if _, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()}); err == nil {
			t.Fatal("empty name must fail")
		}
	})

	t.Run("get unknown job", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestScheduler()
		if _, err := s.GetJob(ctx, uuid.New()); err != domain.ErrJobNotFound {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("cancel unknown job", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestScheduler()
		if _, err := s.CancelJob(ctx, uuid.New(), "x"); err != domain.ErrJobNotFound {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("cancel running job refused", func(t *testing.T) {
		t.Parallel()
		s, b := newTestScheduler()
		tn := seedTenant(t, s, b)
		j, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
		claimTestJob(t, s, j.ID, "w")
		if _, err := s.CancelJob(ctx, j.ID, "late"); err != domain.ErrJobCannotBeCancelled {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("report invalid final status", func(t *testing.T) {
		t.Parallel()
		s, b := newTestScheduler()
		tn := seedTenant(t, s, b)
		j, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
		if _, err := s.ReportCompletion(ctx, j.ID, "w", domain.StatusCancelled, 0, ""); err != domain.ErrInvalidTransition {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("report unknown job", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestScheduler()
		if _, err := s.ReportCompletion(ctx, uuid.New(), "w", domain.StatusSucceeded, 0, ""); err != domain.ErrJobNotFound {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("dispatch empty is zero not error", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestScheduler()
		if n, err := s.DispatchReady(ctx, 10); n != 0 || err != nil {
			t.Fatalf("got %d %v", n, err)
		}
		if n, err := s.DispatchReady(ctx, 0); n != 0 || err != nil {
			t.Fatalf("batch 0 must default safely, got %d %v", n, err)
		}
	})

	t.Run("retry-due promotes failed to pending", func(t *testing.T) {
		t.Parallel()
		s, b := newTestScheduler()
		tn := seedTenant(t, s, b)
		j, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
		// Leave the job genuinely stuck in failed (e.g. worker crashed after
		// reporting failure but before recycling): drive the repo directly.
		token := claimTestJob(t, s, j.ID, "w")
		_ = b.Jobs.TransitionStatus(ctx, j.ID, domain.StatusRunning, domain.StatusFailed, token)
		stored, _ := b.Jobs.GetByID(ctx, j.ID)
		past := time.Now().UTC().Add(-time.Second)
		stored.NextRetryAt = &past
		_ = b.Jobs.UpdateWithFencingToken(ctx, stored, stored.FencingToken)
		if n, _ := s.RetryDue(ctx, 10); n != 1 {
			t.Fatalf("retry-due=%d want 1", n)
		}
		if got, _ := s.GetJob(ctx, j.ID); got.Status != domain.StatusPending {
			t.Fatalf("recycled status=%s want pending", got.Status)
		}
	})

	t.Run("reconcile without broker skips safely", func(t *testing.T) {
		t.Parallel()
		s, b := newTestScheduler()
		s.bus = nil
		tn := seedTenant(t, s, b)
		_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
		s.ReconcileThreshold = 0
		if n, err := s.ReconcileOutbox(ctx, 10); n != 0 || err != nil {
			t.Fatalf("nil broker must skip, got %d %v", n, err)
		}
	})

	t.Run("list reflects tenant scope", func(t *testing.T) {
		t.Parallel()
		s, b := newTestScheduler()
		tn := seedTenant(t, s, b)
		_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
		jobs, total, err := s.ListJobs(ctx, tn.ID, repository.JobListFilter{Limit: 10})
		if err != nil || total != 1 || len(jobs) != 1 {
			t.Fatalf("list: %d %d %v", len(jobs), total, err)
		}
	})
}

func TestScheduler_RetryBackoffBlocksReadyDispatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), &domain.ConstantBackoff{Delay: time.Hour})
	tn := seedTenant(t, s, b)
	j, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "j", Priority: domain.PriorityDefault, MaxAttempts: 2, ScheduledAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DispatchReady(ctx, 1); err != nil {
		t.Fatal(err)
	}
	claimTestJob(t, s, j.ID, "w")
	running, _ := s.GetJob(ctx, j.ID)
	if _, err := s.ReportCompletion(ctx, j.ID, "w", domain.StatusFailed, running.FencingToken, "temporary"); err != nil {
		t.Fatal(err)
	}
	if ready, err := b.Jobs.GetReadyJobs(ctx, 10); err != nil || len(ready) != 0 {
		t.Fatalf("retry must remain unavailable until its backoff expires, ready=%d err=%v", len(ready), err)
	}
}
