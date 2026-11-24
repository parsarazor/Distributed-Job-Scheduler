package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/memory"
	"github.com/parsarazor/distributed-job-scheduler/internal/repository"
	apiv1 "github.com/parsarazor/distributed-job-scheduler/pkg/api/v1"
)

func listAll() repository.JobListFilter { return repository.JobListFilter{} }

func newTestWorker(s *Scheduler) *Worker {
	w := NewWorker("test-worker", s, 4, 30*time.Second)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	return w
}

// Principle: worker-lifecycle tests. The worker is the only component that
// touches leases, semaphores, heartbeats and user code at once — its edges
// (drain, panic, failure, unknown jobs) are exactly where production dies.
func TestWorker_DrainStopsIntake(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	w := newTestWorker(s)
	if w.IsDraining() {
		t.Fatal("fresh worker must not be draining")
	}
	w.Drain()
	if !w.IsDraining() {
		t.Fatal("Drain must stick")
	}
	if n, _ := w.RunOnce(ctx, 10); n != 0 {
		t.Fatalf("drained worker must run 0, ran %d", n)
	}
}

func TestWorker_HandlerErrorFailsJobWithRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	w := NewWorker("w", s, 4, 30*time.Second)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error {
		return errors.New("downstream 500")
	})
	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("ran %d want 1", n)
	}
	// Failed with attempts left → back to pending with a scheduled retry.
	jobs, _, _ := s.ListJobs(ctx, tn.ID, listAll())
	if len(jobs) != 1 || jobs[0].Status != domain.StatusPending {
		t.Fatalf("handler error must recycle to pending, got %+v", jobs)
	}
	if jobs[0].NextRetryAt == nil {
		t.Fatal("recycled job must carry NextRetryAt")
	}
}

func TestWorker_HandlerPanicFailsJobNotWorker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	w := NewWorker("w", s, 4, 30*time.Second)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error {
		panic("nil deref in user code")
	})
	if n, _ := w.RunOnce(ctx, 10); n != 1 {
		t.Fatalf("panicking job must still count as handled, ran %d", n)
	}
	jobs, _, _ := s.ListJobs(ctx, tn.ID, listAll())
	if len(jobs) != 1 || jobs[0].Status != domain.StatusPending {
		t.Fatalf("panic must recycle to pending, got %+v", jobs)
	}
	if len(w.ActiveJobs()) != 0 {
		t.Fatal("panicking job must leave no active residue")
	}
	// Worker survives: second job executes normally.
	_, _ = s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	if n, _ := w.RunOnce(ctx, 10); n < 1 {
		t.Fatalf("worker died after panic, ran %d", n)
	}
}

func TestWorker_HeartbeatAndCancelUnknown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := newTestScheduler()
	w := newTestWorker(s)
	if renewed, abort := w.Heartbeat(ctx, uuid.New(), 1); renewed || !abort {
		t.Fatalf("unknown job heartbeat must be (false, true), got (%v, %v)", renewed, abort)
	}
	if w.CancelExecution(ctx, uuid.New(), 1) {
		t.Fatal("cancel of unknown execution must be false")
	}
	if len(w.ActiveJobs()) != 0 {
		t.Fatal("fresh worker must have no active jobs")
	}
}

func TestWorker_LeaseContentionSkipsJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	job, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: time.Now().UTC()})

	// Another worker holds the lease: our worker must skip, not steal.
	if _, err := b.Jobs.ClaimJob(ctx, job.ID, "rival", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	w := NewWorker("us", s, 4, 30*time.Second)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	if n, _ := w.RunOnce(ctx, 10); n != 0 {
		t.Fatalf("leased job must be skipped, ran %d", n)
	}
	stored, _ := s.GetJob(ctx, job.ID)
	if stored.Status != domain.StatusRunning || stored.LeaseOwner == nil || *stored.LeaseOwner != "rival" || stored.FencingToken != 1 {
		t.Fatalf("skipped job must retain rival's claim, got %+v", stored)
	}
}

type renewFailJobRepo struct {
	repository.JobRepository
	err         error
	block       bool
	calls       atomic.Int32
	completions atomic.Int32
}

func (r *renewFailJobRepo) RenewLease(ctx context.Context, _ uuid.UUID, _ string, _ int64, _ time.Duration) error {
	r.calls.Add(1)
	if r.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return r.err
}

func (r *renewFailJobRepo) CompleteLease(ctx context.Context, id uuid.UUID, owner string, token int64, final domain.JobStatus) (*domain.Job, error) {
	r.completions.Add(1)
	return r.JobRepository.CompleteLease(ctx, id, owner, token, final)
}

func TestWorker_HeartbeatFailureCancelsHandlerWithoutCompletion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		err   error
		block bool
	}{
		{name: "lost ownership", err: domain.ErrLeaseLost},
		{name: "persistent transient error", err: errors.New("database unavailable")},
		{name: "renewal exceeds deadline", block: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			s, b := newTestScheduler()
			tn := seedTenant(t, s, b)
			job, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default"})
			if err != nil {
				t.Fatal(err)
			}
			repo := &renewFailJobRepo{JobRepository: b.Jobs, err: tc.err, block: tc.block}
			s.jobs = repo
			w := NewWorker("w", s, 1, 300*time.Millisecond)
			cause := make(chan error, 1)
			w.RegisterHandler("default", func(ctx context.Context, _ *domain.Job) error {
				<-ctx.Done()
				cause <- context.Cause(ctx)
				return ctx.Err()
			})
			if n, err := w.RunOnce(ctx, 1); err != nil || n != 1 {
				t.Fatalf("worker result n=%d err=%v, want one handled job", n, err)
			}
			if got := <-cause; !errors.Is(got, domain.ErrLeaseLost) {
				t.Fatalf("handler context cause=%v, want ErrLeaseLost", got)
			}
			if repo.calls.Load() == 0 {
				t.Fatal("heartbeat did not attempt lease renewal")
			}
			if repo.completions.Load() != 0 {
				t.Fatal("lost lease must not report completion")
			}
			stored, err := b.Jobs.GetByID(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Status != domain.StatusRunning || stored.AttemptCount != 0 || stored.LeaseOwner == nil || *stored.LeaseOwner != w.ID() {
				t.Fatalf("lost lease must leave recovery to the reaper: %+v", stored)
			}
			if len(w.ActiveJobs()) != 0 {
				t.Fatal("handler return must stop heartbeat and remove active job")
			}
		})
	}
}

type observingLeaseRepo struct {
	repository.JobRepository
	renewed chan struct{}
	calls   atomic.Int32
}

func (r *observingLeaseRepo) RenewLease(ctx context.Context, id uuid.UUID, owner string, token int64, ttl time.Duration) error {
	if err := r.JobRepository.RenewLease(ctx, id, owner, token, ttl); err != nil {
		return err
	}
	r.calls.Add(1)
	select {
	case r.renewed <- struct{}{}:
	default:
	}
	return nil
}

func TestWorker_HeartbeatKeepsLongHandlerLeaseAlive(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default"})
	if err != nil {
		t.Fatal(err)
	}
	repo := &observingLeaseRepo{JobRepository: b.Jobs, renewed: make(chan struct{}, 8)}
	s.jobs = repo
	w := NewWorker("w", s, 1, 300*time.Millisecond)
	w.RegisterHandler("default", func(ctx context.Context, _ *domain.Job) error {
		for i := 0; i < 4; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-repo.renewed:
			}
		}
		return nil
	})
	if n, err := w.RunOnce(ctx, 1); n != 1 || err != nil {
		t.Fatalf("long handler n=%d err=%v", n, err)
	}
	stored, err := s.GetJob(ctx, job.ID)
	if err != nil || stored.Status != domain.StatusSucceeded || stored.FencingToken != 1 || stored.LeaseOwner != nil || stored.LeaseExpiresAt != nil || repo.calls.Load() < 4 {
		t.Fatalf("renewed handler must complete with a single execution token: job=%+v err=%v", stored, err)
	}
}

func TestGRPC_HeartbeatRenewsOwnedLease(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default"})
	if err != nil {
		t.Fatal(err)
	}
	repo := &observingLeaseRepo{JobRepository: b.Jobs, renewed: make(chan struct{}, 8)}
	s.jobs = repo
	w := NewWorker("w", s, 1, time.Minute)
	started := make(chan struct{})
	release := make(chan struct{})
	w.RegisterHandler("default", func(ctx context.Context, _ *domain.Job) error {
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	})
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		_, runErr = w.RunOnce(ctx, 1)
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("handler never started")
	}
	_, wc := dialTestServers(t, s.tenants, s, w)
	ctx = credCtx(ctx, adminTestKey)
	before, err := s.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := wc.Heartbeat(ctx, &apiv1.HeartbeatRequest{JobId: job.ID.String(), FencingToken: before.FencingToken})
	if err != nil || !hb.Renewed || hb.AbortRequested || repo.calls.Load() != 1 {
		t.Fatalf("heartbeat must use repository renewal: response=%+v err=%v", hb, err)
	}
	after, err := s.GetJob(ctx, job.ID)
	if err != nil || !after.LeaseExpiresAt.After(*before.LeaseExpiresAt) || after.FencingToken != before.FencingToken {
		t.Fatalf("heartbeat must extend the current lease: job=%+v err=%v", after, err)
	}
	stale, err := wc.Heartbeat(ctx, &apiv1.HeartbeatRequest{JobId: job.ID.String(), FencingToken: before.FencingToken - 1})
	if err != nil || stale.Renewed || !stale.AbortRequested || repo.calls.Load() != 1 {
		t.Fatalf("stale heartbeat must request abort without renewal: response=%+v err=%v", stale, err)
	}
	close(release)
	<-done
	if runErr != nil {
		t.Fatalf("worker completion after RPC renewal: %v", runErr)
	}
}

func TestWorker_RegisterHandlerOverwrite(t *testing.T) {
	t.Parallel()
	s, _ := newTestScheduler()
	w := newTestWorker(s)
	called := ""
	w.RegisterHandler("h", func(ctx context.Context, job *domain.Job) error { called = "v1"; return nil })
	w.RegisterHandler("h", func(ctx context.Context, job *domain.Job) error { called = "v2"; return nil })
	w.handlers["h"](context.Background(), &domain.Job{})
	if called != "v2" {
		t.Fatalf("last registration must win, called=%q", called)
	}
}
