package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func workerAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("worker operation did not finish")
		var zero T
		return zero
	}
}

func TestWorker_AbortedHandlerCannotRenewBeforeItReturns(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "fenced cancellation", true: "timeout"}[timeout], func(t *testing.T) {
			t.Parallel()
			s, b := newTestScheduler()
			tenant := seedTenant(t, s, b)
			job, err := s.SubmitJob(t.Context(), SubmitInput{TenantID: tenant.ID, Name: "blocked"})
			if err != nil {
				t.Fatal(err)
			}
			repo := &observingLeaseRepo{JobRepository: b.Jobs, renewed: make(chan struct{}, 8)}
			s.jobs = repo
			w := NewWorker("worker", s, 1, time.Minute)
			if timeout {
				w.SetJobTimeout(20 * time.Millisecond)
			}
			started, aborted, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			w.RegisterHandler("blocked", func(ctx context.Context, _ *domain.Job) error {
				close(started)
				<-ctx.Done()
				close(aborted)
				<-release
				return nil
			})
			done := make(chan error, 1)
			go func() { _, err := w.RunOnce(t.Context(), 1); done <- err }()
			workerAwait(t, started)
			if !timeout {
				if w.CancelExecution(t.Context(), job.ID, 0) || w.CancelExecution(t.Context(), job.ID, 2) {
					t.Fatal("unfenced or stale cancel was accepted")
				}
				if !w.CancelExecution(t.Context(), job.ID, 1) {
					t.Fatal("matching cancel rejected")
				}
			}
			workerAwait(t, aborted)
			if renewed, abort := w.Heartbeat(t.Context(), job.ID, 1); renewed || !abort {
				t.Fatalf("heartbeat=(%t,%t)", renewed, abort)
			}
			if repo.calls.Load() != 0 {
				t.Fatal("aborted execution reached RenewLease")
			}
			once.Do(func() { close(release) })
			if err := workerAwait(t, done); err != nil {
				t.Fatal(err)
			}
			stored, _ := s.GetJob(t.Context(), job.ID)
			if stored.Status == domain.StatusSucceeded || stored.AttemptCount != 1 {
				t.Fatalf("late handler success overwrote abort: %+v", stored)
			}
		})
	}
}

type blockedRenewRepo struct {
	repository.JobRepository
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (r *blockedRenewRepo) RenewLease(ctx context.Context, _ uuid.UUID, _ string, _ int64, _ time.Duration) error {
	r.calls.Add(1)
	close(r.entered)
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestWorker_HeartbeatAdmissionHonorsRPCDeadline(t *testing.T) {
	t.Parallel()
	s, _ := newTestScheduler()
	r := &blockedRenewRepo{JobRepository: s.jobs, entered: make(chan struct{}), release: make(chan struct{})}
	s.jobs = r
	w := NewWorker("worker", s, 1, time.Minute)
	id := uuid.New()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	w.active[id] = &activeJob{info: ActiveInfo{JobID: id, FencingToken: 1}, ctx: ctx, cancel: cancel, gate: semaphore.NewWeighted(1), deadline: time.Now().Add(time.Minute)}
	first := make(chan bool, 1)
	go func() { renewed, _ := w.Heartbeat(t.Context(), id, 1); first <- renewed }()
	workerAwait(t, r.entered)
	deadline, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	started := time.Now()
	if renewed, abort := w.Heartbeat(deadline, id, 1); renewed || abort {
		t.Fatalf("queued heartbeat=(%t,%t)", renewed, abort)
	}
	if time.Since(started) > 250*time.Millisecond || r.calls.Load() != 1 {
		t.Fatal("RPC deadline did not bound gate admission")
	}
	close(r.release)
	if !workerAwait(t, first) {
		t.Fatal("queued heartbeat disrupted existing renewal")
	}
}

type completionProbeRepo struct {
	repository.JobRepository
	entered   chan struct{}
	release   chan struct{}
	failures  int32
	delay     time.Duration
	calls     atomic.Int32
	renews    atomic.Int32
	ambiguous *atomic.Bool
}

func (r *completionProbeRepo) CompleteLease(ctx context.Context, id uuid.UUID, owner string, token int64, final domain.JobStatus) (*domain.Job, error) {
	n := r.calls.Add(1)
	if r.entered != nil {
		close(r.entered)
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if n <= r.failures {
		if !waitCompletionRetry(ctx, r.delay) {
			return nil, ctx.Err()
		}
		return nil, errors.New("temporary completion failure")
	}
	job, err := r.JobRepository.CompleteLease(ctx, id, owner, token, final)
	if err == nil && r.ambiguous != nil {
		r.ambiguous.Store(true)
	}
	return job, err
}
func (r *completionProbeRepo) RenewLease(ctx context.Context, id uuid.UUID, owner string, token int64, ttl time.Duration) error {
	r.renews.Add(1)
	return r.JobRepository.RenewLease(ctx, id, owner, token, ttl)
}

func TestWorker_FinalizationRejectsLateCancellation(t *testing.T) {
	t.Parallel()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, _ := s.SubmitJob(t.Context(), SubmitInput{TenantID: tn.ID, Name: "finish"})
	r := &completionProbeRepo{JobRepository: b.Jobs, entered: make(chan struct{}), release: make(chan struct{})}
	s.jobs = r
	w := NewWorker("worker", s, 1, time.Minute)
	w.RegisterHandler("finish", func(context.Context, *domain.Job) error { return nil })
	done := make(chan error, 1)
	go func() { _, err := w.RunOnce(t.Context(), 1); done <- err }()
	workerAwait(t, r.entered)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if w.CancelExecution(ctx, job.ID, 1) {
		t.Fatal("cancel acknowledged after successful finalization started")
	}
	close(r.release)
	if err := workerAwait(t, done); err != nil {
		t.Fatal(err)
	}
	if w.CancelExecution(t.Context(), job.ID, 1) {
		t.Fatal("finalized execution acknowledged cancel")
	}
	stored, _ := s.GetJob(t.Context(), job.ID)
	if stored.Status != domain.StatusSucceeded {
		t.Fatalf("completion changed: %+v", stored)
	}
}

func TestWorker_CompletionRetriesKeepLeaseAlive(t *testing.T) {
	t.Parallel()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, _ := s.SubmitJob(t.Context(), SubmitInput{TenantID: tn.ID, Name: "retry"})
	r := &completionProbeRepo{JobRepository: b.Jobs, failures: 3, delay: 40 * time.Millisecond}
	s.jobs = r
	w := NewWorker("worker", s, 1, 180*time.Millisecond)
	var runs atomic.Int32
	w.RegisterHandler("retry", func(context.Context, *domain.Job) error { runs.Add(1); return nil })
	if n, err := w.RunOnce(t.Context(), 1); n != 1 || err != nil {
		t.Fatalf("run=%d err=%v", n, err)
	}
	stored, _ := s.GetJob(t.Context(), job.ID)
	if stored.Status != domain.StatusSucceeded || r.calls.Load() != 4 || r.renews.Load() == 0 || runs.Load() != 1 {
		t.Fatalf("completion retries status=%s calls=%d renews=%d runs=%d", stored.Status, r.calls.Load(), r.renews.Load(), runs.Load())
	}
}

type ambiguousCompletionTx struct {
	repository.Transactor
	armed *atomic.Bool
}

func (tx ambiguousCompletionTx) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	err := tx.Transactor.WithinTransaction(ctx, fn)
	if err == nil && tx.armed.Swap(false) {
		return errors.New("commit reply lost")
	}
	return err
}

func TestWorker_AmbiguousCompletionReconcilesWithoutReexecuting(t *testing.T) {
	t.Parallel()
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failed], func(t *testing.T) {
			s, b := newTestScheduler()
			tn := seedTenant(t, s, b)
			job, _ := s.SubmitJob(t.Context(), SubmitInput{TenantID: tn.ID, Name: "ambiguous"})
			armed := &atomic.Bool{}
			r := &completionProbeRepo{JobRepository: b.Jobs, ambiguous: armed}
			s.jobs = r
			s.tx = ambiguousCompletionTx{b.Tx, armed}
			w := NewWorker("worker", s, 1, time.Second)
			var runs atomic.Int32
			w.RegisterHandler("ambiguous", func(context.Context, *domain.Job) error {
				runs.Add(1)
				if failed {
					return errors.New("secret handler details")
				}
				return nil
			})
			payload, _ := json.Marshal(job)
			if err := w.HandleMessage(t.Context(), payload); err != nil {
				t.Fatal(err)
			}
			if err := w.HandleMessage(t.Context(), payload); err != nil {
				t.Fatal(err)
			}
			if r.calls.Load() != 1 || runs.Load() != 1 {
				t.Fatalf("ambiguous result duplicated work: commits=%d handlers=%d", r.calls.Load(), runs.Load())
			}
		})
	}
}

func TestWorker_ExecutePreservesExpectedAttempt(t *testing.T) {
	t.Parallel()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, _ := s.SubmitJob(t.Context(), SubmitInput{TenantID: tn.ID, Name: "canonical"})
	stale := DomainJobToProto(job)
	job.AttemptCount = 1
	if err := b.Jobs.UpdateWithFencingToken(t.Context(), job, job.FencingToken); err != nil {
		t.Fatal(err)
	}
	w := NewWorker("worker", s, 1, time.Second)
	var ran atomic.Int32
	w.RegisterHandler("canonical", func(context.Context, *domain.Job) error { ran.Add(1); return nil })
	response, err := NewWorkerServer(w).Execute(t.Context(), &apiv1.ExecuteRequest{Job: stale})
	if err != nil || !response.Accepted {
		t.Fatalf("execute=%+v %v", response, err)
	}
	if err := w.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 0 {
		t.Fatal("stale RPC claimed a newer attempt")
	}
}

func TestWorker_ExecuteUsesCanonicalDataAndTraceOnlyContext(t *testing.T) {
	t.Parallel()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, _ := s.SubmitJob(t.Context(), SubmitInput{TenantID: tn.ID, Name: "canonical", Payload: json.RawMessage(`{"database":true}`)})
	w := NewWorker("worker", s, 1, time.Second)
	received := make(chan context.Context, 1)
	release := make(chan struct{})
	w.RegisterHandler("canonical", func(ctx context.Context, j *domain.Job) error {
		if j.Name != "canonical" || string(j.Payload) != `{"database":true}` {
			t.Error("RPC replaced canonical handler data")
		}
		received <- ctx
		<-release
		return nil
	})
	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	ctx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}))
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer admin-secret"))
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer worker-secret"))
	ctx, cancel := context.WithCancel(ctx)
	request := DomainJobToProto(job)
	request.Name = "untrusted"
	request.Payload = []byte(`{"request":true}`)
	response, err := NewWorkerServer(w).Execute(ctx, &apiv1.ExecuteRequest{Job: request})
	cancel()
	if err != nil || !response.Accepted {
		t.Fatalf("execute=%+v %v", response, err)
	}
	hctx := workerAwait(t, received)
	if hctx.Err() != nil {
		t.Fatal("RPC cancellation leaked into detached execution")
	}
	if md, ok := metadata.FromIncomingContext(hctx); ok && len(md) > 0 {
		t.Fatal("incoming bearer metadata reached handler")
	}
	if md, ok := metadata.FromOutgoingContext(hctx); ok && len(md) > 0 {
		t.Fatal("outgoing bearer metadata reached handler")
	}
	if trace.SpanContextFromContext(hctx).TraceID() != traceID {
		t.Fatal("trace was not preserved")
	}
	close(release)
	if err := w.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestWorker_DrainRPCHonorsDeadlineAndStopsIntake(t *testing.T) {
	t.Parallel()
	s, b := newTestScheduler()
	tn := seedTenant(t, s, b)
	job, _ := s.SubmitJob(t.Context(), SubmitInput{TenantID: tn.ID, Name: "blocked"})
	w := NewWorker("worker", s, 1, time.Second)
	intake, stop := context.WithCancel(t.Context())
	defer stop()
	w.SetIntakeCancel(stop)
	started, release := make(chan struct{}), make(chan struct{})
	w.RegisterHandler("blocked", func(context.Context, *domain.Job) error { close(started); <-release; return nil })
	w.StartExecution(t.Context(), job)
	workerAwait(t, started)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := NewWorkerServer(w).DrainWorker(ctx, &apiv1.DrainWorkerRequest{TimeoutSeconds: 30})
	if status.Code(err) != codes.DeadlineExceeded || intake.Err() == nil || !w.IsDraining() {
		t.Fatalf("drain err=%v intake=%v", err, intake.Err())
	}
	close(release)
	if err := w.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestWorker_PoisonAndObsoleteAnnouncements(t *testing.T) {
	t.Parallel()
	s, _ := newTestScheduler()
	w := NewWorker("worker", s, 1, time.Second)
	for _, body := range []string{`garbage`, `{}`, `{"id":"not-a-uuid"}`, `null`, `{"id":"00000000-0000-0000-0000-000000000000"}`} {
		if err := w.HandleMessage(t.Context(), []byte(body)); !broker.IsPermanent(err) {
			t.Fatalf("poison %q returned %v", body, err)
		}
	}
	if err := w.HandleMessage(t.Context(), []byte(`{"id":"`+uuid.NewString()+`"}`)); err != nil {
		t.Fatalf("obsolete missing job retried: %v", err)
	}
}

type unsafeHandlerError struct{}

func (unsafeHandlerError) Error() string { panic("must not format user error") }
func (unsafeHandlerError) Is(error) bool { panic("must survive user error matching") }

func TestWorker_HandlerResultsAreSafeAndRegistrationIsExact(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		handler Handler
		message string
	}{
		{"error", func(context.Context, *domain.Job) error { return unsafeHandlerError{} }, "handler failed"},
		{"panic", func(context.Context, *domain.Job) error { panic(unsafeHandlerError{}) }, "handler panicked"},
		{"unknown", nil, "handler not registered"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, b := newTestScheduler()
			tn := seedTenant(t, s, b)
			job, _ := s.SubmitJob(t.Context(), SubmitInput{TenantID: tn.ID, Name: tc.name})
			w := NewWorker("worker", s, 1, time.Second)
			w.RegisterHandler("default", func(context.Context, *domain.Job) error { t.Error("implicit fallback ran"); return nil })
			if tc.handler != nil {
				w.RegisterHandler(tc.name, tc.handler)
			}
			if _, err := w.RunOnce(t.Context(), 1); err != nil {
				t.Fatal(err)
			}
			events, err := b.Events.ListByJobID(t.Context(), job.ID)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, event := range events {
				if event.ToStatus == domain.StatusFailed {
					found = true
					if event.ErrorMessage == nil || *event.ErrorMessage != tc.message {
						t.Fatalf("unsafe error %+v", event.ErrorMessage)
					}
				}
			}
			if !found {
				t.Fatal("missing failed handler audit event")
			}
		})
	}
}
