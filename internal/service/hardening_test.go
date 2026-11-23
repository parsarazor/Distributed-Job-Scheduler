package service

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestSubmissionIdempotencyQuotaAndFairness(t *testing.T) {
	s, b := newTestScheduler()
	tenant := seedTenant(t, s, b)
	s.TenantQuota, s.TenantRate = 2, 100
	input := SubmitInput{TenantID: tenant.ID, Name: "same", IdempotencyKey: "request-1"}
	ids := make(chan uuid.UUID, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			job, err := s.SubmitJob(t.Context(), input)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- job.ID
		})
	}
	wg.Wait()
	close(ids)
	var first uuid.UUID
	for id := range ids {
		if first == uuid.Nil {
			first = id
		}
		if first != id {
			t.Fatal("idempotent submission created multiple jobs")
		}
	}
	events, _ := b.Events.ListByJobID(t.Context(), first)
	if len(events) != 1 {
		t.Fatalf("idempotent submission recorded %d events", len(events))
	}
	_, err := s.SubmitJob(t.Context(), SubmitInput{TenantID: tenant.ID, Name: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitJob(t.Context(), SubmitInput{TenantID: tenant.ID, Name: "over-quota"}); !errors.Is(err, domain.ErrTenantLimit) {
		t.Fatalf("quota bypass: %v", err)
	}
	if _, err := s.SubmitJob(t.Context(), input); err != nil {
		t.Fatalf("idempotent retry must work at quota: %v", err)
	}
	other, _ := domain.NewTenant("other", "other-key")
	if err := b.Tenants.Create(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	input.TenantID = other.ID
	job, err := s.SubmitJob(t.Context(), input)
	if err != nil || job.ID == first {
		t.Fatalf("idempotency keys must be tenant-scoped: %+v %v", job, err)
	}
	for range 2 {
		if n, err := s.DispatchReady(t.Context(), 1); n != 1 || err != nil {
			t.Fatalf("dispatch %d %v", n, err)
		}
	}
	rows, _ := b.Outbox.FetchUnpublished(t.Context(), 10, 0)
	owners := map[uuid.UUID]bool{}
	for _, row := range rows {
		j, _ := s.GetJob(t.Context(), row.JobID)
		owners[j.TenantID] = true
	}
	if len(owners) != 2 {
		t.Fatal("batch size one must rotate between eligible tenants")
	}
	s.TenantRate = 1
	if _, err := s.SubmitJob(t.Context(), SubmitInput{TenantID: other.ID, Name: "rate-limited"}); !errors.Is(err, domain.ErrTenantLimit) {
		t.Fatalf("rate limit bypass: %v", err)
	}
}

func TestAuthenticatedRPCScopesTenantAndAdministration(t *testing.T) {
	s, b := newTestScheduler()
	one, _ := domain.NewTenant("one", "one-key")
	two, _ := domain.NewTenant("two", "two-key")
	_ = b.Tenants.Create(t.Context(), one)
	_ = b.Tenants.Create(t.Context(), two)
	foreign, _ := s.SubmitJob(t.Context(), SubmitInput{TenantID: two.ID, Name: "foreign"})
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(Authenticator(b.Tenants, "worker-secret", "admin-secret")))
	apiv1.RegisterSchedulerServiceServer(srv, NewSchedulerServer(s))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := apiv1.NewSchedulerServiceClient(conn)
	if _, err := client.ListJobs(t.Context(), &apiv1.ListJobsRequest{TenantId: one.ID.String()}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated RPC: %v", err)
	}
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("authorization", "Bearer one-key"))
	sub, err := client.SubmitJob(ctx, &apiv1.SubmitJobRequest{TenantId: two.ID.String(), Name: "scoped", IdempotencyKey: "rpc-retry"})
	if err != nil || sub.Job.TenantId != one.ID.String() {
		t.Fatalf("tenant must come from credential: %+v %v", sub, err)
	}
	if _, err := client.GetJob(ctx, &apiv1.GetJobRequest{JobId: foreign.ID.String()}); status.Code(err) != codes.NotFound {
		t.Fatalf("cross-tenant read: %v", err)
	}
	if _, err := client.CancelJob(ctx, &apiv1.CancelJobRequest{JobId: foreign.ID.String()}); status.Code(err) != codes.NotFound {
		t.Fatalf("cross-tenant cancel: %v", err)
	}
	if _, err := client.GetJobEvents(ctx, &apiv1.GetJobEventsRequest{JobId: foreign.ID.String()}); status.Code(err) != codes.NotFound {
		t.Fatalf("cross-tenant audit: %v", err)
	}
	if _, err := client.Dispatch(ctx, &apiv1.DispatchRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("tenant accessed administration: %v", err)
	}
	admin := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("authorization", "Bearer admin-secret"))
	if _, err := client.Dispatch(admin, &apiv1.DispatchRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListJobs(ctx, &apiv1.ListJobsRequest{Offset: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("negative pagination: %v", err)
	}
}

func TestTimeoutAndDeadRequeuePreserveAttemptIdentity(t *testing.T) {
	s, b := newTestScheduler()
	tenant := seedTenant(t, s, b)
	job, err := s.SubmitJob(t.Context(), SubmitInput{TenantID: tenant.ID, Name: "timeout", MaxAttempts: 1, Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorker("timeout-worker", s, 1, time.Second)
	keys := make(chan string, 2)
	w.RegisterHandler("timeout", func(ctx context.Context, job *domain.Job) error {
		keys <- job.IdempotencyKey()
		<-ctx.Done()
		return ctx.Err()
	})
	if n, err := w.RunOnce(t.Context(), 1); n != 1 || err != nil {
		t.Fatalf("run: %d %v", n, err)
	}
	dead, _ := s.GetJob(t.Context(), job.ID)
	if dead.Status != domain.StatusDead || dead.LeaseOwner != nil {
		t.Fatalf("timeout must dead-letter and release lease: %+v", dead)
	}
	reset, err := s.RequeueDead(t.Context(), job.ID)
	if err != nil || reset.AttemptCount != 1 || reset.FencingToken <= dead.FencingToken {
		t.Fatalf("requeue must preserve attempt history: %+v %v", reset, err)
	}
	w.RegisterHandler("timeout", func(_ context.Context, job *domain.Job) error { keys <- job.IdempotencyKey(); return nil })
	if n, err := w.RunOnce(t.Context(), 1); n != 1 || err != nil {
		t.Fatalf("rerun: %d %v", n, err)
	}
	firstKey, secondKey := <-keys, <-keys
	if firstKey == secondKey {
		t.Fatal("requeue must not reuse the previous attempt's idempotency key")
	}
}

func TestMemoryTransactionContentionCancelsLeaseOnTime(t *testing.T) {
	s, b := newTestScheduler()
	tenant := seedTenant(t, s, b)
	_, err := s.SubmitJob(t.Context(), SubmitInput{TenantID: tenant.ID, Name: "blocked"})
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorker("worker", s, 1, 150*time.Millisecond)
	started := make(chan struct{})
	cancelled := make(chan error, 1)
	w.RegisterHandler("blocked", func(ctx context.Context, _ *domain.Job) error {
		close(started)
		<-ctx.Done()
		cancelled <- context.Cause(ctx)
		return ctx.Err()
	})
	done := make(chan struct{})
	go func() { defer close(done); _, _ = w.RunOnce(t.Context(), 1) }()
	<-started
	locked := make(chan struct{})
	release := make(chan struct{})
	txDone := make(chan struct{})
	go func() {
		defer close(txDone)
		_ = b.Tx.WithinTransaction(t.Context(), func(context.Context) error { close(locked); <-release; return nil })
	}()
	defer func() { close(release); <-txDone; <-done }()
	<-locked
	select {
	case err := <-cancelled:
		if !errors.Is(err, domain.ErrLeaseLost) {
			t.Fatalf("cause=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("memory lock blocked lease deadline cancellation")
	}
}

func TestTraceContextSurvivesDispatch(t *testing.T) {
	s, b := newTestScheduler()
	tenant := seedTenant(t, s, b)
	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	ctx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled}))
	job, err := s.SubmitJob(ctx, SubmitInput{TenantID: tenant.ID, Name: "traced"})
	if err != nil || job.TraceParent == "" {
		t.Fatalf("trace not persisted: %+v %v", job, err)
	}
	if _, err := s.DispatchReady(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	rows, _ := b.Outbox.FetchUnpublished(t.Context(), 1, 0)
	w := NewWorker("worker", s, 1, time.Second)
	w.RegisterHandler("traced", func(ctx context.Context, _ *domain.Job) error {
		if got := trace.SpanContextFromContext(ctx).TraceID(); got != traceID {
			t.Errorf("trace ID changed: %s", got)
		}
		return nil
	})
	if err := w.HandleMessage(t.Context(), rows[0].Payload); err != nil {
		t.Fatal(err)
	}
}
