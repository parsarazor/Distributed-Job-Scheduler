package service

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// dialScheduler serves only the scheduler and returns a client that sends key
// on every RPC, as a deployed database-free worker does.
func dialScheduler(t *testing.T, s *Scheduler, key string) apiv1.SchedulerServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(AuthenticatorWithWorkers(s.tenants, s.workerCreds, workerTestKey, adminTestKey)))
	apiv1.RegisterSchedulerServiceServer(srv, NewSchedulerServer(s))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key), method, req, reply, cc, opts...)
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return apiv1.NewSchedulerServiceClient(conn)
}

// submitAndAnnounce returns the broker announcement for one dispatched job.
func submitAndAnnounce(t *testing.T, s *Scheduler, name string, payload string) (*domain.Job, []byte) {
	t.Helper()
	ctx := context.Background()
	tn, _ := domain.NewTenant("acme", tenantTestKey)
	if err := s.tenants.Create(ctx, tn); err != nil {
		t.Fatal(err)
	}
	job, err := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: name, Payload: json.RawMessage(payload), MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.DispatchReady(ctx, 10); err != nil || n != 1 {
		t.Fatalf("dispatch: n=%d err=%v", n, err)
	}
	if _, err := s.ReconcileOutbox(ctx, 10); err != nil {
		t.Fatal(err)
	}
	published := s.bus.(*broker.MemoryBroker).Published()
	if len(published) == 0 {
		t.Fatal("no announcement published")
	}
	return job, published[len(published)-1].Payload
}

func newRemotePair(t *testing.T, key string, leaseTTL time.Duration) (*Scheduler, *Worker) {
	t.Helper()
	s, _, _ := newWiredPair()
	w := NewRemoteWorker("remote-worker", NewRemoteJobControl(dialScheduler(t, s, key)), 2, leaseTTL)
	return s, w
}

func TestRemoteWorker_ExecutesThroughSchedulerRPCs(t *testing.T) {
	t.Parallel()
	s, w := newRemotePair(t, workerTestKey, 30*time.Second)
	var runs atomic.Int32
	var seen atomic.Value
	w.RegisterHandler("thumbnail", func(_ context.Context, job *domain.Job) error {
		runs.Add(1)
		seen.Store(string(job.Payload))
		return nil
	})
	job, announcement := submitAndAnnounce(t, s, "thumbnail", `{"image":"a.png"}`)
	if err := w.HandleMessage(context.Background(), announcement); err != nil {
		t.Fatalf("handle: %v", err)
	}
	stored, err := s.GetJob(context.Background(), job.ID)
	if err != nil || stored.Status != domain.StatusSucceeded || stored.LeaseOwner != nil {
		t.Fatalf("job not completed through the scheduler: %+v %v", stored, err)
	}
	if got, _ := seen.Load().(string); got != `{"image":"a.png"}` {
		t.Fatalf("handler payload = %q", got)
	}
	// A redelivered announcement must not run the handler again.
	if err := w.HandleMessage(context.Background(), announcement); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if runs.Load() != 1 {
		t.Fatalf("handler ran %d times", runs.Load())
	}
}

func TestRemoteWorker_RenewsLeaseOverRPC(t *testing.T) {
	t.Parallel()
	s, w := newRemotePair(t, workerTestKey, 300*time.Millisecond)
	w.RegisterHandler("slow", func(ctx context.Context, _ *domain.Job) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return nil
		}
	})
	job, announcement := submitAndAnnounce(t, s, "slow", `{}`)
	if err := w.HandleMessage(context.Background(), announcement); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if stored, _ := s.GetJob(context.Background(), job.ID); stored.Status != domain.StatusSucceeded {
		t.Fatalf("job outlived several leases but status=%s", stored.Status)
	}
}

func TestRemoteWorker_FailureSchedulesRetry(t *testing.T) {
	t.Parallel()
	s, w := newRemotePair(t, workerTestKey, 30*time.Second)
	w.RegisterHandler("flaky", func(context.Context, *domain.Job) error { return errors.New("smtp down") })
	job, announcement := submitAndAnnounce(t, s, "flaky", `{}`)
	if err := w.HandleMessage(context.Background(), announcement); err != nil {
		t.Fatalf("handle: %v", err)
	}
	stored, _ := s.GetJob(context.Background(), job.ID)
	if stored.Status != domain.StatusPending || stored.AttemptCount != 1 || stored.NextRetryAt == nil {
		t.Fatalf("failed attempt was not scheduled for retry: %+v", stored)
	}
}

func TestGetExecutionJob_ScopedToTheExecution(t *testing.T) {
	t.Parallel()
	s, _, _ := newWiredPair()
	job, _ := submitAndAnnounce(t, s, "x", `{"card":"4242"}`)
	stored, _ := s.GetJob(context.Background(), job.ID)
	client := dialScheduler(t, s, workerTestKey)
	other := &apiv1.GetExecutionJobRequest{JobId: job.ID.String(), WorkerId: "w", FencingToken: stored.FencingToken + 1}
	if _, err := client.GetExecutionJob(context.Background(), other); status.Code(err) != codes.Aborted {
		t.Fatalf("lookup with another attempt's token: %v", err)
	}
	token := claimTestJob(t, s, job.ID, "owner")
	for worker, want := range map[string]codes.Code{"intruder": codes.PermissionDenied, "": codes.InvalidArgument} {
		req := &apiv1.GetExecutionJobRequest{JobId: job.ID.String(), WorkerId: worker, FencingToken: token}
		if _, err := client.GetExecutionJob(context.Background(), req); status.Code(err) != want {
			t.Fatalf("worker %q read another worker's execution: %v", worker, err)
		}
	}
	resp, err := client.GetExecutionJob(context.Background(), &apiv1.GetExecutionJobRequest{JobId: job.ID.String(), WorkerId: "owner", FencingToken: token})
	if err != nil || resp.GetLeaseOwner() != "owner" || len(resp.GetJob().GetPayload()) != 0 {
		t.Fatalf("owner lookup must return metadata without payload: %+v %v", resp, err)
	}

	// Once finished, only the worker that ran the attempt may confirm it.
	if _, err := client.ReportJobCompletion(context.Background(), &apiv1.ReportJobCompletionRequest{JobId: job.ID.String(), WorkerId: "owner", FinalStatus: apiv1.JobStatus_JOB_STATUS_SUCCEEDED, FencingToken: token}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	finished := &apiv1.GetExecutionJobRequest{JobId: job.ID.String(), WorkerId: "intruder", FencingToken: token}
	if _, err := client.GetExecutionJob(context.Background(), finished); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("another worker read a finished job: %v", err)
	}
	finished.WorkerId = "owner"
	if resp, err := client.GetExecutionJob(context.Background(), finished); err != nil || resp.GetJob().GetStatus() != apiv1.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Fatalf("the worker that ran it must confirm completion: %+v %v", resp, err)
	}
}

func TestClaimJobExecution_RejectsUndispatchedJobs(t *testing.T) {
	t.Parallel()
	s, _, _ := newWiredPair()
	tn, _ := domain.NewTenant("acme", tenantTestKey)
	_ = s.tenants.Create(context.Background(), tn)
	job, err := s.SubmitJob(context.Background(), SubmitInput{TenantID: tn.ID, Name: "x", Payload: json.RawMessage(`{}`), MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	req := &apiv1.ClaimJobExecutionRequest{JobId: job.ID.String(), AttemptCount: int32(job.AttemptCount), FencingToken: job.FencingToken, WorkerId: "w", LeaseTtlMs: 1000}
	if _, err := dialScheduler(t, s, workerTestKey).ClaimJobExecution(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("worker claimed a job the scheduler never dispatched: %v", err)
	}
	if stored, _ := s.GetJob(context.Background(), job.ID); stored.Status != domain.StatusPending {
		t.Fatalf("undispatched job changed state: %s", stored.Status)
	}
}

type deliveryRecorder struct {
	repository.OutboxRepository
	delivered []uuid.UUID
}

func (r *deliveryRecorder) MarkDelivered(ctx context.Context, id uuid.UUID) error {
	r.delivered = append(r.delivered, id)
	return r.OutboxRepository.MarkDelivered(ctx, id)
}

func TestClaimJobExecution_CanceledAnnouncementReleasesDispatchSlot(t *testing.T) {
	t.Parallel()
	s, _, _ := newWiredPair()
	job, _ := submitAndAnnounce(t, s, "x", `{}`)
	announced, err := s.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelJob(context.Background(), job.ID, "customer changed mind"); err != nil {
		t.Fatal(err)
	}
	rec := &deliveryRecorder{OutboxRepository: s.outbox}
	s.outbox = rec
	req := &apiv1.ClaimJobExecutionRequest{JobId: job.ID.String(), AttemptCount: int32(announced.AttemptCount), FencingToken: announced.FencingToken, WorkerId: "w", LeaseTtlMs: 1000}
	if _, err := dialScheduler(t, s, workerTestKey).ClaimJobExecution(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("canceled job claimed: %v", err)
	}
	want := domain.DispatchID(job.ID, announced.AttemptCount, announced.FencingToken)
	if len(rec.delivered) != 1 || rec.delivered[0] != want {
		t.Fatalf("canceled announcement did not release its dispatch slot: %v", rec.delivered)
	}
}

func TestWorkerRPCs_RequireWorkerCredentialAndBoundedLease(t *testing.T) {
	t.Parallel()
	s, _, _ := newWiredPair()
	job, _ := submitAndAnnounce(t, s, "x", `{}`)
	req := &apiv1.ClaimJobExecutionRequest{JobId: job.ID.String(), WorkerId: "w", LeaseTtlMs: 1000}
	if _, err := dialScheduler(t, s, tenantTestKey).ClaimJobExecution(context.Background(), req); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("tenant credential claimed work: %v", err)
	}
	worker := dialScheduler(t, s, workerTestKey)
	for _, ttl := range []int64{0, -1, (11 * time.Minute).Milliseconds()} {
		bad := &apiv1.ClaimJobExecutionRequest{JobId: job.ID.String(), WorkerId: "w", LeaseTtlMs: ttl}
		if _, err := worker.ClaimJobExecution(context.Background(), bad); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("lease %dms accepted: %v", ttl, err)
		}
	}
	if _, err := worker.RenewJobLease(context.Background(), &apiv1.RenewJobLeaseRequest{JobId: job.ID.String(), WorkerId: "w", FencingToken: 99, LeaseTtlMs: 1000}); !errors.Is(fromGRPCError(err), domain.ErrLeaseLost) {
		t.Fatalf("renewal without a lease must map to ErrLeaseLost: %v", err)
	}
}
