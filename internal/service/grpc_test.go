package service

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Principle: handler tests over a real transport. bufconn runs the full gRPC
// stack (marshaling, status codes, routing) in-process — no mocks, no ports.
func dialTestServers(t *testing.T, tenants repository.TenantRepository, sched *Scheduler, w *Worker) (apiv1.SchedulerServiceClient, apiv1.WorkerServiceClient) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	// Same interceptor chain as production: identity comes from bearer
	// credentials over the wire, never from client-side context values.
	srv := grpc.NewServer(grpc.UnaryInterceptor(Authenticator(tenants, workerTestKey, adminTestKey)))
	apiv1.RegisterSchedulerServiceServer(srv, NewSchedulerServer(sched))
	apiv1.RegisterWorkerServiceServer(srv, NewWorkerServer(w))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return apiv1.NewSchedulerServiceClient(conn), apiv1.NewWorkerServiceClient(conn)
}

const (
	workerTestKey = "worker-test-key"
	adminTestKey  = "admin-test-key"
	tenantTestKey = "k"
)

// tenantCtx / workerCtx / adminCtx attach the bearer credential the real
// Authenticator on the test server resolves.
func credCtx(ctx context.Context, key string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
}

func newWiredPair() (*Scheduler, *Worker, *memory.Bundle) {
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	s.ReconcileThreshold = 0
	w := NewWorker("grpc-worker", s, 4, 30*time.Second)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	return s, w, b
}

func TestGRPC_SchedulerFullFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, w, b := newWiredPair()
	sc, _ := dialTestServers(t, b.Tenants, s, w)

	tn, _ := domain.NewTenant("acme", tenantTestKey)
	_ = b.Tenants.Create(ctx, tn)
	ctx = credCtx(ctx, tenantTestKey)

	sub, err := sc.SubmitJob(ctx, &apiv1.SubmitJobRequest{TenantId: tn.ID.String(), Name: "default", Payload: []byte(`{}`), Priority: apiv1.JobPriority_JOB_PRIORITY_HIGH})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if sub.Job.Priority != apiv1.JobPriority_JOB_PRIORITY_HIGH {
		t.Fatalf("priority did not survive the wire: %v", sub.Job.Priority)
	}
	jobID := sub.Job.Id

	got, err := sc.GetJob(ctx, &apiv1.GetJobRequest{TenantId: tn.ID.String(), JobId: jobID})
	if err != nil || got.Job.Id != jobID {
		t.Fatalf("get: %+v %v", got, err)
	}
	list, err := sc.ListJobs(ctx, &apiv1.ListJobsRequest{TenantId: tn.ID.String(), Limit: 10})
	if err != nil || list.TotalCount != 1 {
		t.Fatalf("list: %+v %v", list, err)
	}
	disp, err := sc.Dispatch(credCtx(context.Background(), adminTestKey), &apiv1.DispatchRequest{BatchSize: 10})
	if err != nil || disp.DispatchedCount != 1 {
		t.Fatalf("dispatch: %+v %v", disp, err)
	}
	claimTestJob(t, s, mustParseUUID(t, jobID), "w")
	stored, _ := s.GetJob(ctx, mustParseUUID(t, jobID))
	rep, err := sc.ReportJobCompletion(credCtx(context.Background(), workerTestKey), &apiv1.ReportJobCompletionRequest{JobId: jobID, WorkerId: "w", FinalStatus: apiv1.JobStatus_JOB_STATUS_SUCCEEDED, FencingToken: stored.FencingToken})
	if err != nil || !rep.Acknowledged {
		t.Fatalf("report: %+v %v", rep, err)
	}
	evs, err := sc.GetJobEvents(ctx, &apiv1.GetJobEventsRequest{TenantId: tn.ID.String(), JobId: jobID})
	if err != nil || len(evs.Events) == 0 {
		t.Fatalf("events: %+v %v", evs, err)
	}
	rec, err := sc.TriggerReconciliation(credCtx(context.Background(), adminTestKey), &apiv1.TriggerReconciliationRequest{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	_ = rec
	// Cancel-after-terminal must be a clean FailedPrecondition, not a 500.
	if _, err := sc.CancelJob(ctx, &apiv1.CancelJobRequest{TenantId: tn.ID.String(), JobId: jobID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cancel of succeeded job must be FailedPrecondition, got %v", err)
	}
}

func TestGRPC_ValidationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, w, _ := newWiredPair()
	sc, _ := dialTestServers(t, s.tenants, s, w)
	tn, _ := domain.NewTenant("acme", tenantTestKey)
	_ = s.tenants.Create(ctx, tn)
	ctx = credCtx(ctx, tenantTestKey)

	if _, err := sc.SubmitJob(ctx, &apiv1.SubmitJobRequest{TenantId: tn.ID.String(), Name: ""}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty name must be InvalidArgument, got %v", err)
	}
	if _, err := sc.GetJob(ctx, &apiv1.GetJobRequest{JobId: "nope"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad job id must be InvalidArgument, got %v", err)
	}
	if _, err := sc.GetJob(ctx, &apiv1.GetJobRequest{JobId: uuid.NewString()}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown job must be NotFound, got %v", err)
	}
	if _, err := sc.ReportJobCompletion(credCtx(context.Background(), workerTestKey), &apiv1.ReportJobCompletionRequest{JobId: "nope"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad report id must be InvalidArgument, got %v", err)
	}
	// Unknown parent dependency → FailedPrecondition through the RPC.
	ghost := uuid.NewString()
	if _, err := sc.SubmitJob(ctx, &apiv1.SubmitJobRequest{TenantId: tn.ID.String(), Name: "x", DependsOn: &ghost}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ghost parent must be FailedPrecondition, got %v", err)
	}
}

func TestGRPC_WorkerService(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, w, b := newWiredPair()
	_, wc := dialTestServers(t, b.Tenants, s, w)
	workerCtx := credCtx(ctx, workerTestKey)
	adminCtx := credCtx(ctx, adminTestKey)

	// Control-plane RPCs are admin-only: a worker token must not read active
	// jobs, cancel an execution or drain the fleet.
	if _, err := wc.GetWorkerStatus(workerCtx, &apiv1.GetWorkerStatusRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("worker token must not reach control status, got %v", err)
	}
	if _, err := wc.DrainWorker(workerCtx, &apiv1.DrainWorkerRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("worker token must not drain the fleet, got %v", err)
	}
	if _, err := wc.CancelExecution(workerCtx, &apiv1.CancelExecutionRequest{JobId: uuid.NewString()}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("worker token must not cancel executions, got %v", err)
	}
	st, err := wc.GetWorkerStatus(adminCtx, &apiv1.GetWorkerStatusRequest{})
	if err != nil || st.WorkerId != "grpc-worker" {
		t.Fatalf("status: %+v %v", st, err)
	}
	if _, err := wc.Execute(workerCtx, &apiv1.ExecuteRequest{}); err != nil {
		t.Fatalf("nil-job execute must be a soft reject, got %v", err)
	}
	hb, err := wc.Heartbeat(workerCtx, &apiv1.HeartbeatRequest{JobId: "nope"})
	if err != nil || !hb.AbortRequested {
		t.Fatalf("bad heartbeat must request abort, got %+v %v", hb, err)
	}
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	j, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: ctxTime()})
	ex, err := wc.Execute(workerCtx, &apiv1.ExecuteRequest{Job: DomainJobToProto(j)})
	if err != nil || !ex.Accepted {
		t.Fatalf("execute: %+v %v", ex, err)
	}
	// Let the async execution settle, then drain must acknowledge.
	deadline := time.Now().Add(3 * time.Second)
	for {
		jobs, _, _ := s.ListJobs(ctx, tn.ID, listAll())
		if len(jobs) == 1 && jobs[0].IsTerminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("async execute never reached terminal state")
		}
		time.Sleep(10 * time.Millisecond)
	}
	dr, err := wc.DrainWorker(adminCtx, &apiv1.DrainWorkerRequest{})
	if err != nil || !dr.Acknowledged {
		t.Fatalf("drain: %+v %v", dr, err)
	}
	cx, err := wc.CancelExecution(adminCtx, &apiv1.CancelExecutionRequest{JobId: uuid.NewString()})
	if err != nil || cx.Cancelled {
		t.Fatalf("cancel of unknown execution must be false, got %+v %v", cx, err)
	}
}

// GetWorkerStatus is admin-only and must still not disclose the lease fencing
// token: a status reader could otherwise forge completion for a live lease.
func TestGetWorkerStatusOmitsFencingToken(t *testing.T) {
	t.Parallel()
	w := NewWorker("redact-worker", nil, 1, 0)
	jobID, tenantID := uuid.New(), uuid.New()
	w.mu.Lock()
	w.active[jobID] = &activeJob{info: ActiveInfo{JobID: jobID, TenantID: tenantID, Name: "live", StartedAt: time.Now().UTC(), FencingToken: 42}}
	w.mu.Unlock()

	st, err := NewWorkerServer(w).GetWorkerStatus(t.Context(), &apiv1.GetWorkerStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(st.ActiveJobs) != 1 {
		t.Fatalf("want one active job, got %d", len(st.ActiveJobs))
	}
	info := st.ActiveJobs[0]
	if info.GetJobId() != jobID.String() {
		t.Fatalf("active job identity lost: %+v", info)
	}
	//lint:ignore SA1019 asserts deprecated legacy fencing_token field stays unset
	if info.GetFencingToken() != 0 {
		//lint:ignore SA1019 asserts deprecated legacy fencing_token field stays unset
		t.Fatalf("control status leaked fencing token %d", info.GetFencingToken())
	}
}

func ctxTime() (t time.Time) { return time.Now().UTC() }

func mustParseUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
