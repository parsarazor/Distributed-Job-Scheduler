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
	"github.com/prsamohamadi/distributed-job-scheduler/internal/lease"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Principle: handler tests over a real transport. bufconn runs the full gRPC
// stack (marshaling, status codes, routing) in-process — no mocks, no ports.
func dialTestServers(t *testing.T, sched *Scheduler, w *Worker) (apiv1.SchedulerServiceClient, apiv1.WorkerServiceClient) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
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

func newWiredPair() (*Scheduler, *Worker, *memory.Bundle) {
	b := memory.NewBundle()
	s := NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, broker.NewMemoryBroker(), nil)
	s.ReconcileThreshold = 0
	w := NewWorker("grpc-worker", s, lease.NewMemoryLeaseManager(), 4)
	w.RegisterHandler("default", func(ctx context.Context, job *domain.Job) error { return nil })
	return s, w, b
}

func TestGRPC_SchedulerFullFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, w, b := newWiredPair()
	sc, _ := dialTestServers(t, s, w)

	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)

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
	disp, err := sc.Dispatch(ctx, &apiv1.DispatchRequest{BatchSize: 10})
	if err != nil || disp.DispatchedCount != 1 {
		t.Fatalf("dispatch: %+v %v", disp, err)
	}
	stored, _ := s.GetJob(ctx, mustParseUUID(t, jobID))
	rep, err := sc.ReportJobCompletion(ctx, &apiv1.ReportJobCompletionRequest{JobId: jobID, WorkerId: "w", FinalStatus: apiv1.JobStatus_JOB_STATUS_SUCCEEDED, FencingToken: stored.FencingToken})
	if err != nil || !rep.Acknowledged {
		t.Fatalf("report: %+v %v", rep, err)
	}
	evs, err := sc.GetJobEvents(ctx, &apiv1.GetJobEventsRequest{TenantId: tn.ID.String(), JobId: jobID})
	if err != nil || len(evs.Events) == 0 {
		t.Fatalf("events: %+v %v", evs, err)
	}
	rec, err := sc.TriggerReconciliation(ctx, &apiv1.TriggerReconciliationRequest{})
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
	sc, _ := dialTestServers(t, s, w)

	if _, err := sc.SubmitJob(ctx, &apiv1.SubmitJobRequest{TenantId: "nope"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad tenant must be InvalidArgument, got %v", err)
	}
	if _, err := sc.GetJob(ctx, &apiv1.GetJobRequest{JobId: "nope"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad job id must be InvalidArgument, got %v", err)
	}
	if _, err := sc.GetJob(ctx, &apiv1.GetJobRequest{JobId: uuid.NewString()}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown job must be NotFound, got %v", err)
	}
	if _, err := sc.ReportJobCompletion(ctx, &apiv1.ReportJobCompletionRequest{JobId: "nope"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad report id must be InvalidArgument, got %v", err)
	}
	// Unknown parent dependency → FailedPrecondition through the RPC.
	tn, _ := domain.NewTenant("acme", "k")
	_ = s.tenants.Create(ctx, tn)
	ghost := uuid.NewString()
	if _, err := sc.SubmitJob(ctx, &apiv1.SubmitJobRequest{TenantId: tn.ID.String(), Name: "x", DependsOn: &ghost}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ghost parent must be FailedPrecondition, got %v", err)
	}
}

func TestGRPC_WorkerService(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, w, b := newWiredPair()
	_, wc := dialTestServers(t, s, w)

	st, err := wc.GetWorkerStatus(ctx, &apiv1.GetWorkerStatusRequest{})
	if err != nil || st.WorkerId != "grpc-worker" {
		t.Fatalf("status: %+v %v", st, err)
	}
	if _, err := wc.Execute(ctx, &apiv1.ExecuteRequest{}); err != nil {
		t.Fatalf("nil-job execute must be a soft reject, got %v", err)
	}
	hb, err := wc.Heartbeat(ctx, &apiv1.HeartbeatRequest{JobId: "nope"})
	if err != nil || !hb.AbortRequested {
		t.Fatalf("bad heartbeat must request abort, got %+v %v", hb, err)
	}
	tn, _ := domain.NewTenant("acme", "k")
	_ = b.Tenants.Create(ctx, tn)
	j, _ := s.SubmitJob(ctx, SubmitInput{TenantID: tn.ID, Name: "default", Payload: json.RawMessage(`{}`), Priority: domain.PriorityDefault, MaxAttempts: 3, ScheduledAt: ctxTime()})
	ex, err := wc.Execute(ctx, &apiv1.ExecuteRequest{Job: DomainJobToProto(j)})
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
	dr, err := wc.DrainWorker(ctx, &apiv1.DrainWorkerRequest{})
	if err != nil || !dr.Acknowledged {
		t.Fatalf("drain: %+v %v", dr, err)
	}
	cx, err := wc.CancelExecution(ctx, &apiv1.CancelExecutionRequest{JobId: uuid.NewString()})
	if err != nil || cx.Cancelled {
		t.Fatalf("cancel of unknown execution must be false, got %+v %v", cx, err)
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
