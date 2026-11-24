package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/memory"
	"github.com/parsarazor/distributed-job-scheduler/internal/service"
	"github.com/parsarazor/distributed-job-scheduler/internal/testinfra"
	apiv1 "github.com/parsarazor/distributed-job-scheduler/pkg/api/v1"
	"github.com/parsarazor/distributed-job-scheduler/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestSDKWorker_RunsJobsThroughSchedulerAndRabbitMQ drives pkg/worker against
// a real RabbitMQ and a scheduler served over TCP, as a deployment does.
func TestSDKWorker_RunsJobsThroughSchedulerAndRabbitMQ(t *testing.T) {
	server := testinfra.StartRabbitMQ(t)
	ctx := context.Background()
	const workerToken = "sdk-integration-worker-token"
	bus, err := broker.NewRabbitMQBroker(server.URL, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	b := memory.NewBundle()
	s := service.NewScheduler(b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx, bus, nil)
	s.ReconcileThreshold = 0
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(service.AuthenticatorWithWorkers(b.Tenants, nil, workerToken, "sdk-integration-admin-token")))
	apiv1.RegisterSchedulerServiceServer(srv, service.NewSchedulerServer(s))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	w, err := worker.New(worker.Config{
		SchedulerAddr: "passthrough:///" + lis.Addr().String(), RabbitMQURL: server.URL,
		WorkerID: "sdk-worker", Token: workerToken, Insecure: true, LeaseTTL: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	type order struct{ ID int }
	seen := make(chan order, 1)
	w.HandleFunc("invoice", func(_ context.Context, job *worker.Job) error {
		var o order
		if err := job.Decode(&o); err != nil {
			return worker.Permanent(err)
		}
		seen <- o
		return nil
	})
	w.HandleFunc("charge", func(context.Context, *worker.Job) error {
		return worker.Permanent(errors.New("card declined"))
	})
	// A second worker handles only "report": routing must never hand it to w,
	// which would dead-letter it as an unregistered handler.
	reports, err := worker.New(worker.Config{
		SchedulerAddr: "passthrough:///" + lis.Addr().String(), RabbitMQURL: server.URL,
		WorkerID: "sdk-reports", Token: workerToken, Insecure: true, LeaseTTL: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	reports.HandleFunc("report", func(context.Context, *worker.Job) error { return nil })
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 2)
	go func() { done <- w.Run(runCtx) }()
	go func() { done <- reports.Run(runCtx) }()
	deadline := time.Now().Add(30 * time.Second)
	for !w.Ready() || !reports.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("worker never became ready")
		}
		time.Sleep(50 * time.Millisecond)
	}

	tn, _ := domain.NewTenant("sdk", "sdk-integration-tenant-key")
	if err := b.Tenants.Create(ctx, tn); err != nil {
		t.Fatal(err)
	}
	submit := func(name, payload string) uuid.UUID {
		job, err := s.SubmitJob(ctx, service.SubmitInput{TenantID: tn.ID, Name: name, Payload: json.RawMessage(payload), MaxAttempts: 3})
		if err != nil {
			t.Fatal(err)
		}
		return job.ID
	}
	invoice, charge := submit("invoice", `{"ID":42}`), submit("charge", `{}`)
	var reportIDs []uuid.UUID
	for range 5 {
		reportIDs = append(reportIDs, submit("report", `{}`))
	}
	orphan := submit("no-such-handler", `{}`)
	if _, err := s.DispatchReady(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReconcileOutbox(ctx, 10); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-seen:
		if o.ID != 42 {
			t.Fatalf("decoded payload %+v", o)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("handler never ran")
	}
	awaitStatus := func(id uuid.UUID, want domain.JobStatus) *domain.Job {
		t.Helper()
		for time.Now().Before(time.Now().Add(time.Second)) {
			job, err := s.GetJob(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if job.Status == want {
				return job
			}
			if time.Now().After(deadline.Add(30 * time.Second)) {
				t.Fatalf("job %s is %s, want %s", id, job.Status, want)
			}
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	}
	awaitStatus(invoice, domain.StatusSucceeded)
	if job := awaitStatus(charge, domain.StatusDead); job.AttemptCount != 1 {
		t.Fatalf("permanent failure used %d attempts", job.AttemptCount)
	}
	for _, id := range reportIDs {
		if job := awaitStatus(id, domain.StatusSucceeded); job.LastLeaseOwner == nil || *job.LastLeaseOwner != "sdk-reports" {
			t.Fatalf("report %s ran on %v, want sdk-reports", id, job.LastLeaseOwner)
		}
	}
	if job, err := s.GetJob(ctx, orphan); err != nil || job.Status != domain.StatusPending || job.DispatchedAt != nil || job.NextRetryAt == nil {
		t.Fatalf("job with no worker must wait undispatched: %+v %v", job, err)
	}
	stop()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("clean drain must return nil: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("Run did not return after cancellation")
		}
	}
	if w.Ready() {
		t.Fatal("stopped worker reports ready")
	}
}
