// Command scheduler runs the gRPC SchedulerService plus background
// dispatch / retry / outbox-reconciliation loops.
//
// Backends are chosen from the environment (see internal/app):
// unset -> fully in-memory and runnable with zero dependencies.
// DATABASE_URL / RABBITMQ_URL / REDIS_ADDR -> production wiring.
//
// Ports: :50051 gRPC. Env: PORT overrides.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/app"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	ctx := context.Background()
	deps, err := app.NewFromEnv(ctx)
	if err != nil {
		log.Fatalf("wire backends: %v", err)
	}
	defer deps.Cleanup()

	// Demo data is useful for the dependency-free local mode, but must never
	// be inserted into a configured production database.
	if os.Getenv("DATABASE_URL") == "" {
		seedDemo(ctx, deps)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	apiv1.RegisterSchedulerServiceServer(grpcServer, service.NewSchedulerServer(deps.Scheduler))
	reflection.Register(grpcServer)

	// Background loops: dispatch, retry, reconcile, purge.
	stop := make(chan struct{})
	go loop(stop, 2*time.Second, func() {
		n, _ := deps.Scheduler.DispatchReady(context.Background(), 100)
		if n > 0 {
			log.Printf("dispatch: %d jobs -> running", n)
		}
	})
	go loop(stop, 10*time.Second, func() {
		if n, _ := deps.Scheduler.RetryDue(context.Background(), 100); n > 0 {
			log.Printf("retry: %d jobs -> pending", n)
		}
	})
	go loop(stop, 5*time.Second, func() {
		n, _ := deps.Scheduler.ReconcileOutbox(context.Background(), 100)
		if n > 0 {
			log.Printf("reconcile: %d outbox events published", n)
		}
	})

	go func() {
		log.Printf("scheduler listening on :%s (in-memory=%v)", port, os.Getenv("DATABASE_URL") == "")
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stop)
	grpcServer.GracefulStop()
	log.Println("scheduler stopped")
}

func loop(stop <-chan struct{}, every time.Duration, fn func()) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			fn()
		}
	}
}

// seedDemo creates a demo tenant + a few jobs on empty in-memory backends so
// `go run ./cmd/scheduler` does something observable immediately.
func seedDemo(ctx context.Context, deps *app.Deps) {
	tenant, err := domain.NewTenant("demo", "demo-key-123")
	if err != nil {
		return
	}
	_ = deps.Tenants.Create(ctx, tenant)
	for i, name := range []string{"send-email", "generate-report", "cleanup-tmp"} {
		prio := domain.PriorityDefault
		if i == 0 {
			prio = domain.PriorityHigh
		}
		_, _ = deps.Scheduler.SubmitJob(ctx, service.SubmitInput{
			TenantID:    tenant.ID,
			Name:        name,
			Payload:     json.RawMessage(`{"demo":true}`),
			Priority:    prio,
			MaxAttempts: 3,
			ScheduledAt: time.Now().UTC(),
		})
	}
	fmt.Println("seeded demo tenant (key: demo-key-123) with 3 jobs")
}

var _ = uuid.New
