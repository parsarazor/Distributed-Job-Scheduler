// Command scheduler runs the gRPC SchedulerService plus background
// dispatch / retry / outbox-reconciliation loops.
//
// Backends are chosen from STORAGE (see internal/app). STORAGE=memory is an
// explicit local-only mode; unset or postgres requires DATABASE_URL and RABBITMQ_URL.
//
// gRPC defaults to 127.0.0.1:50051.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/app"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/telemetry"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("scheduler exited", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) (runErr error) {
	shutdownTracing, err := telemetry.Init(ctx, "job-scheduler")
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			slog.Warn("shutdown tracing", "error", err)
		}
	}()
	deps, err := app.NewFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("wire backends: %w", err)
	}
	defer deps.Cleanup()

	if deps.SeedDemo {
		if err := seedDemo(ctx, deps); err != nil {
			return err
		}
	}

	lis, err := app.ListenRPC("50051")
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	defer lis.Close()
	grpcServer, health, err := app.NewServer(deps, apiv1.SchedulerService_ServiceDesc.ServiceName,
		// Recycled connections make clients re-resolve, so new replicas get
		// traffic. Scheduler RPCs are short; the worker's DrainWorker is not.
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 5 * time.Minute, MaxConnectionAgeGrace: time.Minute}))
	if err != nil {
		return err
	}
	apiv1.RegisterSchedulerServiceServer(grpcServer, service.NewSchedulerServer(deps.Scheduler))

	// Without the grace, the reaper could re-run jobs whose results workers
	// are still retrying, so it gates serving and the reaper.
	if err := grantLeaseGrace(ctx, deps); err != nil {
		return err
	}

	// Background loops: dispatch, retry, reconcile and lease recovery.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	ops, err := app.ServeOperations(runCtx, deps, health, "127.0.0.1:9090", nil)
	if err != nil {
		return fmt.Errorf("listen for operations HTTP: %w", err)
	}
	var loops sync.WaitGroup
	app.WakeLoop(runCtx, &loops, deps.DispatchInterval, deps.Scheduler.DispatchWake(), "dispatch", func(ctx context.Context) error { _, err := deps.Scheduler.DispatchReady(ctx, 100); return err })
	app.WakeLoop(runCtx, &loops, time.Second, deps.Scheduler.PublishWake(), "outbox", func(ctx context.Context) error { _, err := deps.Scheduler.ReconcileOutbox(ctx, 100); return err })
	app.Loop(runCtx, &loops, 10*time.Second, "retry recovery", func(ctx context.Context) error { _, err := deps.Scheduler.RetryDue(ctx, 100); return err })
	app.Loop(runCtx, &loops, deps.ReaperInterval, "lease reaper", func(ctx context.Context) error { _, err := deps.Scheduler.ReapExpiredLeases(ctx, 100); return err })
	app.Loop(runCtx, &loops, time.Hour, "retention", func(ctx context.Context) error { return deps.Operations.Prune(ctx, 7*24*time.Hour, 90*24*time.Hour) })

	serveErrors := make(chan error, 1)
	go func() { serveErrors <- health.Serve(grpcServer, lis) }()
	slog.Info("scheduler listening", "addr", lis.Addr().String(), "storage", storageMode())
	select {
	case <-ctx.Done():
	case err := <-ops.Errors:
		runErr = fmt.Errorf("operations server stopped: %w", err)
	case err := <-serveErrors:
		if ctx.Err() == nil {
			runErr = fmt.Errorf("gRPC server stopped: %w", errors.Join(errors.New("unexpected Serve exit"), err))
		}
	}
	stop()
	health.Shutdown()
	app.StopServer(grpcServer)
	loops.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ops.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("shutdown operations: %w", err))
	}
	slog.Info("scheduler stopped")
	return runErr
}

func grantLeaseGrace(ctx context.Context, deps *app.Deps) error {
	graceCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		n, err := deps.Scheduler.GrantLeaseGrace(graceCtx, deps.LeaseRestartGrace)
		if err == nil {
			if n > 0 {
				slog.Info("extended lapsed leases after restart", "leases", n, "grace", deps.LeaseRestartGrace)
			}
			return nil
		}
		slog.Warn("lease restart grace failed; retrying", "error", err)
		select {
		case <-graceCtx.Done():
			return fmt.Errorf("extend leases after restart: %w", err)
		case <-time.After(time.Second):
		}
	}
}

func storageMode() string {
	if os.Getenv("STORAGE") == "" {
		return "postgres"
	}
	return os.Getenv("STORAGE")
}

// seedDemo creates a known tenant and sample jobs in an opted-in local instance.
func seedDemo(ctx context.Context, deps *app.Deps) error {
	tenant, err := domain.NewTenant("demo", "demo-key-123")
	if err != nil {
		return err
	}
	if err := deps.Tenants.Create(ctx, tenant); err != nil {
		return fmt.Errorf("seed demo tenant: %w", err)
	}
	for i, name := range []string{"send-email", "generate-report", "cleanup-tmp"} {
		prio := domain.PriorityDefault
		if i == 0 {
			prio = domain.PriorityHigh
		}
		if _, err := deps.Scheduler.SubmitJob(ctx, service.SubmitInput{
			TenantID:    tenant.ID,
			Name:        name,
			Payload:     json.RawMessage(`{"demo":true}`),
			Priority:    prio,
			MaxAttempts: 3,
			ScheduledAt: time.Now().UTC(),
		}); err != nil {
			return fmt.Errorf("seed demo job %s: %w", name, err)
		}
	}
	fmt.Println("seeded demo tenant (key: demo-key-123) with 3 jobs")
	return nil
}
