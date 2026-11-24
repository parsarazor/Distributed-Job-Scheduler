// Command worker runs pkg/worker with the admin-only worker gRPC port and the
// operations (metrics and readiness) server.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/examples/sendemail"
	"github.com/parsarazor/distributed-job-scheduler/internal/app"
	"github.com/parsarazor/distributed-job-scheduler/internal/service"
	"github.com/parsarazor/distributed-job-scheduler/internal/telemetry"
	"github.com/parsarazor/distributed-job-scheduler/internal/workersdk"
	apiv1 "github.com/parsarazor/distributed-job-scheduler/pkg/api/v1"
	"github.com/parsarazor/distributed-job-scheduler/pkg/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	switch os.Getenv("STORAGE") {
	case "", "postgres":
	default:
		return errors.New("cmd/worker needs a scheduler (SCHEDULER_ADDR); use cmd/demo for an in-process run")
	}
	shutdownTracing, err := telemetry.Init(ctx, "job-worker")
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()
	cfg, err := worker.ConfigFromEnv()
	if err != nil {
		return err
	}
	w, err := worker.New(cfg)
	if err != nil {
		return err
	}
	if env := os.Getenv("APP_ENV"); env == "development" || env == "demo" {
		slog.Warn("registering simulated send-email handler", "environment", env)
		w.HandleFunc(sendemail.Name, sendemail.Handle)
	}
	ready := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !w.Ready() {
			return errors.New("worker not ready")
		}
		return nil
	}
	engine := workersdk.Service(w)
	deps, err := app.NewWorkerPortDeps(engine, ready)
	if err != nil {
		return err
	}
	lis, err := app.ListenRPC("50052")
	if err != nil {
		return err
	}
	defer lis.Close()
	grpcServer, health, err := app.NewServer(deps, apiv1.WorkerService_ServiceDesc.ServiceName)
	if err != nil {
		return err
	}
	apiv1.RegisterWorkerServiceServer(grpcServer, service.NewWorkerServer(engine))
	// Operations outlive an RPC DrainWorker: metrics remain available during drain.
	operations, err := app.ServeOperations(ctx, deps, health, "127.0.0.1:9091", ready)
	if err != nil {
		return err
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	serveDone := make(chan error, 1)
	go func() { serveDone <- health.Serve(grpcServer, lis) }()
	go func() {
		select {
		case err := <-serveDone:
			cancelRun(fmtExit("gRPC server exited", err))
		case err := <-operations.Errors:
			cancelRun(fmtExit("operations server exited", err))
		case <-runCtx.Done():
		}
	}()
	runErr := w.Run(runCtx)
	health.Shutdown()
	app.StopServer(grpcServer)
	opsCtx, stopOps := context.WithTimeout(context.Background(), time.Second)
	_ = operations.Shutdown(opsCtx)
	stopOps()
	var fatal error
	if cause := context.Cause(runCtx); ctx.Err() == nil && cause != nil && !errors.Is(cause, context.Canceled) {
		fatal = cause
	}
	return errors.Join(fatal, runErr)
}

func fmtExit(message string, err error) error {
	if err == nil {
		return errors.New(message)
	}
	return errors.Join(errors.New(message), err)
}
