// Command worker serves execution RPCs and drains intake before bounded shutdown.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/internal/app"
	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/service"
	"github.com/parsarazor/distributed-job-scheduler/internal/telemetry"
	apiv1 "github.com/parsarazor/distributed-job-scheduler/pkg/api/v1"
)

func main() {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	if err := run(signals); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run(signals <-chan os.Signal) error {
	ctx := context.Background()
	startupCtx, observedSignals, stopSignals := relayWorkerSignals(ctx, signals)
	defer stopSignals()
	signals = observedSignals
	shutdownTracing, err := telemetry.Init(startupCtx, "job-worker")
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()
	deps, err := app.NewWorkerFromEnv(startupCtx)
	if err != nil {
		return err
	}
	// Cleanup is safe before intake starts. After startup a noncooperative
	// handler may require process exit rather than an unbounded Broker.Close.
	cleanup := true
	defer func() {
		if cleanup {
			deps.Cleanup()
		}
	}()
	registerDemoHandler(deps.Worker, os.Getenv("APP_ENV"))
	lis, err := app.ListenRPC("50052")
	if err != nil {
		return err
	}
	defer lis.Close()
	grpcServer, health, err := app.NewServer(deps, apiv1.WorkerService_ServiceDesc.ServiceName)
	if err != nil {
		return err
	}
	apiv1.RegisterWorkerServiceServer(grpcServer, service.NewWorkerServer(deps.Worker))
	intakeCtx, stopIntake := context.WithCancel(ctx)
	defer stopIntake()
	deps.Worker.SetIntakeCancel(func() { stopIntake(); health.Shutdown() })
	consumerReady := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if deps.Worker.IsDraining() || intakeCtx.Err() != nil {
			return errors.New("worker intake stopped")
		}
		if b, ok := deps.Broker.(interface{ ConsumerReady() bool }); ok && !b.ConsumerReady() {
			return errors.New("worker subscription unavailable")
		}
		return nil
	}
	// Operations outlive RPC DrainWorker: metrics remain available during drain.
	operations, err := app.ServeOperations(ctx, deps, health, "127.0.0.1:9091", consumerReady)
	if err != nil {
		return err
	}
	intakeDone := make(chan error, 1)
	go func() { intakeDone <- consume(intakeCtx, deps) }()
	serveDone := make(chan error, 1)
	go func() { serveDone <- health.Serve(grpcServer, lis) }()
	cleanup = false
	var fatal error
	intakeExited, serveExited := false, false
	select {
	case <-signals:
	case err := <-serveDone:
		serveExited = true
		fatal = fmtExit("gRPC server exited", err)
	case err := <-operations.Errors:
		fatal = fmtExit("operations server exited", err)
	case err := <-intakeDone:
		intakeExited = true
		// An explicit drain stops intake while the process stays observable.
		if !deps.Worker.IsDraining() {
			fatal = fmtExit("worker intake exited", err)
		} else {
			select {
			case <-signals:
			case err := <-serveDone:
				serveExited = true
				fatal = fmtExit("gRPC server exited", err)
			case err := <-operations.Errors:
				fatal = fmtExit("operations server exited", err)
			}
		}
	}
	slog.Info("draining worker")
	deps.Worker.Drain()
	health.Shutdown()
	shutdownCtx, cancelShutdown := context.WithTimeout(ctx, 25*time.Second)
	defer cancelShutdown()
	serverStopped := make(chan struct{})
	go func() { app.StopServer(grpcServer); close(serverStopped) }()
	drained := drainWorker(shutdownCtx, deps.Worker, 15*time.Second, 5*time.Second, signals)
	// Settlement and consumer cancellation also share the process budget.
	if !intakeExited {
		select {
		case <-intakeDone:
			intakeExited = true
		case <-shutdownCtx.Done():
		}
	}
	opsCtx, stopOps := context.WithTimeout(shutdownCtx, time.Second)
	_ = operations.Shutdown(opsCtx)
	stopOps()
	if !serveExited {
		select {
		case <-serverStopped:
		case <-shutdownCtx.Done():
		}
	}
	if drained && intakeExited {
		// Backend cleanup itself must not extend the process shutdown budget.
		cleaned := make(chan struct{})
		go func() { deps.Cleanup(); close(cleaned) }()
		select {
		case <-cleaned:
		case <-shutdownCtx.Done():
		}
	} else {
		slog.Warn("worker shutdown escalated; exiting with unfinished handlers")
		if fatal == nil {
			fatal = errors.New("worker shutdown budget exhausted")
		}
	}
	return fatal
}

// The first signal interrupts startup without canceling post-start executions.
// Signals remain queued for the shutdown loop and a second signal escalates it.
func relayWorkerSignals(ctx context.Context, signals <-chan os.Signal) (context.Context, <-chan os.Signal, func()) {
	startup, cancel := context.WithCancel(ctx)
	observed := make(chan os.Signal, 2)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case sig, ok := <-signals:
				if !ok {
					return
				}
				cancel()
				select {
				case observed <- sig:
				default:
				}
			case <-done:
				return
			}
		}
	}()
	return startup, observed, func() { close(done); cancel() }
}

func fmtExit(message string, err error) error {
	if err == nil {
		return errors.New(message)
	}
	return errors.Join(errors.New(message), err)
}

func consume(ctx context.Context, deps *app.Deps) error {
	if consumer, ok := deps.Broker.(broker.Consumer); ok {
		return consumer.Consume(ctx, broker.JobsQueue, broker.JobsRoutingKey, int(deps.Worker.MaxConcurrency()), deps.Worker.HandleMessage)
	}
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := deps.Worker.RunOnce(context.Background(), int(deps.Worker.MaxConcurrency())); err != nil && ctx.Err() == nil {
				slog.Warn("worker polling failed", "error", err)
			}
		}
	}
}

func drainWorker(ctx context.Context, w *service.Worker, grace, unwind time.Duration, force <-chan os.Signal) bool {
	waitCtx, cancel := context.WithTimeout(ctx, grace)
	result := make(chan error, 1)
	go func() { result <- w.Wait(waitCtx) }()
	select {
	case err := <-result:
		cancel()
		if err == nil {
			return true
		}
	case <-force:
		cancel()
	case <-ctx.Done():
		cancel()
	}
	w.CancelAll()
	unwindCtx, stop := context.WithTimeout(ctx, unwind)
	defer stop()
	return w.Wait(unwindCtx) == nil
}

func registerDemoHandler(w *service.Worker, environment string) {
	if environment != "development" && environment != "demo" {
		return
	}
	slog.Warn("registering simulated send-email handler", "environment", environment)
	w.RegisterHandler("send-email", func(ctx context.Context, _ *domain.Job) error {
		t := time.NewTimer(100 * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	})
}
