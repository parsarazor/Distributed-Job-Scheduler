package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/internal/service"
	"github.com/parsarazor/distributed-job-scheduler/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// NewServer enables authenticated RPCs, TLS, tracing and bounded RPC sizes.
func NewServer(deps *Deps, name string, extra ...grpc.ServerOption) (*grpc.Server, *RuntimeHealth, error) {
	cfg := deps.serverConfig
	if cfg == nil {
		env, err := loadServerConfig(os.Getenv("STORAGE"))
		if err != nil {
			return nil, nil, err
		}
		cfg = &env
	}
	h := newRuntimeHealth(name, *cfg)
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(2 << 20),
		grpc.MaxSendMsgSize(4 << 20),
		grpc.MaxHeaderListSize(16 << 10),
		grpc.MaxConcurrentStreams(128),
		grpc.ConnectionTimeout(5 * time.Second),
		grpc.ChainUnaryInterceptor(service.AuthenticatorWithWorkers(deps.Tenants, deps.WorkerCredentials, cfg.sharedWorkerToken(), cfg.adminToken)),
		grpc.StreamInterceptor(h.watchInterceptor),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	}
	if cfg.tls != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(cfg.tls)))
	}
	srv := grpc.NewServer(append(options, extra...)...)
	healthpb.RegisterHealthServer(srv, h)
	return srv, h, nil
}

type OperationsServer struct {
	Errors <-chan error
	http   *http.Server
	cancel context.CancelFunc
	done   <-chan struct{}
}

// Shutdown owns sampler cancellation so callers can shut down operations even
// when their process context is still live.
func (s *OperationsServer) Shutdown(ctx context.Context) error {
	s.cancel()
	err := s.http.Shutdown(ctx)
	if err != nil {
		_ = s.http.Close()
	}
	select {
	case <-s.done:
		return err
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
}

// ServeOperations exposes HTTP health/metrics and samples persisted gauges.
// A worker passes its intake check. The scheduler checks the publisher.
func ServeOperations(ctx context.Context, deps *Deps, health *RuntimeHealth, defaultAddr string, intakeCheck func(context.Context) error) (*OperationsServer, error) {
	address := os.Getenv("METRICS_ADDR")
	if address == "" {
		address = defaultAddr
	}
	listener, err := listenTCP(address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	if intakeCheck == nil {
		if b, ok := deps.Broker.(interface{ Ping(context.Context) error }); ok {
			intakeCheck = b.Ping
		}
	}
	check := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if deps.checkSchema != nil {
			// Checks the Postgres schema, or scheduler health for a remote worker.
			if err := deps.checkSchema(ctx); err != nil {
				return err
			}
		} else if deps.Operations != nil {
			if err := deps.Operations.Ping(ctx); err != nil {
				return err
			}
		}
		if intakeCheck != nil {
			return intakeCheck(ctx)
		}
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		Timeout: 3 * time.Second, MaxRequestsInFlight: 4,
	})))
	probes := make(chan struct{}, 16)
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, req *http.Request) {
		if ctx.Err() != nil || !health.isServing() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		select {
		case probes <- struct{}{}:
			defer func() { <-probes }()
		default:
			http.Error(w, "readiness busy", http.StatusServiceUnavailable)
			return
		}
		checkCtx, done := context.WithTimeout(req.Context(), 2*time.Second)
		defer done()
		if check(checkCtx) != nil || ctx.Err() != nil || !health.isServing() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{
		Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	serveErrors := make(chan error, 1)
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			health.Shutdown()
			cancel()
			serveErrors <- err
		}
	}()
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		defer health.Shutdown()
		sampleOperations(ctx, check, deps, health)
	}()
	return &OperationsServer{Errors: serveErrors, http: srv, cancel: cancel, done: samplerDone}, nil
}

func sampleOperations(ctx context.Context, check func(context.Context) error, deps *Deps, health *RuntimeHealth) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var priorReadinessErr, priorStatsErr bool
	for ctx.Err() == nil {
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := check(checkCtx)
		cancel()
		health.setReady(err == nil)
		if ctx.Err() != nil {
			return
		}
		logSamplingState("readiness check", err, &priorReadinessErr)
		if deps.Operations == nil {
			// Database-free workers have no persisted gauges to sample.
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}

		statsCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		stats, err := deps.Operations.Stats(statsCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		logSamplingState("persistent metrics sample", err, &priorStatsErr)
		if err != nil {
			telemetry.StatsSampleFailures.Inc()
		} else {
			for _, status := range []string{"pending", "running", "failed", "succeeded", "dead", "cancelled"} {
				telemetry.QueueDepth.WithLabelValues(status).Set(float64(stats.Jobs[status]))
			}
			telemetry.OutboxDepth.Set(float64(stats.OutboxPending))
			telemetry.OutboxLag.Set(stats.OutboxLag.Seconds())
			telemetry.StatsLastSuccess.SetToCurrentTime()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func logSamplingState(name string, err error, failed *bool) {
	if err != nil && !*failed {
		slog.Warn(name+" failed", "error", err)
	} else if err == nil && *failed {
		slog.Info(name + " recovered")
	}
	*failed = err != nil
}

// Loop runs bounded background operations and participates in shutdown.
// A panic inside a single tick is recovered and logged so one bad tick
// cannot crash the scheduler process; the loop keeps ticking.
func Loop(ctx context.Context, wg *sync.WaitGroup, interval time.Duration, name string, fn func(context.Context) error) {
	WakeLoop(ctx, wg, interval, nil, name, fn)
}

// WakeLoop is Loop that also runs fn whenever wake fires.
func WakeLoop(ctx context.Context, wg *sync.WaitGroup, interval time.Duration, wake <-chan struct{}, name string, fn func(context.Context) error) {
	wg.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-wake:
			case <-ticker.C:
			}
			if ctx.Err() != nil {
				return
			}
			func() {
				defer func() {
					if p := recover(); p != nil {
						slog.Error(name+": tick panicked", "panic", p, "stack", string(debug.Stack()))
					}
				}()
				runCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
				defer cancel()
				if err := fn(runCtx); err != nil && ctx.Err() == nil {
					slog.Error(name, "error", err)
				}
			}()
		}
	})
}

// StopServer gives active RPCs a bounded grace period. grpc-go can hold its
// shutdown lock while waiting for a handler that ignores cancellation, so both
// stop paths have a deadline.
func StopServer(server *grpc.Server) {
	stopServer(server, 10*time.Second, time.Second)
}

func stopServer(server *grpc.Server, grace, forced time.Duration) {
	done := make(chan struct{})
	go func() { server.GracefulStop(); close(done) }()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
	}
	stopped := make(chan struct{})
	go func() { server.Stop(); close(stopped) }()
	timer.Reset(forced)
	select {
	case <-stopped:
	case <-done:
	case <-timer.C:
		slog.Warn("gRPC server shutdown exceeded its time budget")
	}
}
