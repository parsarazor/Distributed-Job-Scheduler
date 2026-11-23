package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// NewWorkerFromEnv wires a worker. STORAGE=memory keeps the process-private
// in-process bundle. Otherwise the worker holds no database connection: it
// consumes RabbitMQ announcements and reaches job state only through the
// scheduler at SCHEDULER_ADDR, authenticated with WORKER_TOKEN.
func NewWorkerFromEnv(ctx context.Context) (*Deps, error) {
	storage := os.Getenv("STORAGE")
	switch storage {
	case "memory":
		return NewFromEnv(ctx)
	case "", "postgres":
	default:
		return nil, fmt.Errorf("unknown STORAGE %q: expected memory or postgres", storage)
	}
	ctx, cancelStartup := context.WithTimeout(ctx, 15*time.Second)
	defer cancelStartup()
	if os.Getenv("DATABASE_URL") != "" {
		return nil, errors.New("workers must not receive DATABASE_URL: the scheduler owns job state; set SCHEDULER_ADDR")
	}
	schedulerAddr, rabbitURL := os.Getenv("SCHEDULER_ADDR"), os.Getenv("RABBITMQ_URL")
	if schedulerAddr == "" || rabbitURL == "" {
		return nil, errors.New("SCHEDULER_ADDR and RABBITMQ_URL are required unless STORAGE=memory")
	}
	cfg, err := loadServerConfig(storage)
	if err != nil {
		return nil, err
	}
	if cfg.workerToken == "" {
		return nil, errors.New("WORKER_TOKEN is required to call the scheduler")
	}
	if err := validateRabbitURL(rabbitURL, cfg.development); err != nil {
		return nil, err
	}
	limits, err := workerLimitsFromEnv()
	if err != nil {
		return nil, err
	}
	if limits.leaseTTL < time.Millisecond || limits.leaseTTL > service.MaxWorkerLease {
		return nil, fmt.Errorf("LEASE_TTL must be between 1ms and %s for scheduler-backed workers", service.MaxWorkerLease)
	}
	transport, secure, err := schedulerTransport(cfg.development)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(schedulerAddr,
		grpc.WithTransportCredentials(transport),
		grpc.WithPerRPCCredentials(bearerCredential{token: cfg.workerToken, secure: secure}),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("configure scheduler client: %w", err)
	}
	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	cleanups = append(cleanups, func() { _ = conn.Close() })
	health := healthpb.NewHealthClient(conn)
	scheduler := apiv1.NewSchedulerServiceClient(conn)
	schedulerReady := func(ctx context.Context) error {
		resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return fmt.Errorf("scheduler health: %w", err)
		}
		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("scheduler is %s", resp.GetStatus())
		}
		// Health is public; probe a worker-role RPC so a rejected WORKER_TOKEN
		// keeps the worker unready instead of failing every claim.
		_, err = scheduler.GetExecutionJob(ctx, &apiv1.GetExecutionJobRequest{JobId: uuid.Nil.String()})
		switch status.Code(err) {
		case codes.OK, codes.NotFound, codes.Aborted:
			return nil
		case codes.Unauthenticated, codes.PermissionDenied:
			return errors.New("scheduler rejected WORKER_TOKEN")
		default:
			return fmt.Errorf("scheduler worker RPCs: %w", err)
		}
	}
	if err := schedulerReady(ctx); err != nil {
		cleanup()
		return nil, fmt.Errorf("connect scheduler: %w", err)
	}
	rb, err := broker.NewRabbitMQBrokerContext(ctx, rabbitURL, "jobs")
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("connect RabbitMQ: %w", err)
	}
	cleanups = append(cleanups, func() { _ = rb.Close() })
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}
	control := service.NewRemoteJobControl(scheduler)
	worker := service.NewRemoteWorker(workerID, control, limits.maxConcurrency, limits.leaseTTL)
	worker.SetJobTimeout(limits.jobTimeout)
	worker.SetCompletionWindow(limits.completionWindow)
	return &Deps{Worker: worker, Broker: rb, Cleanup: cleanup, checkSchema: schedulerReady, serverConfig: &cfg}, nil
}

// schedulerTransport uses TLS (system roots or SCHEDULER_CA_FILE) everywhere
// except explicit development without a CA file.
func schedulerTransport(development bool) (credentials.TransportCredentials, bool, error) {
	caFile := os.Getenv("SCHEDULER_CA_FILE")
	if development && caFile == "" {
		return insecure.NewCredentials(), false, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: os.Getenv("SCHEDULER_TLS_SERVER_NAME")}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, false, fmt.Errorf("read SCHEDULER_CA_FILE: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, false, errors.New("SCHEDULER_CA_FILE contains no PEM certificates")
		}
		config.RootCAs = pool
	}
	return credentials.NewTLS(config), true, nil
}

type bearerCredential struct {
	token  string
	secure bool
}

func (b bearerCredential) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearerCredential) RequireTransportSecurity() bool { return b.secure }
