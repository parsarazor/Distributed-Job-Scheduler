package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/parsarazor/distributed-job-scheduler/internal/broker"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/memory"
	"github.com/parsarazor/distributed-job-scheduler/internal/postgres"
	"github.com/parsarazor/distributed-job-scheduler/internal/repository"
	"github.com/parsarazor/distributed-job-scheduler/internal/service"
)

// Deps bundles everything a binary needs.
type Deps struct {
	Scheduler         *service.Scheduler
	Worker            *service.Worker
	Broker            broker.Broker
	Cleanup           func()
	Jobs              repository.JobRepository
	Tenants           repository.TenantRepository
	WorkerCredentials repository.WorkerCredentialRepository
	Events            repository.JobEventRepository
	Outbox            repository.OutboxRepository
	Tx                repository.Transactor
	Operations        repository.Operations
	ReaperInterval    time.Duration
	// DispatchInterval bounds dispatch latency when no wake signal fires.
	DispatchInterval time.Duration
	// LeaseRestartGrace extends lapsed leases at startup so workers that lost
	// contact during a scheduler outage can still renew or complete.
	LeaseRestartGrace time.Duration
	SeedDemo          bool
	checkSchema       func(context.Context) error
	serverConfig      *serverConfig
}

// NewFromEnv validates configuration before connecting. Only STORAGE=memory
// enables the local, process-private bundle; the default requires Postgres/RabbitMQ.
func NewFromEnv(ctx context.Context) (*Deps, error) {
	ctx, cancelStartup := context.WithTimeout(ctx, 15*time.Second)
	defer cancelStartup()
	storage := os.Getenv("STORAGE")
	switch storage {
	case "memory":
	case "", "postgres":
		for _, name := range []string{"DATABASE_URL", "RABBITMQ_URL"} {
			if os.Getenv(name) == "" {
				return nil, fmt.Errorf("%s is required unless STORAGE=memory", name)
			}
		}
	default:
		return nil, fmt.Errorf("unknown STORAGE %q: expected memory or postgres", storage)
	}
	if storage == "memory" && os.Getenv("APP_ENV") != "development" {
		return nil, fmt.Errorf("STORAGE=memory requires APP_ENV=development")
	}
	cfg, err := loadServerConfig(storage)
	if err != nil {
		return nil, err
	}
	limits, err := workerLimitsFromEnv()
	if err != nil {
		return nil, err
	}
	reaperInterval, err := durationFromEnv("REAPER_INTERVAL", 10*time.Second)
	if err != nil {
		return nil, err
	}
	dispatchInterval, err := durationFromEnv("DISPATCH_INTERVAL", 2*time.Second)
	if err != nil {
		return nil, err
	}
	leaseRestartGrace, err := durationFromEnv("LEASE_RESTART_GRACE", 30*time.Second)
	if err != nil {
		return nil, err
	}
	var (
		jobs        repository.JobRepository
		tenants     repository.TenantRepository
		workerCreds repository.WorkerCredentialRepository
		events      repository.JobEventRepository
		outbox      repository.OutboxRepository
		tx          repository.Transactor
		bus         broker.Broker
		cleanups    []func()
		checkSchema func(context.Context) error
	)
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	if storage == "memory" {
		log.Print("WARNING: STORAGE=memory is for LOCAL DEVELOPMENT/TESTS ONLY; data and leases are process-private and not durable")
		b := memory.NewBundle()
		jobs, tenants, events, outbox, tx = b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx
		workerCreds = b.WorkerCredentials
		bus = broker.NewMemoryBroker()
	} else {
		pgConfig, err := parseDatabaseConfig(os.Getenv("DATABASE_URL"), cfg.development)
		if err != nil {
			return nil, err
		}
		rabbitURL := os.Getenv("RABBITMQ_URL")
		if err := validateRabbitURL(rabbitURL, cfg.development); err != nil {
			return nil, err
		}
		p, err := postgres.NewWithConfig(ctx, pgConfig)
		if err != nil {
			return nil, fmt.Errorf("initialize Postgres pool: %w", err)
		}
		cleanups = append(cleanups, p.Close)
		if err := p.Ping(ctx); err != nil {
			cleanup()
			return nil, fmt.Errorf("connect Postgres: %w", err)
		}
		checkSchema = p.CheckSchema
		if err := checkSchema(ctx); err != nil {
			cleanup()
			return nil, fmt.Errorf("check Postgres schema: %w", err)
		}
		repos := postgres.NewRepositories(p)
		jobs, tenants, events, outbox, tx = repos.Jobs, repos.Tenants, repos.Events, repos.Outbox, repos.Tx
		workerCreds = repos.WorkerCredentials
		rb, err := broker.NewRabbitMQBrokerContext(ctx, rabbitURL, "jobs")
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("connect RabbitMQ: %w", err)
		}
		cleanups = append(cleanups, func() { _ = rb.Close() })
		bus = rb
	}
	sched := service.NewScheduler(jobs, tenants, events, outbox, tx, bus, nil)
	sched.SetWorkerCredentials(workerCreds)
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}
	if !domain.ValidWorkerID(workerID) {
		cleanup()
		return nil, fmt.Errorf("WORKER_ID %q must match %s", workerID, domain.WorkerIDPattern)
	}
	worker := service.NewWorker(workerID, sched, limits.maxConcurrency, limits.leaseTTL)
	worker.SetJobTimeout(limits.jobTimeout)
	worker.SetCompletionWindow(limits.completionWindow)
	ops := tx.(repository.Operations)
	return &Deps{
		Scheduler: sched, Worker: worker, Broker: bus,
		Jobs: jobs, Tenants: tenants, WorkerCredentials: workerCreds, Events: events, Outbox: outbox, Tx: tx,
		Cleanup: cleanup, ReaperInterval: reaperInterval, DispatchInterval: dispatchInterval,
		LeaseRestartGrace: leaseRestartGrace,
		Operations:        ops, SeedDemo: cfg.seedDemo, checkSchema: checkSchema, serverConfig: &cfg,
	}, nil
}

type workerLimits struct {
	maxConcurrency int64
	leaseTTL       time.Duration
	jobTimeout     time.Duration
	// completionWindow bounds result retries during a scheduler outage.
	completionWindow time.Duration
}

func workerLimitsFromEnv() (workerLimits, error) {
	limits := workerLimits{maxConcurrency: 10}
	if raw := os.Getenv("MAX_CONCURRENCY"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 || parsed > 65535 {
			return limits, fmt.Errorf("invalid MAX_CONCURRENCY %q", raw)
		}
		limits.maxConcurrency = parsed
	}
	var err error
	if limits.leaseTTL, err = durationFromEnv("LEASE_TTL", 30*time.Second); err != nil {
		return limits, err
	}
	if limits.jobTimeout, err = durationFromEnv("JOB_TIMEOUT", 5*time.Minute); err != nil {
		return limits, err
	}
	if limits.completionWindow, err = durationFromEnv("COMPLETION_RETRY_WINDOW", service.DefaultCompletionWindow); err != nil {
		return limits, err
	}
	return limits, nil
}

func durationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid %s %q: expected a positive duration", name, raw)
	}
	return d, nil
}
