package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/postgres"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
)

// Deps bundles everything a binary needs.
type Deps struct {
	Scheduler      *service.Scheduler
	Worker         *service.Worker
	Broker         broker.Broker
	Cleanup        func()
	Jobs           repository.JobRepository
	Tenants        repository.TenantRepository
	Events         repository.JobEventRepository
	Outbox         repository.OutboxRepository
	Tx             repository.Transactor
	ReaperInterval time.Duration
}

// NewFromEnv validates configuration before connecting. Only STORAGE=memory
// enables the local, process-private bundle; the default requires Postgres/RabbitMQ.
func NewFromEnv(ctx context.Context) (*Deps, error) {
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
	maxConcurrency := int64(10)
	if raw := os.Getenv("MAX_CONCURRENCY"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("invalid MAX_CONCURRENCY %q", raw)
		}
		maxConcurrency = parsed
	}
	leaseTTL, err := durationFromEnv("LEASE_TTL", 30*time.Second)
	if err != nil {
		return nil, err
	}
	reaperInterval, err := durationFromEnv("REAPER_INTERVAL", 10*time.Second)
	if err != nil {
		return nil, err
	}

	var (
		jobs     repository.JobRepository
		tenants  repository.TenantRepository
		events   repository.JobEventRepository
		outbox   repository.OutboxRepository
		tx       repository.Transactor
		bus      broker.Broker
		cleanups []func()
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
		bus = broker.NewMemoryBroker()
	} else {
		p, err := postgres.New(ctx, os.Getenv("DATABASE_URL"))
		if err != nil {
			return nil, fmt.Errorf("DATABASE_URL: %w", err)
		}
		cleanups = append(cleanups, p.Close)
		if err := p.Ping(ctx); err != nil {
			cleanup()
			return nil, fmt.Errorf("connect Postgres: %w", err)
		}
		repos := postgres.NewRepositories(p)
		jobs, tenants, events, outbox, tx = repos.Jobs, repos.Tenants, repos.Events, repos.Outbox, repos.Tx
		rb, err := broker.NewRabbitMQBroker(os.Getenv("RABBITMQ_URL"), "jobs")
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("connect RabbitMQ: %w", err)
		}
		cleanups = append(cleanups, func() { _ = rb.Close() })
		bus = rb
	}
	sched := service.NewScheduler(jobs, tenants, events, outbox, tx, bus, nil)
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}
	worker := service.NewWorker(workerID, sched, maxConcurrency, leaseTTL)
	return &Deps{
		Scheduler: sched, Worker: worker, Broker: bus,
		Jobs: jobs, Tenants: tenants, Events: events, Outbox: outbox, Tx: tx,
		Cleanup: cleanup, ReaperInterval: reaperInterval,
	}, nil
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
