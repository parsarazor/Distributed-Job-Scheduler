package app

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/lease"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/postgres"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/service"
)

// Deps bundles everything a binary needs.
type Deps struct {
	Scheduler *service.Scheduler
	Worker    *service.Worker
	Broker    broker.Broker
	Cleanup   func()
	Jobs      repository.JobRepository
	Tenants   repository.TenantRepository
	Events    repository.JobEventRepository
	Outbox    repository.OutboxRepository
	Tx        repository.Transactor
}

// NewFromEnv wires backends from the environment:
// DATABASE_URL -> Postgres, else in-memory.
// RABBITMQ_URL -> RabbitMQ, else in-memory broker.
// REDIS_ADDR   -> Redis leases, else in-memory leases.
func NewFromEnv(ctx context.Context) (*Deps, error) {
	var (
		jobs     repository.JobRepository
		tenants  repository.TenantRepository
		events   repository.JobEventRepository
		outbox   repository.OutboxRepository
		tx       repository.Transactor
		cleanups []func()
	)

	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		p, err := postgres.New(ctx, dsn)
		if err != nil {
			return nil, err
		}
		cleanups = append(cleanups, p.Close)
		repos := postgres.NewRepositories(p)
		jobs, tenants, events, outbox, tx = repos.Jobs, repos.Tenants, repos.Events, repos.Outbox, repos.Tx
	} else {
		b := memory.NewBundle()
		jobs, tenants, events, outbox, tx = b.Jobs, b.Tenants, b.Events, b.Outbox, b.Tx
	}

	var bus broker.Broker
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		rb, err := broker.NewRabbitMQBroker(url, "jobs")
		if err != nil {
			return nil, err
		}
		cleanups = append(cleanups, func() { _ = rb.Close() })
		bus = rb
	} else {
		bus = broker.NewMemoryBroker()
	}

	var leases lease.LeaseManager
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		leases = lease.NewRedisLeaseManager(addr)
	} else {
		leases = lease.NewMemoryLeaseManager()
	}

	sched := service.NewScheduler(jobs, tenants, events, outbox, tx, bus, nil)
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}
	maxConcurrency := int64(10)
	if raw := os.Getenv("MAX_CONCURRENCY"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("invalid MAX_CONCURRENCY %q", raw)
		}
		maxConcurrency = parsed
	}
	worker := service.NewWorker(workerID, sched, leases, maxConcurrency)
	worker.RegisterHandler("default", nil)

	return &Deps{
		Scheduler: sched, Worker: worker, Broker: bus,
		Jobs: jobs, Tenants: tenants, Events: events, Outbox: outbox, Tx: tx,
		Cleanup: func() {
			for _, fn := range cleanups {
				fn()
			}
		},
	}, nil
}
