package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
)

// Env-driven backend selection is load-bearing production logic, so pin its
// explicit local mode and fail-fast production defaults here.
func TestNewFromEnv_ExplicitMemory(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	t.Setenv("DEMO_SEED", "false")
	t.Setenv("STORAGE", "memory")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RABBITMQ_URL", "")
	t.Setenv("WORKER_ID", "")

	deps, err := NewFromEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer deps.Cleanup()
	if _, ok := deps.Jobs.(*memory.JobRepo); !ok {
		t.Fatalf("memory jobs backend must be selected, got %T", deps.Jobs)
	}
	if _, ok := deps.Tenants.(*memory.TenantRepo); !ok {
		t.Fatalf("memory tenants backend must be selected, got %T", deps.Tenants)
	}
	if _, ok := deps.Tx.(*memory.Transactor); !ok {
		t.Fatalf("memory transactor must be selected, got %T", deps.Tx)
	}
	if deps.Worker.ID() != "worker-1" {
		t.Fatalf("default worker id must be worker-1, got %q", deps.Worker.ID())
	}
	if deps.Scheduler == nil || deps.Broker == nil {
		t.Fatal("scheduler and broker must be wired")
	}
	if deps.SeedDemo {
		t.Fatal("demo seeding must be opt in")
	}
}

func TestNewFromEnv_WorkerIDOverride(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	t.Setenv("STORAGE", "memory")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RABBITMQ_URL", "")
	t.Setenv("WORKER_ID", "edge-7")

	deps, err := NewFromEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer deps.Cleanup()
	if deps.Worker.ID() != "edge-7" {
		t.Fatalf("worker id must follow env, got %q", deps.Worker.ID())
	}
}

func TestNewFromEnv_BadDSNErrors(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	t.Setenv("STORAGE", "postgres")
	t.Setenv("DATABASE_URL", "://not-a-valid-dsn")
	t.Setenv("RABBITMQ_URL", "amqp://scheduler:scheduler@localhost:5672/")
	if _, err := NewFromEnv(context.Background()); err == nil {
		t.Fatal("malformed DATABASE_URL must fail fast, not boot half-wired")
	}
}

func TestNewFromEnv_RequiresProductionConfiguration(t *testing.T) {
	for _, storage := range []string{"", "postgres"} {
		t.Run(storage, func(t *testing.T) {
			t.Setenv("STORAGE", storage)
			t.Setenv("DATABASE_URL", "")
			t.Setenv("RABBITMQ_URL", "")
			if _, err := NewFromEnv(context.Background()); err == nil || err.Error() != "DATABASE_URL is required unless STORAGE=memory" {
				t.Fatalf("missing database URL must fail before connecting, got %v", err)
			}

			t.Setenv("DATABASE_URL", "postgres://unused")
			if _, err := NewFromEnv(context.Background()); err == nil || err.Error() != "RABBITMQ_URL is required unless STORAGE=memory" {
				t.Fatalf("missing RabbitMQ URL must fail before connecting, got %v", err)
			}
		})
	}
}

func TestNewFromEnv_RejectsUnknownStorage(t *testing.T) {
	t.Setenv("STORAGE", "other")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RABBITMQ_URL", "")
	if _, err := NewFromEnv(context.Background()); err == nil || err.Error() != `unknown STORAGE "other": expected memory or postgres` {
		t.Fatalf("unknown storage must be rejected, got %v", err)
	}
}

// M2: one panicking tick must not take down the loop; the next tick runs.
func TestLoop_RecoversFromPanickingTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	ticks := make(chan int, 4)
	go Loop(ctx, &wg, 2*time.Millisecond, "panic-loop", func(context.Context) error {
		ticks <- 1
		panic("boom")
	})
	deadline := time.After(2 * time.Second)
	seen := 0
	for seen < 2 {
		select {
		case <-ticks:
			seen++
		case <-deadline:
			t.Fatalf("loop died after panic, ticks=%d", seen)
		}
	}
	cancel()
	wg.Wait()
}
