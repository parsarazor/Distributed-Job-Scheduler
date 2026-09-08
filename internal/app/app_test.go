package app

import (
	"context"
	"os"
	"testing"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/memory"
)

// Principle: wiring test. Env-driven backend selection is load-bearing
// production logic (wrong default = outage); pin the defaults here.
func TestNewFromEnv_MemoryDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RABBITMQ_URL", "")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("WORKER_ID", "")

	deps, err := NewFromEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer deps.Cleanup()
	if _, ok := deps.Jobs.(*memory.JobRepo); !ok {
		t.Fatalf("default jobs backend must be memory, got %T", deps.Jobs)
	}
	if _, ok := deps.Tenants.(*memory.TenantRepo); !ok {
		t.Fatalf("default tenants backend must be memory, got %T", deps.Tenants)
	}
	if _, ok := deps.Tx.(*memory.Transactor); !ok {
		t.Fatalf("default transactor must be memory, got %T", deps.Tx)
	}
	if deps.Worker.ID() != "worker-1" {
		t.Fatalf("default worker id must be worker-1, got %q", deps.Worker.ID())
	}
	if deps.Scheduler == nil || deps.Broker == nil {
		t.Fatal("scheduler and broker must be wired")
	}
}

func TestNewFromEnv_WorkerIDOverride(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RABBITMQ_URL", "")
	t.Setenv("REDIS_ADDR", "")
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
	t.Setenv("DATABASE_URL", "://not-a-valid-dsn")
	t.Setenv("RABBITMQ_URL", "")
	t.Setenv("REDIS_ADDR", "")
	if _, err := NewFromEnv(context.Background()); err == nil {
		t.Fatal("malformed DATABASE_URL must fail fast, not boot half-wired")
	}
}

var _ = os.Getenv
