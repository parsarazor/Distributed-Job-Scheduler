package lease

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Principle: same contract, live backend. Skips without REDIS_ADDR;
// CI provides Redis (see docker-compose.yml).
func TestLiveRedis_Lifecycle(t *testing.T) {
	if os.Getenv("REDIS_ADDR") == "" {
		t.Skip("REDIS_ADDR unset: live Redis lease test skipped (runs in CI)")
	}
	ctx := context.Background()
	r := NewRedisLeaseManager(os.Getenv("REDIS_ADDR"))
	job := uuid.New()
	tok, err := r.Acquire(ctx, job, "w1", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := r.Acquire(ctx, job, "w2", 30*time.Second); err != ErrLeaseHeld {
		t.Fatalf("second owner must get ErrLeaseHeld, got %v", err)
	}
	if err := r.Renew(ctx, job, "w1", tok, 30*time.Second); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := r.Release(ctx, job, "w1"); err != nil {
		t.Fatalf("release: %v", err)
	}
}
