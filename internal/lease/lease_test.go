package lease

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Principle: lifecycle contract. Acquire -> renew -> release is the only
// legal path; every other order must fail loudly.
func TestMemoryLease_Lifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemoryLeaseManager()
	job := uuid.New()

	tok, err := m.Acquire(ctx, job, "w1", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if tok <= 0 {
		t.Fatalf("fencing token must be positive, got %d", tok)
	}
	if err := m.Renew(ctx, job, "w1", tok, 30*time.Second); err != nil {
		t.Fatalf("renew by owner: %v", err)
	}
	if err := m.Renew(ctx, job, "w2", tok, 30*time.Second); err == nil {
		t.Fatal("renew by non-owner must fail")
	}
	if err := m.Release(ctx, job, "w2"); err != nil {
		t.Fatalf("release by non-owner should be a safe no-op: %v", err)
	}
	if err := m.Release(ctx, job, "w1"); err != nil {
		t.Fatalf("release by owner: %v", err)
	}
	if err := m.Renew(ctx, job, "w1", tok, 30*time.Second); err == nil {
		t.Fatal("renew after release must fail")
	}
	// Re-acquire must mint a HIGHER fencing token (monotonic per ADR 002).
	tok2, err := m.Acquire(ctx, job, "w2", 30*time.Second)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if tok2 <= tok {
		t.Fatalf("fencing token not monotonic: %d -> %d", tok, tok2)
	}
}

// Principle: mutual exclusion under contention. Exactly one of N racers may
// hold the lease; the rest get ErrLeaseHeld. Run with -race.
func TestMemoryLease_ContentionSingleWinner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemoryLeaseManager()
	job := uuid.New()

	const racers = 8
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := m.Acquire(ctx, job, string(rune('a'+i)), 30*time.Second); err == nil {
				wins.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("exactly one winner required, got %d", got)
	}
}

// Principle: expiry is enforced. A lease with a past TTL must not block a
// new owner (covers GC-pause / crash scenarios from ADR 002).
func TestMemoryLease_ExpiredLeaseReacquirable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemoryLeaseManager()
	job := uuid.New()

	if _, err := m.Acquire(ctx, job, "crashed-worker", time.Millisecond); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := m.Acquire(ctx, job, "new-worker", 30*time.Second); err != nil {
		t.Fatalf("expired lease must be acquirable: %v", err)
	}
}

func TestMemoryLeaseManager_RejectsReentrantAcquire(t *testing.T) {
	t.Parallel()
	m := NewMemoryLeaseManager()
	job := uuid.New()
	if _, err := m.Acquire(context.Background(), job, "w", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Acquire(context.Background(), job, "w", time.Minute); err != ErrLeaseHeld {
		t.Fatalf("second acquire by the same worker must not create a concurrent execution, got %v", err)
	}
}
