package broker

import (
	"context"
	"sync"
	"testing"
)

// Principle: at-least-once + idempotent redelivery (ADR 011). Republishing
// the same (routingKey, messageID) must not duplicate; a different messageID
// must not be swallowed.
func TestMemoryBroker_IdempotentPublish(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewMemoryBroker()
	payload := []byte(`{"job":"1"}`)

	if err := b.Publish(ctx, "jobs.execute", payload, "job-1"); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, "jobs.execute", payload, "job-1"); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, "jobs.execute", payload, "job-2"); err != nil {
		t.Fatal(err)
	}
	got := b.Published()
	if len(got) != 2 {
		t.Fatalf("want 2 stored messages after duplicate publish, got %d", len(got))
	}
}

// Principle: defensive copy. Mutating the caller's slice after Publish must
// not corrupt the broker's record (a classic aliasing bug in queue code).
func TestMemoryBroker_PayloadIsolation(t *testing.T) {
	t.Parallel()
	b := NewMemoryBroker()
	payload := []byte(`{"v":1}`)
	if err := b.Publish(context.Background(), "k", payload, "m"); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if got := b.Published()[0].Payload[0]; got != '{' {
		t.Fatalf("stored payload corrupted by caller mutation: %q", got)
	}
}

// Principle: safe under concurrent publishers. N goroutines x M messages must
// yield exactly N*M records with no data race (run with -race).
func TestMemoryBroker_ConcurrentPublishers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := NewMemoryBroker()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				id := string(rune('a'+w)) + string(rune('0'+i/10)) + string(rune('0'+i%10))
				_ = b.Publish(ctx, "k", []byte(`{}`), id)
			}
		}(w)
	}
	wg.Wait()
	if got := len(b.Published()); got != 200 {
		t.Fatalf("want 200 messages, got %d", got)
	}
}
