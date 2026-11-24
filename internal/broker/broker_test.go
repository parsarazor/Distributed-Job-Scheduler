package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// RabbitMQ treats message_id as metadata: an outbox retry can publish the same
// ID again. Memory mode must expose those duplicates to test durable deduplication.
func TestMemoryBroker_AtLeastOncePublish(t *testing.T) {
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
	if len(got) != 3 {
		t.Fatalf("want all 3 publish attempts recorded, got %d", len(got))
	}
	if got[0].MessageID != got[1].MessageID {
		t.Fatal("repeated message ID was not preserved")
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
	view := b.Published()
	view[0].Payload[0] = 'Y'
	view[0].MessageID = "changed"
	if got := b.Published()[0]; got.Payload[0] != '{' || got.MessageID != "m" {
		t.Fatalf("Published returned mutable broker state: %+v", got)
	}
}

func TestMemoryBroker_CancellationAndClose(t *testing.T) {
	t.Parallel()
	b := NewMemoryBroker()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Publish(ctx, "k", nil, "m"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Publish = %v", err)
	}
	if len(b.Published()) != 0 {
		t.Fatal("canceled publish recorded a message")
	}
	if err := b.Publish(context.Background(), "k", []byte("retained"), "m"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			_ = b.Close()
			_ = b.Published()
			if err := b.Publish(context.Background(), "k", nil, "m"); !errors.Is(err, ErrClosed) {
				t.Errorf("publish after close = %v", err)
			}
		})
	}
	wg.Wait()
	if len(b.Published()) != 1 {
		t.Fatal("Close lost introspection or accepted another message")
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
