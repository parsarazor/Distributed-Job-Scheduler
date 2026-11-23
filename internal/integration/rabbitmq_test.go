package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/testinfra"
	amqp "github.com/rabbitmq/amqp091-go"
)

func rabbitBroker(t *testing.T, server *testinfra.RabbitMQ) (*broker.RabbitMQBroker, string) {
	t.Helper()
	exchange := "integration." + uuid.NewString()
	b, err := broker.NewRabbitMQBroker(server.URL, exchange)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("close RabbitMQ broker: %v", err)
		}
	})
	return b, exchange
}

func rabbitAdmin(t *testing.T, server *testinfra.RabbitMQ) *amqp.Channel {
	t.Helper()
	conn, err := amqp.DialConfig(server.URL, amqp.Config{Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseDeadline(time.Now().Add(5 * time.Second)) })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func rabbitCase(t *testing.T, server *testinfra.RabbitMQ) (*broker.RabbitMQBroker, *amqp.Channel) {
	t.Helper()
	b, exchange := rabbitBroker(t, server)
	ch := rabbitAdmin(t, server)
	t.Cleanup(func() {
		if _, err := ch.QueueDelete(broker.JobsQueue, false, false, false); err != nil {
			t.Errorf("delete test queue: %v", err)
		}
		if err := ch.ExchangeDelete(exchange, false, false); err != nil {
			t.Errorf("delete test exchange: %v", err)
		}
	})
	return b, ch
}

func awaitQueue(t *testing.T, ch *amqp.Channel, description string, ready func(amqp.Queue) bool) amqp.Queue {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		q, err := ch.QueueDeclarePassive(broker.JobsQueue, true, false, false, false, nil)
		if err != nil {
			t.Fatalf("inspect jobs queue: %v", err)
		}
		if ready(q) {
			return q
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: last queue state=%+v", description, q)
		case <-ticker.C:
		}
	}
}

type consumerRun struct {
	cancel context.CancelFunc
	op     *operation
}

func startConsumer(t *testing.T, b *broker.RabbitMQBroker, prefetch int, handler func(context.Context, []byte) error) *consumerRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	run := &consumerRun{cancel: cancel}
	run.op = launchOperation(func() (int, error) {
		return 0, b.Consume(ctx, broker.JobsQueue, broker.JobsRoutingKey, prefetch, handler)
	})
	t.Cleanup(func() { run.stop(t) })
	return run
}

func (r *consumerRun) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case <-r.op.done:
		if err := r.op.result.err; !errors.Is(err, context.Canceled) && !errors.Is(err, broker.ErrClosed) && err != nil {
			t.Errorf("consumer stopped with unexpected error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Error("consumer did not drain and stop")
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a RabbitMQ delivery or handler")
		var zero T
		return zero
	}
}

func TestRabbitMQ(t *testing.T) {
	server := testinfra.StartRabbitMQ(t)
	t.Run("confirmed publish is routed before any consumer starts", func(t *testing.T) {
		b, ch := rabbitCase(t, server)
		body := []byte(`{"job":"published-before-consumer"}`)
		id := uuid.NewString()
		if err := b.Publish(t.Context(), broker.JobsRoutingKey, body, id); err != nil {
			t.Fatal(err)
		}
		// No test code declares the canonical queue or binding. The publisher
		// must create them before returning a successful confirmation.
		awaitQueue(t, ch, "one routed message without a consumer", func(q amqp.Queue) bool { return q.Messages == 1 && q.Consumers == 0 })
		delivery, ok, err := ch.Get(broker.JobsQueue, false)
		if err != nil || !ok {
			t.Fatalf("confirmed message must be available: ok=%v err=%v", ok, err)
		}
		if !bytes.Equal(delivery.Body, body) || delivery.MessageId != id || delivery.RoutingKey != broker.JobsRoutingKey || delivery.DeliveryMode != amqp.Persistent || delivery.ContentType != "application/json" {
			t.Fatalf("durable publication lost body or routing metadata: %+v", delivery)
		}
		if err := delivery.Nack(false, true); err != nil {
			t.Fatal(err)
		}
		delivered := make(chan []byte, 1)
		run := startConsumer(t, b, 1, func(_ context.Context, body []byte) error {
			select {
			case delivered <- append([]byte(nil), body...):
			default:
				return errors.New("unexpected extra delivery")
			}
			return nil
		})
		if got := receive(t, delivered); !bytes.Equal(got, body) {
			t.Fatalf("consumer received %s, want %s", got, body)
		}
		run.stop(t)
		awaitQueue(t, ch, "acknowledged message removed", func(q amqp.Queue) bool { return q.Messages == 0 && q.Consumers == 0 })
	})

	t.Run("unroutable publish fails and next publish reconnects", func(t *testing.T) {
		b, ch := rabbitCase(t, server)
		if err := b.Publish(t.Context(), "missing."+uuid.NewString(), []byte(`{}`), uuid.NewString()); !errors.Is(err, broker.ErrUnroutable) {
			t.Fatalf("mandatory unroutable publication must fail: %v", err)
		}
		id := uuid.NewString()
		if err := b.Publish(t.Context(), broker.JobsRoutingKey, []byte(`{}`), id); err != nil {
			t.Fatalf("publication after failed confirmation must recover: %v", err)
		}
		awaitQueue(t, ch, "only the routed publication queued", func(q amqp.Queue) bool { return q.Messages == 1 })
		delivery, ok, err := ch.Get(broker.JobsQueue, true)
		if err != nil || !ok || delivery.MessageId != id {
			t.Fatalf("wrong recovered publication: delivery=%+v ok=%v err=%v", delivery, ok, err)
		}
	})

	t.Run("another binding cannot confirm a missing execution queue", func(t *testing.T) {
		b, exchange := rabbitBroker(t, server)
		ch := rabbitAdmin(t, server)
		alternate := "other." + uuid.NewString()
		if _, err := ch.QueueDeclare(alternate, false, true, false, false, nil); err != nil {
			t.Fatal(err)
		}
		if err := ch.QueueBind(alternate, broker.JobsRoutingKey, exchange, false, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := ch.QueueDelete(broker.JobsQueue, false, false, false); err != nil {
			t.Fatal(err)
		}
		if err := b.Publish(t.Context(), broker.JobsRoutingKey, []byte(`{}`), uuid.NewString()); !errors.Is(err, broker.ErrUnroutable) {
			t.Fatalf("another queue acknowledged the execution destination: %v", err)
		}
		if err := b.Publish(t.Context(), broker.JobsRoutingKey, []byte(`{}`), uuid.NewString()); err != nil {
			t.Fatal(err)
		}
		if _, err := ch.QueueDelete(broker.JobsQueue, false, false, false); err != nil {
			t.Fatal(err)
		}
		if _, err := ch.QueueDelete(alternate, false, false, false); err != nil {
			t.Fatal(err)
		}
		if err := ch.ExchangeDelete(exchange, false, false); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("handler error requeues until successful acknowledgement", func(t *testing.T) {
		b, ch := rabbitCase(t, server)
		body := []byte(`{"job":"retry-handler"}`)
		attempts := make(chan []byte, 3)
		var calls atomic.Int32
		run := startConsumer(t, b, 1, func(_ context.Context, body []byte) error {
			select {
			case attempts <- append([]byte(nil), body...):
			default:
			}
			if calls.Add(1) == 1 {
				return errors.New("transient handler failure")
			}
			return nil
		})
		awaitQueue(t, ch, "consumer subscribed", func(q amqp.Queue) bool { return q.Consumers == 1 })
		if err := b.Publish(t.Context(), broker.JobsRoutingKey, body, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if got := receive(t, attempts); !bytes.Equal(got, body) {
				t.Fatalf("requeued delivery changed payload: %s", got)
			}
		}
		run.stop(t)
		if calls.Load() != 2 {
			t.Fatalf("want one failed and one successful delivery, got %d", calls.Load())
		}
		awaitQueue(t, ch, "successful retry acknowledged", func(q amqp.Queue) bool { return q.Messages == 0 && q.Consumers == 0 })
	})

	t.Run("permanent poison is confirmed in quarantine", func(t *testing.T) {
		b, ch := rabbitCase(t, server)
		if _, err := ch.QueuePurge(broker.JobsQuarantineQueue, false); err != nil {
			t.Fatal(err)
		}
		var calls atomic.Int32
		seen := make(chan struct{}, 1)
		run := startConsumer(t, b, 1, func(context.Context, []byte) error {
			calls.Add(1)
			select {
			case seen <- struct{}{}:
			default:
			}
			return broker.Permanent(errors.New("malformed announcement"))
		})
		const id = "poison-probe"
		if err := b.Publish(t.Context(), broker.JobsRoutingKey, []byte("{broken"), id); err != nil {
			t.Fatal(err)
		}
		receive(t, seen)
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			q, err := ch.QueueDeclarePassive(broker.JobsQuarantineQueue, true, false, false, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			if q.Messages == 1 {
				break
			}
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Fatal("poison was not quarantined")
			}
		}
		run.stop(t)
		if calls.Load() != 1 {
			t.Fatalf("poison redelivered %d times", calls.Load())
		}
		delivery, ok, err := ch.Get(broker.JobsQuarantineQueue, true)
		if err != nil || !ok || string(delivery.Body) != "{broken" || delivery.MessageId != id || delivery.DeliveryMode != amqp.Persistent {
			t.Fatalf("quarantine=%+v ok=%t err=%v", delivery, ok, err)
		}
		awaitQueue(t, ch, "poison removed from execution queue", func(q amqp.Queue) bool { return q.Messages == 0 && q.Consumers == 0 })
	})

	t.Run("prefetch allows bounded concurrent handlers", func(t *testing.T) {
		b, ch := rabbitCase(t, server)
		const total, concurrency = 8, 3
		for i := range total {
			if err := b.Publish(t.Context(), broker.JobsRoutingKey, fmt.Appendf(nil, `{"job":%d}`, i), fmt.Sprint(i)); err != nil {
				t.Fatal(err)
			}
		}
		gate := newGate(t)
		defer gate.stop()
		started := make(chan struct{}, total)
		finished := make(chan string, total)
		var active, peak atomic.Int32
		run := startConsumer(t, b, concurrency, func(ctx context.Context, body []byte) error {
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			select {
			case started <- struct{}{}:
			default:
				return errors.New("unexpected extra handler")
			}
			ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			if err := gate.wait(ctx); err != nil {
				return err
			}
			select {
			case finished <- string(body):
			default:
				return errors.New("unexpected extra delivery")
			}
			return nil
		})
		for range concurrency {
			receive(t, started)
		}
		select {
		case <-started:
			t.Fatal("consumer exceeded the configured handler concurrency")
		case <-time.After(100 * time.Millisecond):
		}
		gate.stop()
		seen := make(map[string]int)
		for range total {
			seen[receive(t, finished)]++
		}
		run.stop(t)
		if peak.Load() != concurrency || active.Load() != 0 || len(seen) != total {
			t.Fatalf("want %d concurrent handlers and each delivery once: peak=%d active=%d unique=%d", concurrency, peak.Load(), active.Load(), len(seen))
		}
		for body, count := range seen {
			if count != 1 {
				t.Fatalf("delivery %s handled %d times", body, count)
			}
		}
		awaitQueue(t, ch, "all concurrent deliveries acknowledged", func(q amqp.Queue) bool { return q.Messages == 0 && q.Consumers == 0 })
	})

	t.Run("cancel stops intake and drains active handler before acknowledgement", func(t *testing.T) {
		b, ch := rabbitCase(t, server)
		for _, id := range []string{"active", "queued"} {
			if err := b.Publish(t.Context(), broker.JobsRoutingKey, []byte(id), id); err != nil {
				t.Fatal(err)
			}
		}
		gate := newGate(t)
		defer gate.stop()
		finished := make(chan error, 1)
		run := startConsumer(t, b, 1, func(ctx context.Context, _ []byte) error {
			ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			err := gate.wait(ctx)
			select {
			case finished <- err:
			default:
			}
			return err
		})
		gate.await(t)
		if !b.ConsumerReady() {
			t.Fatal("live subscribed consumer must be ready")
		}
		run.cancel()
		if b.ConsumerReady() {
			t.Fatal("canceled intake must become unready while its handler drains")
		}
		select {
		case <-run.op.done:
			t.Fatalf("Consume returned while the handler was still active: %v", run.op.result.err)
		case <-time.After(100 * time.Millisecond):
		}
		gate.stop()
		if err := receive(t, finished); err != nil {
			t.Fatalf("intake cancellation must not cancel the active handler: %v", err)
		}
		run.stop(t)
		awaitQueue(t, ch, "active message acknowledged and unstarted message retained", func(q amqp.Queue) bool { return q.Messages == 1 && q.Consumers == 0 })
		delivery, ok, err := ch.Get(broker.JobsQueue, true)
		if err != nil || !ok || delivery.MessageId != "queued" {
			t.Fatalf("unstarted delivery must remain queued: delivery=%+v ok=%v err=%v", delivery, ok, err)
		}
	})
}

func restartRabbitMQ(t *testing.T, server *testinfra.RabbitMQ) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	if err := server.Restart(ctx); err != nil {
		t.Fatalf("restart RabbitMQ: %v", err)
	}
}

func TestRabbitMQRestart(t *testing.T) {
	server := testinfra.StartRabbitMQ(t)
	if server.Container == nil && os.Getenv("TEST_RABBITMQ_RESTART") == "" {
		t.Skip("native broker restart requires an explicit TEST_RABBITMQ_RESTART executable")
	}
	t.Run("confirmed persistent messages survive before consumers exist", func(t *testing.T) {
		b, _ := rabbitBroker(t, server)
		const total = 8
		for i := range total {
			if err := b.Publish(t.Context(), broker.JobsRoutingKey, []byte(fmt.Sprintf("before-%d", i)), fmt.Sprint(i)); err != nil {
				t.Fatal(err)
			}
		}
		restartRabbitMQ(t, server)
		ch := rabbitAdmin(t, server)
		awaitQueue(t, ch, "all confirmed messages survived restart without consumers", func(q amqp.Queue) bool { return q.Messages == total && q.Consumers == 0 })
		delivered := make(chan string, total+2)
		run := startConsumer(t, b, 2, func(_ context.Context, body []byte) error {
			select {
			case delivered <- string(body):
			default:
				return errors.New("unexpected extra delivery after restart")
			}
			return nil
		})
		seen := make(map[string]int)
		for range total {
			seen[receive(t, delivered)]++
		}
		for i := range total {
			if seen[fmt.Sprintf("before-%d", i)] != 1 {
				t.Fatalf("confirmed payload was lost or duplicated after restart: %v", seen)
			}
		}
		publishAfterRestart(t, b, []byte("publisher-reconnected"))
		if body := receive(t, delivered); body != "publisher-reconnected" {
			t.Fatalf("publisher did not reconnect: %q", body)
		}
		awaitQueue(t, ch, "reconnected publisher deliveries drained", func(q amqp.Queue) bool { return q.Messages == 0 })
		run.stop(t)
		awaitQueue(t, ch, "all persistent messages acknowledged", func(q amqp.Queue) bool { return q.Messages == 0 && q.Consumers == 0 })
	})
	t.Run("active consumer automatically resubscribes after container restart", func(t *testing.T) {
		b, _ := rabbitBroker(t, server)
		delivered := make(chan string, 16)
		run := startConsumer(t, b, 2, func(_ context.Context, body []byte) error {
			select {
			case delivered <- string(body):
			default:
				return errors.New("unexpected extra delivery after reconnect")
			}
			return nil
		})
		before := rabbitAdmin(t, server)
		awaitQueue(t, before, "consumer subscribed before restart", func(q amqp.Queue) bool { return q.Consumers == 1 })
		if err := b.Publish(t.Context(), broker.JobsRoutingKey, []byte("before-restart"), uuid.NewString()); err != nil {
			t.Fatal(err)
		}
		if body := receive(t, delivered); body != "before-restart" {
			t.Fatalf("unexpected initial delivery: %s", body)
		}
		restartRabbitMQ(t, server)
		select {
		case <-run.op.done:
			t.Fatalf("Consume must reconnect instead of exiting after broker restart: %v", run.op.result.err)
		default:
		}
		after := rabbitAdmin(t, server)
		awaitQueue(t, after, "same Consume call resubscribed after restart", func(q amqp.Queue) bool { return q.Consumers == 1 })
		publishAfterRestart(t, b, []byte("after-restart"))
		found := false
		for range 16 {
			if body := receive(t, delivered); body == "after-restart" {
				found = true
				break
			} else if body != "before-restart" {
				t.Fatalf("unexpected redelivery after restart: %s", body)
			}
		}
		if !found {
			t.Fatal("consumer never delivered the publication after restart")
		}
		awaitQueue(t, after, "reconnected consumer deliveries drained", func(q amqp.Queue) bool { return q.Messages == 0 })
		run.stop(t)
		awaitQueue(t, after, "reconnected consumer acknowledged and stopped", func(q amqp.Queue) bool { return q.Messages == 0 && q.Consumers == 0 })
	})
}

// A failed publish is ambiguous and callers own retries. Keep the message ID
// stable while allowing the broker to discard its stale pre-restart connection.
func publishAfterRestart(t *testing.T, b *broker.RabbitMQBroker, body []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	id := uuid.NewString()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := b.Publish(ctx, broker.JobsRoutingKey, body, id)
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("publisher did not reconnect after restart: %v", err)
		case <-ticker.C:
		}
	}
}
