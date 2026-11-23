package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func eventuallyReady(t *testing.T, b *RabbitMQBroker, want bool) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for b.ConsumerReady() != want {
		select {
		case <-deadline.C:
			t.Fatalf("consumer readiness never became %t", want)
		case <-tick.C:
		}
	}
}

func TestRabbitMQ_ConstructorHonorsStartupContextWithIndependentLifetime(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := newRabbitMQBrokerContext(ctx, "amqp://test", "jobs", func(ctx context.Context, _ string) (amqpConnection, error) { <-ctx.Done(); return nil, ctx.Err() })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup: %v", err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	conn := newFakeConnection()
	b, err := newRabbitMQBrokerContext(ctx, "amqp://test", "jobs", func(context.Context, string) (amqpConnection, error) { return conn, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	cancel()
	if err := b.Publish(t.Context(), JobsRoutingKey, nil, "after-startup"); err != nil {
		t.Fatalf("startup cancel closed broker lifetime: %v", err)
	}
}

func TestRabbitMQ_ReadinessTracksSubscriptionReconnectAndIntakeStop(t *testing.T) {
	t.Parallel()
	pub, first, second := newFakeConnection(), newFakeConnection(), newFakeConnection()
	var dials atomic.Int32
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) {
		switch dials.Add(1) {
		case 1:
			return pub, nil
		case 2:
			return first, nil
		default:
			return second, nil
		}
	})
	if b.ConsumerReady() {
		t.Fatal("publisher connectivity reported consumer readiness")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	go func() {
		done <- b.Consume(ctx, JobsQueue, JobsRoutingKey, 1, func(context.Context, []byte) error { started <- struct{}{}; <-release; return nil })
	}()
	await(t, first.ch.subscribed)
	eventuallyReady(t, b, true)
	first.Abort()
	if b.ConsumerReady() {
		t.Fatal("closed subscription remained ready")
	}
	await(t, second.ch.subscribed)
	eventuallyReady(t, b, true)
	ack := &fakeAcknowledger{conn: second, settled: make(chan settlement, 1)}
	second.ch.deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1}
	await(t, started)
	cancel()
	if b.ConsumerReady() {
		t.Fatal("canceled intake remained ready during drain")
	}
	await(t, second.ch.canceled)
	if second.IsClosed() {
		t.Fatal("drain closed connection before handler could settle")
	}
	close(release)
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !await(t, ack.settled).ack {
		t.Fatal("draining handler result was not acknowledged")
	}
}

func TestRabbitMQ_SettlementFailureMarksConsumerUnready(t *testing.T) {
	t.Parallel()
	pub, sub := newFakeConnection(), newFakeConnection()
	var dials atomic.Int32
	reconnecting := make(chan struct{})
	b := newTestBroker(t, func(ctx context.Context, _ string) (amqpConnection, error) {
		switch dials.Add(1) {
		case 1:
			return pub, nil
		case 2:
			return sub, nil
		default:
			close(reconnecting)
			<-ctx.Done()
			return nil, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- b.Consume(ctx, JobsQueue, JobsRoutingKey, 1, func(context.Context, []byte) error { return nil })
	}()
	await(t, sub.ch.subscribed)
	eventuallyReady(t, b, true)
	sub.ch.deliveries <- amqp.Delivery{Acknowledger: &fakeAcknowledger{conn: sub, err: errors.New("ack write failed")}, DeliveryTag: 1}
	await(t, sub.aborted)
	if b.ConsumerReady() {
		t.Fatal("failed settlement left readiness true")
	}
	await(t, reconnecting)
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRabbitMQ_PermanentPoisonQuarantinedAndTransientFailureDelayed(t *testing.T) {
	t.Parallel()
	pub, sub := newFakeConnection(), newFakeConnection()
	written := make(chan publication, 1)
	pub.ch.publishHook = func(_ context.Context, p publication) error { written <- p; return nil }
	var dials atomic.Int32
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) {
		if dials.Add(1) == 1 {
			return pub, nil
		}
		return sub, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- b.Consume(ctx, JobsQueue, JobsRoutingKey, 1, func(_ context.Context, payload []byte) error {
			if string(payload) == "poison" {
				return Permanent(errors.New("malformed"))
			}
			return errors.New("database unavailable")
		})
	}()
	await(t, sub.ch.subscribed)
	ack := &fakeAcknowledger{conn: sub, settled: make(chan settlement, 1)}
	sub.ch.deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Body: []byte("poison")}
	p := await(t, written)
	if p.exchange != "" || p.key != JobsQuarantineQueue || string(p.message.Body) != "poison" || p.message.DeliveryMode != amqp.Persistent {
		t.Fatalf("quarantine publication=%+v", p)
	}
	assertPending(t, ack.settled)
	pub.ch.confirms <- amqp.Confirmation{DeliveryTag: p.tag, Ack: true}
	if got := await(t, ack.settled); !got.ack || got.requeue {
		t.Fatalf("poison settlement=%+v", got)
	}
	for n := uint64(2); n <= 3; n++ {
		started := time.Now()
		sub.ch.deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: n, Body: []byte("transient")}
		got := await(t, ack.settled)
		if got.ack || !got.requeue || time.Since(started) < reconnectMin*time.Duration(n-1)*9/10 {
			t.Fatalf("retry was immediate: %+v after %s", got, time.Since(started))
		}
	}
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRabbitMQ_CancelJoinsReconnectAttempt(t *testing.T) {
	t.Parallel()
	pub := newFakeConnection()
	var dials atomic.Int32
	dialEntered, dialExited := make(chan struct{}), make(chan struct{})
	b := newTestBroker(t, func(ctx context.Context, _ string) (amqpConnection, error) {
		if dials.Add(1) == 1 {
			return pub, nil
		}
		close(dialEntered)
		<-ctx.Done()
		close(dialExited)
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- b.Consume(ctx, JobsQueue, JobsRoutingKey, 1, func(context.Context, []byte) error { return nil })
	}()
	await(t, dialEntered)
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-dialExited:
	default:
		t.Fatal("consumer returned without joining canceled dial")
	}
	if b.ConsumerReady() {
		t.Fatal("failed subscription was ready")
	}
}
