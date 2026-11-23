package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type topologyCall struct {
	method, name, key, exchange, kind string
	flags                             []bool
}

type publication struct {
	exchange, key        string
	mandatory, immediate bool
	message              amqp.Publishing
	tag                  uint64
}

type fakeAMQPConnection struct {
	ch      *fakeAMQPChannel
	aborted chan struct{}
	closed  chan *amqp.Error
	once    sync.Once
	openErr error
}

func newFakeConnection() *fakeAMQPConnection {
	c := &fakeAMQPConnection{aborted: make(chan struct{}), closed: make(chan *amqp.Error)}
	c.ch = &fakeAMQPChannel{
		conn: c, confirms: make(chan amqp.Confirmation, 1), returns: make(chan amqp.Return, 1),
		deliveries: make(chan amqp.Delivery, 32), subscribed: make(chan struct{}), canceled: make(chan struct{}),
	}
	return c
}

func (c *fakeAMQPConnection) Channel() (amqpChannel, error) { return c.ch, c.openErr }
func (c *fakeAMQPConnection) IsClosed() bool {
	select {
	case <-c.aborted:
		return true
	default:
		return false
	}
}
func (c *fakeAMQPConnection) Abort() {
	c.once.Do(func() { close(c.aborted); close(c.closed) })
}
func (c *fakeAMQPConnection) Close() { c.Abort() }

type fakeAMQPChannel struct {
	conn *fakeAMQPConnection
	mu   sync.Mutex

	topology      []topologyCall
	published     []publication
	prefetch      int
	consumeName   string
	consumeTag    string
	consumeFlags  []bool
	confirmNoWait bool

	confirms    chan amqp.Confirmation
	returns     chan amqp.Return
	deliveries  chan amqp.Delivery
	subscribed  chan struct{}
	canceled    chan struct{}
	cancelOnce  sync.Once
	bindErr     error
	consumeErr  error
	publishHook func(context.Context, publication) error
}

func (c *fakeAMQPChannel) IsClosed() bool { return c.conn.IsClosed() }
func (c *fakeAMQPChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, _ amqp.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topology = append(c.topology, topologyCall{method: "exchange", name: name, kind: kind, flags: []bool{durable, autoDelete, internal, noWait}})
	return nil
}
func (c *fakeAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, _ amqp.Table) (amqp.Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topology = append(c.topology, topologyCall{method: "queue", name: name, flags: []bool{durable, autoDelete, exclusive, noWait}})
	return amqp.Queue{Name: name}, nil
}
func (c *fakeAMQPChannel) QueueBind(name, key, exchange string, noWait bool, _ amqp.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topology = append(c.topology, topologyCall{method: "bind", name: name, key: key, exchange: exchange, flags: []bool{noWait}})
	return c.bindErr
}
func (c *fakeAMQPChannel) Confirm(noWait bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirmNoWait = noWait
	c.topology = append(c.topology, topologyCall{method: "confirm"})
	return nil
}
func (c *fakeAMQPChannel) NotifyPublish(chan amqp.Confirmation) chan amqp.Confirmation {
	return c.confirms
}
func (c *fakeAMQPChannel) NotifyReturn(chan amqp.Return) chan amqp.Return { return c.returns }
func (c *fakeAMQPChannel) NotifyClose(chan *amqp.Error) chan *amqp.Error  { return c.conn.closed }
func (c *fakeAMQPChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	c.mu.Lock()
	p := publication{exchange: exchange, key: key, mandatory: mandatory, immediate: immediate, message: msg, tag: uint64(len(c.published) + 1)}
	p.message.Body = append([]byte(nil), msg.Body...)
	c.published = append(c.published, p)
	c.mu.Unlock()
	if c.publishHook != nil {
		return c.publishHook(ctx, p)
	}
	select {
	case <-c.conn.aborted:
		return amqp.ErrClosed
	case c.confirms <- amqp.Confirmation{DeliveryTag: p.tag, Ack: true}:
		return nil
	}
}
func (c *fakeAMQPChannel) Qos(prefetch, size int, global bool) error {
	if size != 0 || global {
		return errors.New("expected count-based per-consumer QoS")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prefetch = prefetch
	return nil
}
func (c *fakeAMQPChannel) Consume(queue, tag string, autoAck, exclusive, noLocal, noWait bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	c.consumeName, c.consumeTag = queue, tag
	c.consumeFlags = []bool{autoAck, exclusive, noLocal, noWait}
	c.mu.Unlock()
	if c.consumeErr != nil {
		return nil, c.consumeErr
	}
	close(c.subscribed)
	return c.deliveries, nil
}
func (c *fakeAMQPChannel) Cancel(tag string, noWait bool) error {
	if tag != consumerTag || noWait {
		return errors.New("expected synchronous cancellation of the registered consumer")
	}
	c.cancelOnce.Do(func() { close(c.canceled) })
	return nil
}

type settlement struct {
	tag      uint64
	ack      bool
	multiple bool
	requeue  bool
}

type fakeAcknowledger struct {
	conn    *fakeAMQPConnection
	settled chan settlement
	err     error
}

func (a *fakeAcknowledger) record(s settlement) error {
	if a.conn.IsClosed() {
		return amqp.ErrClosed
	}
	if a.err != nil {
		return a.err
	}
	a.settled <- s
	return nil
}
func (a *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	return a.record(settlement{tag: tag, ack: true, multiple: multiple})
}
func (a *fakeAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	return a.record(settlement{tag: tag, multiple: multiple, requeue: requeue})
}
func (a *fakeAcknowledger) Reject(tag uint64, requeue bool) error {
	return a.record(settlement{tag: tag, requeue: requeue})
}

func newTestBroker(t *testing.T, dial amqpDialer) *RabbitMQBroker {
	t.Helper()
	b, err := newRabbitMQBroker("amqp://test", "jobs", dial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for broker operation")
		var zero T
		return zero
	}
}

func assertPending[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case value := <-ch:
		t.Fatalf("operation completed before it was released: %v", value)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestRabbitMQ_PublishDeclaresDurableRouteAndRetainsDuplicateIDs(t *testing.T) {
	t.Parallel()
	conn := newFakeConnection()
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) { return conn, nil })
	want := []topologyCall{
		{method: "exchange", name: "jobs", kind: "topic", flags: []bool{true, false, false, false}},
		{method: "queue", name: JobsQueue, flags: []bool{true, false, false, false}},
		{method: "bind", name: JobsQueue, key: JobsRoutingKey, exchange: "jobs", flags: []bool{false}},
		{method: "queue", name: JobsQuarantineQueue, flags: []bool{true, false, false, false}},
		{method: "confirm"},
	}
	if !reflect.DeepEqual(conn.ch.topology, want) || conn.ch.confirmNoWait {
		t.Fatalf("topology before first publish = %+v", conn.ch.topology)
	}
	for i := 0; i < 2; i++ {
		if err := b.Publish(context.Background(), JobsRoutingKey, []byte(`{"job":1}`), "same-job"); err != nil {
			t.Fatal(err)
		}
	}
	if len(conn.ch.published) != 2 {
		t.Fatal("message IDs were deduplicated")
	}
	for _, p := range conn.ch.published {
		if !p.mandatory || p.immediate || p.exchange != "" || p.key != JobsRoutingKey ||
			p.message.DeliveryMode != amqp.Persistent || p.message.ContentType != "application/json" || p.message.MessageId != "same-job" {
			t.Fatalf("unsafe publication: %+v", p)
		}
	}
}

func TestRabbitMQ_PublishWaitsForConfirmation(t *testing.T) {
	t.Parallel()
	conn := newFakeConnection()
	written := make(chan publication, 1)
	conn.ch.publishHook = func(_ context.Context, p publication) error { written <- p; return nil }
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) { return conn, nil })
	done := make(chan error, 1)
	go func() { done <- b.Publish(context.Background(), JobsRoutingKey, nil, "job") }()
	p := await(t, written)
	assertPending(t, done)
	conn.ch.confirms <- amqp.Confirmation{DeliveryTag: p.tag, Ack: true}
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestRabbitMQ_PublishFailureRetiresSessionAndNextCallReconnects(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("socket write failed")
	for _, tc := range []struct {
		name string
		want error
		fail func(*fakeAMQPConnection, publication) error
	}{
		{"nack", ErrPublishNack, func(c *fakeAMQPConnection, p publication) error {
			c.ch.confirms <- amqp.Confirmation{DeliveryTag: p.tag, Ack: false}
			return nil
		}},
		{"return-before-ack", ErrUnroutable, func(c *fakeAMQPConnection, p publication) error {
			c.ch.returns <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", Exchange: "jobs", RoutingKey: "unbound"}
			c.ch.confirms <- amqp.Confirmation{DeliveryTag: p.tag, Ack: true}
			return nil
		}},
		{"connection-loss", amqp.ErrClosed, func(c *fakeAMQPConnection, _ publication) error { c.Abort(); return nil }},
		{"write-error", writeErr, func(*fakeAMQPConnection, publication) error { return writeErr }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, second := newFakeConnection(), newFakeConnection()
			first.ch.publishHook = func(_ context.Context, p publication) error { return tc.fail(first, p) }
			var attempts atomic.Int32
			b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) {
				if attempts.Add(1) == 1 {
					return first, nil
				}
				return second, nil
			})
			if err := b.Publish(context.Background(), "unbound", nil, "job"); !errors.Is(err, tc.want) {
				t.Fatalf("Publish = %v, want %v", err, tc.want)
			}
			if attempts.Load() != 1 || !first.IsClosed() {
				t.Fatal("failed publication retried in place or retained its connection")
			}
			if err := b.Publish(context.Background(), JobsRoutingKey, nil, "job"); err != nil {
				t.Fatal(err)
			}
			if attempts.Load() != 2 || len(second.ch.topology) != 5 || len(second.ch.published) != 1 {
				t.Fatal("next publish did not reconnect and restore topology")
			}
		})
	}
}

func TestRabbitMQ_PublishCancellationInterruptsWriteAndConfirmation(t *testing.T) {
	t.Parallel()
	for _, blockedWrite := range []bool{true, false} {
		t.Run(fmt.Sprintf("blocked-write=%t", blockedWrite), func(t *testing.T) {
			t.Parallel()
			first, second := newFakeConnection(), newFakeConnection()
			written := make(chan struct{})
			first.ch.publishHook = func(context.Context, publication) error {
				close(written)
				if blockedWrite {
					<-first.aborted
					return amqp.ErrClosed
				}
				return nil
			}
			var attempts atomic.Int32
			b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) {
				if attempts.Add(1) == 1 {
					return first, nil
				}
				return second, nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- b.Publish(ctx, JobsRoutingKey, nil, "job") }()
			await(t, written)
			cancel()
			if err := await(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("Publish = %v", err)
			}
			if !first.IsClosed() {
				t.Fatal("canceled transport was retained")
			}
			if err := b.Publish(context.Background(), JobsRoutingKey, nil, "job"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRabbitMQ_PublishAdmissionHonorsDeadline(t *testing.T) {
	t.Parallel()
	conn := newFakeConnection()
	written := make(chan publication, 1)
	conn.ch.publishHook = func(_ context.Context, p publication) error { written <- p; return nil }
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) { return conn, nil })
	first := make(chan error, 1)
	go func() { first <- b.Publish(context.Background(), JobsRoutingKey, nil, "first") }()
	p := await(t, written)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := b.Publish(ctx, JobsRoutingKey, nil, "second"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting Publish = %v", err)
	}
	if conn.IsClosed() {
		t.Fatal("waiting publisher aborted another publisher's connection")
	}
	conn.ch.confirms <- amqp.Confirmation{DeliveryTag: p.tag, Ack: true}
	if err := await(t, first); err != nil {
		t.Fatal(err)
	}
	if len(conn.ch.published) != 1 {
		t.Fatal("expired waiting publisher wrote a message")
	}
}

func TestRabbitMQ_ReconnectDialHonorsPublishDeadline(t *testing.T) {
	t.Parallel()
	first := newFakeConnection()
	var attempts atomic.Int32
	b := newTestBroker(t, func(ctx context.Context, _ string) (amqpConnection, error) {
		if attempts.Add(1) == 1 {
			return first, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	first.Abort()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := b.Publish(ctx, JobsRoutingKey, nil, "job"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish = %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("dial attempts = %d", attempts.Load())
	}
}

func TestRabbitMQ_ConsumeConcurrencySettlementAndCancellationDrain(t *testing.T) {
	t.Parallel()
	pub, sub := newFakeConnection(), newFakeConnection()
	var dials atomic.Int32
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) {
		if dials.Add(1) == 1 {
			return pub, nil
		}
		return sub, nil
	})
	type contextKey struct{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), contextKey{}, "trace"), time.Minute)
	defer cancel()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	started := make(chan context.Context, 4)
	done := make(chan error, 1)
	var active, peak atomic.Int32
	go func() {
		done <- b.Consume(ctx, JobsQueue, JobsRoutingKey, 3, func(hctx context.Context, body []byte) error {
			n := active.Add(1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			defer active.Add(-1)
			started <- hctx
			<-release
			if string(body) == "2" {
				return errors.New("database commit failed")
			}
			return nil
		})
	}()
	await(t, sub.ch.subscribed)
	if sub.ch.prefetch != 3 || sub.ch.consumeName != JobsQueue || !reflect.DeepEqual(sub.ch.consumeFlags, []bool{false, false, false, false}) {
		t.Fatalf("unsafe consumer configuration: prefetch=%d queue=%q flags=%v", sub.ch.prefetch, sub.ch.consumeName, sub.ch.consumeFlags)
	}
	ack := &fakeAcknowledger{conn: sub, settled: make(chan settlement, 4)}
	for tag := uint64(1); tag <= 4; tag++ {
		sub.ch.deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: tag, Body: []byte(fmt.Sprint(tag))}
	}
	var handlerContexts []context.Context
	for i := 0; i < 3; i++ {
		hctx := await(t, started)
		if _, deadline := hctx.Deadline(); deadline || hctx.Value(contextKey{}) != "trace" {
			t.Fatal("handler context did not detach deadline and retain values")
		}
		handlerContexts = append(handlerContexts, hctx)
	}
	assertPending(t, started)
	assertPending(t, ack.settled)
	cancel()
	await(t, sub.ch.canceled)
	assertPending(t, done)
	if sub.IsClosed() {
		t.Fatal("shutdown closed the connection before active handlers drained")
	}
	for _, hctx := range handlerContexts {
		if hctx.Err() != nil {
			t.Fatal("intake cancellation canceled a handler")
		}
	}
	releaseOnce.Do(func() { close(release) })
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume = %v", err)
	}
	seen := map[uint64]settlement{}
	for i := 0; i < 3; i++ {
		s := await(t, ack.settled)
		seen[s.tag] = s
	}
	for tag := uint64(1); tag <= 3; tag++ {
		s, ok := seen[tag]
		if !ok || s.multiple || s.ack != (tag != 2) || s.requeue != (tag == 2) {
			t.Fatalf("incorrect settlement for %d: %+v", tag, s)
		}
	}
	if peak.Load() != 3 || active.Load() != 0 {
		t.Fatalf("handler concurrency peak=%d active=%d", peak.Load(), active.Load())
	}
	if len(started) != 0 || !sub.IsClosed() || pub.IsClosed() {
		t.Fatal("shutdown started buffered work or affected the publisher")
	}
}

func TestRabbitMQ_ConsumeHandlerFailureRequeuesAndContinues(t *testing.T) {
	t.Parallel()
	pub, sub := newFakeConnection(), newFakeConnection()
	var dials atomic.Int32
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) {
		if dials.Add(1) == 1 {
			return pub, nil
		}
		return sub, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	var calls atomic.Int32
	go func() {
		done <- b.Consume(ctx, JobsQueue, JobsRoutingKey, 0, func(context.Context, []byte) error {
			if calls.Add(1) == 1 {
				return errors.New("retry database commit")
			}
			return nil
		})
	}()
	await(t, sub.ch.subscribed)
	if sub.ch.prefetch != 1 {
		t.Fatal("nonpositive prefetch did not default to one")
	}
	ack := &fakeAcknowledger{conn: sub, settled: make(chan settlement, 2)}
	for tag := uint64(1); tag <= 2; tag++ {
		sub.ch.deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: tag, Body: []byte("same-job")}
		s := await(t, ack.settled)
		if s.tag != tag || s.ack != (tag == 2) || s.requeue != (tag == 1) || s.multiple {
			t.Fatalf("settlement = %+v", s)
		}
	}
	if dials.Load() != 2 {
		t.Fatal("handler error restarted the transport")
	}
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRabbitMQ_ConsumeReconnectWaitsForPreviousHandlers(t *testing.T) {
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	started := make(chan struct{}, 2)
	var calls atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- b.Consume(ctx, JobsQueue, JobsRoutingKey, 1, func(context.Context, []byte) error {
			started <- struct{}{}
			if calls.Add(1) == 1 {
				<-release
			}
			return nil
		})
	}()
	await(t, first.ch.subscribed)
	ack := &fakeAcknowledger{conn: first, settled: make(chan settlement, 1)}
	first.ch.deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1}
	await(t, started)
	first.Abort()
	// Wait beyond reconnectMin so backoff alone cannot satisfy the assertion.
	select {
	case <-second.ch.subscribed:
		t.Fatal("resubscribed before the previous handler drained")
	case <-time.After(2 * reconnectMin):
	}
	if err := b.Publish(context.Background(), JobsRoutingKey, nil, "independent-publish"); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	await(t, second.ch.subscribed)
	if second.ch.prefetch != 1 || len(second.ch.topology) != 3 {
		t.Fatal("consumer did not restore topology and QoS")
	}
	ack = &fakeAcknowledger{conn: second, settled: make(chan settlement, 1)}
	second.ch.deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Redelivered: true}
	await(t, started)
	if s := await(t, ack.settled); !s.ack {
		t.Fatal("redelivery was not processed")
	}
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if dials.Load() != 3 || pub.IsClosed() {
		t.Fatal("consumer reconnect affected publishing")
	}
}

func TestRabbitMQ_ConsumeRetriesSetupFailure(t *testing.T) {
	t.Parallel()
	pub, failed, recovered := newFakeConnection(), newFakeConnection(), newFakeConnection()
	failed.ch.bindErr = errors.New("connection lost during queue binding")
	var dials atomic.Int32
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) {
		switch dials.Add(1) {
		case 1:
			return pub, nil
		case 2:
			return failed, nil
		default:
			return recovered, nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- b.Consume(ctx, JobsQueue, JobsRoutingKey, 2, func(context.Context, []byte) error { return nil })
	}()
	await(t, failed.aborted)
	await(t, recovered.ch.subscribed)
	if recovered.ch.prefetch != 2 || dials.Load() != 3 {
		t.Fatal("consumer did not retry failed setup")
	}
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRabbitMQ_ConsumeReconnectsAfterSettlementFailureOrServerCancellation(t *testing.T) {
	t.Parallel()
	for _, ackFailure := range []bool{true, false} {
		t.Run(fmt.Sprintf("ack-failure=%t", ackFailure), func(t *testing.T) {
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- b.Consume(ctx, JobsQueue, JobsRoutingKey, 1, func(context.Context, []byte) error { return nil })
			}()
			await(t, first.ch.subscribed)
			if ackFailure {
				ack := &fakeAcknowledger{conn: first, err: errors.New("ack write failed")}
				first.ch.deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1}
			} else {
				// A server basic.cancel closes deliveries while its channel and
				// connection can remain open (for example after queue deletion).
				close(first.ch.deliveries)
			}
			await(t, first.aborted)
			await(t, second.ch.subscribed)
			cancel()
			if err := await(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if dials.Load() != 3 {
				t.Fatalf("dial attempts = %d, want 3", dials.Load())
			}
		})
	}
}

func TestRabbitMQ_CloseInterruptsBlockedPublish(t *testing.T) {
	t.Parallel()
	conn := newFakeConnection()
	written := make(chan struct{})
	conn.ch.publishHook = func(context.Context, publication) error {
		close(written)
		<-conn.aborted
		return amqp.ErrClosed
	}
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) { return conn, nil })
	published := make(chan error, 1)
	go func() { published <- b.Publish(context.Background(), JobsRoutingKey, nil, "job") }()
	await(t, written)
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	if err := await(t, closed); err != nil {
		t.Fatal(err)
	}
	if err := await(t, published); !errors.Is(err, ErrClosed) {
		t.Fatalf("interrupted Publish = %v", err)
	}
}

func TestRabbitMQ_ConcurrentClosePublishAndConsumeDrains(t *testing.T) {
	t.Parallel()
	pub, sub := newFakeConnection(), newFakeConnection()
	var dials atomic.Int32
	b := newTestBroker(t, func(context.Context, string) (amqpConnection, error) {
		switch dials.Add(1) {
		case 1:
			return pub, nil
		case 2:
			return sub, nil
		default:
			// A publisher admitted before Close can race cancellation while
			// reconnecting. Real dials never reuse the consumer's connection.
			return newFakeConnection(), nil
		}
	})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	started := make(chan struct{})
	consumed := make(chan error, 1)
	go func() {
		consumed <- b.Consume(context.Background(), JobsQueue, JobsRoutingKey, 1, func(context.Context, []byte) error {
			close(started)
			<-release
			return nil
		})
	}()
	await(t, sub.ch.subscribed)
	ack := &fakeAcknowledger{conn: sub, settled: make(chan settlement, 1)}
	sub.ch.deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1}
	await(t, started)
	var writers sync.WaitGroup
	for i := 0; i < 32; i++ {
		writers.Go(func() {
			if err := b.Publish(context.Background(), JobsRoutingKey, nil, "same-job"); err != nil && !errors.Is(err, ErrClosed) {
				t.Errorf("concurrent Publish = %v", err)
			}
		})
	}
	closed := make(chan error, 8)
	for i := 0; i < cap(closed); i++ {
		go func() { closed <- b.Close() }()
	}
	await(t, sub.ch.canceled)
	assertPending(t, closed)
	if err := b.Publish(context.Background(), JobsRoutingKey, nil, "after-close"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close began = %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	for i := 0; i < cap(closed); i++ {
		if err := await(t, closed); err != nil {
			t.Fatal(err)
		}
	}
	writers.Wait()
	if err := await(t, consumed); !errors.Is(err, ErrClosed) {
		t.Fatalf("Consume after Close = %v", err)
	}
	if s := await(t, ack.settled); !s.ack {
		t.Fatal("Close failed to drain successful handler")
	}
	if !pub.IsClosed() || !sub.IsClosed() {
		t.Fatal("Close leaked a transport")
	}
}

func TestRabbitMQ_ReconnectBackoffIsBoundedAndCancellable(t *testing.T) {
	t.Parallel()
	delay := reconnectMin
	for i := 0; i < 100; i++ {
		if delay < reconnectMin || delay > reconnectMax {
			t.Fatalf("backoff out of bounds: %s", delay)
		}
		delay = nextBackoff(delay)
	}
	if delay != reconnectMax {
		t.Fatalf("backoff did not reach maximum: %s", delay)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitReconnect(ctx, reconnectMax) {
		t.Fatal("canceled backoff resumed reconnecting")
	}
}

func TestRabbitMQ_DialCancellationInterruptsAMQPHandshake(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := dialRabbitMQ(ctx, "amqp://guest:guest@"+listener.Addr().String()+"/"); done <- err }()
	conn := await(t, accepted)
	defer conn.Close()
	if err := await(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled handshake = %v", err)
	}
}
