package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	operationTimeout = 10 * time.Second
	reconnectMin     = 100 * time.Millisecond
	reconnectMax     = 5 * time.Second
	consumerTag      = "scheduler-worker"
)

var (
	ErrUnroutable  = errors.New("RabbitMQ returned an unroutable message")
	ErrPublishNack = errors.New("RabbitMQ negatively acknowledged publication")
)

// RabbitMQBroker uses a dedicated publisher connection and a separate connection
// for each Consume call. Only one publication is in flight at a time, so a
// mandatory return can be attributed even when message IDs are repeated.
type RabbitMQBroker struct {
	url      string
	exchange string
	dial     amqpDialer
	ctx      context.Context
	cancel   context.CancelFunc

	mu           sync.Mutex // protects closed and admission to active
	closed       bool
	active       sync.WaitGroup
	closeOnce    sync.Once
	pubGate      chan struct{}
	publisher    *publisherSession // protected by pubGate, or after active.Wait
	consuming    bool
	consumerCtx  context.Context
	subscription *consumerSession
}

var (
	_ Broker   = (*RabbitMQBroker)(nil)
	_ Consumer = (*RabbitMQBroker)(nil)
)

type rabbitSession struct {
	conn   amqpConnection
	ch     amqpChannel
	closed <-chan *amqp.Error
}

type publisherSession struct {
	*rabbitSession
	confirms <-chan amqp.Confirmation
	returns  <-chan amqp.Return
	next     uint64
}

type consumerSession struct {
	*rabbitSession
	deliveries <-chan amqp.Delivery
}

// NewRabbitMQBroker declares the durable topic exchange and the durable
// jobs.execute queue/binding before any publication, without requiring a
// consumer to have started. Construction is bounded by a ten-second timeout.
func NewRabbitMQBroker(url, exchange string) (*RabbitMQBroker, error) {
	return NewRabbitMQBrokerContext(context.Background(), url, exchange)
}

// NewRabbitMQBrokerContext bounds initialization by startupCtx (and ten seconds).
// The successful broker owns a separate lifetime, terminated only by Close.
func NewRabbitMQBrokerContext(startupCtx context.Context, url, exchange string) (*RabbitMQBroker, error) {
	return newRabbitMQBrokerContext(startupCtx, url, exchange, dialRabbitMQ)
}

func newRabbitMQBroker(url, exchange string, dial amqpDialer) (*RabbitMQBroker, error) {
	return newRabbitMQBrokerContext(context.Background(), url, exchange, dial)
}

func newRabbitMQBrokerContext(startupCtx context.Context, url, exchange string, dial amqpDialer) (*RabbitMQBroker, error) {
	lifeCtx, cancel := context.WithCancel(context.Background())
	r := &RabbitMQBroker{
		url: url, exchange: exchange, dial: dial, ctx: lifeCtx, cancel: cancel,
		pubGate: make(chan struct{}, 1),
	}
	ctx, stop := context.WithTimeout(startupCtx, operationTimeout)
	defer stop()
	publisher, err := r.newPublisher(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	r.publisher = publisher
	return r, nil
}

func (r *RabbitMQBroker) begin() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	r.active.Add(1)
	return nil
}

func (r *RabbitMQBroker) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	stop := context.AfterFunc(r.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

func (r *RabbitMQBroker) operationError(err error) error {
	if r.ctx.Err() != nil {
		return ErrClosed
	}
	return err
}

func (r *RabbitMQBroker) openSession(ctx context.Context, queue, routingKey string) (*rabbitSession, error) {
	conn, err := r.dial(ctx, r.url)
	if err != nil {
		return nil, err
	}
	var session *rabbitSession
	err = withConnectionContext(ctx, conn, func() error {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		session = &rabbitSession{conn: conn, ch: ch, closed: ch.NotifyClose(make(chan *amqp.Error, 1))}
		if err := ch.ExchangeDeclare(r.exchange, "topic", true, false, false, false, nil); err != nil {
			return err
		}
		if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			return err
		}
		return ch.QueueBind(queue, routingKey, r.exchange, false, nil)
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return session, nil
}

func (r *RabbitMQBroker) newPublisher(ctx context.Context) (*publisherSession, error) {
	session, err := r.openSession(ctx, JobsQueue, JobsRoutingKey)
	if err != nil {
		return nil, err
	}
	publisher := &publisherSession{
		rabbitSession: session, next: 1,
		confirms: session.ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
		returns:  session.ch.NotifyReturn(make(chan amqp.Return, 1)),
	}
	if err := withConnectionContext(ctx, session.conn, func() error {
		if _, err := session.ch.QueueDeclare(JobsQuarantineQueue, true, false, false, false, amqp.Table{
			"x-message-ttl": int64(ExecutionMessageTTL / time.Millisecond),
		}); err != nil {
			return err
		}
		if err := session.ch.QueueBind(JobsQuarantineQueue, JobsQuarantineQueue, r.exchange, false, nil); err != nil {
			return err
		}
		return session.ch.Confirm(false)
	}); err != nil {
		session.conn.Close()
		return nil, err
	}
	return publisher, nil
}

// Publish waits for a positive publisher confirmation and rejects mandatory
// returns. Its entire operation (including admission, reconnect, topology, socket
// write and confirmation) uses ctx, capped at ten seconds. Failure is ambiguous:
// the broker may have accepted the message. There is no retry inside this call;
// a subsequent call reconnects and redeclares topology after any failure.
func (r *RabbitMQBroker) Publish(ctx context.Context, routingKey string, payload []byte, messageID string) error {
	if err := r.begin(); err != nil {
		return err
	}
	defer r.active.Done()
	ctx, cancel := r.operationContext(ctx)
	defer cancel()
	select {
	case r.pubGate <- struct{}{}:
		defer func() { <-r.pubGate }()
	case <-ctx.Done():
		return r.operationError(ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return r.operationError(err)
	}
	if r.publisher != nil && (r.publisher.conn.IsClosed() || r.publisher.ch.IsClosed()) {
		r.publisher.conn.Close()
		r.publisher = nil
	}
	if r.publisher == nil {
		var err error
		r.publisher, err = r.newPublisher(ctx)
		if err != nil {
			return r.operationError(err)
		}
	}
	publisher := r.publisher
	err := withConnectionContext(ctx, publisher.conn, func() error {
		// The default exchange routes by exact queue name. A wildcard binding
		// on the public topic exchange must not confirm a missing jobs queue.
		expiration := ""
		if routingKey == JobsRoutingKey {
			expiration = strconv.FormatInt(ExecutionMessageTTL.Milliseconds(), 10)
		}
		if err := publisher.ch.PublishWithContext(ctx, "", routingKey, true, false, amqp.Publishing{
			ContentType: "application/json", Body: payload, MessageId: messageID, DeliveryMode: amqp.Persistent,
			Expiration: expiration,
		}); err != nil {
			return err
		}
		return publisher.waitConfirm(ctx)
	})
	if err != nil {
		publisher.conn.Close()
		r.publisher = nil
		return r.operationError(err)
	}
	return nil
}

// Ping verifies publisher connectivity and restores topology after a disconnect.
func (r *RabbitMQBroker) Ping(ctx context.Context) error {
	if err := r.begin(); err != nil {
		return err
	}
	defer r.active.Done()
	ctx, cancel := r.operationContext(ctx)
	defer cancel()
	select {
	case r.pubGate <- struct{}{}:
		defer func() { <-r.pubGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.publisher != nil && (r.publisher.conn.IsClosed() || r.publisher.ch.IsClosed()) {
		r.publisher.conn.Close()
		r.publisher = nil
	}
	if r.publisher == nil {
		var err error
		r.publisher, err = r.newPublisher(ctx)
		return err
	}
	err := withConnectionContext(ctx, r.publisher.conn, func() error {
		return r.publisher.ch.ExchangeDeclare(r.exchange, "topic", true, false, false, false, nil)
	})
	if err != nil {
		r.publisher.conn.Close()
		r.publisher = nil
	}
	return err
}

func returnedError(ret amqp.Return) error {
	return fmt.Errorf("%w: %d %s (exchange=%q routing_key=%q)", ErrUnroutable, ret.ReplyCode, ret.ReplyText, ret.Exchange, ret.RoutingKey)
}

func (p *publisherSession) waitConfirm(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return amqp.ErrClosed
	case ret, ok := <-p.returns:
		if !ok {
			return amqp.ErrClosed
		}
		return returnedError(ret)
	case confirmation, ok := <-p.confirms:
		if !ok {
			return amqp.ErrClosed
		}
		if confirmation.DeliveryTag != p.next {
			return fmt.Errorf("unexpected publisher confirmation %d, expected %d", confirmation.DeliveryTag, p.next)
		}
		p.next++
		// RabbitMQ sends basic.return before basic.ack for an unroutable
		// mandatory publication. The client dispatches them in wire order, but
		// select can choose the confirmation while the return is also buffered.
		select {
		case ret, ok := <-p.returns:
			if !ok {
				return amqp.ErrClosed
			}
			return returnedError(ret)
		default:
		}
		if !confirmation.Ack {
			return ErrPublishNack
		}
		return nil
	}
}

func (r *RabbitMQBroker) newConsumer(ctx context.Context, queue, routingKey string, prefetch int) (*consumerSession, error) {
	session, err := r.openSession(ctx, queue, routingKey)
	if err != nil {
		return nil, err
	}
	var deliveries <-chan amqp.Delivery
	err = withConnectionContext(ctx, session.conn, func() error {
		if err := session.ch.Qos(prefetch, 0, false); err != nil {
			return err
		}
		var err error
		deliveries, err = session.ch.Consume(queue, consumerTag, false, false, false, false, nil)
		return err
	})
	if err != nil {
		session.conn.Close()
		return nil, err
	}
	return &consumerSession{rabbitSession: session, deliveries: deliveries}, nil
}

// Consume retries connection, topology and subscription failures with exponential
// backoff between 100ms and 5s. It uses prefetch concurrent handlers and manual,
// individual acknowledgments. Nonpositive prefetch defaults to one.
//
// Canceling ctx (or closing the broker) cancels the AMQP subscription, drains
// active handlers, and closes the connection to requeue unstarted deliveries.
// Handler contexts are independent of intake cancellation. Reconnection also
// waits for the previous session's handlers, preserving the concurrency limit.
func (r *RabbitMQBroker) Consume(ctx context.Context, queue, routingKey string, prefetch int, handler func(context.Context, []byte) error) error {
	if queue == "" || handler == nil {
		return errors.New("consumer requires a named queue and a handler")
	}
	if prefetch <= 0 {
		prefetch = 1
	}
	if prefetch > 65535 {
		return errors.New("consumer prefetch exceeds AMQP maximum of 65535")
	}
	if err := r.begin(); err != nil {
		return err
	}
	defer r.active.Done()
	intakeCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer func() { stop(); cancel() }()
	r.mu.Lock()
	if r.consuming {
		r.mu.Unlock()
		return errors.New("broker already has an active consumer")
	}
	r.consuming, r.consumerCtx = true, intakeCtx
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.consuming, r.consumerCtx, r.subscription = false, nil, nil
		r.mu.Unlock()
	}()
	handlerCtx := context.WithoutCancel(ctx)
	backoff := reconnectMin
	for {
		if intakeCtx.Err() != nil {
			return r.operationError(ctx.Err())
		}
		setupCtx, setupCancel := context.WithTimeout(intakeCtx, operationTimeout)
		session, err := r.newConsumer(setupCtx, queue, routingKey, prefetch)
		setupCancel()
		if err == nil {
			r.mu.Lock()
			r.subscription = session
			r.mu.Unlock()
			started := time.Now()
			err = session.run(intakeCtx, handlerCtx, prefetch, handler, func(delivery amqp.Delivery) error {
				// Confirm the quarantine copy before acknowledging the original.
				// This also works with existing queues declared without DLX args.
				quarantineCtx, cancel := context.WithTimeout(context.Background(), operationTimeout)
				defer cancel()
				return r.Publish(quarantineCtx, JobsQuarantineQueue, delivery.Body, delivery.MessageId)
			}, func() {
				r.mu.Lock()
				r.subscription = nil
				r.mu.Unlock()
			})
			// A stable connection resets the delay; repeated short-lived
			// subscriptions continue backing off rather than spinning.
			if time.Since(started) >= reconnectMax {
				backoff = reconnectMin
			}
		}
		if intakeCtx.Err() == nil {
			slog.Warn("RabbitMQ consumer disconnected; retrying", "queue", queue, "retry_delay", backoff, "error", err)
		}
		if !waitReconnect(intakeCtx, backoff) {
			return r.operationError(ctx.Err())
		}
		backoff = nextBackoff(backoff)
	}
}

// ConsumerReady reports a live subscription, not publisher connectivity. Intake
// cancellation and socket closure are visible even while handlers are draining.
func (r *RabbitMQBroker) ConsumerReady() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.subscription
	return !r.closed && s != nil && r.consumerCtx != nil && r.consumerCtx.Err() == nil && !s.conn.IsClosed() && !s.ch.IsClosed()
}

func waitReconnect(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(delay time.Duration) time.Duration {
	if delay >= reconnectMax/2 {
		return reconnectMax
	}
	return delay * 2
}

func (s *consumerSession) settle(delivery amqp.Delivery, handlerErr error) error {
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	return withConnectionContext(ctx, s.conn, func() error {
		if handlerErr != nil {
			return delivery.Nack(false, !IsPermanent(handlerErr))
		}
		return delivery.Ack(false)
	})
}

func (s *consumerSession) run(intakeCtx, handlerCtx context.Context, prefetch int, handler func(context.Context, []byte) error, quarantine func(amqp.Delivery) error, onStop func()) error {
	defer s.conn.Close()
	work := make(chan amqp.Delivery)
	settleErrors := make(chan error, 1)
	var workers sync.WaitGroup
	for i := 0; i < prefetch; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			delay := reconnectMin
			for delivery := range work {
				handlerErr := invokeMessageHandler(handlerCtx, handler, delivery.Body)
				if IsPermanent(handlerErr) {
					handlerErr = quarantine(delivery)
				}
				if handlerErr != nil && !IsPermanent(handlerErr) {
					// Keep the delivery unacked during backoff; an immediate
					// nack/requeue loop can saturate RabbitMQ and the database.
					waitReconnect(intakeCtx, delay)
					delay = nextBackoff(delay)
				} else {
					delay = reconnectMin
				}
				if err := s.settle(delivery, handlerErr); err != nil {
					select {
					case settleErrors <- err:
					default:
					}
					s.conn.Abort()
				}
			}
		}()
	}

intake:
	for {
		if intakeCtx.Err() != nil {
			break
		}
		select {
		case <-intakeCtx.Done():
			break intake
		case <-s.closed:
			break intake
		case <-settleErrors:
			break intake
		case delivery, ok := <-s.deliveries:
			if !ok || intakeCtx.Err() != nil {
				break intake
			}
			select {
			case <-intakeCtx.Done():
				break intake
			case <-s.closed:
				break intake
			case <-settleErrors:
				break intake
			case work <- delivery:
			}
		}
	}
	onStop() // Mark unready before waiting for user handlers.
	close(work)
	if intakeCtx.Err() != nil {
		// Intake cancellation must not close the connection before handlers
		// commit and acknowledge. Cancel has its own bounded shutdown context.
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		if err := withConnectionContext(ctx, s.conn, func() error { return s.ch.Cancel(consumerTag, false) }); err != nil {
			s.conn.Abort()
		}
		cancel()
	} else {
		// A broken session cannot safely settle more deliveries. Closing the
		// socket requeues them, while its handlers finish before reconnecting.
		s.conn.Abort()
	}
	workers.Wait()
	return amqp.ErrClosed
}

func invokeMessageHandler(ctx context.Context, handler func(context.Context, []byte) error, payload []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("message handler panicked")
		}
	}()
	return handler(ctx, payload)
}

// Close is terminal and idempotent. It interrupts publications and reconnect
// attempts, stops consumers, and waits for their active handlers to drain.
// Like Consume, it requires handlers to return; it must not be called by a handler.
func (r *RabbitMQBroker) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		r.active.Wait()
		if r.publisher != nil {
			r.publisher.conn.Close()
			r.publisher = nil
		}
	})
	return nil
}
