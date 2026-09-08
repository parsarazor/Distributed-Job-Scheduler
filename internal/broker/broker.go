package broker

import (
	"context"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

// Broker publishes job messages. Implementations must be idempotent on
// messageID (job_id as AMQP message_id per ADR 011).
type Broker interface {
	Publish(ctx context.Context, routingKey string, payload []byte, messageID string) error
	Close() error
}

// MemoryBroker records publishes in memory. It is the default for local runs.
type MemoryBroker struct {
	mu        sync.Mutex
	published []PublishedMessage
}

// PublishedMessage is one broker publish (for tests / demo output).
type PublishedMessage struct {
	RoutingKey string
	Payload    []byte
	MessageID  string
}

// NewMemoryBroker creates an empty broker.
func NewMemoryBroker() *MemoryBroker { return &MemoryBroker{} }

func (m *MemoryBroker) Publish(_ context.Context, routingKey string, payload []byte, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Idempotent on messageID: drop exact duplicates (ADR 011).
	for _, p := range m.published {
		if p.MessageID == messageID && p.RoutingKey == routingKey {
			return nil
		}
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	m.published = append(m.published, PublishedMessage{RoutingKey: routingKey, Payload: cp, MessageID: messageID})
	return nil
}

// Published returns a copy of all published messages.
func (m *MemoryBroker) Published() []PublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]PublishedMessage, len(m.published))
	copy(out, m.published)
	return out
}

func (m *MemoryBroker) Close() error { return nil }

// RabbitMQBroker publishes to a real RabbitMQ exchange via amqp091-go.
type RabbitMQBroker struct {
	conn     *amqp091.Connection
	ch       *amqp091.Channel
	exchange string
}

// NewRabbitMQBroker dials url and declares a durable topic exchange.
func NewRabbitMQBroker(url, exchange string) (*RabbitMQBroker, error) {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	return &RabbitMQBroker{conn: conn, ch: ch, exchange: exchange}, nil
}

func (r *RabbitMQBroker) Publish(ctx context.Context, routingKey string, payload []byte, messageID string) error {
	return r.ch.PublishWithContext(ctx, r.exchange, routingKey, false, false, amqp091.Publishing{
		ContentType:  "application/json",
		Body:         payload,
		MessageId:    messageID,
		DeliveryMode: amqp091.Persistent,
	})
}

// Consume binds queue to routingKey and invokes handler for each delivery.
// A handler error leaves the delivery unacknowledged and requeues it, which
// provides at-least-once delivery across worker crashes.
func (r *RabbitMQBroker) Consume(ctx context.Context, queue, routingKey string, prefetch int, handler func(context.Context, []byte) error) error {
	if prefetch <= 0 {
		prefetch = 1
	}
	if err := r.ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	if _, err := r.ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := r.ch.QueueBind(queue, routingKey, r.exchange, false, nil); err != nil {
		return err
	}
	deliveries, err := r.ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return amqp091.ErrClosed
			}
			if err := handler(ctx, d.Body); err != nil {
				if nackErr := d.Nack(false, true); nackErr != nil {
					return nackErr
				}
				continue
			}
			if err := d.Ack(false); err != nil {
				return err
			}
		}
	}
}

func (r *RabbitMQBroker) Close() error {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
