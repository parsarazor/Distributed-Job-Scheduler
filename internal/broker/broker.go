package broker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Broker publishes job messages with at-least-once semantics. A failed Publish
// can have reached the queue; retrying may deliver the same messageID again.
// Message IDs are metadata, not a broker-side deduplication key.
type Broker interface {
	Publish(ctx context.Context, routingKey string, payload []byte, messageID string) error
	Close() error
}

// Consumer is optional and is currently implemented only by RabbitMQBroker.
// A successful handler must have committed its durable result before returning.
// Canceling ctx stops intake and waits for active handlers, whose contexts retain
// ctx's values but do not inherit its cancellation or deadline. Handlers must
// impose their own execution timeouts so that shutdown can finish.
type Consumer interface {
	Consume(ctx context.Context, queue, routingKey string, prefetch int, handler func(context.Context, []byte) error) error
	ConsumeQueues(ctx context.Context, queues []string, prefetch int, handler func(context.Context, []byte) error) error
}

var ErrClosed = errors.New("broker is closed")

// Permanent marks a poison message that must be quarantined before settlement.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

const (
	JobsQueue           = "jobs.execute"
	JobsRoutingKey      = "jobs.execute"
	JobsQuarantineQueue = "jobs.execute.quarantine"
	ExecutionMessageTTL = 5 * time.Minute
)

// MemoryBroker records every publish in memory, including repeated message IDs.
// It supports introspection, but does not implement Consumer or disk durability.
type MemoryBroker struct {
	mu        sync.Mutex
	published []PublishedMessage
	closed    bool
}

// PublishedMessage is one broker publish (for tests / demo output).
type PublishedMessage struct {
	RoutingKey string
	Payload    []byte
	MessageID  string
}

// NewMemoryBroker creates an empty broker.
func NewMemoryBroker() *MemoryBroker { return &MemoryBroker{} }

func (m *MemoryBroker) Publish(ctx context.Context, routingKey string, payload []byte, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
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
	for i, p := range m.published {
		out[i] = p
		out[i].Payload = append([]byte(nil), p.Payload...)
	}
	return out
}

func (m *MemoryBroker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
