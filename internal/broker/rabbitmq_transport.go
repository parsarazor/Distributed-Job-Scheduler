package broker

import (
	"context"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// These small transport interfaces let unit tests exercise connection failures
// and AMQP ordering without requiring a running RabbitMQ server.
type amqpChannel interface {
	IsClosed() bool
	ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error
	QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	QueueBind(string, string, string, bool, amqp.Table) error
	Confirm(bool) error
	NotifyPublish(chan amqp.Confirmation) chan amqp.Confirmation
	NotifyReturn(chan amqp.Return) chan amqp.Return
	NotifyClose(chan *amqp.Error) chan *amqp.Error
	PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error
	Qos(int, int, bool) error
	Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(string, bool) error
}

type amqpConnection interface {
	Channel() (amqpChannel, error)
	IsClosed() bool
	Abort()
	Close()
}

type amqpDialer func(context.Context, string) (amqpConnection, error)

type rabbitConnection struct {
	*amqp.Connection
	socket net.Conn
}

func (c *rabbitConnection) Channel() (amqpChannel, error) { return c.Connection.Channel() }

// Abort interrupts socket writes as well as reads. amqp091-go's context-aware
// publish methods do not interrupt an already started write.
func (c *rabbitConnection) Abort() { _ = c.socket.Close() }

func (c *rabbitConnection) Close() {
	c.Abort()
	// Also release AMQP resources if its reader has not yet noticed the closed
	// socket. An immediate deadline prevents a second shutdown handshake wait.
	_ = c.Connection.CloseDeadline(time.Now())
}

func dialRabbitMQ(ctx context.Context, url string) (amqpConnection, error) {
	var socket net.Conn
	var stop func()
	conn, err := amqp.DialConfig(url, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			var err error
			socket, err = (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			// This covers TLS and AMQP handshakes. Using cancellation instead
			// of a socket deadline also preserves the context's timeout error.
			stop = abortOnCancel(ctx, func() { _ = socket.Close() })
			return socket, nil
		},
	})
	if stop != nil {
		stop()
	}
	if err != nil || ctx.Err() != nil {
		if socket != nil {
			_ = socket.Close()
		}
		if conn != nil {
			_ = conn.CloseDeadline(time.Now())
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return &rabbitConnection{Connection: conn, socket: socket}, nil
}

// Stop also joins an already-running callback, so it cannot close a reused
// connection after the next operation has acquired it.
func abortOnCancel(ctx context.Context, abort func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		abort()
	})
	return func() {
		if !stop() {
			<-done
		}
	}
}

// withConnectionContext bounds synchronous AMQP operations, including RPCs and
// blocked writes. A canceled operation retires the entire dedicated connection.
func withConnectionContext(ctx context.Context, conn amqpConnection, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := abortOnCancel(ctx, conn.Abort)
	err := fn()
	stop()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
