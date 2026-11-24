package broker

import (
	"errors"
	"net/url"

	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ValidateURL checks a RabbitMQ URL. Outside development it requires amqps,
// an explicit host and a non-development credential, and a complete client
// certificate pair when mutual TLS is configured.
func ValidateURL(raw string, development bool) error {
	uri, err := amqp.ParseURI(raw)
	if err != nil {
		return errors.New("invalid RABBITMQ_URL")
	}
	if development {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || uri.Scheme != "amqps" || u.Hostname() == "" {
		return errors.New("RABBITMQ_URL requires amqps with an explicit hostname")
	}
	if u.User == nil || u.User.Username() == "" {
		return errors.New("RABBITMQ_URL requires an explicit production credential")
	}
	password, _ := u.User.Password()
	if password == "" || domain.DevelopmentPassword(password) {
		return errors.New("RABBITMQ_URL requires a non-development password")
	}
	if (uri.CertFile == "") != (uri.KeyFile == "") {
		return errors.New("RABBITMQ_URL requires both client certificate and key when configuring mutual TLS")
	}
	return nil
}
