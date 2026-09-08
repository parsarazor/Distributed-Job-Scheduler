package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// MaxOutboxRetries caps reconciler attempts for a single outbox row. Past this
// ceiling the event is abandoned (dead-lettered) instead of retried forever.
const MaxOutboxRetries = 10

// OutboxEvent encapsulates a message to be reliably dispatched to RabbitMQ
// using the Transactional Outbox pattern.
type OutboxEvent struct {
	ID          uuid.UUID       `json:"id"`
	JobID       uuid.UUID       `json:"job_id"`
	RoutingKey  string          `json:"routing_key"`
	Payload     json.RawMessage `json:"payload"`
	Published   bool            `json:"published"`
	RetryCount  int             `json:"retry_count"`
	LastError   *string         `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
}

// NewOutboxEvent builds an unpublished outbox row for a job.
func NewOutboxEvent(jobID uuid.UUID, routingKey string, payload json.RawMessage) *OutboxEvent {
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	return &OutboxEvent{
		ID:         uuid.New(),
		JobID:      jobID,
		RoutingKey: routingKey,
		Payload:    payload,
		Published:  false,
		RetryCount: 0,
		CreatedAt:  time.Now().UTC(),
	}
}

// ExceededMaxRetries reports whether the event has failed too many times and
// must be abandoned instead of re-fetched on every reconciliation tick.
func (e *OutboxEvent) ExceededMaxRetries() bool {
	return e.RetryCount >= MaxOutboxRetries
}

// RecordFailure increments the retry counter and stores the last error.
func (e *OutboxEvent) RecordFailure(errMsg string) {
	e.RetryCount++
	e.LastError = &errMsg
}

// MarkPublished flags the event as successfully handed to the broker.
func (e *OutboxEvent) MarkPublished() {
	now := time.Now().UTC()
	e.Published = true
	e.PublishedAt = &now
}
