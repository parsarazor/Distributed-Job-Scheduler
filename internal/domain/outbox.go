package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// MaxOutboxRetries is an alert threshold. Publication keeps retrying so an
// extended broker outage cannot permanently strand a dispatched job.
const MaxOutboxRetries = 10

// OutboxEvent encapsulates a message to be reliably dispatched to RabbitMQ
// using the Transactional Outbox pattern.
type OutboxEvent struct {
	ID            uuid.UUID       `json:"id"`
	JobID         uuid.UUID       `json:"job_id"`
	RoutingKey    string          `json:"routing_key"`
	Payload       json.RawMessage `json:"payload"`
	Published     bool            `json:"published"`
	RetryCount    int             `json:"retry_count"`
	LastError     *string         `json:"last_error,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	PublishedAt   *time.Time      `json:"published_at,omitempty"`
	NextPublishAt *time.Time      `json:"-"`
	DeliveredAt   *time.Time      `json:"-"`
}

// DispatchID identifies an announcement generation, including before its claim.
func DispatchID(jobID uuid.UUID, attempt int, token int64) uuid.UUID {
	return uuid.NewSHA1(jobID, []byte(fmt.Sprintf("dispatch:%d:%d", attempt, token)))
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

// ExceededMaxRetries reports whether the event has reached the alert threshold.
func (e *OutboxEvent) ExceededMaxRetries() bool {
	return e.RetryCount >= MaxOutboxRetries
}

// RecordFailure increments the retry counter and stores the last error.
func (e *OutboxEvent) RecordFailure(errMsg string) {
	e.RetryCount++
	e.LastError = &errMsg
	next := time.Now().UTC().Add(time.Second * time.Duration(1<<min(e.RetryCount-1, 8)))
	e.NextPublishAt = &next
}

// MarkPublished flags the event as successfully handed to the broker.
func (e *OutboxEvent) MarkPublished() {
	now := time.Now().UTC()
	e.Published = true
	e.PublishedAt = &now
}
