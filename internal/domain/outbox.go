package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// MaxOutboxRetries is the publication retry ceiling. A row whose publication
// keeps failing is dead-lettered (kept for inspection, never re-published) once
// its retry count reaches this value, so one unroutable message cannot spin the
// publisher forever.
const MaxOutboxRetries = 10

// MaxDeadLetteredDispatches bounds how many announcements for one job may
// dead-letter before the job itself is terminalized. Every recovery pass clears
// dispatched_at and stages a fresh announcement, so without the cap a poison
// pill would be re-dispatched forever.
const MaxDeadLetteredDispatches = 3

// DeadLetterRetention keeps dead-lettered rows available for inspection and
// exhaustion counting before pruning removes them.
const DeadLetterRetention = 7 * 24 * time.Hour

// PublishingLease bounds how long a publisher may hold a claim on an outbox
// row. Rows claimed longer than this (for example after a scheduler crash
// mid-publish) are reclaimed and retried by the next reconciliation pass.
const PublishingLease = 30 * time.Second

// OutboxEvent encapsulates a message to be reliably dispatched to RabbitMQ
// using the Transactional Outbox pattern.
type OutboxEvent struct {
	ID              uuid.UUID       `json:"id"`
	JobID           uuid.UUID       `json:"job_id"`
	RoutingKey      string          `json:"routing_key"`
	Payload         json.RawMessage `json:"payload"`
	Published       bool            `json:"published"`
	DeadLettered    bool            `json:"dead_lettered"`
	PublishingSince *time.Time      `json:"publishing_since,omitempty"`
	RetryCount      int             `json:"retry_count"`
	LastError       *string         `json:"last_error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	PublishedAt     *time.Time      `json:"published_at,omitempty"`
	NextPublishAt   *time.Time      `json:"-"`
	DeliveredAt     *time.Time      `json:"-"`
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

// ExceededMaxRetries reports whether the event has reached the retry ceiling.
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
