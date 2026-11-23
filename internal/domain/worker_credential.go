package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrWorkerCredentialNotFound indicates the worker credential does not exist.
var ErrWorkerCredentialNotFound = errors.New("worker credential not found")

// WorkerCredential is a revocable bearer token bound to one worker ID. Several
// active credentials may share a worker ID so tokens rotate without downtime.
type WorkerCredential struct {
	ID        uuid.UUID  `json:"id"`
	WorkerID  string     `json:"worker_id"`
	TokenHash string     `json:"-"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}
