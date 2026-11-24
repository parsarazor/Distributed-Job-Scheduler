package domain

import (
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// ErrWorkerCredentialNotFound indicates the worker credential does not exist.
var ErrWorkerCredentialNotFound = errors.New("worker credential not found")

// WorkerIDPattern is the only accepted WORKER_ID format, so IDs stay safe in
// logs, metric labels, shells and credential bindings.
const WorkerIDPattern = `^[A-Za-z0-9._-]{1,255}$`

var workerIDRE = regexp.MustCompile(WorkerIDPattern)

// ValidWorkerID reports whether id matches WorkerIDPattern.
func ValidWorkerID(id string) bool { return workerIDRE.MatchString(id) }

// WorkerCredential is a revocable bearer token bound to one worker ID. Several
// active credentials may share a worker ID so tokens rotate without downtime.
type WorkerCredential struct {
	ID        uuid.UUID  `json:"id"`
	WorkerID  string     `json:"worker_id"`
	TokenHash string     `json:"-"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}
