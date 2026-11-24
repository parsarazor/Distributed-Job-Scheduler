package domain

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

// SafeHandlerMessages are the fixed execution outcomes allowed in audit output.
func SafeHandlerMessages() []string {
	return []string{"handler failed", "handler panicked", "handler canceled", "handler timed out", "handler not registered", ReasonPermanentFailure}
}

// ReasonHandlerNotRegistered is both a safe handler message and the
// non-retryable signal: ReportCompletion dead-letters a job whose failure
// reason is this instead of consuming its retry budget.
const ReasonHandlerNotRegistered = "handler not registered"

// ReasonNoWorker explains a job that stayed unroutable because no worker
// declared a queue for its job name within the scheduler's no-worker timeout.
const ReasonNoWorker = "no worker for job name"

// ReasonPermanentFailure records a handler failure the worker reported as non-retryable.
const ReasonPermanentFailure = "handler failed permanently"

func SafeHandlerMessage(message string) string {
	for _, safe := range SafeHandlerMessages() {
		if safe == message {
			return safe
		}
	}
	return ""
}

// AuditText removes control characters and bounds UTF-8 text without cutting a rune.
func AuditText(value string, limit int) string {
	var result strings.Builder
	for _, char := range value {
		if unicode.IsControl(char) {
			continue
		}
		if result.Len()+utf8.RuneLen(char) > limit {
			break
		}
		result.WriteRune(char)
	}
	return result.String()
}

// JobEvent documents an immutable audit record of a job state transition.
type JobEvent struct {
	ID           uuid.UUID `json:"id"`
	JobID        uuid.UUID `json:"job_id"`
	FromStatus   JobStatus `json:"from_status"`
	ToStatus     JobStatus `json:"to_status"`
	OccurredAt   time.Time `json:"occurred_at"`
	WorkerID     *string   `json:"worker_id,omitempty"`
	ErrorMessage *string   `json:"error_message,omitempty"`
}
