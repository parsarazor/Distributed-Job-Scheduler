package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

// JobQueuePrefix starts every per-job-name execution queue.
const JobQueuePrefix = JobsQueue + "."

// JobTypeQueueExpiry deletes a job-name queue that has had no consumer for this
// long, so announcements for a job name nobody handles become unroutable.
const JobTypeQueueExpiry = time.Hour

const maxQueueName = 255

// JobQueue is the execution queue for jobs named name. Names that would exceed
// RabbitMQ's 255-byte limit, contain anything but visible ASCII (RabbitMQ may
// rewrite such names on declaration), or collide with the quarantine queue or
// the hashed namespace use a SHA-256 prefix instead.
func JobQueue(name string) string {
	q := JobQueuePrefix + name
	visible := !strings.ContainsFunc(name, func(r rune) bool { return r < '!' || r > '~' })
	if len(q) > maxQueueName || !visible || q == JobsQuarantineQueue || strings.HasPrefix(name, "h.") {
		sum := sha256.Sum256([]byte(name))
		return JobQueuePrefix + "h." + hex.EncodeToString(sum[:16])
	}
	return q
}

// IsJobTypeQueue reports whether queue is a per-job-name execution queue.
func IsJobTypeQueue(queue string) bool {
	return strings.HasPrefix(queue, JobQueuePrefix) && queue != JobsQuarantineQueue
}
