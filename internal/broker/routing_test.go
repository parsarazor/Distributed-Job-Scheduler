package broker

import (
	"strings"
	"testing"
)

func TestJobQueue(t *testing.T) {
	if q := JobQueue("send-email"); q != "jobs.execute.send-email" || !IsJobTypeQueue(q) {
		t.Fatalf("plain name: %q", q)
	}
	long := strings.Repeat("x", 255)
	for _, name := range []string{long, "quarantine", "h.abc", "two\nlines", "with space", "émail", "tab\t"} {
		q := JobQueue(name)
		if !strings.HasPrefix(q, JobQueuePrefix+"h.") || len(q) > maxQueueName || !IsJobTypeQueue(q) || q == JobsQuarantineQueue {
			t.Fatalf("%q mapped to %q", name, q)
		}
		if JobQueue(name) != q {
			t.Fatal("JobQueue must be deterministic")
		}
	}
	// The 255-byte limit includes the prefix: 242 name bytes fit, 243 do not.
	if q := JobQueue(strings.Repeat("a", maxQueueName-len(JobQueuePrefix))); len(q) != maxQueueName || strings.HasPrefix(q, JobQueuePrefix+"h.") {
		t.Fatalf("name at the limit must stay readable: %q", q)
	}
	if q := JobQueue(strings.Repeat("a", maxQueueName-len(JobQueuePrefix)+1)); !strings.HasPrefix(q, JobQueuePrefix+"h.") || len(q) > maxQueueName {
		t.Fatalf("name over the limit must be hashed: %q", q)
	}
	// Unhashed queues never use the "h." namespace, so they cannot equal a hash.
	if hashed := JobQueue(strings.Repeat("z", 300)); JobQueue(strings.TrimPrefix(hashed, JobQueuePrefix)) == hashed {
		t.Fatal("a job named like a hashed queue must not map onto it")
	}
	if JobQueue("a") == JobQueue("b") || JobQueue(long) == JobQueue(long+"y") {
		t.Fatal("distinct names must map to distinct queues")
	}
	if IsJobTypeQueue(JobsQueue) || IsJobTypeQueue(JobsQuarantineQueue) {
		t.Fatal("shared and quarantine queues are not job-type queues")
	}
}
