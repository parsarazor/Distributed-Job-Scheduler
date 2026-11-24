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
	if JobQueue("a") == JobQueue("b") || JobQueue(long) == JobQueue(long+"y") {
		t.Fatal("distinct names must map to distinct queues")
	}
	if IsJobTypeQueue(JobsQueue) || IsJobTypeQueue(JobsQuarantineQueue) {
		t.Fatal("shared and quarantine queues are not job-type queues")
	}
}
