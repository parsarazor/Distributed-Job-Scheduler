package service

import (
	"testing"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
)

func TestPriorityMapper_ExplicitWeights(t *testing.T) {
	cases := []struct {
		proto apiv1.JobPriority
		want  domain.JobPriority
	}{
		{apiv1.JobPriority_JOB_PRIORITY_LOW, domain.PriorityLow},           // 1 -> 0
		{apiv1.JobPriority_JOB_PRIORITY_DEFAULT, domain.PriorityDefault},   // 2 -> 2
		{apiv1.JobPriority_JOB_PRIORITY_HIGH, domain.PriorityHigh},         // 3 -> 5
		{apiv1.JobPriority_JOB_PRIORITY_CRITICAL, domain.PriorityCritical}, // 4 -> 10
		{apiv1.JobPriority_JOB_PRIORITY_UNSPECIFIED, domain.PriorityDefault},
	}
	for _, c := range cases {
		if got := ProtoToDomainPriority(c.proto); got != c.want {
			t.Errorf("proto %v -> %d, want %d", c.proto, int(got), int(c.want))
		}
	}
	// Round-trip must preserve weights.
	for _, p := range []domain.JobPriority{domain.PriorityLow, domain.PriorityDefault, domain.PriorityHigh, domain.PriorityCritical} {
		if back := ProtoToDomainPriority(DomainToProtoPriority(p)); back != p {
			t.Errorf("round-trip %d -> %d", int(p), int(back))
		}
	}
}

func TestStatusMapper_RoundTrip(t *testing.T) {
	for _, s := range []domain.JobStatus{
		domain.StatusPending, domain.StatusRunning, domain.StatusSucceeded,
		domain.StatusFailed, domain.StatusCancelled, domain.StatusDead,
	} {
		if back := ProtoToDomainStatus(DomainToProtoStatus(s)); back != s {
			t.Errorf("status round-trip %q -> %q", s, back)
		}
	}
}
