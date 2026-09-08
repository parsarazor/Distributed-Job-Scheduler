package service

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc/status"
)

func grpcCodeOf(err error) string {
	if err == nil {
		return "OK"
	}
	return status.Code(err).String()
}

// Principle: boundary-mapping test. The gRPC boundary is where the C2-class
// bug lived, so every mapped field (not just priority) gets asserted here.
func TestDomainJobToProto_FieldMapping(t *testing.T) {
	t.Parallel()
	dep := uuid.New()
	next := time.Now().UTC().Add(time.Minute).Truncate(time.Millisecond)
	j := &domain.Job{
		ID: uuid.New(), TenantID: uuid.New(), Name: "n", Status: domain.StatusRunning,
		Payload: json.RawMessage(`{"a":1}`), Priority: domain.PriorityCritical,
		AttemptCount: 2, MaxAttempts: 5, DependsOn: &dep, FencingToken: 7,
		ScheduledAt: time.Now().UTC(), NextRetryAt: &next,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	got := DomainJobToProto(j)
	if got.Id != j.ID.String() || got.TenantId != j.TenantID.String() || got.Name != "n" {
		t.Fatalf("identity fields wrong: %+v", got)
	}
	if got.Status != apiv1.JobStatus_JOB_STATUS_RUNNING {
		t.Fatalf("status wrong: %v", got.Status)
	}
	if got.Priority != apiv1.JobPriority_JOB_PRIORITY_CRITICAL {
		t.Fatalf("priority wrong: %v", got.Priority)
	}
	if got.AttemptCount != 2 || got.MaxAttempts != 5 || got.FencingToken != 7 {
		t.Fatalf("counter fields wrong: %+v", got)
	}
	if got.DependsOn == nil || *got.DependsOn != dep.String() {
		t.Fatalf("depends_on wrong: %+v", got.DependsOn)
	}
	if got.NextRetryAt == nil {
		t.Fatal("next_retry_at must be set")
	}
	if string(got.Payload) != `{"a":1}` {
		t.Fatalf("payload wrong: %s", got.Payload)
	}
}

func TestDomainJobToProto_NilAndEmpty(t *testing.T) {
	t.Parallel()
	if DomainJobToProto(nil) != nil {
		t.Fatal("nil job must map to nil proto")
	}
	j := &domain.Job{ID: uuid.New(), TenantID: uuid.New(), Priority: domain.PriorityLow, Status: domain.StatusPending}
	got := DomainJobToProto(j)
	if got.DependsOn != nil || got.NextRetryAt != nil {
		t.Fatal("absent optionals must stay nil on the wire")
	}
	if got.Priority != apiv1.JobPriority_JOB_PRIORITY_LOW {
		t.Fatalf("low weight must map to LOW ordinal, got %v", got.Priority)
	}
}

func TestParseJobID_Table(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	if got, err := ParseJobID(id.String()); err != nil || got != id {
		t.Fatalf("valid uuid: %v %v", got, err)
	}
	for _, bad := range []string{"", "not-a-uuid", "12345"} {
		if _, err := ParseJobID(bad); err == nil {
			t.Fatalf("input %q must fail", bad)
		}
	}
}

func TestProtoToDomainStatus_Table(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   apiv1.JobStatus
		want domain.JobStatus
	}{
		{apiv1.JobStatus_JOB_STATUS_PENDING, domain.StatusPending},
		{apiv1.JobStatus_JOB_STATUS_RUNNING, domain.StatusRunning},
		{apiv1.JobStatus_JOB_STATUS_SUCCEEDED, domain.StatusSucceeded},
		{apiv1.JobStatus_JOB_STATUS_FAILED, domain.StatusFailed},
		{apiv1.JobStatus_JOB_STATUS_CANCELLED, domain.StatusCancelled},
		{apiv1.JobStatus_JOB_STATUS_DEAD, domain.StatusDead},
		{apiv1.JobStatus_JOB_STATUS_UNSPECIFIED, domain.StatusPending},
	}
	for _, tc := range cases {
		if got := ProtoToDomainStatus(tc.in); got != tc.want {
			t.Errorf("%v -> %q want %q", tc.in, got, tc.want)
		}
	}
}

// Principle: gRPC error-code contract. Clients retry on Aborted, not on
// FailedPrecondition — the mapping must be exact, not best-effort.
func TestToGRPCError_Codes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err  error
		want string // code string for readability
	}{
		{domain.ErrJobNotFound, "NotFound"},
		{domain.ErrTenantNotFound, "NotFound"},
		{domain.ErrInvalidTransition, "FailedPrecondition"},
		{domain.ErrJobCannotBeCancelled, "FailedPrecondition"},
		{domain.ErrDependencyNotMet, "FailedPrecondition"},
		{domain.ErrDependencyCycle, "FailedPrecondition"},
		{domain.ErrFencingTokenStale, "Aborted"},
		{nil, "OK"},
	}
	for _, tc := range cases {
		err := toGRPCError(tc.err)
		got := "OK"
		if err != nil {
			got = grpcCodeOf(err)
		}
		if got != tc.want {
			t.Errorf("err %v -> %s want %s", tc.err, got, tc.want)
		}
	}
}
