package service

import (
	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProtoToDomainPriority is the REQUIRED explicit mapping between proto
// ordinals (1-4) and domain DB weights (0,2,5,10).
//
// NEVER cast int32(req.Priority) to domain.JobPriority: CRITICAL(4) would be
// stored as weight 4 instead of 10 and HIGH(3) as 3 instead of 5, silently
// compressing priority spacing. UNSPECIFIED maps to Default.
func ProtoToDomainPriority(p apiv1.JobPriority) domain.JobPriority {
	switch p {
	case apiv1.JobPriority_JOB_PRIORITY_LOW:
		return domain.PriorityLow
	case apiv1.JobPriority_JOB_PRIORITY_HIGH:
		return domain.PriorityHigh
	case apiv1.JobPriority_JOB_PRIORITY_CRITICAL:
		return domain.PriorityCritical
	case apiv1.JobPriority_JOB_PRIORITY_DEFAULT:
		return domain.PriorityDefault
	default:
		return domain.PriorityDefault
	}
}

// DomainToProtoPriority maps domain weights back to proto ordinals.
func DomainToProtoPriority(p domain.JobPriority) apiv1.JobPriority {
	switch p {
	case domain.PriorityLow:
		return apiv1.JobPriority_JOB_PRIORITY_LOW
	case domain.PriorityHigh:
		return apiv1.JobPriority_JOB_PRIORITY_HIGH
	case domain.PriorityCritical:
		return apiv1.JobPriority_JOB_PRIORITY_CRITICAL
	default:
		return apiv1.JobPriority_JOB_PRIORITY_DEFAULT
	}
}

// ProtoToDomainStatus maps proto status to domain status.
func ProtoToDomainStatus(s apiv1.JobStatus) domain.JobStatus {
	switch s {
	case apiv1.JobStatus_JOB_STATUS_PENDING:
		return domain.StatusPending
	case apiv1.JobStatus_JOB_STATUS_RUNNING:
		return domain.StatusRunning
	case apiv1.JobStatus_JOB_STATUS_SUCCEEDED:
		return domain.StatusSucceeded
	case apiv1.JobStatus_JOB_STATUS_FAILED:
		return domain.StatusFailed
	case apiv1.JobStatus_JOB_STATUS_CANCELLED:
		return domain.StatusCancelled
	case apiv1.JobStatus_JOB_STATUS_DEAD:
		return domain.StatusDead
	default:
		return domain.StatusPending
	}
}

// DomainToProtoStatus maps domain status to proto status.
func DomainToProtoStatus(s domain.JobStatus) apiv1.JobStatus {
	switch s {
	case domain.StatusPending:
		return apiv1.JobStatus_JOB_STATUS_PENDING
	case domain.StatusRunning:
		return apiv1.JobStatus_JOB_STATUS_RUNNING
	case domain.StatusSucceeded:
		return apiv1.JobStatus_JOB_STATUS_SUCCEEDED
	case domain.StatusFailed:
		return apiv1.JobStatus_JOB_STATUS_FAILED
	case domain.StatusCancelled:
		return apiv1.JobStatus_JOB_STATUS_CANCELLED
	case domain.StatusDead:
		return apiv1.JobStatus_JOB_STATUS_DEAD
	default:
		return apiv1.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

// DomainJobToProto converts a domain Job to its wire representation.
func DomainJobToProto(j *domain.Job) *apiv1.Job {
	if j == nil {
		return nil
	}
	out := &apiv1.Job{
		Id:           j.ID.String(),
		TenantId:     j.TenantID.String(),
		Name:         j.Name,
		Status:       DomainToProtoStatus(j.Status),
		Payload:      []byte(j.Payload),
		Priority:     DomainToProtoPriority(j.Priority),
		AttemptCount: int32(j.AttemptCount),
		MaxAttempts:  int32(j.MaxAttempts),
		ScheduledAt:  timestamppb.New(j.ScheduledAt),
		CreatedAt:    timestamppb.New(j.CreatedAt),
		UpdatedAt:    timestamppb.New(j.UpdatedAt),
		FencingToken: j.FencingToken,
	}
	if j.DependsOn != nil {
		s := j.DependsOn.String()
		out.DependsOn = &s
	}
	if j.NextRetryAt != nil {
		out.NextRetryAt = timestamppb.New(*j.NextRetryAt)
	}
	return out
}

// ParseJobID parses a wire job id, returning uuid.Nil on empty input.
func ParseJobID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}
