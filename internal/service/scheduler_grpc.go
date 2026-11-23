package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SchedulerServer implements apiv1.SchedulerServiceServer.
type SchedulerServer struct {
	apiv1.UnimplementedSchedulerServiceServer
	sched *Scheduler
}

// NewSchedulerServer wraps a Scheduler as a gRPC server.
func NewSchedulerServer(s *Scheduler) *SchedulerServer {
	return &SchedulerServer{sched: s}
}

func toGRPCError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "context canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	case errors.Is(err, errTenantCredentialRequired):
		return status.Error(codes.PermissionDenied, "tenant credential required")
	case errors.Is(err, domain.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, "invalid input")
	case errors.Is(err, domain.ErrInvalidLease):
		return status.Error(codes.InvalidArgument, "invalid lease")
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "submission key already used for different input")
	case errors.Is(err, domain.ErrTenantLimit):
		return status.Error(codes.ResourceExhausted, "tenant quota or submission rate exceeded")
	case errors.Is(err, domain.ErrJobNotFound), errors.Is(err, domain.ErrTenantNotFound), errors.Is(err, domain.ErrWorkerCredentialNotFound):
		return status.Error(codes.NotFound, "resource not found")
	case errors.Is(err, domain.ErrPayloadExpired), errors.Is(err, domain.ErrInvalidTransition), errors.Is(err, domain.ErrJobCannotBeCancelled), errors.Is(err, domain.ErrDependencyCycle), errors.Is(err, domain.ErrDependencyNotMet), errors.Is(err, domain.ErrJobNotClaimable), errors.Is(err, domain.ErrMaxAttemptsReached):
		return status.Error(codes.FailedPrecondition, "job operation precondition failed")
	case errors.Is(err, domain.ErrFencingTokenStale), errors.Is(err, domain.ErrLeaseLost):
		return status.Error(codes.Aborted, "job execution no longer owns the lease")
	default:
		return status.Error(codes.Internal, "internal service error")
	}
}

func (s *SchedulerServer) SubmitJob(ctx context.Context, req *apiv1.SubmitJobRequest) (*apiv1.SubmitJobResponse, error) {
	tenantID, err := requestTenant(ctx, req.TenantId)
	if err != nil {
		return nil, err
	}
	var dependsOn *uuid.UUID
	if req.DependsOn != nil {
		d, err := uuid.Parse(*req.DependsOn)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid depends_on")
		}
		dependsOn = &d
	}
	maxAttempts := int(req.MaxAttempts)
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	var scheduledAt time.Time
	if req.ScheduledAt != nil {
		if err := req.ScheduledAt.CheckValid(); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid scheduled_at")
		}
		scheduledAt = req.ScheduledAt.AsTime()
	}
	if req.TimeoutMs < 0 || req.TimeoutMs > int64((24*time.Hour)/time.Millisecond) || req.MaxAttempts < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid timeout or max_attempts")
	}
	if _, ok := apiv1.JobPriority_name[int32(req.Priority)]; !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid priority")
	}
	job, err := s.sched.SubmitJob(ctx, SubmitInput{
		TenantID:       tenantID,
		Name:           req.Name,
		Payload:        json.RawMessage(req.Payload),
		Priority:       ProtoToDomainPriority(req.Priority),
		MaxAttempts:    maxAttempts,
		DependsOn:      dependsOn,
		ScheduledAt:    scheduledAt,
		IdempotencyKey: req.IdempotencyKey,
		Timeout:        time.Duration(req.TimeoutMs) * time.Millisecond,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.SubmitJobResponse{Job: DomainJobToProto(job)}, nil
}

func (s *SchedulerServer) GetJob(ctx context.Context, req *apiv1.GetJobRequest) (*apiv1.GetJobResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	job, err := s.ownedJob(ctx, id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.GetJobResponse{Job: DomainJobToProto(job)}, nil
}

func (s *SchedulerServer) ListJobs(ctx context.Context, req *apiv1.ListJobsRequest) (*apiv1.ListJobsResponse, error) {
	tenantID, err := requestTenant(ctx, req.TenantId)
	if err != nil {
		return nil, err
	}
	limit, err := pageLimit(req.Limit, req.Offset)
	if err != nil {
		return nil, err
	}
	filter := repository.JobListFilter{Limit: limit, Offset: int(req.Offset), MetadataOnly: true}
	if req.Status != nil {
		if _, ok := apiv1.JobStatus_name[int32(*req.Status)]; !ok || *req.Status == apiv1.JobStatus_JOB_STATUS_UNSPECIFIED {
			return nil, status.Error(codes.InvalidArgument, "invalid status")
		}
		st := ProtoToDomainStatus(*req.Status)
		filter.Status = &st
	}
	if req.Priority != nil {
		if _, ok := apiv1.JobPriority_name[int32(*req.Priority)]; !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid priority")
		}
		p := ProtoToDomainPriority(*req.Priority)
		filter.Priority = &p
	}
	jobs, total, err := s.sched.ListJobs(ctx, tenantID, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}
	out := &apiv1.ListJobsResponse{TotalCount: total, NextOffset: req.Offset}
	pageBytes := pageEnvelopeBytes
	for _, j := range jobs {
		if len(out.Jobs) >= limit {
			break
		}
		pj := DomainJobMetadataToProto(j)
		n := repeatedMessageBytes(pj)
		if pageBytes+n > maxPageBytes {
			if len(out.Jobs) == 0 {
				return nil, status.Error(codes.ResourceExhausted, "job metadata exceeds page size")
			}
			break
		}
		out.Jobs = append(out.Jobs, pj)
		pageBytes += n
	}
	out.NextOffset += int32(len(out.Jobs))
	out.HasMore = int64(out.NextOffset) < total
	return out, nil
}

func (s *SchedulerServer) CancelJob(ctx context.Context, req *apiv1.CancelJobRequest) (*apiv1.CancelJobResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	if _, err := s.ownedJob(ctx, id); err != nil {
		return nil, toGRPCError(err)
	}
	job, err := s.sched.CancelJob(ctx, id, req.Reason)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.CancelJobResponse{Job: DomainJobToProto(job), Message: "cancelled"}, nil
}

func (s *SchedulerServer) GetJobEvents(ctx context.Context, req *apiv1.GetJobEventsRequest) (*apiv1.GetJobEventsResponse, error) {
	limit, err := pageLimit(req.Limit, req.Offset)
	if err != nil {
		return nil, err
	}
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	if err := s.ownsJobMetadata(ctx, id); err != nil {
		return nil, toGRPCError(err)
	}
	events, err := s.sched.events.ListByJobIDPage(ctx, id, limit+1, int(req.Offset))
	if err != nil {
		return nil, toGRPCError(err)
	}
	out := &apiv1.GetJobEventsResponse{NextOffset: req.Offset}
	pageBytes := pageEnvelopeBytes
	for _, e := range events {
		pe := &apiv1.JobEvent{
			Id: e.ID.String(), JobId: e.JobID.String(),
			FromStatus: DomainToProtoStatus(e.FromStatus), ToStatus: DomainToProtoStatus(e.ToStatus),
		}
		if !e.OccurredAt.IsZero() {
			pe.OccurredAt = toTimestamp(e.OccurredAt)
		}
		if e.WorkerID != nil {
			pe.WorkerId = *e.WorkerID
		}
		if e.ErrorMessage != nil {
			pe.Message = domain.SafeHandlerMessage(*e.ErrorMessage)
		}
		n := repeatedMessageBytes(pe)
		if len(out.Events) >= limit || pageBytes+n > maxPageBytes {
			if len(out.Events) == 0 {
				return nil, status.Error(codes.ResourceExhausted, "event metadata exceeds page size")
			}
			out.HasMore = true
			break
		}
		out.Events = append(out.Events, pe)
		pageBytes += n
	}
	out.NextOffset += int32(len(out.Events))
	return out, nil
}

const (
	maxPageBytes = 1 << 20
	// Covers count, next_offset, has_more and their protobuf tags/varints.
	pageEnvelopeBytes = 32
)

func pageLimit(limit, offset int32) (int, error) {
	if limit < 0 || limit > 1000 || offset < 0 {
		return 0, status.Error(codes.InvalidArgument, "invalid pagination")
	}
	if limit == 0 {
		limit = 100
	}
	if int64(offset)+int64(limit) > 1<<31-1 {
		return 0, status.Error(codes.InvalidArgument, "pagination offset is too large")
	}
	return int(limit), nil
}

// All repeated messages here use field 1, so the tag occupies one byte.
func repeatedMessageBytes(message proto.Message) int {
	n := proto.Size(message)
	varintBytes := 1
	for value := n; value >= 128; value >>= 7 {
		varintBytes++
	}
	return 1 + varintBytes + n
}

func (s *SchedulerServer) ReportJobCompletion(ctx context.Context, req *apiv1.ReportJobCompletionRequest) (*apiv1.ReportJobCompletionResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	if err := workerBound(ctx, req.WorkerId); err != nil {
		return nil, err
	}
	message := ""
	if req.FinalStatus == apiv1.JobStatus_JOB_STATUS_FAILED {
		message = domain.SafeHandlerMessage(req.ErrorMessage)
		if message == "" {
			message = "handler failed"
		}
	}
	n, err := s.sched.ReportCompletion(ctx, id, req.WorkerId, ProtoToDomainStatus(req.FinalStatus), req.FencingToken, message)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.ReportJobCompletionResponse{Acknowledged: true, DependentsUnlocked: int32(n)}, nil
}

// MaxWorkerLease bounds the lease a worker may request for one renewal period.
const MaxWorkerLease = 10 * time.Minute

func workerLease(ms int64) (time.Duration, error) {
	if ms <= 0 || ms > MaxWorkerLease.Milliseconds() {
		return 0, status.Error(codes.InvalidArgument, "lease_ttl_ms must be between 1 and 600000")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func (s *SchedulerServer) ClaimJobExecution(ctx context.Context, req *apiv1.ClaimJobExecutionRequest) (*apiv1.ClaimJobExecutionResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil || id == uuid.Nil || req.AttemptCount < 0 || req.FencingToken < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid job announcement")
	}
	if err := workerBound(ctx, req.WorkerId); err != nil {
		return nil, err
	}
	ttl, err := workerLease(req.LeaseTtlMs)
	if err != nil {
		return nil, err
	}
	// Record receipt before the dispatch check: canceling a dispatched job
	// clears dispatched_at, and its announcement must still release the slot.
	if err := s.sched.outbox.MarkDelivered(ctx, domain.DispatchID(id, int(req.AttemptCount), req.FencingToken)); err != nil {
		return nil, toGRPCError(err)
	}
	// Workers may only claim work the scheduler has announced.
	stored, err := s.sched.GetJob(ctx, id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	if stored.DispatchedAt == nil {
		return nil, status.Error(codes.FailedPrecondition, "job has not been dispatched")
	}
	announcement := &domain.Job{ID: id, AttemptCount: int(req.AttemptCount), FencingToken: req.FencingToken}
	job, token, err := s.sched.ClaimExecution(ctx, announcement, req.WorkerId, ttl)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.ClaimJobExecutionResponse{Job: DomainJobToProto(job), FencingToken: token}, nil
}

func (s *SchedulerServer) RenewJobLease(ctx context.Context, req *apiv1.RenewJobLeaseRequest) (*apiv1.RenewJobLeaseResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	if err := workerBound(ctx, req.WorkerId); err != nil {
		return nil, err
	}
	ttl, err := workerLease(req.LeaseTtlMs)
	if err != nil {
		return nil, err
	}
	if err := s.sched.jobs.RenewLease(ctx, id, req.WorkerId, req.FencingToken, ttl); err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.RenewJobLeaseResponse{}, nil
}

func (s *SchedulerServer) GetExecutionJob(ctx context.Context, req *apiv1.GetExecutionJobRequest) (*apiv1.GetExecutionJobResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	if err := workerBound(ctx, req.WorkerId); err != nil {
		return nil, err
	}
	job, err := s.sched.GetJob(ctx, id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	// The shared worker credential must not read arbitrary jobs: only the
	// execution that holds (or just released) this attempt may look it up.
	if job.FencingToken != req.FencingToken || (job.LeaseOwner != nil && *job.LeaseOwner != req.WorkerId) {
		return nil, toGRPCError(domain.ErrLeaseLost)
	}
	out := &apiv1.GetExecutionJobResponse{Job: DomainJobMetadataToProto(job), LeaseOwner: job.LeaseOwner}
	if job.LeaseExpiresAt != nil {
		out.LeaseExpiresAt = timestamppb.New(*job.LeaseExpiresAt)
	}
	return out, nil
}

func (s *SchedulerServer) Dispatch(ctx context.Context, req *apiv1.DispatchRequest) (*apiv1.DispatchResponse, error) {
	n, err := s.sched.DispatchReady(ctx, int(req.BatchSize))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.DispatchResponse{DispatchedCount: int32(n)}, nil
}

func (s *SchedulerServer) TriggerReconciliation(ctx context.Context, req *apiv1.TriggerReconciliationRequest) (*apiv1.TriggerReconciliationResponse, error) {
	n, err := s.sched.ReconcileOutbox(ctx, 100)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.TriggerReconciliationResponse{ReconciledCount: int32(n)}, nil
}

func (s *SchedulerServer) RequeueDeadJob(ctx context.Context, req *apiv1.RequeueDeadJobRequest) (*apiv1.RequeueDeadJobResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	if _, err := s.ownedJob(ctx, id); err != nil {
		return nil, toGRPCError(err)
	}
	job, err := s.sched.RequeueDead(ctx, id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.RequeueDeadJobResponse{Job: DomainJobToProto(job)}, nil
}
