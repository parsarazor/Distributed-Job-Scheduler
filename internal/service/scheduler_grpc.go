package service

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	switch err {
	case domain.ErrJobNotFound, domain.ErrTenantNotFound:
		return status.Error(codes.NotFound, err.Error())
	case domain.ErrInvalidTransition, domain.ErrJobCannotBeCancelled, domain.ErrDependencyCycle, domain.ErrDependencyNotMet:
		return status.Error(codes.FailedPrecondition, err.Error())
	case domain.ErrFencingTokenStale:
		return status.Error(codes.Aborted, err.Error())
	case nil:
		return nil
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *SchedulerServer) SubmitJob(ctx context.Context, req *apiv1.SubmitJobRequest) (*apiv1.SubmitJobResponse, error) {
	tenantID, err := uuid.Parse(req.TenantId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid tenant_id")
	}
	var dependsOn *uuid.UUID
	if req.DependsOn != nil && *req.DependsOn != "" {
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
	var scheduledAt = nowUTC()
	if req.ScheduledAt != nil {
		scheduledAt = req.ScheduledAt.AsTime()
	}
	job, err := s.sched.SubmitJob(ctx, SubmitInput{
		TenantID:    tenantID,
		Name:        req.Name,
		Payload:     json.RawMessage(req.Payload),
		Priority:    ProtoToDomainPriority(req.Priority),
		MaxAttempts: maxAttempts,
		DependsOn:   dependsOn,
		ScheduledAt: scheduledAt,
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
	job, err := s.sched.GetJob(ctx, id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.GetJobResponse{Job: DomainJobToProto(job)}, nil
}

func (s *SchedulerServer) ListJobs(ctx context.Context, req *apiv1.ListJobsRequest) (*apiv1.ListJobsResponse, error) {
	tenantID, err := uuid.Parse(req.TenantId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid tenant_id")
	}
	filter := repository.JobListFilter{Limit: int(req.Limit), Offset: int(req.Offset)}
	if req.Status != nil {
		st := ProtoToDomainStatus(*req.Status)
		filter.Status = &st
	}
	if req.Priority != nil {
		p := ProtoToDomainPriority(*req.Priority)
		filter.Priority = &p
	}
	jobs, total, err := s.sched.ListJobs(ctx, tenantID, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}
	out := make([]*apiv1.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, DomainJobToProto(j))
	}
	return &apiv1.ListJobsResponse{Jobs: out, TotalCount: total}, nil
}

func (s *SchedulerServer) CancelJob(ctx context.Context, req *apiv1.CancelJobRequest) (*apiv1.CancelJobResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	job, err := s.sched.CancelJob(ctx, id, req.Reason)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.CancelJobResponse{Job: DomainJobToProto(job), Message: "cancelled"}, nil
}

func (s *SchedulerServer) GetJobEvents(ctx context.Context, req *apiv1.GetJobEventsRequest) (*apiv1.GetJobEventsResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	events, err := s.sched.events.ListByJobID(ctx, id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	out := make([]*apiv1.JobEvent, 0, len(events))
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
			pe.Message = *e.ErrorMessage
		}
		out = append(out, pe)
	}
	return &apiv1.GetJobEventsResponse{Events: out}, nil
}

func (s *SchedulerServer) ReportJobCompletion(ctx context.Context, req *apiv1.ReportJobCompletionRequest) (*apiv1.ReportJobCompletionResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid job_id")
	}
	n, err := s.sched.ReportCompletion(ctx, id, req.WorkerId, ProtoToDomainStatus(req.FinalStatus), req.FencingToken, req.ErrorMessage)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.ReportJobCompletionResponse{Acknowledged: true, DependentsUnlocked: int32(n)}, nil
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
