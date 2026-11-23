package service

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// WorkerServer implements apiv1.WorkerServiceServer over the local Worker.
type WorkerServer struct {
	apiv1.UnimplementedWorkerServiceServer
	worker *Worker
}

// NewWorkerServer wraps a Worker as a gRPC server.
func NewWorkerServer(w *Worker) *WorkerServer { return &WorkerServer{worker: w} }

func (s *WorkerServer) Execute(ctx context.Context, req *apiv1.ExecuteRequest) (*apiv1.ExecuteResponse, error) {
	if req == nil || req.Job == nil {
		return &apiv1.ExecuteResponse{Accepted: false, Message: "nil job"}, nil
	}
	id, err := uuid.Parse(req.Job.Id)
	if err != nil || id == uuid.Nil || req.Job.AttemptCount < 0 || req.Job.FencingToken < 0 {
		return &apiv1.ExecuteResponse{Accepted: false, Message: "invalid job id"}, nil
	}
	job, err := s.worker.sched.GetJob(ctx, id)
	if err != nil {
		slog.Error("worker execute job lookup failed", "job_id", id, "error", err)
		if errors.Is(err, domain.ErrJobNotFound) {
			return &apiv1.ExecuteResponse{Accepted: false, Message: "job not found"}, nil
		}
		return &apiv1.ExecuteResponse{Accepted: false, Message: "job lookup failed"}, nil
	}
	// The request is an announcement, not authority to substitute current
	// attempt identity. Canonical payload/name/timeout still come from storage.
	job.AttemptCount, job.FencingToken = int(req.Job.AttemptCount), req.Job.FencingToken
	if !s.worker.StartExecution(traceOnlyContext(ctx), job) {
		return &apiv1.ExecuteResponse{Accepted: false, Message: "worker is draining or at capacity"}, nil
	}
	return &apiv1.ExecuteResponse{Accepted: true, Message: "accepted"}, nil
}

func (s *WorkerServer) Heartbeat(ctx context.Context, req *apiv1.HeartbeatRequest) (*apiv1.HeartbeatResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return &apiv1.HeartbeatResponse{Renewed: false, AbortRequested: true, Message: "invalid job id"}, nil
	}
	renewed, abort := s.worker.Heartbeat(ctx, id, req.FencingToken)
	return &apiv1.HeartbeatResponse{Renewed: renewed, AbortRequested: abort}, nil
}

func (s *WorkerServer) GetWorkerStatus(ctx context.Context, _ *apiv1.GetWorkerStatusRequest) (*apiv1.GetWorkerStatusResponse, error) {
	active := s.worker.ActiveJobs()
	infos := make([]*apiv1.ActiveJobInfo, 0, len(active))
	for _, a := range active {
		// Fencing tokens stay on the lease path; status must not hand them out.
		infos = append(infos, &apiv1.ActiveJobInfo{
			JobId: a.JobID.String(), TenantId: a.TenantID.String(), Name: a.Name,
			StartedAt: timestamppb.New(a.StartedAt),
		})
	}
	s.worker.mu.RLock()
	handlers := make([]string, 0, len(s.worker.handlers))
	for k := range s.worker.handlers {
		handlers = append(handlers, k)
	}
	sort.Strings(handlers)
	draining := s.worker.draining
	uptime := int64(time.Since(s.worker.startedAt).Seconds())
	s.worker.mu.RUnlock()
	return &apiv1.GetWorkerStatusResponse{
		WorkerId: s.worker.id, ActiveJobsCount: int32(len(active)),
		MaxConcurrency: int32(s.worker.MaxConcurrency()), RegisteredHandlers: handlers,
		UptimeSeconds: uptime,
		IsDraining:    draining, ActiveJobs: infos,
	}, nil
}

func (s *WorkerServer) CancelExecution(ctx context.Context, req *apiv1.CancelExecutionRequest) (*apiv1.CancelExecutionResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return &apiv1.CancelExecutionResponse{Cancelled: false, Message: "invalid job id"}, nil
	}
	ok := s.worker.CancelExecution(ctx, id, req.FencingToken)
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &apiv1.CancelExecutionResponse{Cancelled: ok}, nil
}

func (s *WorkerServer) DrainWorker(ctx context.Context, req *apiv1.DrainWorkerRequest) (*apiv1.DrainWorkerResponse, error) {
	if req.TimeoutSeconds < 0 {
		return nil, status.Error(codes.InvalidArgument, "timeout_seconds must be nonnegative")
	}
	timeout := 30 * time.Second
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := s.worker.Wait(ctx)
	active := s.worker.ActiveJobs()
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &apiv1.DrainWorkerResponse{Acknowledged: true, RemainingActiveJobs: int32(len(active))}, nil
}
