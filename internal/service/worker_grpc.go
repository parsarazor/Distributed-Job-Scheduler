package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
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
	if req.Job == nil {
		return &apiv1.ExecuteResponse{Accepted: false, Message: "nil job"}, nil
	}
	id, err := uuid.Parse(req.Job.Id)
	if err != nil {
		return &apiv1.ExecuteResponse{Accepted: false, Message: "invalid job id"}, nil
	}
	job, err := s.worker.sched.GetJob(ctx, id)
	if err != nil {
		return &apiv1.ExecuteResponse{Accepted: false, Message: err.Error()}, nil
	}
	if !s.worker.StartExecution(context.WithoutCancel(ctx), job) {
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
		infos = append(infos, &apiv1.ActiveJobInfo{
			JobId: a.JobID.String(), TenantId: a.TenantID.String(), Name: a.Name,
			StartedAt: timestamppb.New(a.StartedAt), FencingToken: a.FencingToken,
		})
	}
	s.worker.mu.RLock()
	handlers := make([]string, 0, len(s.worker.handlers))
	for k := range s.worker.handlers {
		handlers = append(handlers, k)
	}
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

func (s *WorkerServer) CancelExecution(_ context.Context, req *apiv1.CancelExecutionRequest) (*apiv1.CancelExecutionResponse, error) {
	id, err := uuid.Parse(req.JobId)
	if err != nil {
		return &apiv1.CancelExecutionResponse{Cancelled: false, Message: "invalid job id"}, nil
	}
	ok := s.worker.CancelExecution(id)
	return &apiv1.CancelExecutionResponse{Cancelled: ok}, nil
}

func (s *WorkerServer) DrainWorker(_ context.Context, _ *apiv1.DrainWorkerRequest) (*apiv1.DrainWorkerResponse, error) {
	s.worker.Drain()
	active := s.worker.ActiveJobs()
	return &apiv1.DrainWorkerResponse{Acknowledged: true, RemainingActiveJobs: int32(len(active))}, nil
}
