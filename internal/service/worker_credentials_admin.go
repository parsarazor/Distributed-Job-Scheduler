package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	workerTokenPrefix = "djw_"
	maxWorkerIDLen    = 255
)

// SetWorkerCredentials enables per-worker credential administration.
func (s *Scheduler) SetWorkerCredentials(repo repository.WorkerCredentialRepository) {
	s.workerCreds = repo
}

func (s *Scheduler) workerCredentials() (repository.WorkerCredentialRepository, error) {
	if s.workerCreds == nil {
		return nil, status.Error(codes.Unimplemented, "worker credentials are not configured")
	}
	return s.workerCreds, nil
}

// CreateWorkerCredential issues a token bound to workerID and returns it once.
func (s *Scheduler) CreateWorkerCredential(ctx context.Context, workerID string) (*domain.WorkerCredential, string, error) {
	repo, err := s.workerCredentials()
	if err != nil {
		return nil, "", err
	}
	if !domain.ValidWorkerID(workerID) {
		return nil, "", domain.ErrInvalidInput
	}
	token, err := randomToken(workerTokenPrefix)
	if err != nil {
		return nil, "", err
	}
	cred := &domain.WorkerCredential{
		ID: uuid.New(), WorkerID: workerID, TokenHash: domain.HashAPIKey(token), CreatedAt: time.Now().UTC(),
	}
	if err := repo.Create(ctx, cred); err != nil {
		return nil, "", err
	}
	return cred, token, nil
}

func (s *Scheduler) ListWorkerCredentials(ctx context.Context, limit, offset int) ([]*domain.WorkerCredential, error) {
	repo, err := s.workerCredentials()
	if err != nil {
		return nil, err
	}
	if limit < 0 || limit > MaxTenantPageSize || offset < 0 {
		return nil, domain.ErrInvalidInput
	}
	if limit == 0 {
		limit = 100
	}
	return repo.List(ctx, limit, offset)
}

// RevokeWorkerCredential makes the token stop authenticating on every replica.
func (s *Scheduler) RevokeWorkerCredential(ctx context.Context, id uuid.UUID) (*domain.WorkerCredential, error) {
	repo, err := s.workerCredentials()
	if err != nil {
		return nil, err
	}
	return repo.Revoke(ctx, id, time.Now().UTC())
}

func workerCredentialToProto(c *domain.WorkerCredential) *apiv1.WorkerCredential {
	out := &apiv1.WorkerCredential{Id: c.ID.String(), WorkerId: c.WorkerID, CreatedAt: timestamppb.New(c.CreatedAt)}
	if c.RevokedAt != nil {
		out.RevokedAt = timestamppb.New(*c.RevokedAt)
	}
	return out
}

func (s *SchedulerServer) CreateWorkerCredential(ctx context.Context, req *apiv1.CreateWorkerCredentialRequest) (*apiv1.CreateWorkerCredentialResponse, error) {
	cred, token, err := s.sched.CreateWorkerCredential(ctx, req.WorkerId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.CreateWorkerCredentialResponse{Credential: workerCredentialToProto(cred), Token: token}, nil
}

func (s *SchedulerServer) ListWorkerCredentials(ctx context.Context, req *apiv1.ListWorkerCredentialsRequest) (*apiv1.ListWorkerCredentialsResponse, error) {
	creds, err := s.sched.ListWorkerCredentials(ctx, int(req.Limit), int(req.Offset))
	if err != nil {
		return nil, toGRPCError(err)
	}
	out := &apiv1.ListWorkerCredentialsResponse{Credentials: make([]*apiv1.WorkerCredential, 0, len(creds))}
	for _, c := range creds {
		out.Credentials = append(out.Credentials, workerCredentialToProto(c))
	}
	return out, nil
}

func (s *SchedulerServer) RevokeWorkerCredential(ctx context.Context, req *apiv1.RevokeWorkerCredentialRequest) (*apiv1.RevokeWorkerCredentialResponse, error) {
	id, err := uuid.Parse(req.CredentialId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid credential_id")
	}
	cred, err := s.sched.RevokeWorkerCredential(ctx, id)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &apiv1.RevokeWorkerCredentialResponse{Credential: workerCredentialToProto(cred)}, nil
}
