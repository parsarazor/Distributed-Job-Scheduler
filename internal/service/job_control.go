package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	apiv1 "github.com/prsamohamadi/distributed-job-scheduler/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// JobControl is a worker's only path to job state. The scheduler owns the
// database: in-process workers call it directly, deployed workers call it over
// gRPC through RemoteJobControl and need no database credentials.
type JobControl interface {
	// ClaimExecution records the announcement's delivery and claims its attempt.
	ClaimExecution(ctx context.Context, announcement *domain.Job, workerID string, ttl time.Duration) (*domain.Job, int64, error)
	RenewLease(ctx context.Context, jobID uuid.UUID, workerID string, token int64, ttl time.Duration) error
	ReportCompletion(ctx context.Context, jobID uuid.UUID, workerID string, final domain.JobStatus, token int64, message string) error
	// GetExecutionJob returns the stored job including its lease state. Remote
	// lookups omit the payload and fail with ErrLeaseLost unless token is the
	// job's current fencing token and any lease belongs to workerID.
	GetExecutionJob(ctx context.Context, jobID uuid.UUID, workerID string, token int64) (*domain.Job, error)
}

// ClaimExecution commits delivery receipt separately from the claim, so
// obsolete or canceled announcements still release their broker credit and no
// outbox lock spans a job lock.
func (s *Scheduler) ClaimExecution(ctx context.Context, announcement *domain.Job, workerID string, ttl time.Duration) (*domain.Job, int64, error) {
	if err := s.outbox.MarkDelivered(ctx, domain.DispatchID(announcement.ID, announcement.AttemptCount, announcement.FencingToken)); err != nil {
		return nil, 0, err
	}
	var job *domain.Job
	var token int64
	err := s.tx.WithinTransaction(ctx, func(ctx context.Context) error {
		stored, err := s.jobs.GetByIDForUpdate(ctx, announcement.ID)
		if err != nil {
			return err
		}
		if stored.FencingToken != announcement.FencingToken || stored.AttemptCount != announcement.AttemptCount {
			return domain.ErrJobNotClaimable
		}
		claimedAt := time.Now()
		token, err = s.jobs.ClaimJob(ctx, stored.ID, workerID, ttl)
		if err != nil {
			return err
		}
		job, err = s.jobs.GetByID(ctx, stored.ID)
		if err != nil {
			return err
		}
		if err := s.recordTransition(ctx, job.ID, domain.StatusPending, domain.StatusRunning, workerID, ""); err != nil {
			return err
		}
		if time.Since(claimedAt) >= ttl {
			return domain.ErrLeaseLost
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return job, token, nil
}

type schedulerControl struct{ s *Scheduler }

func (c schedulerControl) ClaimExecution(ctx context.Context, announcement *domain.Job, workerID string, ttl time.Duration) (*domain.Job, int64, error) {
	return c.s.ClaimExecution(ctx, announcement, workerID, ttl)
}

func (c schedulerControl) RenewLease(ctx context.Context, jobID uuid.UUID, workerID string, token int64, ttl time.Duration) error {
	return c.s.jobs.RenewLease(ctx, jobID, workerID, token, ttl)
}

func (c schedulerControl) ReportCompletion(ctx context.Context, jobID uuid.UUID, workerID string, final domain.JobStatus, token int64, message string) error {
	_, err := c.s.ReportCompletion(ctx, jobID, workerID, final, token, message)
	return err
}

func (c schedulerControl) GetExecutionJob(ctx context.Context, jobID uuid.UUID, _ string, _ int64) (*domain.Job, error) {
	return c.s.GetJob(ctx, jobID)
}

// RemoteJobControl reaches the scheduler's worker RPCs. The client connection
// must attach the worker bearer credential.
type RemoteJobControl struct{ client apiv1.SchedulerServiceClient }

// NewRemoteJobControl wraps a scheduler client for a database-free worker.
func NewRemoteJobControl(client apiv1.SchedulerServiceClient) *RemoteJobControl {
	return &RemoteJobControl{client: client}
}

func (c *RemoteJobControl) ClaimExecution(ctx context.Context, announcement *domain.Job, workerID string, ttl time.Duration) (*domain.Job, int64, error) {
	resp, err := c.client.ClaimJobExecution(ctx, &apiv1.ClaimJobExecutionRequest{
		JobId: announcement.ID.String(), AttemptCount: int32(announcement.AttemptCount),
		FencingToken: announcement.FencingToken, WorkerId: workerID, LeaseTtlMs: ttl.Milliseconds(),
	})
	if err != nil {
		return nil, 0, fromGRPCError(err)
	}
	job, err := protoToExecutionJob(resp.GetJob())
	if err != nil {
		return nil, 0, err
	}
	job.TraceParent, job.TraceState = announcement.TraceParent, announcement.TraceState
	return job, resp.GetFencingToken(), nil
}

func (c *RemoteJobControl) RenewLease(ctx context.Context, jobID uuid.UUID, workerID string, token int64, ttl time.Duration) error {
	_, err := c.client.RenewJobLease(ctx, &apiv1.RenewJobLeaseRequest{
		JobId: jobID.String(), WorkerId: workerID, FencingToken: token, LeaseTtlMs: ttl.Milliseconds(),
	})
	return fromGRPCError(err)
}

func (c *RemoteJobControl) ReportCompletion(ctx context.Context, jobID uuid.UUID, workerID string, final domain.JobStatus, token int64, message string) error {
	_, err := c.client.ReportJobCompletion(ctx, &apiv1.ReportJobCompletionRequest{
		JobId: jobID.String(), WorkerId: workerID, FinalStatus: DomainToProtoStatus(final),
		FencingToken: token, ErrorMessage: message,
	})
	return fromGRPCError(err)
}

func (c *RemoteJobControl) GetExecutionJob(ctx context.Context, jobID uuid.UUID, workerID string, token int64) (*domain.Job, error) {
	resp, err := c.client.GetExecutionJob(ctx, &apiv1.GetExecutionJobRequest{JobId: jobID.String(), WorkerId: workerID, FencingToken: token})
	if err != nil {
		return nil, fromGRPCError(err)
	}
	job, err := protoToExecutionJob(resp.GetJob())
	if err != nil {
		return nil, err
	}
	if resp.LeaseOwner != nil {
		owner := resp.GetLeaseOwner()
		job.LeaseOwner = &owner
	}
	if resp.LeaseExpiresAt != nil {
		expires := resp.GetLeaseExpiresAt().AsTime()
		job.LeaseExpiresAt = &expires
	}
	return job, nil
}

// fromGRPCError restores the domain sentinels the worker branches on.
func fromGRPCError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	var sentinel error
	switch st.Code() {
	case codes.Canceled:
		sentinel = context.Canceled
	case codes.DeadlineExceeded:
		sentinel = context.DeadlineExceeded
	case codes.NotFound:
		sentinel = domain.ErrJobNotFound
	case codes.FailedPrecondition:
		sentinel = domain.ErrJobNotClaimable
	case codes.Aborted:
		sentinel = domain.ErrLeaseLost
	case codes.InvalidArgument:
		sentinel = domain.ErrInvalidInput
	default:
		return err
	}
	return fmt.Errorf("%w: %s", sentinel, st.Message())
}

func protoToExecutionJob(p *apiv1.Job) (*domain.Job, error) {
	if p == nil {
		return nil, errors.New("scheduler returned no job")
	}
	id, err := uuid.Parse(p.Id)
	if err != nil {
		return nil, fmt.Errorf("scheduler returned invalid job id: %w", err)
	}
	tenant, err := uuid.Parse(p.TenantId)
	if err != nil {
		return nil, fmt.Errorf("scheduler returned invalid tenant id: %w", err)
	}
	job := &domain.Job{
		ID: id, TenantID: tenant, Name: p.Name, Status: ProtoToDomainStatus(p.Status),
		PayloadExpired: p.PayloadExpired, Priority: ProtoToDomainPriority(p.Priority),
		AttemptCount: int(p.AttemptCount), MaxAttempts: int(p.MaxAttempts), FencingToken: p.FencingToken,
		SubmissionKey: p.IdempotencyKey, Timeout: time.Duration(p.TimeoutMs) * time.Millisecond,
		ScheduledAt: p.ScheduledAt.AsTime(), CreatedAt: p.CreatedAt.AsTime(), UpdatedAt: p.UpdatedAt.AsTime(),
	}
	if len(p.Payload) > 0 {
		job.Payload = json.RawMessage(p.Payload)
	}
	if p.DependsOn != nil {
		parent, err := uuid.Parse(p.GetDependsOn())
		if err != nil {
			return nil, fmt.Errorf("scheduler returned invalid depends_on: %w", err)
		}
		job.DependsOn = &parent
	}
	if p.NextRetryAt != nil {
		next := p.NextRetryAt.AsTime()
		job.NextRetryAt = &next
	}
	return job, nil
}
