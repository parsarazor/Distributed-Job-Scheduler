package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

// JobListFilter defines query parameters and pagination options for listing jobs.
type JobListFilter struct {
	Status   *domain.JobStatus
	Priority *domain.JobPriority
	Limit    int
	Offset   int
}

// JobRepository defines the abstract data access contract for jobs in PostgreSQL.
// All implementations must support participating in transactions initiated by Transactor.
type JobRepository interface {
	// Create inserts a new job into the database.
	Create(ctx context.Context, job *domain.Job) error

	// GetByID retrieves a job by its unique UUID. Returns domain.ErrJobNotFound if absent.
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Job, error)

	// GetByIDForUpdate retrieves a job and locks its row (SELECT ... FOR UPDATE).
	GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*domain.Job, error)

	// ListByTenant lists paginated jobs belonging to a tenant according to the filter.
	// Returns slice of jobs and the total matching count.
	ListByTenant(ctx context.Context, tenantID uuid.UUID, filter JobListFilter) ([]*domain.Job, int64, error)

	// Cancel transitions a job to StatusCancelled, strictly checking that current status is pending.
	// Returns domain.ErrJobCannotBeCancelled if current status is not pending.
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Job, error)

	// TransitionStatus requires exact token equality and clears leases on exit from running.
	// Returns domain.ErrFencingTokenStale if the fencing token validation fails.
	TransitionStatus(ctx context.Context, id uuid.UUID, from domain.JobStatus, to domain.JobStatus, fencingToken int64) error

	// UpdateWithFencingToken updates payload, attempts and retry scheduling with exact fencing.
	// Status and token must match; only ClaimJob and ExpireLease may mint tokens.
	UpdateWithFencingToken(ctx context.Context, job *domain.Job, expectedToken int64) error

	// GetReadyJobs fetches jobs in 'pending' status whose scheduled_at <= NOW(),
	// with no dependency or whose dependency is already in 'succeeded' status, ordered by priority DESC, scheduled_at ASC.
	GetReadyJobs(ctx context.Context, limit int) ([]*domain.Job, error)

	// GetPendingRetryJobs fetches failed jobs whose next_retry_at <= NOW() and attempt_count < max_attempts.
	GetPendingRetryJobs(ctx context.Context, limit int) ([]*domain.Job, error)

	// GetDependentJobs retrieves all jobs that have depends_on matching parentJobID.
	GetDependentJobs(ctx context.Context, parentJobID uuid.UUID) ([]*domain.Job, error)

	// ClaimJob atomically claims a ready pending job and mints its execution token once.
	// Missing, non-pending and not-yet-ready jobs return ErrJobNotClaimable.
	ClaimJob(ctx context.Context, id uuid.UUID, workerID string, ttl time.Duration) (int64, error)

	// RenewLease requires a live running lease with the exact owner and token.
	// Zero matching rows return ErrLeaseLost; expired leases cannot be revived.
	RenewLease(ctx context.Context, id uuid.UUID, workerID string, token int64, ttl time.Duration) error

	// GetExpiredLeasesForUpdate locks expired running jobs until the transaction ends.
	// Postgres uses FOR UPDATE SKIP LOCKED. Call within WithinTransaction.
	GetExpiredLeasesForUpdate(ctx context.Context, limit int) ([]*domain.Job, error)

	// ExpireLease invalidates an expired execution, bumps its token, increments attempts,
	// clears the lease and transitions running -> failed. Call in the reaper transaction.
	ExpireLease(ctx context.Context, id uuid.UUID, token int64) (*domain.Job, error)
}
