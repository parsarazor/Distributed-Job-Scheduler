package repository

import (
	"context"

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

	// TransitionStatus transitions a job from an expected status to a target status,
	// asserting that the provided fencingToken matches or exceeds the stored token.
	// Returns domain.ErrFencingTokenStale if the fencing token validation fails.
	TransitionStatus(ctx context.Context, id uuid.UUID, from domain.JobStatus, to domain.JobStatus, fencingToken int64) error

	// UpdateWithFencingToken updates job state (e.g. attempt_count, payload, next_retry_at, status)
	// guarded by the fencing token. If the stored token > expectedToken, domain.ErrFencingTokenStale is returned.
	UpdateWithFencingToken(ctx context.Context, job *domain.Job, expectedToken int64) error

	// GetReadyJobs fetches jobs in 'pending' status whose scheduled_at <= NOW(),
	// with no dependency or whose dependency is already in 'succeeded' status, ordered by priority DESC, scheduled_at ASC.
	GetReadyJobs(ctx context.Context, limit int) ([]*domain.Job, error)

	// GetPendingRetryJobs fetches failed jobs whose next_retry_at <= NOW() and attempt_count < max_attempts.
	GetPendingRetryJobs(ctx context.Context, limit int) ([]*domain.Job, error)

	// GetDependentJobs retrieves all jobs that have depends_on matching parentJobID.
	GetDependentJobs(ctx context.Context, parentJobID uuid.UUID) ([]*domain.Job, error)

	// IncrementFencingToken atomically increments and returns the new fencing token for a job.
	IncrementFencingToken(ctx context.Context, id uuid.UUID) (int64, error)
}
