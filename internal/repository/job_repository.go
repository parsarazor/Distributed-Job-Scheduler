package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

// JobListFilter defines query parameters and pagination options for listing jobs.
type JobListFilter struct {
	Status       *domain.JobStatus
	Priority     *domain.JobPriority
	Limit        int
	Offset       int
	MetadataOnly bool
}

// JobRepository defines the abstract data access contract for jobs in PostgreSQL.
// All implementations must support participating in transactions initiated by Transactor.
type JobRepository interface {
	// Create inserts a new job into the database.
	Create(ctx context.Context, job *domain.Job) error
	GetBySubmissionKey(ctx context.Context, tenantID uuid.UUID, key string) (*domain.Job, error)
	// CheckAdmission runs under a tenant row lock held until commit.
	CheckAdmission(ctx context.Context, tenantID uuid.UUID, maxOutstanding, perMinute int) error
	// Now returns the storage clock. Read it after acquiring the admission lock.
	Now(ctx context.Context) (time.Time, error)
	// RequeueDead starts a fresh retry budget while preserving execution tokens and attempts.
	RequeueDead(ctx context.Context, id uuid.UUID) (*domain.Job, error)

	// GetByID retrieves a job by its unique UUID. Returns domain.ErrJobNotFound if absent.
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Job, error)

	// GetByIDForUpdate retrieves a job and locks its row (SELECT ... FOR UPDATE).
	GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*domain.Job, error)

	// GetDependency reads tenant-scoped parent metadata, optionally locking that parent's row.
	GetDependency(ctx context.Context, tenantID, id uuid.UUID, lock bool) (*domain.Job, error)

	// ListByTenant lists paginated jobs belonging to a tenant according to the filter.
	// Returns slice of jobs and the total matching count.
	ListByTenant(ctx context.Context, tenantID uuid.UUID, filter JobListFilter) ([]*domain.Job, int64, error)

	// Cancel transitions a job to StatusCancelled, strictly checking that current status is pending.
	// Returns domain.ErrJobCannotBeCancelled if current status is not pending.
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Job, error)
	// CancelDependents locks and cancels pending or failed descendants in the parent's tenant.
	// It returns the pre-cancellation state for audit.
	CancelDependents(ctx context.Context, parentID uuid.UUID) ([]*domain.Job, error)
	CountWaitingDependents(ctx context.Context, parentID uuid.UUID) (int, error)

	// TransitionStatus requires exact token equality and cannot enter or leave running.
	// Claim, completion and expiry require their dedicated lease operations.
	// Returns domain.ErrFencingTokenStale if the fencing token validation fails.
	TransitionStatus(ctx context.Context, id uuid.UUID, from domain.JobStatus, to domain.JobStatus, fencingToken int64) error

	// UpdateWithFencingToken updates non-running payload, attempts and retry scheduling with exact fencing.
	// Status and token must match; claims, expiry and explicit dead-job requeue mint tokens.
	UpdateWithFencingToken(ctx context.Context, job *domain.Job, expectedToken int64) error

	// SetNextRetryAt records the retry schedule of a locked failed job.
	SetNextRetryAt(ctx context.Context, id uuid.UUID, token int64, next time.Time) error

	// GetReadyJobs fetches jobs in 'pending' status whose scheduled_at <= NOW(),
	// with no dependency or whose dependency is already in 'succeeded' status, ordered by priority DESC, scheduled_at ASC.
	GetReadyJobs(ctx context.Context, limit int) ([]*domain.Job, error)

	// GetUndispatchedJobsForUpdate locks ready jobs without an outbox announcement.
	// Only announcement fields are populated.
	// Call within a transaction; Postgres uses FOR UPDATE SKIP LOCKED.
	GetUndispatchedJobsForUpdate(ctx context.Context, limit int) ([]*domain.Job, error)

	// MarkDispatched records that this pending attempt has an outbox announcement.
	MarkDispatched(ctx context.Context, id uuid.UUID, token int64) error
	// RecoverStaleDispatches clears old confirmed dispatches for unclaimed jobs and invalidates old announcements.
	RecoverStaleDispatches(ctx context.Context, limit int, age time.Duration) error

	// GetPendingRetryJobs locks legacy failed jobs that are due, unscheduled or exhausted.
	GetPendingRetryJobs(ctx context.Context, limit int) ([]*domain.Job, error)

	// GetDependentJobs retrieves all jobs that have depends_on matching parentJobID.
	GetDependentJobs(ctx context.Context, parentJobID uuid.UUID) ([]*domain.Job, error)

	// ClaimJob atomically claims a ready pending job and mints its execution token once.
	// Missing, non-pending and not-yet-ready jobs return ErrJobNotClaimable.
	ClaimJob(ctx context.Context, id uuid.UUID, workerID string, ttl time.Duration) (int64, error)

	// RenewLease requires a live running lease with the exact owner and token.
	// Zero matching rows return ErrLeaseLost; expired leases cannot be revived.
	RenewLease(ctx context.Context, id uuid.UUID, workerID string, token int64, ttl time.Duration) error

	// CompleteLease validates ownership, token and expiry using the storage clock,
	// transitions to succeeded/failed and counts a failed attempt atomically.
	CompleteLease(ctx context.Context, id uuid.UUID, workerID string, token int64, final domain.JobStatus) (*domain.Job, error)

	// GetExpiredLeasesForUpdate locks expired running jobs until the transaction ends.
	// Postgres uses FOR UPDATE SKIP LOCKED. Call within WithinTransaction.
	GetExpiredLeasesForUpdate(ctx context.Context, limit int) ([]*domain.Job, error)

	// ExpireLease invalidates an expired execution, bumps its token, increments attempts,
	// clears the lease and transitions running -> failed. Call in the reaper transaction.
	ExpireLease(ctx context.Context, id uuid.UUID, token int64) (*domain.Job, error)
}
