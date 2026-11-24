package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
	"github.com/parsarazor/distributed-job-scheduler/internal/repository"
)

// DBTX abstracts pgxpool.Pool and pgx.Tx for transaction-aware queries.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txKey struct{}

// Pool wraps pgxpool and acts as Transactor with context-propagated tx.
type Pool struct {
	pool *pgxpool.Pool
}

// New creates a Pool from a Postgres DSN.
func New(ctx context.Context, dsn string) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return NewWithConfig(ctx, cfg)
}

// NewWithConfig opens a pool using the validated connection settings.
func NewWithConfig(ctx context.Context, cfg *pgxpool.Config) (*Pool, error) {
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Pool{pool: p}, nil
}

// Close releases connections.
func (p *Pool) Close() { p.pool.Close() }

// Ping verifies connectivity.
func (p *Pool) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

const SchemaVersion = 22

// CheckSchema rejects missing, partially applied or incompatible migrations.
func (p *Pool) CheckSchema(ctx context.Context) error {
	var version int
	var dirty bool
	err := p.pool.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty)
	if err != nil {
		return fmt.Errorf("read schema migration version: %w", err)
	}
	if version != SchemaVersion || dirty {
		return fmt.Errorf("database schema must be at clean version %d (found version %d, dirty=%t)", SchemaVersion, version, dirty)
	}
	return nil
}

func dbFromCtx(ctx context.Context, p *Pool) DBTX {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return p.pool
}

// WithinTransaction runs fn inside a Postgres transaction.
func (p *Pool) WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Repositories bundles every Postgres repository sharing one pool.
type Repositories struct {
	Jobs    *JobRepo
	Tenants *TenantRepo
	Events  *EventRepo
	Outbox  *OutboxRepo
	Tx      *Pool

	WorkerCredentials *WorkerCredentialRepo
}

// NewRepositories builds all Postgres repositories over one pool.
func NewRepositories(p *Pool) *Repositories {
	return &Repositories{
		Jobs:    &JobRepo{db: p},
		Tenants: &TenantRepo{db: p},
		Events:  &EventRepo{db: p},
		Outbox:  &OutboxRepo{db: p},
		Tx:      p,

		WorkerCredentials: &WorkerCredentialRepo{db: p},
	}
}

// JobRepo implements repository.JobRepository on Postgres.
type JobRepo struct{ db *Pool }

// TenantRepo implements repository.TenantRepository on Postgres.
type TenantRepo struct{ db *Pool }

// EventRepo implements repository.JobEventRepository on Postgres.
type EventRepo struct{ db *Pool }

// OutboxRepo implements repository.OutboxRepository on Postgres.
type OutboxRepo struct{ db *Pool }

var (
	_ repository.JobRepository      = (*JobRepo)(nil)
	_ repository.TenantRepository   = (*TenantRepo)(nil)
	_ repository.JobEventRepository = (*EventRepo)(nil)
	_ repository.OutboxRepository   = (*OutboxRepo)(nil)
	_ repository.Transactor         = (*Pool)(nil)
)

func scanJob(row pgx.Row, prefix ...any) (*domain.Job, error) {
	var j domain.Job
	var dependsOn *uuid.UUID
	var nextRetry *time.Time
	var payload []byte
	if err := row.Scan(append(prefix,
		&j.ID, &j.TenantID, &j.Name, &j.Status, &payload,
		&j.Priority, &j.AttemptCount, &j.MaxAttempts, &dependsOn,
		&j.FencingToken, &j.ScheduledAt, &nextRetry, &j.CreatedAt, &j.UpdatedAt,
		&j.LeaseOwner, &j.LeaseExpiresAt, &j.DispatchedAt, &j.SubmissionKey, &j.Timeout, &j.TraceParent, &j.TraceState, &j.SubmissionHash, &j.PayloadExpired, &j.LastLeaseOwner,
	)...); err != nil {
		return nil, err
	}
	j.Payload = payload
	j.DependsOn = dependsOn
	j.NextRetryAt = nextRetry
	return &j, nil
}

const jobColumnsTail = `, priority, attempt_count, max_attempts, depends_on, fencing_token, scheduled_at, next_retry_at, created_at, updated_at, lease_owner, lease_expires_at, dispatched_at, COALESCE(submission_key,''), timeout_ns, traceparent, tracestate, submission_hash, payload_expired, last_lease_owner`
const jobColumns = `id, tenant_id, name, status, payload` + jobColumnsTail
const jobMetadataColumns = `id, tenant_id, name, status, NULL::jsonb` + jobColumnsTail

func (r *JobRepo) Create(ctx context.Context, job *domain.Job) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `INSERT INTO jobs (id, tenant_id, name, status, payload, priority, attempt_count, max_attempts, depends_on, fencing_token, scheduled_at, next_retry_at, created_at, updated_at, lease_owner, lease_expires_at, dispatched_at, submission_key, timeout_ns, traceparent, tracestate, submission_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NULLIF($18,''),$19,$20,$21,$22)
		ON CONFLICT (id) DO NOTHING`,
		job.ID, job.TenantID, job.Name, string(job.Status), string(job.Payload),
		int(job.Priority), job.AttemptCount, job.MaxAttempts, job.DependsOn,
		job.FencingToken, job.ScheduledAt, job.NextRetryAt, job.CreatedAt, job.UpdatedAt,
		job.LeaseOwner, job.LeaseExpiresAt, job.DispatchedAt, job.SubmissionKey, int64(job.Timeout), job.TraceParent, job.TraceState, job.SubmissionHash)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.ConstraintName {
		case "idx_jobs_submission_key":
			return domain.ErrIdempotencyConflict
		case "jobs_depends_on_fkey", "fk_jobs_dependency_tenant":
			return domain.ErrDependencyNotMet
		}
	}
	return err
}

func (r *JobRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	row := db.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, id)
	j, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrJobNotFound
		}
		return nil, err
	}
	return j, nil
}

func (r *JobRepo) GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	row := db.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1 FOR UPDATE`, id)
	j, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrJobNotFound
		}
		return nil, err
	}
	return j, nil
}

func (r *JobRepo) lockJob(ctx context.Context, id uuid.UUID) error {
	var found uuid.UUID
	err := dbFromCtx(ctx, r.db).QueryRow(ctx, `SELECT id FROM jobs WHERE id=$1 FOR UPDATE`, id).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrJobNotFound
	}
	return err
}

func (r *JobRepo) ListByTenant(ctx context.Context, tenantID uuid.UUID, filter repository.JobListFilter) ([]*domain.Job, int64, error) {
	if filter.Offset < 0 || filter.Limit < 0 {
		return nil, 0, domain.ErrInvalidInput
	}
	db := dbFromCtx(ctx, r.db)
	where := `WHERE tenant_id=$1`
	args := []any{tenantID}
	idx := 2
	if filter.Status != nil {
		where += ` AND status=$` + itoa(idx)
		args = append(args, string(*filter.Status))
		idx++
	}
	if filter.Priority != nil {
		where += ` AND priority=$` + itoa(idx)
		args = append(args, int(*filter.Priority))
		idx++
	}
	innerColumns := jobColumns
	if filter.MetadataOnly {
		innerColumns = `id, tenant_id, name, status, NULL::jsonb AS payload` + jobColumnsTail
	}
	q := `SELECT total.n, page.* FROM (SELECT count(*) AS n FROM jobs ` + where + `) total
		LEFT JOIN LATERAL (SELECT ` + innerColumns + ` FROM jobs ` + where + ` ORDER BY created_at DESC, id`
	if filter.Limit > 0 {
		q += ` LIMIT $` + itoa(idx)
		args = append(args, filter.Limit)
		idx++
	}
	if filter.Offset > 0 {
		q += ` OFFSET $` + itoa(idx)
		args = append(args, filter.Offset)
	}
	rows, err := db.Query(ctx, q+`) page ON true ORDER BY created_at DESC, id`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []*domain.Job
	var total int64
	for rows.Next() {
		if rows.RawValues()[1] == nil {
			// The left join still returns the total when the requested page is empty.
			targets := make([]any, len(rows.RawValues()))
			targets[0] = &total
			if err := rows.Scan(targets...); err != nil {
				return nil, 0, err
			}
			continue
		}
		j, err := scanJob(rows, &total)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, j)
	}
	return out, total, rows.Err()
}

func (r *JobRepo) Cancel(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	// Strictly only pending jobs may be cancelled (matches CanCancel).
	j, err := scanJob(db.QueryRow(ctx, `UPDATE jobs SET status='cancelled', lease_owner=NULL, lease_expires_at=NULL,
		next_retry_at=NULL, dispatched_at=NULL WHERE id=$1 AND status='pending' RETURNING `+jobColumns, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Distinguish not-found from wrong-status.
			if _, gerr := r.GetByID(ctx, id); gerr != nil {
				return nil, gerr
			}
			return nil, domain.ErrJobCannotBeCancelled
		}
		return nil, err
	}
	return j, nil
}

func (r *JobRepo) TransitionStatus(ctx context.Context, id uuid.UUID, from, to domain.JobStatus, fencingToken int64) error {
	if to == domain.StatusRunning {
		return domain.ErrInvalidTransition // Running requires an atomic claim and lease.
	}
	db := dbFromCtx(ctx, r.db)
	if err := domain.ValidateTransition(from, to); err != nil {
		return err
	}
	tag, err := db.Exec(ctx, `UPDATE jobs SET status=$1::text,
		dispatched_at=CASE WHEN $1::text='pending' THEN NULL ELSE dispatched_at END,
		next_retry_at=CASE WHEN $1::text IN ('dead','cancelled') THEN NULL ELSE next_retry_at END,
		lease_owner=NULL, lease_expires_at=NULL
		WHERE id=$2 AND status=$3::text AND fencing_token=$4
		AND status<>'running'`, string(to), id, string(from), fencingToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		j, gerr := r.GetByID(ctx, id)
		if gerr != nil {
			return gerr
		}
		if j.FencingToken != fencingToken {
			return domain.ErrFencingTokenStale
		}
		if j.Status == from && from == domain.StatusRunning {
			return domain.ErrLeaseLost
		}
		return domain.ErrInvalidTransition
	}
	return nil
}

func (r *JobRepo) SetNextRetryAt(ctx context.Context, id uuid.UUID, token int64, next time.Time) error {
	tag, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE jobs SET next_retry_at=$3
		WHERE id=$1 AND fencing_token=$2 AND status='failed'`, id, token, next)
	if err == nil && tag.RowsAffected() != 1 {
		return domain.ErrInvalidTransition
	}
	return err
}

func (r *JobRepo) UpdateWithFencingToken(ctx context.Context, job *domain.Job, expectedToken int64) error {
	if job.FencingToken != expectedToken {
		return domain.ErrFencingTokenStale
	}
	db := dbFromCtx(ctx, r.db)
	tag, err := db.Exec(ctx, `UPDATE jobs
		SET payload=$1, attempt_count=$2, max_attempts=$3, next_retry_at=$4
		WHERE id=$5 AND fencing_token=$6 AND status=$7
		AND status<>'running'`,
		string(job.Payload), job.AttemptCount, job.MaxAttempts,
		job.NextRetryAt, job.ID, expectedToken, string(job.Status))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		stored, gerr := r.GetByID(ctx, job.ID)
		if gerr != nil {
			return gerr
		}
		if stored.FencingToken != expectedToken {
			return domain.ErrFencingTokenStale
		}
		if stored.Status == job.Status && job.Status == domain.StatusRunning {
			return domain.ErrLeaseLost
		}
		return domain.ErrInvalidTransition
	}
	return nil
}

func (r *JobRepo) GetReadyJobs(ctx context.Context, limit int) ([]*domain.Job, error) {
	return r.readyJobs(ctx, limit, false)
}

func (r *JobRepo) GetUndispatchedJobsForUpdate(ctx context.Context, limit int) ([]*domain.Job, error) {
	return r.fairReadyJobs(ctx, limit)
}

func (r *JobRepo) readyJobs(ctx context.Context, limit int, undispatched bool) ([]*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT ` + jobColumns + ` FROM jobs
		WHERE status='pending' AND attempt_count < max_attempts AND scheduled_at <= NOW()
		AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		AND (depends_on IS NULL OR EXISTS (SELECT 1 FROM jobs p WHERE p.id=jobs.depends_on AND p.tenant_id=jobs.tenant_id AND p.status='succeeded'))`
	if undispatched {
		query += ` AND dispatched_at IS NULL`
	}
	query += ` ORDER BY priority DESC, scheduled_at ASC, id LIMIT $1`
	if undispatched {
		query += ` FOR UPDATE SKIP LOCKED`
	}
	rows, err := db.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (r *JobRepo) MarkDispatched(ctx context.Context, id uuid.UUID, token int64) error {
	tag, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE jobs SET dispatched_at=clock_timestamp()
		WHERE id=$1 AND status='pending' AND fencing_token=$2 AND dispatched_at IS NULL`, id, token)
	if err == nil && tag.RowsAffected() != 1 {
		return domain.ErrJobNotClaimable
	}
	if err != nil {
		return err
	}
	_, err = dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE tenants SET last_dispatched_at=clock_timestamp() WHERE id=(SELECT tenant_id FROM jobs WHERE id=$1)`, id)
	return err
}

func (r *JobRepo) GetPendingRetryJobs(ctx context.Context, limit int) ([]*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	if limit <= 0 {
		limit = 100
	}
	// Split the OR-heavy predicate into UNION arms so each side can use its
	// own index (idx_jobs_retry for due retries; the partial index also covers
	// the IS NULL arm). A shared OR forces a sequential scan of every failed
	// job on every scheduler tick. IDs are deduplicated in the CTE, then the
	// final join preserves the previous (next_retry_at NULLS FIRST, id)
	// ordering and limit semantics under row locks.
	rows, err := db.Query(ctx, `WITH due AS (
		SELECT id FROM jobs WHERE status='failed' AND attempt_count >= max_attempts
		UNION
		SELECT id FROM jobs WHERE status='failed' AND next_retry_at IS NULL
		UNION
		SELECT id FROM jobs WHERE status='failed' AND next_retry_at <= statement_timestamp()
	)
	SELECT `+jobMetadataColumns+` FROM jobs JOIN due USING (id)
	ORDER BY jobs.next_retry_at ASC NULLS FIRST, jobs.id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (r *JobRepo) GetDependentJobs(ctx context.Context, parentJobID uuid.UUID) ([]*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	rows, err := db.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE depends_on=$1
		AND tenant_id=(SELECT tenant_id FROM jobs WHERE id=$1) ORDER BY id`, parentJobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (r *JobRepo) ClaimJob(ctx context.Context, id uuid.UUID, workerID string, ttl time.Duration) (int64, error) {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); !ok {
		var token int64
		err := r.db.WithinTransaction(ctx, func(txctx context.Context) error {
			var err error
			token, err = r.ClaimJob(txctx, id, workerID, ttl)
			return err
		})
		return token, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if workerID == "" || ttl <= 0 {
		return 0, domain.ErrInvalidLease
	}
	// Calculate the lease in a new statement after any row-lock wait.
	if err := r.lockJob(ctx, id); err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			return 0, domain.ErrJobNotClaimable
		}
		return 0, err
	}
	var token int64
	err := dbFromCtx(ctx, r.db).QueryRow(ctx, `UPDATE jobs
		SET status='running', fencing_token=fencing_token+1,
		lease_owner=$2, last_lease_owner=$2, lease_expires_at=clock_timestamp()+make_interval(secs => $3)
		WHERE id=$1 AND status='pending' AND scheduled_at <= clock_timestamp()
		AND (next_retry_at IS NULL OR next_retry_at <= clock_timestamp())
		AND attempt_count < max_attempts
		AND (depends_on IS NULL OR EXISTS (SELECT 1 FROM jobs p WHERE p.id=jobs.depends_on AND p.tenant_id=jobs.tenant_id AND p.status='succeeded'))
		RETURNING fencing_token`, id, workerID, ttl.Seconds()).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, domain.ErrJobNotClaimable
	}
	return token, err
}

func (r *JobRepo) RenewLease(ctx context.Context, id uuid.UUID, workerID string, token int64, ttl time.Duration) error {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); !ok {
		return r.db.WithinTransaction(ctx, func(txctx context.Context) error { return r.RenewLease(txctx, id, workerID, token, ttl) })
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if workerID == "" || ttl <= 0 {
		return domain.ErrInvalidLease
	}
	if err := r.lockJob(ctx, id); err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			return domain.ErrLeaseLost
		}
		return err
	}
	tag, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE jobs
		SET lease_expires_at=clock_timestamp()+make_interval(secs => $4)
		WHERE id=$1 AND status='running' AND fencing_token=$2 AND lease_owner=$3
		AND lease_expires_at > clock_timestamp()`, id, token, workerID, ttl.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrLeaseLost
	}
	return nil
}

func (r *JobRepo) CompleteLease(ctx context.Context, id uuid.UUID, workerID string, token int64, final domain.JobStatus) (*domain.Job, error) {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); !ok {
		var job *domain.Job
		err := r.db.WithinTransaction(ctx, func(txctx context.Context) error {
			var err error
			job, err = r.CompleteLease(txctx, id, workerID, token, final)
			return err
		})
		return job, err
	}
	if final != domain.StatusSucceeded && final != domain.StatusFailed {
		return nil, domain.ErrInvalidTransition
	}
	// A fresh statement after lock acquisition evaluates expiry after any wait.
	if err := r.lockJob(ctx, id); err != nil {
		return nil, err
	}
	j, err := scanJob(dbFromCtx(ctx, r.db).QueryRow(ctx, `UPDATE jobs SET status=$4::text,
		attempt_count=attempt_count+CASE WHEN $4::text='failed' THEN 1 ELSE 0 END,
		last_lease_owner=lease_owner, lease_owner=NULL, lease_expires_at=NULL
		WHERE id=$1 AND status='running' AND lease_owner=$2 AND fencing_token=$3
		AND lease_expires_at > clock_timestamp() RETURNING `+jobColumns, id, workerID, token, string(final)))
	if errors.Is(err, pgx.ErrNoRows) {
		stored, getErr := r.GetByID(ctx, id)
		if getErr != nil {
			return nil, getErr
		}
		if stored.FencingToken != token {
			return nil, domain.ErrFencingTokenStale
		}
		if stored.Status != domain.StatusRunning {
			return nil, domain.ErrInvalidTransition
		}
		return nil, domain.ErrLeaseLost
	}
	return j, err
}

func (r *JobRepo) GetExpiredLeasesForUpdate(ctx context.Context, limit int) ([]*domain.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := dbFromCtx(ctx, r.db).Query(ctx, `SELECT `+jobMetadataColumns+` FROM jobs
		WHERE status='running' AND lease_expires_at <= statement_timestamp()
		ORDER BY lease_expires_at ASC, id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (r *JobRepo) ExpireLease(ctx context.Context, id uuid.UUID, token int64) (*domain.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	j, err := scanJob(dbFromCtx(ctx, r.db).QueryRow(ctx, `UPDATE jobs
		SET status='failed', fencing_token=fencing_token+1, attempt_count=attempt_count+1,
		last_lease_owner=COALESCE(lease_owner, last_lease_owner), lease_owner=NULL, lease_expires_at=NULL
		WHERE id=$1 AND status='running' AND fencing_token=$2 AND lease_expires_at <= clock_timestamp()
		RETURNING `+jobColumns, id, token))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrLeaseLost
	}
	return j, err
}

func (r *JobRepo) ExtendRunningLeases(ctx context.Context, grace time.Duration) (int, error) {
	tag, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE jobs
		SET lease_expires_at=clock_timestamp()+make_interval(secs => $1)
		WHERE status='running' AND lease_expires_at < clock_timestamp()+make_interval(secs => $1)`, grace.Seconds())
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// --- Tenants ---

func (r *TenantRepo) Create(ctx context.Context, tenant *domain.Tenant) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `INSERT INTO tenants (id, name, api_key_hash, created_at, max_in_flight, disabled_at)
		VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO NOTHING`,
		tenant.ID, tenant.Name, tenant.APIKeyHash, tenant.CreatedAt, tenant.InFlightLimit(), tenant.DisabledAt)
	return err
}

const tenantColumns = `id, name, api_key_hash, created_at, max_in_flight, disabled_at`

func (r *TenantRepo) List(ctx context.Context, limit, offset int) ([]*domain.Tenant, error) {
	rows, err := dbFromCtx(ctx, r.db).Query(ctx, `SELECT `+tenantColumns+` FROM tenants ORDER BY created_at, id LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Tenant
	for rows.Next() {
		var t domain.Tenant
		if err := rows.Scan(&t.ID, &t.Name, &t.APIKeyHash, &t.CreatedAt, &t.MaxInFlight, &t.DisabledAt); err != nil {
			return nil, err
		}
		out = append(out, &t)
	}
	return out, rows.Err()
}

func (r *TenantRepo) Update(ctx context.Context, tenant *domain.Tenant) error {
	tag, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE tenants SET api_key_hash=$2, max_in_flight=$3, disabled_at=$4 WHERE id=$1`,
		tenant.ID, tenant.APIKeyHash, tenant.InFlightLimit(), tenant.DisabledAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrTenantNotFound
	}
	return nil
}

func (r *TenantRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error) {
	db := dbFromCtx(ctx, r.db)
	var t domain.Tenant
	err := db.QueryRow(ctx, `SELECT `+tenantColumns+` FROM tenants WHERE id=$1`, id).
		Scan(&t.ID, &t.Name, &t.APIKeyHash, &t.CreatedAt, &t.MaxInFlight, &t.DisabledAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrTenantNotFound
		}
		return nil, err
	}
	return &t, nil
}

func (r *TenantRepo) GetByAPIKeyHash(ctx context.Context, apiKeyHash string) (*domain.Tenant, error) {
	db := dbFromCtx(ctx, r.db)
	var t domain.Tenant
	err := db.QueryRow(ctx, `SELECT `+tenantColumns+` FROM tenants WHERE api_key_hash=$1`, apiKeyHash).
		Scan(&t.ID, &t.Name, &t.APIKeyHash, &t.CreatedAt, &t.MaxInFlight, &t.DisabledAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrTenantNotFound
		}
		return nil, err
	}
	return &t, nil
}

// --- Events ---

func (r *EventRepo) Record(ctx context.Context, event *domain.JobEvent) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `INSERT INTO job_events (id, job_id, from_status, to_status, occurred_at, worker_id, error_message)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		event.ID, event.JobID, string(event.FromStatus), string(event.ToStatus),
		event.OccurredAt, event.WorkerID, event.ErrorMessage)
	return err
}

func (r *EventRepo) ListByJobID(ctx context.Context, jobID uuid.UUID) ([]*domain.JobEvent, error) {
	return r.listEvents(ctx, jobID, 0, 0)
}

func (r *EventRepo) ListByJobIDPage(ctx context.Context, jobID uuid.UUID, limit, offset int) ([]*domain.JobEvent, error) {
	if limit <= 0 || limit > 1001 || offset < 0 {
		return nil, domain.ErrInvalidInput
	}
	return r.listEvents(ctx, jobID, limit, offset)
}

func (r *EventRepo) listEvents(ctx context.Context, jobID uuid.UUID, limit, offset int) ([]*domain.JobEvent, error) {
	db := dbFromCtx(ctx, r.db)
	q := `SELECT id, job_id, from_status, to_status, occurred_at, worker_id, error_message
		FROM job_events WHERE job_id=$1 ORDER BY occurred_at ASC, id`
	args := []any{jobID}
	if limit > 0 {
		q = `SELECT id, job_id, from_status, to_status, occurred_at,
			NULLIF(left(regexp_replace(worker_id, '[[:cntrl:]]', '', 'g'), 128), ''),
			CASE WHEN error_message=ANY($4::text[]) THEN error_message END
			FROM job_events WHERE job_id=$1 ORDER BY occurred_at ASC, id LIMIT $2 OFFSET $3`
		args = append(args, limit, offset, domain.SafeHandlerMessages())
	}
	rows, err := db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.JobEvent
	for rows.Next() {
		var e domain.JobEvent
		if err := rows.Scan(&e.ID, &e.JobID, &e.FromStatus, &e.ToStatus, &e.OccurredAt, &e.WorkerID, &e.ErrorMessage); err != nil {
			return nil, err
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}

// --- Outbox ---

func (r *OutboxRepo) Store(ctx context.Context, event *domain.OutboxEvent) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `INSERT INTO job_outbox (id, job_id, routing_key, payload, published, dead_lettered, retry_count, last_error, created_at, published_at, next_publish_at, delivered_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (id) DO NOTHING`,
		event.ID, event.JobID, event.RoutingKey, string(event.Payload),
		event.Published, event.DeadLettered, event.RetryCount, event.LastError, event.CreatedAt, event.PublishedAt, event.NextPublishAt, event.DeliveredAt)
	return err
}

func (r *OutboxRepo) FetchUnpublished(ctx context.Context, limit int, olderThan time.Duration) ([]*domain.OutboxEvent, error) {
	db := dbFromCtx(ctx, r.db)
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.Query(ctx, `SELECT id, job_id, routing_key, payload, published, dead_lettered, publishing_since, retry_count, last_error, created_at, published_at, next_publish_at, delivered_at
		FROM job_outbox
		WHERE NOT published AND created_at <= statement_timestamp()-make_interval(secs => $1)
		ORDER BY created_at ASC, id LIMIT $2`, olderThan.Seconds(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.OutboxEvent
	for rows.Next() {
		e, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// FetchPublishable claims one due outbox row for publication. The claim is the
// only thing this call does inside the caller's transaction: it stamps
// publishing_since and commits. The RabbitMQ publish itself must happen after
// the transaction ends so no row lock or DB transaction is held across the
// broker round trip. Rows whose claim is older than the publishing lease are
// skipped here and reclaimed by ReclaimStalePublishing.
func (r *OutboxRepo) FetchPublishable(ctx context.Context, olderThan time.Duration, exclude []uuid.UUID) (*domain.OutboxEvent, error) {
	event, err := scanOutbox(dbFromCtx(ctx, r.db).QueryRow(ctx, `UPDATE job_outbox SET publishing_since=clock_timestamp()
		WHERE id = (
			SELECT id FROM job_outbox
			WHERE NOT published AND NOT dead_lettered
			AND created_at <= statement_timestamp()-make_interval(secs => $1)
			AND (next_publish_at IS NULL OR next_publish_at <= statement_timestamp())
			AND (publishing_since IS NULL OR publishing_since <= clock_timestamp()-make_interval(secs => $3))
			AND NOT (id=ANY(COALESCE($2::uuid[], ARRAY[]::uuid[])))
			ORDER BY created_at, id LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, job_id, routing_key, payload, published, dead_lettered, publishing_since, retry_count, last_error, created_at, published_at, next_publish_at, delivered_at`,
		olderThan.Seconds(), exclude, int64(domain.PublishingLease/time.Second)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return event, err
}

// ReclaimStalePublishing releases claims abandoned by a crashed publisher so
// those rows become eligible again.
func (r *OutboxRepo) ReclaimStalePublishing(ctx context.Context) (int64, error) {
	tag, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE job_outbox SET publishing_since=NULL
		WHERE NOT published AND NOT dead_lettered AND publishing_since <= clock_timestamp()-make_interval(secs => $1)`,
		int64(domain.PublishingLease/time.Second))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func scanOutbox(row pgx.Row) (*domain.OutboxEvent, error) {
	var e domain.OutboxEvent
	var payload []byte
	if err := row.Scan(&e.ID, &e.JobID, &e.RoutingKey, &payload, &e.Published, &e.DeadLettered, &e.PublishingSince, &e.RetryCount, &e.LastError, &e.CreatedAt, &e.PublishedAt, &e.NextPublishAt, &e.DeliveredAt); err != nil {
		return nil, err
	}
	e.Payload = payload
	return &e, nil
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, id uuid.UUID) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `UPDATE job_outbox SET published=TRUE, published_at=clock_timestamp(), publishing_since=NULL, next_publish_at=NULL WHERE id=$1`, id)
	return err
}

// MarkFailed records a publish failure, schedules the retry with jittered
// backoff and dead-letters the row once the retry ceiling is reached so it
// stops consuming publisher passes.
func (r *OutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `UPDATE job_outbox SET retry_count=retry_count+1, last_error=$2, publishing_since=NULL,
		dead_lettered = (retry_count+1 >= $3),
		next_publish_at = CASE WHEN retry_count+1 >= $3 THEN NULL
			ELSE clock_timestamp()+make_interval(secs => (1 << least(retry_count, 8))::double precision) END
		WHERE id=$1 AND NOT published`, id, lastErr, domain.MaxOutboxRetries)
	return err
}

func (r *OutboxRepo) MarkUnroutable(ctx context.Context, id uuid.UUID, reason string) error {
	_, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE job_outbox SET published=TRUE, published_at=clock_timestamp(),
		delivered_at=clock_timestamp(), publishing_since=NULL, next_publish_at=NULL, last_error=$2
		WHERE id=$1 AND NOT published`, id, reason)
	return err
}

func (r *OutboxRepo) MarkDelivered(ctx context.Context, id uuid.UUID) error {
	_, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE job_outbox SET delivered_at=clock_timestamp() WHERE id=$1 AND delivered_at IS NULL`, id)
	return err
}

func (r *OutboxRepo) RetireUnpublished(ctx context.Context, jobID uuid.UUID) error {
	_, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE job_outbox
	SET published=TRUE, published_at=clock_timestamp(),
		delivered_at=CASE WHEN retry_count=0 THEN clock_timestamp() ELSE delivered_at END, next_publish_at=NULL
 WHERE job_id=$1 AND NOT published`, jobID)
	return err
}

func (r *OutboxRepo) DeletePublished(ctx context.Context, olderThan time.Duration) (int64, error) {
	db := dbFromCtx(ctx, r.db)
	tag, err := db.Exec(ctx, `DELETE FROM job_outbox WHERE published=TRUE AND published_at IS NOT NULL AND published_at < clock_timestamp()-make_interval(secs => $1)`, olderThan.Seconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func itoa(i int) string {
	const digits = "0123456789"
	if i < 10 {
		return digits[i : i+1]
	}
	s := ""
	for i > 0 {
		s = string(digits[i%10]) + s
		i /= 10
	}
	return s
}
