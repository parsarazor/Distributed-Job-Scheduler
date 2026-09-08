package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
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
}

// NewRepositories builds all Postgres repositories over one pool.
func NewRepositories(p *Pool) *Repositories {
	return &Repositories{
		Jobs:    &JobRepo{db: p},
		Tenants: &TenantRepo{db: p},
		Events:  &EventRepo{db: p},
		Outbox:  &OutboxRepo{db: p},
		Tx:      p,
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

func scanJob(row pgx.Row) (*domain.Job, error) {
	var j domain.Job
	var dependsOn *uuid.UUID
	var nextRetry *time.Time
	var payload []byte
	if err := row.Scan(
		&j.ID, &j.TenantID, &j.Name, &j.Status, &payload,
		&j.Priority, &j.AttemptCount, &j.MaxAttempts, &dependsOn,
		&j.FencingToken, &j.ScheduledAt, &nextRetry, &j.CreatedAt, &j.UpdatedAt,
	); err != nil {
		return nil, err
	}
	j.Payload = payload
	j.DependsOn = dependsOn
	j.NextRetryAt = nextRetry
	return &j, nil
}

const jobColumns = `id, tenant_id, name, status, payload, priority, attempt_count, max_attempts, depends_on, fencing_token, scheduled_at, next_retry_at, created_at, updated_at`

func (r *JobRepo) Create(ctx context.Context, job *domain.Job) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `INSERT INTO jobs (`+jobColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (id) DO NOTHING`,
		job.ID, job.TenantID, job.Name, string(job.Status), string(job.Payload),
		int(job.Priority), job.AttemptCount, job.MaxAttempts, job.DependsOn,
		job.FencingToken, job.ScheduledAt, job.NextRetryAt, job.CreatedAt, job.UpdatedAt)
	return err
}

func (r *JobRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	row := db.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, id)
	j, err := scanJob(row)
	if err != nil {
		if err == pgx.ErrNoRows {
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
		if err == pgx.ErrNoRows {
			return nil, domain.ErrJobNotFound
		}
		return nil, err
	}
	return j, nil
}

func (r *JobRepo) ListByTenant(ctx context.Context, tenantID uuid.UUID, filter repository.JobListFilter) ([]*domain.Job, int64, error) {
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
	var total int64
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM jobs `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	q := `SELECT ` + jobColumns + ` FROM jobs ` + where + ` ORDER BY created_at DESC`
	if filter.Limit > 0 {
		q += ` LIMIT $` + itoa(idx)
		args = append(args, filter.Limit)
		idx++
	}
	if filter.Offset > 0 {
		q += ` OFFSET $` + itoa(idx)
		args = append(args, filter.Offset)
	}
	rows, err := db.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
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
	var updatedID uuid.UUID
	err := db.QueryRow(ctx, `UPDATE jobs SET status='cancelled'
		WHERE id=$1 AND status='pending' RETURNING id`, id).Scan(&updatedID)
	if err != nil {
		if err == pgx.ErrNoRows {
			// Distinguish not-found from wrong-status.
			if _, gerr := r.GetByID(ctx, id); gerr != nil {
				return nil, gerr
			}
			return nil, domain.ErrJobCannotBeCancelled
		}
		return nil, err
	}
	return r.GetByID(ctx, id)
}

func (r *JobRepo) TransitionStatus(ctx context.Context, id uuid.UUID, from, to domain.JobStatus, fencingToken int64) error {
	db := dbFromCtx(ctx, r.db)
	if err := domain.ValidateTransition(from, to); err != nil {
		return err
	}
	tag, err := db.Exec(ctx, `UPDATE jobs SET status=$1
		WHERE id=$2 AND status=$3 AND fencing_token <= $4`, string(to), id, string(from), fencingToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		j, gerr := r.GetByID(ctx, id)
		if gerr != nil {
			return gerr
		}
		if j.FencingToken > fencingToken {
			return domain.ErrFencingTokenStale
		}
		return domain.ErrInvalidTransition
	}
	return nil
}

func (r *JobRepo) UpdateWithFencingToken(ctx context.Context, job *domain.Job, expectedToken int64) error {
	db := dbFromCtx(ctx, r.db)
	tag, err := db.Exec(ctx, `UPDATE jobs
		SET status=$1, payload=$2, attempt_count=$3, max_attempts=$4, next_retry_at=$5, fencing_token=$6
		WHERE id=$7 AND fencing_token <= $8`,
		string(job.Status), string(job.Payload), job.AttemptCount, job.MaxAttempts,
		job.NextRetryAt, job.FencingToken, job.ID, expectedToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		stored, gerr := r.GetByID(ctx, job.ID)
		if gerr != nil {
			return gerr
		}
		if stored.FencingToken > expectedToken {
			return domain.ErrFencingTokenStale
		}
		return domain.ErrJobNotFound
	}
	return nil
}

func (r *JobRepo) GetReadyJobs(ctx context.Context, limit int) ([]*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.Query(ctx, `SELECT `+jobColumns+` FROM jobs
		WHERE status='pending' AND scheduled_at <= NOW()
		AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		AND (depends_on IS NULL OR depends_on IN (SELECT id FROM jobs WHERE status='succeeded'))
		ORDER BY priority DESC, scheduled_at ASC LIMIT $1`, limit)
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

func (r *JobRepo) GetPendingRetryJobs(ctx context.Context, limit int) ([]*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.Query(ctx, `SELECT `+jobColumns+` FROM jobs
		WHERE status='failed' AND next_retry_at IS NOT NULL AND next_retry_at <= NOW()
		AND attempt_count < max_attempts
		ORDER BY next_retry_at ASC LIMIT $1`, limit)
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
	rows, err := db.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE depends_on=$1`, parentJobID)
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

func (r *JobRepo) IncrementFencingToken(ctx context.Context, id uuid.UUID) (int64, error) {
	db := dbFromCtx(ctx, r.db)
	var token int64
	err := db.QueryRow(ctx, `UPDATE jobs SET fencing_token = fencing_token + 1 WHERE id=$1 RETURNING fencing_token`, id).Scan(&token)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, domain.ErrJobNotFound
		}
		return 0, err
	}
	return token, nil
}

// --- Tenants ---

func (r *TenantRepo) Create(ctx context.Context, tenant *domain.Tenant) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `INSERT INTO tenants (id, name, api_key_hash, created_at)
		VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO NOTHING`,
		tenant.ID, tenant.Name, tenant.APIKeyHash, tenant.CreatedAt)
	return err
}

func (r *TenantRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error) {
	db := dbFromCtx(ctx, r.db)
	var t domain.Tenant
	err := db.QueryRow(ctx, `SELECT id, name, api_key_hash, created_at FROM tenants WHERE id=$1`, id).
		Scan(&t.ID, &t.Name, &t.APIKeyHash, &t.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, domain.ErrTenantNotFound
		}
		return nil, err
	}
	return &t, nil
}

func (r *TenantRepo) GetByAPIKeyHash(ctx context.Context, apiKeyHash string) (*domain.Tenant, error) {
	db := dbFromCtx(ctx, r.db)
	var t domain.Tenant
	err := db.QueryRow(ctx, `SELECT id, name, api_key_hash, created_at FROM tenants WHERE api_key_hash=$1`, apiKeyHash).
		Scan(&t.ID, &t.Name, &t.APIKeyHash, &t.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
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
	db := dbFromCtx(ctx, r.db)
	rows, err := db.Query(ctx, `SELECT id, job_id, from_status, to_status, occurred_at, worker_id, error_message
		FROM job_events WHERE job_id=$1 ORDER BY occurred_at ASC`, jobID)
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
	_, err := db.Exec(ctx, `INSERT INTO job_outbox (id, job_id, routing_key, payload, published, retry_count, last_error, created_at, published_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO NOTHING`,
		event.ID, event.JobID, event.RoutingKey, string(event.Payload),
		event.Published, event.RetryCount, event.LastError, event.CreatedAt, event.PublishedAt)
	return err
}

func (r *OutboxRepo) FetchUnpublished(ctx context.Context, limit int, olderThan time.Duration) ([]*domain.OutboxEvent, error) {
	db := dbFromCtx(ctx, r.db)
	if limit <= 0 {
		limit = 100
	}
	cutoff := time.Now().UTC().Add(-olderThan)
	rows, err := db.Query(ctx, `SELECT id, job_id, routing_key, payload, published, retry_count, last_error, created_at, published_at
		FROM job_outbox
		WHERE published = FALSE AND created_at < $1 AND retry_count < $2
		ORDER BY created_at ASC LIMIT $3
		FOR UPDATE SKIP LOCKED`, cutoff, domain.MaxOutboxRetries, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.OutboxEvent
	for rows.Next() {
		var e domain.OutboxEvent
		var payload []byte
		if err := rows.Scan(&e.ID, &e.JobID, &e.RoutingKey, &payload, &e.Published, &e.RetryCount, &e.LastError, &e.CreatedAt, &e.PublishedAt); err != nil {
			return nil, err
		}
		e.Payload = payload
		out = append(out, &e)
	}
	return out, rows.Err()
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, id uuid.UUID) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `UPDATE job_outbox SET published=TRUE, published_at=NOW() WHERE id=$1`, id)
	return err
}

func (r *OutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error {
	db := dbFromCtx(ctx, r.db)
	_, err := db.Exec(ctx, `UPDATE job_outbox SET retry_count = retry_count + 1, last_error=$2 WHERE id=$1`, id, lastErr)
	return err
}

func (r *OutboxRepo) DeletePublished(ctx context.Context, olderThan time.Duration) (int64, error) {
	db := dbFromCtx(ctx, r.db)
	cutoff := time.Now().UTC().Add(-olderThan)
	tag, err := db.Exec(ctx, `DELETE FROM job_outbox WHERE published=TRUE AND published_at IS NOT NULL AND published_at < $1`, cutoff)
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
