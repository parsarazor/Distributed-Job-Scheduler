package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
)

func (r *TenantRepo) Lock(ctx context.Context, id uuid.UUID) error {
	var found uuid.UUID
	err := dbFromCtx(ctx, r.db).QueryRow(ctx, `SELECT id FROM tenants WHERE id=$1 FOR UPDATE`, id).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrTenantNotFound
	}
	return err
}

func (r *JobRepo) GetBySubmissionKey(ctx context.Context, tenant uuid.UUID, key string) (*domain.Job, error) {
	j, err := scanJob(dbFromCtx(ctx, r.db).QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE tenant_id=$1 AND submission_key=$2`, tenant, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrJobNotFound
	}
	return j, err
}

func (r *JobRepo) CheckAdmission(ctx context.Context, tenant uuid.UUID, maxOutstanding, perMinute int) error {
	var outstanding, recent int
	err := dbFromCtx(ctx, r.db).QueryRow(ctx, `SELECT
		(SELECT count(*) FROM (SELECT 1 FROM jobs WHERE tenant_id=$1 AND status IN ('pending','running','failed') LIMIT $2) active),
		(SELECT count(*) FROM (SELECT 1 FROM jobs WHERE tenant_id=$1 AND created_at > statement_timestamp()-interval '1 minute' LIMIT $3) recent)`, tenant, maxOutstanding, perMinute).Scan(&outstanding, &recent)
	if err != nil {
		return err
	}
	if outstanding >= maxOutstanding || recent >= perMinute {
		return domain.ErrTenantLimit
	}
	return nil
}

func (r *JobRepo) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := dbFromCtx(ctx, r.db).QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now.UTC(), err
}

func (r *JobRepo) GetDependency(ctx context.Context, tenantID, id uuid.UUID, lock bool) (*domain.Job, error) {
	q := `SELECT ` + jobMetadataColumns + ` FROM jobs WHERE tenant_id=$1 AND id=$2`
	if lock {
		q += ` FOR UPDATE`
	}
	job, err := scanJob(dbFromCtx(ctx, r.db).QueryRow(ctx, q, tenantID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrDependencyNotMet
	}
	return job, err
}

func (r *JobRepo) RequeueDead(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	j, err := scanJob(dbFromCtx(ctx, r.db).QueryRow(ctx, `UPDATE jobs SET status='pending', max_attempts=attempt_count+3, fencing_token=fencing_token+1, next_retry_at=NULL, dispatched_at=NULL, lease_owner=NULL, lease_expires_at=NULL WHERE id=$1 AND status='dead' AND NOT payload_expired RETURNING `+jobColumns, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrInvalidTransition
	}
	return j, err
}
