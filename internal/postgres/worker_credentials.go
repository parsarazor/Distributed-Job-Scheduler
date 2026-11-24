package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/parsarazor/distributed-job-scheduler/internal/domain"
)

// WorkerCredentialRepo implements repository.WorkerCredentialRepository.
type WorkerCredentialRepo struct{ db *Pool }

const workerCredentialColumns = `id, worker_id, token_hash, created_at, revoked_at`

type rowScanner interface{ Scan(dest ...any) error }

func scanWorkerCredential(row rowScanner) (*domain.WorkerCredential, error) {
	var c domain.WorkerCredential
	if err := row.Scan(&c.ID, &c.WorkerID, &c.TokenHash, &c.CreatedAt, &c.RevokedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrWorkerCredentialNotFound
		}
		return nil, err
	}
	return &c, nil
}

func (r *WorkerCredentialRepo) Create(ctx context.Context, cred *domain.WorkerCredential) error {
	_, err := dbFromCtx(ctx, r.db).Exec(ctx, `INSERT INTO worker_credentials (id, worker_id, token_hash, created_at, revoked_at)
		VALUES ($1,$2,$3,$4,$5)`, cred.ID, cred.WorkerID, cred.TokenHash, cred.CreatedAt, cred.RevokedAt)
	return err
}

func (r *WorkerCredentialRepo) GetByTokenHash(ctx context.Context, tokenHash string) (*domain.WorkerCredential, error) {
	return scanWorkerCredential(dbFromCtx(ctx, r.db).QueryRow(ctx,
		`SELECT `+workerCredentialColumns+` FROM worker_credentials WHERE token_hash=$1`, tokenHash))
}

func (r *WorkerCredentialRepo) List(ctx context.Context, limit, offset int) ([]*domain.WorkerCredential, error) {
	rows, err := dbFromCtx(ctx, r.db).Query(ctx,
		`SELECT `+workerCredentialColumns+` FROM worker_credentials ORDER BY created_at, id LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.WorkerCredential
	for rows.Next() {
		c, err := scanWorkerCredential(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (r *WorkerCredentialRepo) Revoke(ctx context.Context, id uuid.UUID, at time.Time) (*domain.WorkerCredential, error) {
	return scanWorkerCredential(dbFromCtx(ctx, r.db).QueryRow(ctx,
		`UPDATE worker_credentials SET revoked_at=COALESCE(revoked_at, $2) WHERE id=$1 RETURNING `+workerCredentialColumns, id, at))
}
