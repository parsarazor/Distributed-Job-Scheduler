package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

func (r *JobRepo) CountWaitingDependents(ctx context.Context, parentID uuid.UUID) (int, error) {
	var count int
	err := dbFromCtx(ctx, r.db).QueryRow(ctx, `SELECT count(*) FROM jobs
		WHERE depends_on=$1 AND tenant_id=(SELECT tenant_id FROM jobs WHERE id=$1)
		AND status='pending' AND attempt_count < max_attempts`, parentID).Scan(&count)
	return count, err
}

func (r *JobRepo) CancelDependents(ctx context.Context, parentID uuid.UUID) ([]*domain.Job, error) {
	db := dbFromCtx(ctx, r.db)
	var tenantID uuid.UUID
	if err := db.QueryRow(ctx, `SELECT tenant_id FROM jobs WHERE id=$1`, parentID).Scan(&tenantID); err != nil {
		return nil, err
	}
	seen := map[uuid.UUID]bool{parentID: true}
	queue := []uuid.UUID{parentID}
	var cancelled []*domain.Job
	for len(queue) > 0 {
		// Lock each level before looking for its children. A submission either
		// commits before its parent is locked or sees the cancellation afterward.
		fanout := min(len(queue), 1000)
		parents := queue[:fanout]
		queue = queue[fanout:]
		rows, err := db.Query(ctx, `WITH children AS MATERIALIZED (
			SELECT id, status FROM jobs WHERE tenant_id=$1 AND depends_on=ANY($2::uuid[])
			ORDER BY id FOR UPDATE
		), cancelled AS (
			UPDATE jobs j SET status='cancelled', next_retry_at=NULL,
				dispatched_at=NULL, lease_owner=NULL, lease_expires_at=NULL
			FROM children c WHERE j.id=c.id AND c.status IN ('pending','failed') AND j.id<>$3
			RETURNING j.id
		)
		SELECT c.id, c.status, cancelled.id IS NOT NULL
		FROM children c LEFT JOIN cancelled USING (id) ORDER BY c.id`, tenantID, parents, parentID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			before := new(domain.Job)
			var changed bool
			if err := rows.Scan(&before.ID, &before.Status, &changed); err != nil {
				rows.Close()
				return nil, err
			}
			if !seen[before.ID] {
				seen[before.ID] = true
				queue = append(queue, before.ID)
			}
			if changed {
				cancelled = append(cancelled, before)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return cancelled, nil
}
