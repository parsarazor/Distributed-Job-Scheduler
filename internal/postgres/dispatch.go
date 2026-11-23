package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/broker"
	"github.com/prsamohamadi/distributed-job-scheduler/internal/domain"
)

// fairReadyJobs locks tenants in turn order and then locks only the selected
// jobs. The candidate lists use job IDs and ordering keys without loading payloads.
func (r *JobRepo) fairReadyJobs(ctx context.Context, limit int) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	limit = min(limit, 1000)
	db := dbFromCtx(ctx, r.db)
	rows, err := db.Query(ctx, `
		SELECT t.id FROM tenants t
		WHERE EXISTS (
			SELECT 1 FROM jobs j WHERE j.tenant_id=t.id AND j.status='pending' AND j.dispatched_at IS NULL
			AND j.attempt_count < j.max_attempts AND j.scheduled_at <= statement_timestamp()
			AND (j.next_retry_at IS NULL OR j.next_retry_at <= statement_timestamp())
			AND (j.depends_on IS NULL OR EXISTS (SELECT 1 FROM jobs p WHERE p.id=j.depends_on AND p.tenant_id=j.tenant_id AND p.status='succeeded')))
		AND NOT EXISTS (
			SELECT 1 FROM (
                SELECT 1 FROM jobs a WHERE a.tenant_id=t.id AND a.status IN ('pending','running') AND a.dispatched_at IS NOT NULL
                UNION ALL
                SELECT 1 FROM job_outbox o JOIN jobs a ON a.id=o.job_id
                WHERE a.tenant_id=t.id AND a.status IN ('succeeded','cancelled','dead') AND o.published AND o.delivered_at IS NULL
            ) credits OFFSET 9)
		ORDER BY t.last_dispatched_at, t.id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	var tenants []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		tenants = append(tenants, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(tenants) == 0 {
		return nil, err
	}

	// Read capacity in a fresh statement after acquiring the tenant locks.
	// Another dispatcher might have committed after the first snapshot.
	rows, err = db.Query(ctx, `WITH candidates AS (
		SELECT j.id, t.tenant_order,
			row_number() OVER (PARTITION BY t.id ORDER BY j.priority DESC, j.scheduled_at, j.id) AS turn
		FROM unnest($2::uuid[]) WITH ORDINALITY AS t(id, tenant_order) CROSS JOIN LATERAL (
			SELECT j.id, j.priority, j.scheduled_at FROM jobs j
			WHERE j.tenant_id=t.id AND j.status='pending' AND j.dispatched_at IS NULL
			AND j.attempt_count < j.max_attempts AND j.scheduled_at <= statement_timestamp()
			AND (j.next_retry_at IS NULL OR j.next_retry_at <= statement_timestamp())
			AND (j.depends_on IS NULL OR EXISTS (SELECT 1 FROM jobs p WHERE p.id=j.depends_on AND p.tenant_id=j.tenant_id AND p.status='succeeded'))
			ORDER BY j.priority DESC, j.scheduled_at, j.id
			LIMIT LEAST($1, GREATEST(0, 10-(
				SELECT count(*) FROM (
                    SELECT 1 FROM jobs a WHERE a.tenant_id=t.id AND a.status IN ('pending','running') AND a.dispatched_at IS NOT NULL
                    UNION ALL
                    SELECT 1 FROM job_outbox o JOIN jobs a ON a.id=o.job_id
                    WHERE a.tenant_id=t.id AND a.status IN ('succeeded','cancelled','dead') AND o.published AND o.delivered_at IS NULL
                    LIMIT 10
                ) active)))
		) j
	)
	SELECT j.id, j.attempt_count, j.fencing_token, j.traceparent, j.tracestate
	FROM candidates c JOIN jobs j ON j.id=c.id
	WHERE j.status='pending' AND j.dispatched_at IS NULL
	AND j.attempt_count < j.max_attempts AND j.scheduled_at <= statement_timestamp()
	AND (j.next_retry_at IS NULL OR j.next_retry_at <= statement_timestamp())
	AND (j.depends_on IS NULL OR EXISTS (SELECT 1 FROM jobs p WHERE p.id=j.depends_on AND p.tenant_id=j.tenant_id AND p.status='succeeded'))
	ORDER BY c.turn, c.tenant_order
	LIMIT $1 FOR UPDATE OF j SKIP LOCKED`, limit, tenants)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Job
	for rows.Next() {
		job := new(domain.Job)
		if err := rows.Scan(&job.ID, &job.AttemptCount, &job.FencingToken, &job.TraceParent, &job.TraceState); err != nil {
			return nil, err
		}
		result = append(result, job)
	}
	return result, rows.Err()
}

func (r *JobRepo) RecoverStaleDispatches(ctx context.Context, limit int, age time.Duration) error {
	// Published terminal messages release credit after their broker TTL even
	// when queue loss prevented a worker from acknowledging the announcement.
	if _, err := dbFromCtx(ctx, r.db).Exec(ctx, `UPDATE job_outbox o SET delivered_at=clock_timestamp()
		WHERE o.id IN (
			SELECT o.id FROM job_outbox o JOIN jobs j ON j.id=o.job_id
			WHERE o.published AND o.delivered_at IS NULL AND j.status IN ('succeeded','cancelled','dead')
			AND o.published_at <= statement_timestamp()-make_interval(secs => $1)
			ORDER BY o.published_at, o.id LIMIT $2 FOR UPDATE OF o SKIP LOCKED
		)`, broker.ExecutionMessageTTL.Seconds(), limit); err != nil {
		return err
	}
	if limit <= 0 || age <= 0 {
		return domain.ErrInvalidInput
	}
	_, err := dbFromCtx(ctx, r.db).Exec(ctx, `WITH stale AS (
		SELECT j.id FROM jobs j WHERE j.status='pending'
		AND j.dispatched_at <= statement_timestamp()-make_interval(secs => $1)
		AND NOT EXISTS (
			SELECT 1 FROM job_outbox o WHERE o.job_id=j.id AND NOT o.dead_lettered
			AND (NOT o.published OR o.published_at > statement_timestamp()-make_interval(secs => $1)))
		ORDER BY j.dispatched_at, j.id LIMIT $2 FOR UPDATE SKIP LOCKED
	)
	UPDATE jobs j SET dispatched_at=NULL, fencing_token=j.fencing_token+1
	FROM stale WHERE j.id=stale.id`, age.Seconds(), limit)
	return err
}

// TerminalizeOutboxExhausted fails pending jobs whose dispatch announcements
// have dead-lettered maxDeadLettered times. RecoverStaleDispatches would
// otherwise stage another announcement on every pass, and each stranded
// dispatch keeps consuming one of the tenant's dispatch credits.
func (r *JobRepo) TerminalizeOutboxExhausted(ctx context.Context, limit, maxDeadLettered int) ([]uuid.UUID, error) {
	if limit <= 0 || maxDeadLettered <= 0 {
		return nil, domain.ErrInvalidInput
	}
	rows, err := dbFromCtx(ctx, r.db).Query(ctx, `WITH poisoned AS (
		SELECT j.id FROM jobs j
		JOIN job_outbox o ON o.job_id=j.id AND o.dead_lettered
		WHERE j.status='pending'
		GROUP BY j.id
		HAVING count(*) >= $1
		ORDER BY min(j.updated_at), j.id LIMIT $2
	)
	UPDATE jobs j SET status='dead', dispatched_at=NULL, updated_at=clock_timestamp()
	FROM poisoned WHERE j.id=poisoned.id AND j.status='pending'
	RETURNING j.id`, maxDeadLettered, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
