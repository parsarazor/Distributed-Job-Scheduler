package postgres

import (
	"context"
	"time"

	"github.com/prsamohamadi/distributed-job-scheduler/internal/repository"
)

func (p *Pool) Stats(ctx context.Context) (repository.Stats, error) {
	s := repository.Stats{Jobs: make(map[string]int64)}
	rows, err := p.pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return s, err
		}
		s.Jobs[status] = count
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return s, err
	}
	var lag float64
	err = p.pool.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)),0) FROM job_outbox WHERE NOT published`).Scan(&s.OutboxPending, &lag)
	s.OutboxLag = time.Duration(lag * float64(time.Second))
	return s, err
}

func (p *Pool) Prune(ctx context.Context, outboxAge, auditAge time.Duration) error {
	_, err := p.pool.Exec(ctx, `DELETE FROM job_outbox WHERE id IN (SELECT id FROM job_outbox WHERE published AND published_at < clock_timestamp()-make_interval(secs => $1) LIMIT 1000)`, outboxAge.Seconds())
	if err != nil {
		return err
	}
	_, err = p.pool.Exec(ctx, `DELETE FROM job_events WHERE id IN (SELECT e.id FROM job_events e JOIN jobs j ON j.id=e.job_id WHERE e.occurred_at < clock_timestamp()-make_interval(secs => $1) AND j.status IN ('succeeded','cancelled','dead') LIMIT 1000)`, auditAge.Seconds())
	if err != nil {
		return err
	}
	_, err = p.pool.Exec(ctx, `WITH expired AS (
		SELECT id FROM jobs WHERE status IN ('succeeded','cancelled','dead') AND NOT payload_expired
		AND updated_at < clock_timestamp()-make_interval(secs => $1) ORDER BY updated_at, id LIMIT 1000 FOR UPDATE SKIP LOCKED
	)
	UPDATE jobs j SET payload='{}'::jsonb, payload_expired=TRUE, traceparent='', tracestate=''
	FROM expired e WHERE j.id=e.id`, auditAge.Seconds())
	return err
}
