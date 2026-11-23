-- Repair migration, part 5+: one index per migration so each
-- CREATE INDEX CONCURRENTLY runs outside any transaction block.
CREATE INDEX CONCURRENTLY idx_jobs_stale_dispatch ON jobs (dispatched_at, id) WHERE status = 'pending' AND dispatched_at IS NOT NULL;
