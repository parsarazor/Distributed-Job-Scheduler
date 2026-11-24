-- Repair migration, part 5+: one index per migration so each
-- CREATE INDEX CONCURRENTLY runs outside any transaction block.
CREATE INDEX CONCURRENTLY idx_job_events_page ON job_events (job_id, occurred_at, id);
