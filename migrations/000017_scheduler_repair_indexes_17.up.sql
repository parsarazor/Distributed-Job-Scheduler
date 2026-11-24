-- Repair migration, part 5+: one index per migration so each
-- CREATE INDEX CONCURRENTLY runs outside any transaction block.
CREATE INDEX CONCURRENTLY idx_outbox_undelivered ON job_outbox (job_id) WHERE published AND delivered_at IS NULL;
