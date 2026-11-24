-- Repair migration, part 5+: one index per migration so each
-- CREATE INDEX CONCURRENTLY runs outside any transaction block.
CREATE INDEX CONCURRENTLY idx_jobs_tenant_dispatch_active ON jobs (tenant_id) WHERE status IN ('pending', 'running') AND dispatched_at IS NOT NULL;
