-- Repair migration, part 5+: one index per migration so each
-- CREATE INDEX CONCURRENTLY runs outside any transaction block.
CREATE INDEX CONCURRENTLY idx_jobs_tenant_outstanding ON jobs (tenant_id) WHERE status IN ('pending','running','failed');
