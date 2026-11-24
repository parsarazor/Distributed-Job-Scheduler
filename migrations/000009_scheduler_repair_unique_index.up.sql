-- Repair migration, part 3: the tenant-scoped unique index backing the
-- dependency foreign key. Non-transactional by design: CREATE INDEX
-- CONCURRENTLY cannot run inside a transaction block, so this file must stay a
-- single statement and the migrate DSN must not enable x-multi-statement.
-- The lock_timeout guard is supplied via the connection's options parameter.
CREATE UNIQUE INDEX CONCURRENTLY uq_jobs_tenant_id_id ON jobs (tenant_id, id);
