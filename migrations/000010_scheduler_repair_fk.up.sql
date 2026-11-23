-- Repair migration, part 4: add the dependency-tenant foreign key NOT VALID,
-- then validate it. ADD CONSTRAINT NOT VALID is a catalog-only change; VALIDATE
-- takes SHARE UPDATE EXCLUSIVE and does not block reads or writes.
SET lock_timeout = '5s';

ALTER TABLE jobs ADD CONSTRAINT fk_jobs_dependency_tenant
    FOREIGN KEY (tenant_id, depends_on) REFERENCES jobs (tenant_id, id)
    ON DELETE SET NULL (depends_on) NOT VALID;
ALTER TABLE jobs VALIDATE CONSTRAINT fk_jobs_dependency_tenant;
