DROP INDEX idx_job_events_page;
DROP INDEX idx_outbox_undelivered;
DROP INDEX idx_outbox_job_publication;
DROP INDEX idx_jobs_stale_dispatch;
DROP INDEX idx_jobs_tenant_dispatch_active;
DROP INDEX idx_jobs_tenant_dispatch_ready;
DROP INDEX idx_jobs_tenant_page;
DROP INDEX idx_jobs_tenant_outstanding;

ALTER TABLE jobs DROP CONSTRAINT fk_jobs_dependency_tenant;
ALTER TABLE jobs DROP CONSTRAINT uq_jobs_tenant_id_id;
ALTER TABLE job_outbox DROP COLUMN next_publish_at;
ALTER TABLE job_outbox DROP COLUMN delivered_at;
ALTER TABLE jobs DROP COLUMN submission_hash;
ALTER TABLE jobs DROP COLUMN payload_expired;
ALTER TABLE jobs DROP CONSTRAINT chk_job_priority;
ALTER TABLE jobs ADD CONSTRAINT chk_job_priority CHECK (priority >= 0);
-- Invalid legacy dependencies and old announcement payloads cannot be restored.
