-- Repair migration, part 1 down: undo the transactional data/constraint steps.
-- Index migrations (parts 3 and later) are dropped in their own down files.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS chk_job_priority;
ALTER TABLE jobs ADD CONSTRAINT chk_job_priority CHECK (priority >= 0);
ALTER TABLE jobs DROP COLUMN IF EXISTS payload_expired;
ALTER TABLE jobs DROP COLUMN IF EXISTS submission_hash;
ALTER TABLE job_outbox DROP COLUMN IF EXISTS dead_lettered;
ALTER TABLE job_outbox DROP COLUMN IF EXISTS publishing_since;
ALTER TABLE job_outbox DROP COLUMN IF EXISTS delivered_at;
ALTER TABLE job_outbox DROP COLUMN IF EXISTS next_publish_at;
DROP FUNCTION IF EXISTS update_updated_at_column();
-- Invalid legacy dependencies and old announcement payloads cannot be restored.
