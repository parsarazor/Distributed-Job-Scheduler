DROP INDEX idx_outbox_retention;
DROP INDEX idx_job_events_retention;
DROP INDEX idx_jobs_submission_key;
ALTER TABLE jobs DROP COLUMN submission_key, DROP COLUMN timeout_ns;
