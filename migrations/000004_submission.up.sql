ALTER TABLE jobs ADD COLUMN submission_key TEXT;
ALTER TABLE jobs ADD COLUMN timeout_ns BIGINT NOT NULL DEFAULT 300000000000 CHECK (timeout_ns > 0);
CREATE UNIQUE INDEX idx_jobs_submission_key ON jobs (tenant_id, submission_key) WHERE submission_key IS NOT NULL;
CREATE INDEX idx_job_events_retention ON job_events (occurred_at);
CREATE INDEX idx_outbox_retention ON job_outbox (published_at) WHERE published;
