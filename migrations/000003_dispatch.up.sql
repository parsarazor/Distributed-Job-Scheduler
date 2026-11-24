ALTER TABLE jobs ADD COLUMN dispatched_at TIMESTAMPTZ;
CREATE INDEX idx_jobs_undispatched ON jobs (priority DESC, scheduled_at)
    WHERE status = 'pending' AND dispatched_at IS NULL;

-- Old announcements may have been created before readiness was checked.
-- Retire them; pending jobs will receive a new ready-only announcement.
UPDATE job_outbox SET published = TRUE, published_at = clock_timestamp()
    WHERE published = FALSE;

-- Jobs running before leases were introduced must be recoverable as well.
UPDATE jobs SET lease_expires_at = clock_timestamp()
    WHERE status = 'running' AND lease_expires_at IS NULL;
