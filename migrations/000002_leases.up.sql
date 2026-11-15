ALTER TABLE jobs ADD COLUMN lease_owner TEXT NULL;
ALTER TABLE jobs ADD COLUMN lease_expires_at TIMESTAMPTZ NULL;

CREATE INDEX idx_jobs_expired_leases ON jobs (lease_expires_at) WHERE status = 'running';
