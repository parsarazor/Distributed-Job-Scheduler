DROP INDEX IF EXISTS idx_jobs_expired_leases;
ALTER TABLE jobs DROP COLUMN lease_expires_at;
ALTER TABLE jobs DROP COLUMN lease_owner;
