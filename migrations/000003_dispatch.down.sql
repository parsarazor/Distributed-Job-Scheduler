DROP INDEX IF EXISTS idx_jobs_undispatched;
ALTER TABLE jobs DROP COLUMN dispatched_at;
