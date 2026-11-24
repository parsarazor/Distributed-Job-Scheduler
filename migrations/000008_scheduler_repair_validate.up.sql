-- Repair migration, part 2: validate the CHECK constraint added NOT VALID in
-- part 1. VALIDATE takes SHARE UPDATE EXCLUSIVE, so reads and writes continue
-- while Postgres scans the table; the brief lock wait is bounded by lock_timeout.
SET lock_timeout = '5s';

ALTER TABLE jobs VALIDATE CONSTRAINT chk_job_priority;
