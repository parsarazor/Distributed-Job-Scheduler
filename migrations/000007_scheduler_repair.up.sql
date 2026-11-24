-- Repair migration, part 1: fast, transactional steps only.
--
-- This file runs as a single implicit transaction (one simple-query round
-- trip), so it is all-or-nothing. Long-running DDL such as
-- CREATE INDEX CONCURRENTLY cannot run inside a transaction and therefore
-- lives in its own single-statement migration (parts 3+).
SET lock_timeout = '5s';

ALTER TABLE jobs ADD COLUMN submission_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN payload_expired BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE job_outbox ADD COLUMN next_publish_at TIMESTAMPTZ;
ALTER TABLE job_outbox ADD COLUMN delivered_at TIMESTAMPTZ;
ALTER TABLE job_outbox ADD COLUMN publishing_since TIMESTAMPTZ;
ALTER TABLE job_outbox ADD COLUMN dead_lettered BOOLEAN NOT NULL DEFAULT FALSE;

-- Refuse unexpected imported priorities instead of silently changing ordering.
-- The pre-check fails this transaction with a clear operator error, leaving the
-- database at the previous clean schema version, instead of letting the CHECK
-- constraint abort mid-migration and mark the database dirty. The constraint is
-- added NOT VALID here and validated in part 2 so no full-table lock is held.
DO $repair_check_priorities$
BEGIN
    IF EXISTS (SELECT 1 FROM jobs WHERE priority NOT IN (0, 2, 5, 10)) THEN
        RAISE EXCEPTION 'repair migration: jobs rows exist with priority outside (0, 2, 5, 10); normalize them before upgrading';
    END IF;
END;
$repair_check_priorities$;

ALTER TABLE jobs DROP CONSTRAINT chk_job_priority;
ALTER TABLE jobs ADD CONSTRAINT chk_job_priority CHECK (priority IN (0, 2, 5, 10)) NOT VALID;
DROP INDEX IF EXISTS idx_tenants_api_key_hash;

-- Quarantine jobs whose execution depended on another tenant's data.
-- Keep terminal history and preserve job IDs, payloads and submission keys.
WITH RECURSIVE unsafe(id, tenant_id) AS (
    SELECT j.id, j.tenant_id
    FROM jobs j JOIN jobs p ON p.id = j.depends_on
    WHERE j.tenant_id <> p.tenant_id
    UNION
    SELECT j.id, j.tenant_id
    FROM jobs j JOIN unsafe u ON j.depends_on = u.id AND j.tenant_id = u.tenant_id
), cancelled AS (
    UPDATE jobs j
    SET status = 'cancelled', fencing_token = fencing_token + 1,
        next_retry_at = NULL, dispatched_at = NULL, lease_owner = NULL, lease_expires_at = NULL
    FROM unsafe u
    WHERE j.id = u.id AND j.status IN ('pending', 'failed', 'running')
    RETURNING j.id
)
INSERT INTO job_events (job_id, from_status, to_status, error_message)
SELECT j.id, j.status, 'cancelled', 'invalid dependency removed'
FROM jobs j JOIN cancelled c USING (id);

UPDATE jobs j SET depends_on = NULL
FROM jobs p WHERE j.depends_on = p.id AND j.tenant_id <> p.tenant_id;

-- Retire incompatible announcements and compact the rest using only original
-- attempt data. Never convert an old payload into authorization for a new attempt.
UPDATE job_outbox
SET published = TRUE, published_at = clock_timestamp(), delivered_at = clock_timestamp()
WHERE NOT published
AND (
    payload->>'id' IS DISTINCT FROM job_id::text
    OR jsonb_typeof(payload->'attempt_count') IS DISTINCT FROM 'number'
    OR jsonb_typeof(payload->'fencing_token') IS DISTINCT FROM 'number'
);

UPDATE job_outbox
SET payload = jsonb_strip_nulls(jsonb_build_object(
    'id', payload->'id', 'attempt_count', payload->'attempt_count',
    'fencing_token', payload->'fencing_token',
    'traceparent', payload->'traceparent', 'tracestate', payload->'tracestate'
))
WHERE NOT published;

-- Retention uses the actual mutation clock even in an older transaction.
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
