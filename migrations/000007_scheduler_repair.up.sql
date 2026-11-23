ALTER TABLE jobs ADD COLUMN submission_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN payload_expired BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE job_outbox ADD COLUMN next_publish_at TIMESTAMPTZ;
ALTER TABLE job_outbox ADD COLUMN delivered_at TIMESTAMPTZ;

-- Refuse unexpected imported priorities instead of silently changing ordering.
ALTER TABLE jobs DROP CONSTRAINT chk_job_priority;
ALTER TABLE jobs ADD CONSTRAINT chk_job_priority CHECK (priority IN (0, 2, 5, 10));
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

ALTER TABLE jobs ADD CONSTRAINT uq_jobs_tenant_id_id UNIQUE (tenant_id, id);
ALTER TABLE jobs ADD CONSTRAINT fk_jobs_dependency_tenant
    FOREIGN KEY (tenant_id, depends_on) REFERENCES jobs (tenant_id, id)
    ON DELETE SET NULL (depends_on);

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

CREATE INDEX idx_jobs_tenant_page ON jobs (tenant_id, created_at DESC, id);
CREATE INDEX idx_jobs_tenant_outstanding ON jobs (tenant_id) WHERE status IN ('pending','running','failed');
CREATE INDEX idx_jobs_tenant_dispatch_ready ON jobs (tenant_id, priority DESC, scheduled_at, id)
    WHERE status = 'pending' AND dispatched_at IS NULL;
CREATE INDEX idx_jobs_tenant_dispatch_active ON jobs (tenant_id)
    WHERE status IN ('pending', 'running') AND dispatched_at IS NOT NULL;
CREATE INDEX idx_jobs_stale_dispatch ON jobs (dispatched_at, id)
    WHERE status = 'pending' AND dispatched_at IS NOT NULL;
CREATE INDEX idx_outbox_job_publication ON job_outbox (job_id, published, published_at);
CREATE INDEX idx_outbox_undelivered ON job_outbox (job_id) WHERE published AND delivered_at IS NULL;
CREATE INDEX idx_job_events_page ON job_events (job_id, occurred_at, id);

-- Retention uses the actual mutation clock even in an older transaction.
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
