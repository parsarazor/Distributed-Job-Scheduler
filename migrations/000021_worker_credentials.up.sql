CREATE TABLE IF NOT EXISTS worker_credentials (
    id UUID PRIMARY KEY,
    worker_id VARCHAR(255) NOT NULL,
    token_hash VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ
);
