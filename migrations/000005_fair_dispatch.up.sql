ALTER TABLE tenants ADD COLUMN last_dispatched_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity';
CREATE INDEX idx_tenants_dispatch ON tenants (last_dispatched_at);
