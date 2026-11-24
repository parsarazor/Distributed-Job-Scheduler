ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_lease_owner TEXT;
-- Leases claimed before this column existed still belong to their owner.
UPDATE jobs SET last_lease_owner = lease_owner WHERE lease_owner IS NOT NULL AND last_lease_owner IS NULL;
