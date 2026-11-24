-- Repair migration, part 2 down: reverting VALIDATE is a no-op; the constraint
-- itself is dropped by part 1's down migration.
SELECT 1;
