-- Repair migration, part 4 down: drop the dependency foreign key. The unique
-- index it referenced is dropped by part 3's down migration.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS fk_jobs_dependency_tenant;
