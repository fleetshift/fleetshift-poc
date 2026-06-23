-- +goose Up

-- Convert all uid columns from TEXT to native UUID.
-- Foreign keys referencing platform_resources(uid) must be dropped
-- before the parent column type can change, then re-added afterward.

-- deployments (standalone, no FK to platform_resources)
UPDATE deployments SET uid = gen_random_uuid()::text WHERE uid = '';
ALTER TABLE deployments ALTER COLUMN uid DROP DEFAULT;
ALTER TABLE deployments ALTER COLUMN uid TYPE UUID USING uid::uuid;
ALTER TABLE deployments ALTER COLUMN uid SET DEFAULT gen_random_uuid();

-- managed_resources (standalone uid)
UPDATE managed_resources SET uid = gen_random_uuid()::text WHERE uid = '';
ALTER TABLE managed_resources ALTER COLUMN uid TYPE UUID USING uid::uuid;

-- platform_resources and dependents: drop FKs, convert all columns, re-add FKs
ALTER TABLE resource_representations DROP CONSTRAINT resource_representations_platform_uid_fkey;
ALTER TABLE resource_aliases DROP CONSTRAINT resource_aliases_platform_uid_fkey;
ALTER TABLE resource_relationships DROP CONSTRAINT resource_relationships_source_uid_fkey;
ALTER TABLE resource_relationships DROP CONSTRAINT resource_relationships_target_uid_fkey;

ALTER TABLE platform_resources ALTER COLUMN uid TYPE UUID USING uid::uuid;
ALTER TABLE resource_representations ALTER COLUMN platform_uid TYPE UUID USING platform_uid::uuid;
ALTER TABLE resource_aliases ALTER COLUMN platform_uid TYPE UUID USING platform_uid::uuid;
ALTER TABLE resource_relationships ALTER COLUMN source_uid TYPE UUID USING source_uid::uuid;
ALTER TABLE resource_relationships ALTER COLUMN target_uid TYPE UUID USING target_uid::uuid;

ALTER TABLE resource_representations ADD CONSTRAINT resource_representations_platform_uid_fkey
    FOREIGN KEY (platform_uid) REFERENCES platform_resources(uid) ON DELETE CASCADE;
ALTER TABLE resource_aliases ADD CONSTRAINT resource_aliases_platform_uid_fkey
    FOREIGN KEY (platform_uid) REFERENCES platform_resources(uid) ON DELETE CASCADE;
ALTER TABLE resource_relationships ADD CONSTRAINT resource_relationships_source_uid_fkey
    FOREIGN KEY (source_uid) REFERENCES platform_resources(uid) ON DELETE CASCADE;
ALTER TABLE resource_relationships ADD CONSTRAINT resource_relationships_target_uid_fkey
    FOREIGN KEY (target_uid) REFERENCES platform_resources(uid) ON DELETE CASCADE;

-- +goose Down
ALTER TABLE resource_representations DROP CONSTRAINT resource_representations_platform_uid_fkey;
ALTER TABLE resource_aliases DROP CONSTRAINT resource_aliases_platform_uid_fkey;
ALTER TABLE resource_relationships DROP CONSTRAINT resource_relationships_source_uid_fkey;
ALTER TABLE resource_relationships DROP CONSTRAINT resource_relationships_target_uid_fkey;

ALTER TABLE resource_relationships ALTER COLUMN target_uid TYPE TEXT;
ALTER TABLE resource_relationships ALTER COLUMN source_uid TYPE TEXT;
ALTER TABLE resource_aliases ALTER COLUMN platform_uid TYPE TEXT;
ALTER TABLE resource_representations ALTER COLUMN platform_uid TYPE TEXT;
ALTER TABLE platform_resources ALTER COLUMN uid TYPE TEXT;

ALTER TABLE resource_representations ADD CONSTRAINT resource_representations_platform_uid_fkey
    FOREIGN KEY (platform_uid) REFERENCES platform_resources(uid) ON DELETE CASCADE;
ALTER TABLE resource_aliases ADD CONSTRAINT resource_aliases_platform_uid_fkey
    FOREIGN KEY (platform_uid) REFERENCES platform_resources(uid) ON DELETE CASCADE;
ALTER TABLE resource_relationships ADD CONSTRAINT resource_relationships_source_uid_fkey
    FOREIGN KEY (source_uid) REFERENCES platform_resources(uid) ON DELETE CASCADE;
ALTER TABLE resource_relationships ADD CONSTRAINT resource_relationships_target_uid_fkey
    FOREIGN KEY (target_uid) REFERENCES platform_resources(uid) ON DELETE CASCADE;

ALTER TABLE managed_resources ALTER COLUMN uid TYPE TEXT;
ALTER TABLE deployments ALTER COLUMN uid DROP DEFAULT;
ALTER TABLE deployments ALTER COLUMN uid TYPE TEXT;
ALTER TABLE deployments ALTER COLUMN uid SET DEFAULT '';
