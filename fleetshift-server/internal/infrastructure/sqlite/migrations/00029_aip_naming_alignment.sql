-- +goose Up

-- platform_resources: relative_name → name, drop collection_id (derivable from name)
DROP INDEX IF EXISTS idx_platform_resources_collection;
ALTER TABLE platform_resources RENAME COLUMN relative_name TO name;
ALTER TABLE platform_resources DROP COLUMN collection_id;

-- resource_representations: relative_name → name, drop collection_id (derivable from name)
--
-- SQLite does not support DROP COLUMN on columns that participate in a
-- composite PRIMARY KEY. Recreate the table with (service_name, name) as PK.
DROP INDEX IF EXISTS idx_resource_representations_platform;

CREATE TABLE resource_representations_new (
    platform_uid  TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    service_name  TEXT NOT NULL,
    version       TEXT NOT NULL,
    name          TEXT NOT NULL,
    roles         TEXT NOT NULL DEFAULT '[]',
    labels        TEXT NOT NULL DEFAULT '{}',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    deleted_at    TEXT,
    PRIMARY KEY (service_name, name)
);

INSERT INTO resource_representations_new
    (platform_uid, service_name, version, name, roles, labels, created_at, updated_at, deleted_at)
SELECT platform_uid, service_name, version, relative_name, roles, labels, created_at, updated_at, deleted_at
FROM resource_representations;

DROP TABLE resource_representations;
ALTER TABLE resource_representations_new RENAME TO resource_representations;
CREATE INDEX idx_resource_representations_platform ON resource_representations (platform_uid);

-- targets: accepted_resource_types → accepted_manifest_types
ALTER TABLE targets RENAME COLUMN accepted_resource_types TO accepted_manifest_types;

-- +goose Down
ALTER TABLE targets RENAME COLUMN accepted_manifest_types TO accepted_resource_types;

DROP INDEX IF EXISTS idx_resource_representations_platform;

CREATE TABLE resource_representations_old (
    platform_uid  TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    service_name  TEXT NOT NULL,
    version       TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    relative_name TEXT NOT NULL,
    roles         TEXT NOT NULL DEFAULT '[]',
    labels        TEXT NOT NULL DEFAULT '{}',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    deleted_at    TEXT,
    PRIMARY KEY (service_name, collection_id, relative_name)
);

INSERT INTO resource_representations_old
    (platform_uid, service_name, version, collection_id, relative_name, roles, labels, created_at, updated_at, deleted_at)
SELECT platform_uid, service_name, version, '', name, roles, labels, created_at, updated_at, deleted_at
FROM resource_representations;

DROP TABLE resource_representations;
ALTER TABLE resource_representations_old RENAME TO resource_representations;
CREATE INDEX idx_resource_representations_platform ON resource_representations (platform_uid);

ALTER TABLE platform_resources ADD COLUMN collection_id TEXT NOT NULL DEFAULT '';
ALTER TABLE platform_resources RENAME COLUMN name TO relative_name;
CREATE INDEX idx_platform_resources_collection ON platform_resources (collection_id);
