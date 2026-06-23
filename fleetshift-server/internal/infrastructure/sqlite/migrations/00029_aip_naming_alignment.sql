-- +goose Up

-- platform_resources: replace (collection_id, relative_name) with
-- (collection_name, resource_id). collection_name is the full parent
-- collection path (e.g. "clusters" or "publishers/123/books") and
-- resource_id is the leaf segment (e.g. "prod"). This enables exact-match
-- listing by collection without prefix matching, which would over-include
-- descendants in nested collection hierarchies.
--
-- The old collection_id was a flat, single-segment value; relative_name
-- was the full collection-qualified path. We split relative_name at the
-- last '/' to derive the two new columns.
DROP INDEX IF EXISTS idx_platform_resources_collection;

CREATE TABLE platform_resources_new (
    uid             TEXT PRIMARY KEY,
    collection_name TEXT NOT NULL,
    resource_id     TEXT NOT NULL,
    labels          TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (collection_name, resource_id)
);

CREATE INDEX idx_platform_resources_collection ON platform_resources_new (collection_name);

-- Backfill: for single-slash names (e.g. "clusters/prod")
INSERT INTO platform_resources_new (uid, collection_name, resource_id, labels, created_at, updated_at)
SELECT uid,
       SUBSTR(relative_name, 1, INSTR(relative_name, '/') - 1),
       SUBSTR(relative_name, INSTR(relative_name, '/') + 1),
       labels, created_at, updated_at
FROM platform_resources
WHERE LENGTH(relative_name) - LENGTH(REPLACE(relative_name, '/', '')) = 1;

-- Backfill: for multi-slash names (e.g. "publishers/123/books/les-mis")
-- Use a recursive CTE to locate the last '/' position.
-- +goose StatementBegin
INSERT INTO platform_resources_new (uid, collection_name, resource_id, labels, created_at, updated_at)
WITH RECURSIVE split(uid, name, pos, last_pos, labels, created_at, updated_at) AS (
    SELECT uid, relative_name, 1, 0, labels, created_at, updated_at
    FROM platform_resources
    WHERE LENGTH(relative_name) - LENGTH(REPLACE(relative_name, '/', '')) > 1
    UNION ALL
    SELECT uid, name, pos + 1,
           CASE WHEN SUBSTR(name, pos, 1) = '/' THEN pos ELSE last_pos END,
           labels, created_at, updated_at
    FROM split
    WHERE pos <= LENGTH(name)
)
SELECT uid,
       SUBSTR(name, 1, last_pos - 1),
       SUBSTR(name, last_pos + 1),
       labels, created_at, updated_at
FROM split
WHERE pos > LENGTH(name);
-- +goose StatementEnd

DROP TABLE platform_resources;
ALTER TABLE platform_resources_new RENAME TO platform_resources;

-- resource_representations: same split.
-- PK changes from (service_name, collection_id, relative_name) to
-- (service_name, collection_name, resource_id).
DROP INDEX IF EXISTS idx_resource_representations_platform;

CREATE TABLE resource_representations_new (
    platform_uid    TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    service_name    TEXT NOT NULL,
    version         TEXT NOT NULL,
    collection_name TEXT NOT NULL,
    resource_id     TEXT NOT NULL,
    roles           TEXT NOT NULL DEFAULT '[]',
    labels          TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    deleted_at      TEXT,
    PRIMARY KEY (service_name, collection_name, resource_id)
);

CREATE INDEX idx_resource_representations_platform ON resource_representations_new (platform_uid);

-- Backfill representations: single-slash names.
INSERT INTO resource_representations_new (platform_uid, service_name, version, collection_name, resource_id, roles, labels, created_at, updated_at, deleted_at)
SELECT platform_uid, service_name, version,
       SUBSTR(relative_name, 1, INSTR(relative_name, '/') - 1),
       SUBSTR(relative_name, INSTR(relative_name, '/') + 1),
       roles, labels, created_at, updated_at, deleted_at
FROM resource_representations
WHERE LENGTH(relative_name) - LENGTH(REPLACE(relative_name, '/', '')) = 1;

-- Backfill representations: multi-slash names.
-- +goose StatementBegin
INSERT INTO resource_representations_new (platform_uid, service_name, version, collection_name, resource_id, roles, labels, created_at, updated_at, deleted_at)
WITH RECURSIVE split(platform_uid, service_name, version, name, pos, last_pos, roles, labels, created_at, updated_at, deleted_at) AS (
    SELECT platform_uid, service_name, version, relative_name, 1, 0, roles, labels, created_at, updated_at, deleted_at
    FROM resource_representations
    WHERE LENGTH(relative_name) - LENGTH(REPLACE(relative_name, '/', '')) > 1
    UNION ALL
    SELECT platform_uid, service_name, version, name, pos + 1,
           CASE WHEN SUBSTR(name, pos, 1) = '/' THEN pos ELSE last_pos END,
           roles, labels, created_at, updated_at, deleted_at
    FROM split
    WHERE pos <= LENGTH(name)
)
SELECT platform_uid, service_name, version,
       SUBSTR(name, 1, last_pos - 1),
       SUBSTR(name, last_pos + 1),
       roles, labels, created_at, updated_at, deleted_at
FROM split
WHERE pos > LENGTH(name);
-- +goose StatementEnd

DROP TABLE resource_representations;
ALTER TABLE resource_representations_new RENAME TO resource_representations;

-- targets: accepted_resource_types → accepted_manifest_types
ALTER TABLE targets RENAME COLUMN accepted_resource_types TO accepted_manifest_types;

-- +goose Down
-- WARNING: this rollback is lossy for nested collections. It stores
-- the full collection path (e.g. "publishers/123/books") in the old
-- collection_id column, but pre-migration collection_id was flat
-- (e.g. "books"). SQLite has no procedural guard to fail fast, so if
-- nested data exists the collection_id semantics will be silently
-- corrupted. Only roll back if you are certain no nested-collection
-- resources have been created.

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
SELECT platform_uid, service_name, version, collection_name, collection_name || '/' || resource_id, roles, labels, created_at, updated_at, deleted_at
FROM resource_representations;

DROP TABLE resource_representations;
ALTER TABLE resource_representations_old RENAME TO resource_representations;
CREATE INDEX idx_resource_representations_platform ON resource_representations (platform_uid);

DROP INDEX IF EXISTS idx_platform_resources_collection;

CREATE TABLE platform_resources_old (
    uid           TEXT PRIMARY KEY,
    collection_id TEXT NOT NULL,
    relative_name TEXT NOT NULL UNIQUE,
    labels        TEXT NOT NULL DEFAULT '{}',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    deleted_at    TEXT
);

INSERT INTO platform_resources_old (uid, collection_id, relative_name, labels, created_at, updated_at)
SELECT uid, collection_name, collection_name || '/' || resource_id, labels, created_at, updated_at
FROM platform_resources;

DROP TABLE platform_resources;
ALTER TABLE platform_resources_old RENAME TO platform_resources;
CREATE INDEX idx_platform_resources_collection ON platform_resources (collection_id);
