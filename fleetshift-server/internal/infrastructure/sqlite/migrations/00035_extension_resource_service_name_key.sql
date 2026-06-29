-- +goose Up

-- 1. extension_resource_types: split resource_type into (service_name, type_name)
--    SQLite lacks ALTER COLUMN / DROP COLUMN in older versions, so
--    rebuild the table.
CREATE TABLE extension_resource_types_new (
    service_name  TEXT NOT NULL,
    type_name     TEXT NOT NULL,
    api_version   TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    management    TEXT,
    inventory     TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (service_name, type_name)
);

INSERT INTO extension_resource_types_new
    (service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at)
SELECT
    substr(resource_type, 1, instr(resource_type, '/') - 1),
    substr(resource_type, instr(resource_type, '/') + 1),
    api_version, collection_id, management, inventory, created_at, updated_at
FROM extension_resource_types;

DROP TABLE extension_resource_types;
ALTER TABLE extension_resource_types_new RENAME TO extension_resource_types;

-- 2. extension_resources: split resource_type, change uniqueness key
CREATE TABLE extension_resources_new (
    uid           TEXT PRIMARY KEY,
    service_name  TEXT NOT NULL,
    type_name     TEXT NOT NULL,
    resource_name TEXT NOT NULL,
    labels        TEXT NOT NULL DEFAULT '{}',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    UNIQUE(service_name, resource_name),
    FOREIGN KEY (service_name, type_name)
        REFERENCES extension_resource_types(service_name, type_name)
);

INSERT INTO extension_resources_new
    (uid, service_name, type_name, resource_name, labels, created_at, updated_at)
SELECT
    uid,
    substr(resource_type, 1, instr(resource_type, '/') - 1),
    substr(resource_type, instr(resource_type, '/') + 1),
    resource_name, labels, created_at, updated_at
FROM extension_resources;

DROP TABLE extension_resources;
ALTER TABLE extension_resources_new RENAME TO extension_resources;

-- Re-create child tables that reference extension_resources(uid) since
-- SQLite foreign keys reference the table name and the old table was
-- dropped. The managed table is small and always present.
CREATE TABLE extension_resource_managed_new (
    extension_resource_uid TEXT PRIMARY KEY
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    current_version        INTEGER NOT NULL,
    fulfillment_id         TEXT NOT NULL
);

INSERT INTO extension_resource_managed_new
SELECT * FROM extension_resource_managed;

DROP TABLE extension_resource_managed;
ALTER TABLE extension_resource_managed_new RENAME TO extension_resource_managed;

-- 3. resource_intents: rekey to (extension_resource_uid, version)
CREATE TABLE resource_intents_new (
    extension_resource_uid TEXT NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    version    INTEGER NOT NULL,
    spec       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (extension_resource_uid, version)
);

INSERT INTO resource_intents_new (extension_resource_uid, version, spec, created_at)
SELECT er.uid, ri.version, ri.spec, ri.created_at
FROM resource_intents ri
JOIN extension_resources er
    ON er.service_name || '/' || er.type_name = ri.resource_type
   AND er.resource_name = ri.name;

DROP TABLE resource_intents;
ALTER TABLE resource_intents_new RENAME TO resource_intents;

-- +goose Down
-- Reversing this migration is destructive and not supported.
