-- +goose Up

-- 1. extension_resource_types: split resource_type into (service_name, type_name)
ALTER TABLE extension_resource_types
    ADD COLUMN service_name TEXT,
    ADD COLUMN type_name    TEXT;

UPDATE extension_resource_types
SET service_name = split_part(resource_type, '/', 1),
    type_name    = split_part(resource_type, '/', 2);

ALTER TABLE extension_resource_types
    ALTER COLUMN service_name SET NOT NULL,
    ALTER COLUMN type_name    SET NOT NULL;

-- Drop the FK from extension_resources before removing the old PK.
ALTER TABLE extension_resources DROP CONSTRAINT IF EXISTS extension_resources_resource_type_fkey;

ALTER TABLE extension_resource_types DROP CONSTRAINT extension_resource_types_pkey;
ALTER TABLE extension_resource_types ADD PRIMARY KEY (service_name, type_name);
ALTER TABLE extension_resource_types DROP COLUMN resource_type;

-- 2. extension_resources: split resource_type, change uniqueness key
ALTER TABLE extension_resources
    ADD COLUMN service_name TEXT,
    ADD COLUMN type_name    TEXT;

UPDATE extension_resources
SET service_name = split_part(resource_type, '/', 1),
    type_name    = split_part(resource_type, '/', 2);

ALTER TABLE extension_resources
    ALTER COLUMN service_name SET NOT NULL,
    ALTER COLUMN type_name    SET NOT NULL;

ALTER TABLE extension_resources DROP CONSTRAINT IF EXISTS extension_resources_resource_type_resource_name_key;
ALTER TABLE extension_resources ADD CONSTRAINT extension_resources_service_name_resource_name_key
    UNIQUE (service_name, resource_name);

ALTER TABLE extension_resources DROP COLUMN resource_type;

ALTER TABLE extension_resources
    ADD CONSTRAINT extension_resources_type_fk
    FOREIGN KEY (service_name, type_name)
    REFERENCES extension_resource_types(service_name, type_name);

-- 3. resource_intents: rekey to (extension_resource_uid, version)
CREATE TABLE resource_intents_new (
    extension_resource_uid TEXT NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    version    INTEGER NOT NULL,
    spec       JSONB NOT NULL,
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
