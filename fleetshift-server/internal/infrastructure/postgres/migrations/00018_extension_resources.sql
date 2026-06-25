-- +goose Up
CREATE TABLE extension_resource_types (
    resource_type     TEXT PRIMARY KEY,
    api_version       TEXT NOT NULL,
    collection_id     TEXT NOT NULL,
    management        JSONB,
    created_at        TIMESTAMPTZ NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL
);

CREATE TABLE extension_resources (
    uid            TEXT PRIMARY KEY,
    resource_type  TEXT NOT NULL REFERENCES extension_resource_types(resource_type),
    resource_name  TEXT NOT NULL,
    labels         JSONB NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    UNIQUE(resource_type, resource_name)
);

CREATE TABLE extension_resource_managed (
    extension_resource_uid TEXT PRIMARY KEY REFERENCES extension_resources(uid) ON DELETE CASCADE,
    current_version        INTEGER NOT NULL,
    fulfillment_id         TEXT NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS extension_resource_managed;
DROP TABLE IF EXISTS extension_resources;
DROP TABLE IF EXISTS extension_resource_types;
