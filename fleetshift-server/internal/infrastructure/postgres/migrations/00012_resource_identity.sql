-- +goose Up
-- Canonical platform resource identity tables (Phase 2).

CREATE TABLE platform_resources (
    uid           TEXT PRIMARY KEY,
    collection_id TEXT NOT NULL,
    relative_name TEXT NOT NULL UNIQUE,
    labels        JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    deleted_at    TIMESTAMPTZ
);

CREATE INDEX idx_platform_resources_collection ON platform_resources (collection_id);

CREATE TABLE resource_representations (
    platform_uid  TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    service_name  TEXT NOT NULL,
    version       TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    relative_name TEXT NOT NULL,
    roles         JSONB NOT NULL DEFAULT '[]',
    labels        JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    deleted_at    TIMESTAMPTZ,
    PRIMARY KEY (service_name, collection_id, relative_name)
);

CREATE INDEX idx_resource_representations_platform ON resource_representations (platform_uid);

CREATE TABLE resource_aliases (
    namespace    TEXT NOT NULL,
    key          TEXT NOT NULL,
    value        TEXT NOT NULL,
    platform_uid TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace, key, value)
);

CREATE INDEX idx_resource_aliases_platform ON resource_aliases (platform_uid);

CREATE TABLE resource_relationships (
    source_uid     TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    type           TEXT NOT NULL,
    target_uid     TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    source_service TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_uid, type, target_uid)
);

-- +goose Down
DROP TABLE IF EXISTS resource_relationships;
DROP TABLE IF EXISTS resource_aliases;
DROP TABLE IF EXISTS resource_representations;
DROP TABLE IF EXISTS platform_resources;
