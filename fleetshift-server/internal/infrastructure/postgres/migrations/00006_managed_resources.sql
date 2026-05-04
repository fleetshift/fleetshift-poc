-- +goose Up
CREATE TABLE managed_resource_types (
    resource_type TEXT NOT NULL PRIMARY KEY,
    relation      JSONB NOT NULL,
    signature     JSONB NOT NULL,
    spec_schema   JSONB,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE TABLE resource_intents (
    resource_type TEXT NOT NULL,
    name          TEXT NOT NULL,
    version       INTEGER NOT NULL,
    spec          JSONB NOT NULL,
    created_at    TEXT NOT NULL,
    PRIMARY KEY (resource_type, name, version)
);

CREATE TABLE managed_resources (
    resource_type   TEXT NOT NULL,
    name            TEXT NOT NULL,
    uid             TEXT NOT NULL UNIQUE,
    current_version INTEGER NOT NULL,
    fulfillment_id  TEXT NOT NULL REFERENCES fulfillments(id),
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    deleted_at      TEXT,
    PRIMARY KEY (resource_type, name)
);

-- +goose Down
DROP TABLE managed_resources;
DROP TABLE resource_intents;
DROP TABLE managed_resource_types;
