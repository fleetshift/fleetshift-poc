-- +goose Up
ALTER TABLE extension_resource_types
    ADD COLUMN inventory JSONB;

CREATE TABLE extension_resource_inventory (
    extension_resource_uid TEXT PRIMARY KEY
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    labels       JSONB NOT NULL DEFAULT '{}',
    observation  JSONB NOT NULL DEFAULT '{}',
    observed_at  TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);

-- Latest condition state per (resource, condition type). Materialized
-- separately from the condition transition history so the common read
-- path and dedup comparison are both a simple PK lookup.
CREATE TABLE extension_resource_inventory_conditions (
    extension_resource_uid TEXT NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    type                   TEXT NOT NULL,
    status                 TEXT NOT NULL,
    reason                 TEXT NOT NULL DEFAULT '',
    message                TEXT NOT NULL DEFAULT '',
    last_transition_time   TIMESTAMPTZ NOT NULL,
    observed_at            TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (extension_resource_uid, type)
);

CREATE TABLE extension_resource_inventory_observations (
    id                     TEXT PRIMARY KEY,
    extension_resource_uid TEXT NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    observation            JSONB NOT NULL DEFAULT '{}',
    observed_at            TIMESTAMPTZ NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_er_inv_observations_resource
    ON extension_resource_inventory_observations(extension_resource_uid, observed_at);

CREATE TABLE extension_resource_inventory_condition_events (
    id                     TEXT PRIMARY KEY,
    extension_resource_uid TEXT NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    type                   TEXT NOT NULL,
    status                 TEXT NOT NULL,
    reason                 TEXT NOT NULL DEFAULT '',
    message                TEXT NOT NULL DEFAULT '',
    last_transition_time   TIMESTAMPTZ NOT NULL,
    observed_at            TIMESTAMPTZ NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_er_inv_condition_events_resource
    ON extension_resource_inventory_condition_events(extension_resource_uid, type, observed_at);

-- +goose Down
DROP INDEX IF EXISTS idx_er_inv_condition_events_resource;
DROP TABLE IF EXISTS extension_resource_inventory_condition_events;
DROP INDEX IF EXISTS idx_er_inv_observations_resource;
DROP TABLE IF EXISTS extension_resource_inventory_observations;
DROP TABLE IF EXISTS extension_resource_inventory_conditions;
DROP TABLE IF EXISTS extension_resource_inventory;
ALTER TABLE extension_resource_types DROP COLUMN IF EXISTS inventory;
