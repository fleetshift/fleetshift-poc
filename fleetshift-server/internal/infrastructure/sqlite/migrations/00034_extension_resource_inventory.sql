-- +goose Up
ALTER TABLE extension_resource_types
    ADD COLUMN inventory TEXT;

CREATE TABLE extension_resource_inventory (
    extension_resource_uid TEXT PRIMARY KEY
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    labels       TEXT NOT NULL DEFAULT '{}',
    observation  TEXT NOT NULL DEFAULT '{}',
    observed_at  TEXT NOT NULL,
    updated_at   TEXT NOT NULL
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
    last_transition_time   TEXT NOT NULL,
    observed_at            TEXT NOT NULL,
    updated_at             TEXT NOT NULL,
    PRIMARY KEY (extension_resource_uid, type)
);

CREATE TABLE extension_resource_inventory_observations (
    id                     TEXT PRIMARY KEY,
    extension_resource_uid TEXT NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    observation            TEXT NOT NULL DEFAULT '{}',
    observed_at            TEXT NOT NULL,
    created_at             TEXT NOT NULL
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
    last_transition_time   TEXT NOT NULL,
    observed_at            TEXT NOT NULL,
    created_at             TEXT NOT NULL
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
-- SQLite does not support DROP COLUMN; the inventory column is left in place on rollback.
