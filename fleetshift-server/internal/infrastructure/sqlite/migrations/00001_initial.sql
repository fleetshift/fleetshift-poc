-- +goose Up
CREATE TABLE targets (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    labels     TEXT NOT NULL DEFAULT '{}',
    properties TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE deployments (
    id                 TEXT PRIMARY KEY,
    manifest_strategy  TEXT NOT NULL,
    placement_strategy TEXT NOT NULL,
    rollout_strategy   TEXT,
    resolved_targets   TEXT NOT NULL DEFAULT '[]',
    state              TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE delivery_records (
    deployment_id TEXT NOT NULL,
    target_id     TEXT NOT NULL,
    manifests     TEXT NOT NULL DEFAULT '[]',
    state         TEXT NOT NULL DEFAULT 'pending',
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (deployment_id, target_id)
);

-- +goose Down
DROP TABLE delivery_records;
DROP TABLE deployments;
DROP TABLE targets;
