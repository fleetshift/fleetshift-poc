-- +goose Up
CREATE TABLE targets (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL UNIQUE,
    type                   TEXT NOT NULL DEFAULT '',
    state                  TEXT NOT NULL DEFAULT 'ready',
    labels                 JSONB NOT NULL DEFAULT '{}',
    properties             JSONB NOT NULL DEFAULT '{}',
    accepted_resource_types JSONB NOT NULL DEFAULT '[]',
    inventory_item_id      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE deployments (
    id                  TEXT PRIMARY KEY,
    uid                 TEXT NOT NULL DEFAULT '',
    manifest_strategy   TEXT NOT NULL,
    placement_strategy  TEXT NOT NULL,
    rollout_strategy    JSONB,
    resolved_targets    JSONB NOT NULL DEFAULT '[]',
    state               TEXT NOT NULL DEFAULT 'creating',
    auth                JSONB NOT NULL DEFAULT '{}',
    provenance          JSONB,
    generation          INTEGER NOT NULL DEFAULT 1,
    observed_generation INTEGER NOT NULL DEFAULT 0,
    active_workflow_gen INTEGER,
    created_at          TEXT NOT NULL DEFAULT NOW(),
    updated_at          TEXT NOT NULL DEFAULT NOW(),
    etag                TEXT NOT NULL DEFAULT ''
);

CREATE TABLE delivery_records (
    deployment_id TEXT NOT NULL,
    target_id     TEXT NOT NULL,
    id            TEXT NOT NULL DEFAULT '',
    manifests     JSONB NOT NULL DEFAULT '[]',
    state         TEXT NOT NULL DEFAULT 'pending',
    created_at    TEXT NOT NULL DEFAULT NOW(),
    updated_at    TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (deployment_id, target_id)
);

CREATE TABLE auth_methods (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    config_json JSONB NOT NULL
);

CREATE TABLE vault_secrets (
    ref  TEXT PRIMARY KEY,
    val  BYTEA NOT NULL
);

CREATE TABLE inventory_items (
    id                 TEXT PRIMARY KEY,
    type               TEXT NOT NULL,
    name               TEXT NOT NULL,
    properties         JSONB NOT NULL DEFAULT '{}',
    labels             JSONB NOT NULL DEFAULT '{}',
    source_delivery_id TEXT,
    created_at         TEXT NOT NULL DEFAULT NOW(),
    updated_at         TEXT NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_inventory_items_type ON inventory_items(type);

CREATE TABLE signer_enrollments (
    id               TEXT PRIMARY KEY,
    subject_id       TEXT NOT NULL,
    issuer           TEXT NOT NULL,
    identity_token   TEXT NOT NULL,
    registry_subject TEXT NOT NULL,
    registry_id      TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    expires_at       TEXT NOT NULL
);

CREATE INDEX idx_se_subject ON signer_enrollments(subject_id, issuer);

-- +goose Down
DROP INDEX IF EXISTS idx_se_subject;
DROP TABLE IF EXISTS signer_enrollments;
DROP TABLE IF EXISTS inventory_items;
DROP TABLE IF EXISTS vault_secrets;
DROP TABLE IF EXISTS auth_methods;
DROP TABLE IF EXISTS delivery_records;
DROP TABLE IF EXISTS deployments;
DROP TABLE IF EXISTS targets;
