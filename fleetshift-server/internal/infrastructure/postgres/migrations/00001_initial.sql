-- +goose Up

-- ── Standalone tables (no foreign keys) ─────────────────────────

CREATE TABLE targets (
    id                      TEXT PRIMARY KEY,
    name                    TEXT NOT NULL UNIQUE,
    type                    TEXT NOT NULL DEFAULT '',
    state                   TEXT NOT NULL DEFAULT 'ready',
    labels                  JSONB NOT NULL DEFAULT '{}',
    properties              JSONB NOT NULL DEFAULT '{}',
    accepted_manifest_types JSONB NOT NULL DEFAULT '[]',
    inventory_item_id       TEXT NOT NULL DEFAULT ''
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

CREATE TABLE auth_methods (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    config_json JSONB NOT NULL
);

CREATE TABLE vault_secrets (
    ref TEXT PRIMARY KEY,
    val BYTEA NOT NULL
);

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

-- ── Fulfillment cluster ─────────────────────────────────────────

CREATE TABLE fulfillments (
    id                         TEXT PRIMARY KEY,
    manifest_strategy_version  INTEGER NOT NULL DEFAULT 0,
    placement_strategy_version INTEGER NOT NULL DEFAULT 0,
    rollout_strategy_version   INTEGER NOT NULL DEFAULT 0,
    resolved_targets           JSONB NOT NULL DEFAULT '[]',
    state                      TEXT NOT NULL DEFAULT 'creating',
    status_reason              TEXT NOT NULL DEFAULT '',
    auth                       JSONB NOT NULL DEFAULT '{}',
    provenance                 TEXT,
    attestation_ref            JSONB,
    generation                 INTEGER NOT NULL DEFAULT 1,
    observed_generation        INTEGER NOT NULL DEFAULT 0,
    active_workflow_gen        INTEGER,
    pause_reason               TEXT NOT NULL DEFAULT '',
    created_at                 TEXT NOT NULL DEFAULT NOW(),
    updated_at                 TEXT NOT NULL DEFAULT NOW()
);

CREATE TABLE deployments (
    name           TEXT PRIMARY KEY,
    uid            UUID NOT NULL,
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id),
    created_at     TEXT NOT NULL DEFAULT NOW(),
    updated_at     TEXT NOT NULL DEFAULT NOW()
);

CREATE TABLE delivery_records (
    fulfillment_id TEXT NOT NULL,
    target_id      TEXT NOT NULL,
    id             TEXT NOT NULL DEFAULT '',
    manifests      JSONB NOT NULL DEFAULT '[]',
    state          TEXT NOT NULL DEFAULT 'pending',
    generation     BIGINT NOT NULL DEFAULT 0,
    operation      TEXT NOT NULL DEFAULT 'deliver',
    created_at     TEXT NOT NULL DEFAULT NOW(),
    updated_at     TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fulfillment_id, target_id)
);

CREATE TABLE manifest_strategies (
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id) ON DELETE CASCADE,
    version        INTEGER NOT NULL,
    spec           JSONB NOT NULL,
    created_at     TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fulfillment_id, version)
);

CREATE TABLE placement_strategies (
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id) ON DELETE CASCADE,
    version        INTEGER NOT NULL,
    spec           JSONB NOT NULL,
    created_at     TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fulfillment_id, version)
);

CREATE TABLE rollout_strategies (
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id) ON DELETE CASCADE,
    version        INTEGER NOT NULL,
    spec           JSONB,
    created_at     TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fulfillment_id, version)
);

-- ── Platform resource identity ──────────────────────────────────

CREATE TABLE platform_resources (
    uid             UUID PRIMARY KEY,
    collection_name TEXT NOT NULL,
    resource_id     TEXT NOT NULL,
    labels          JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (collection_name, resource_id)
);

CREATE INDEX idx_platform_resources_collection ON platform_resources(collection_name);

CREATE TABLE resource_aliases (
    namespace    TEXT NOT NULL,
    key          TEXT NOT NULL,
    value        TEXT NOT NULL,
    platform_uid UUID NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace, key, value)
);

CREATE INDEX idx_resource_aliases_platform ON resource_aliases(platform_uid);

CREATE TABLE resource_relationships (
    source_uid     UUID NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    type           TEXT NOT NULL,
    target_uid     UUID NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    source_service TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_uid, type, target_uid)
);

-- ── Extension resources ─────────────────────────────────────────

CREATE TABLE extension_resource_types (
    service_name  TEXT NOT NULL,
    type_name     TEXT NOT NULL,
    api_version   TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    management    JSONB,
    inventory     JSONB,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (service_name, type_name)
);

CREATE TABLE extension_resources (
    uid           TEXT PRIMARY KEY,
    service_name  TEXT NOT NULL,
    type_name     TEXT NOT NULL,
    resource_name TEXT NOT NULL,
    labels        JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    UNIQUE (service_name, resource_name),
    FOREIGN KEY (service_name, type_name)
        REFERENCES extension_resource_types(service_name, type_name)
);

CREATE TABLE extension_resource_managed (
    extension_resource_uid TEXT PRIMARY KEY
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    current_version        INTEGER NOT NULL,
    fulfillment_id         TEXT NOT NULL
);

CREATE TABLE resource_intents (
    extension_resource_uid TEXT NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    version    INTEGER NOT NULL,
    spec       JSONB NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (extension_resource_uid, version)
);

CREATE TABLE resource_representations (
    platform_uid           UUID NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    service_name           TEXT NOT NULL,
    version                TEXT NOT NULL,
    collection_name        TEXT NOT NULL,
    resource_id            TEXT NOT NULL,
    extension_resource_uid TEXT NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    created_at             TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (service_name, collection_name, resource_id)
);

CREATE INDEX idx_resource_representations_platform ON resource_representations(platform_uid);

-- ── Extension resource inventory ────────────────────────────────

CREATE TABLE extension_resource_inventory (
    extension_resource_uid TEXT PRIMARY KEY
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    labels      JSONB NOT NULL DEFAULT '{}',
    observation JSONB,
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

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

-- +goose Down
DROP TABLE IF EXISTS extension_resource_inventory_observations;
DROP TABLE IF EXISTS extension_resource_inventory_condition_events;
DROP TABLE IF EXISTS extension_resource_inventory_conditions;
DROP TABLE IF EXISTS extension_resource_inventory;
DROP TABLE IF EXISTS resource_representations;
DROP TABLE IF EXISTS resource_intents;
DROP TABLE IF EXISTS extension_resource_managed;
DROP TABLE IF EXISTS extension_resources;
DROP TABLE IF EXISTS extension_resource_types;
DROP TABLE IF EXISTS resource_relationships;
DROP TABLE IF EXISTS resource_aliases;
DROP TABLE IF EXISTS platform_resources;
DROP TABLE IF EXISTS rollout_strategies;
DROP TABLE IF EXISTS placement_strategies;
DROP TABLE IF EXISTS manifest_strategies;
DROP TABLE IF EXISTS delivery_records;
DROP TABLE IF EXISTS deployments;
DROP TABLE IF EXISTS fulfillments;
DROP TABLE IF EXISTS signer_enrollments;
DROP TABLE IF EXISTS vault_secrets;
DROP TABLE IF EXISTS auth_methods;
DROP TABLE IF EXISTS inventory_items;
DROP TABLE IF EXISTS targets;
