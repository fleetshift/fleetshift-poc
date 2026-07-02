-- +goose Up

-- ── Standalone tables (no foreign keys) ─────────────────────────

CREATE TABLE targets (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL UNIQUE,
    type                   TEXT NOT NULL DEFAULT '',
    state                  TEXT NOT NULL DEFAULT 'ready',
    labels                 TEXT NOT NULL DEFAULT '{}',
    properties             TEXT NOT NULL DEFAULT '{}',
    accepted_manifest_types TEXT NOT NULL DEFAULT '[]',
    inventory_item_id      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE inventory_items (
    id                 TEXT PRIMARY KEY,
    type               TEXT NOT NULL,
    name               TEXT NOT NULL,
    properties         TEXT NOT NULL DEFAULT '{}',
    labels             TEXT NOT NULL DEFAULT '{}',
    source_delivery_id TEXT,
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at         TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_inventory_items_type ON inventory_items(type);

CREATE TABLE auth_methods (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    config_json TEXT NOT NULL
);

CREATE TABLE vault_secrets (
    ref TEXT PRIMARY KEY,
    val BLOB NOT NULL
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
    resolved_targets           TEXT NOT NULL DEFAULT '[]',
    state                      TEXT NOT NULL DEFAULT 'creating',
    status_reason              TEXT NOT NULL DEFAULT '',
    auth                       TEXT NOT NULL DEFAULT '{}',
    provenance                 TEXT,
    attestation_ref            TEXT,
    generation                 INTEGER NOT NULL DEFAULT 1,
    observed_generation        INTEGER NOT NULL DEFAULT 0,
    active_workflow_gen        INTEGER,
    pause_reason               TEXT NOT NULL DEFAULT '',
    created_at                 TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at                 TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE deployments (
    name           TEXT PRIMARY KEY,
    uid            TEXT NOT NULL DEFAULT '',
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id),
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE delivery_records (
    fulfillment_id TEXT NOT NULL,
    target_id      TEXT NOT NULL,
    id             TEXT NOT NULL DEFAULT '',
    manifests      TEXT NOT NULL DEFAULT '[]',
    state          TEXT NOT NULL DEFAULT 'pending',
    generation     INTEGER NOT NULL DEFAULT 0,
    operation      TEXT NOT NULL DEFAULT 'deliver',
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (fulfillment_id, target_id)
);

CREATE TABLE manifest_strategies (
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id) ON DELETE CASCADE,
    version        INTEGER NOT NULL,
    spec           TEXT NOT NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (fulfillment_id, version)
);

CREATE TABLE placement_strategies (
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id) ON DELETE CASCADE,
    version        INTEGER NOT NULL,
    spec           TEXT NOT NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (fulfillment_id, version)
);

CREATE TABLE rollout_strategies (
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id) ON DELETE CASCADE,
    version        INTEGER NOT NULL,
    spec           TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (fulfillment_id, version)
);

-- ── Platform resource identity ──────────────────────────────────
--
-- No uid column: per AIP-148, a UID is only warranted for resources
-- that can be deleted and recreated under the same name yet still
-- need to be told apart across that gap. Platform resources have no
-- such generational concept -- (collection_name, resource_id) is the
-- sole, permanent identifier, so it's the primary key directly.
--
-- Platform resources are also virtual by default: a name with
-- representations (derived from extension_resources, see below) but
-- no labels/aliases/relationships of its own never needs a physical
-- row here at all. A row only exists once something -- labels, an
-- alias, a relationship -- actually needs to be stored against the
-- name.

CREATE TABLE platform_resources (
    collection_name TEXT NOT NULL,
    resource_id     TEXT NOT NULL,
    labels          TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (collection_name, resource_id)
);

-- Two unique indexes make value<->resource a bijection per
-- (namespace, key): the primary key stops two different resources
-- from claiming the same value, and this second index stops the same
-- resource from claiming two different values for the same key. Both
-- are checked by a single blind INSERT OR IGNORE (SQLite has no
-- multi-target ON CONFLICT), so a contradictory alias upsert is
-- caught without any read before the insert (see
-- extension_resource_repo.go's inventory-report alias fold-in).
CREATE TABLE resource_aliases (
    namespace                TEXT NOT NULL,
    key                      TEXT NOT NULL,
    value                    TEXT NOT NULL,
    platform_collection_name TEXT NOT NULL,
    platform_resource_id     TEXT NOT NULL,
    created_at               TEXT NOT NULL,
    PRIMARY KEY (namespace, key, value),
    UNIQUE (namespace, key, platform_collection_name, platform_resource_id),
    FOREIGN KEY (platform_collection_name, platform_resource_id)
        REFERENCES platform_resources(collection_name, resource_id) ON DELETE CASCADE
);

CREATE INDEX idx_resource_aliases_platform ON resource_aliases(platform_collection_name, platform_resource_id);

CREATE TABLE resource_relationships (
    source_collection_name TEXT NOT NULL,
    source_resource_id     TEXT NOT NULL,
    type                   TEXT NOT NULL,
    target_collection_name TEXT NOT NULL,
    target_resource_id     TEXT NOT NULL,
    source_service         TEXT NOT NULL,
    created_at             TEXT NOT NULL,
    PRIMARY KEY (source_collection_name, source_resource_id, type, target_collection_name, target_resource_id),
    FOREIGN KEY (source_collection_name, source_resource_id)
        REFERENCES platform_resources(collection_name, resource_id) ON DELETE CASCADE,
    FOREIGN KEY (target_collection_name, target_resource_id)
        REFERENCES platform_resources(collection_name, resource_id) ON DELETE CASCADE
);

-- ── Extension resources ─────────────────────────────────────────

CREATE TABLE extension_resource_types (
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

-- collection_name/resource_id mirror platform_resources so a
-- representation can be derived on read by joining the two tables on
-- that pair, rather than maintained as its own reconciled table (see
-- resource_representations' removal below).
CREATE TABLE extension_resources (
    uid             TEXT PRIMARY KEY,
    service_name    TEXT NOT NULL,
    type_name       TEXT NOT NULL,
    collection_name TEXT NOT NULL,
    resource_id     TEXT NOT NULL,
    labels          TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (service_name, collection_name, resource_id),
    FOREIGN KEY (service_name, type_name)
        REFERENCES extension_resource_types(service_name, type_name)
);

-- Supports deriving representations for a platform resource: join on
-- (collection_name, resource_id) rather than service_name-prefixed.
CREATE INDEX idx_extension_resources_collection_resource
    ON extension_resources(collection_name, resource_id);

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
    spec       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (extension_resource_uid, version)
);

-- Representations are not persisted: a platform resource's
-- representations are derived on read by joining extension_resources
-- to platform_resources on (collection_name, resource_id) and to
-- extension_resource_types for its declared roles/version. This
-- removes the read-modify-write reconciliation that used to be
-- required on every extension resource create/delete, and means a
-- managed resource's representation disappears exactly when its
-- extension_resources row is physically deleted (not before).

-- ── Extension resource inventory ────────────────────────────────

CREATE TABLE extension_resource_inventory (
    extension_resource_uid TEXT PRIMARY KEY
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    observation TEXT,
    observed_at TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- Normalized out of extension_resource_inventory.labels (previously
-- JSON text) so batch label writes can be blind multi-row
-- upserts/deletes against a real table instead of a
-- read-modify-write on a JSON blob.
CREATE TABLE extension_resource_inventory_labels (
    extension_resource_uid TEXT NOT NULL
        REFERENCES extension_resource_inventory(extension_resource_uid) ON DELETE CASCADE,
    key                    TEXT NOT NULL,
    value                  TEXT NOT NULL,
    PRIMARY KEY (extension_resource_uid, key)
);

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

-- +goose Down
DROP TABLE IF EXISTS extension_resource_inventory_observations;
DROP TABLE IF EXISTS extension_resource_inventory_condition_events;
DROP TABLE IF EXISTS extension_resource_inventory_conditions;
DROP TABLE IF EXISTS extension_resource_inventory_labels;
DROP TABLE IF EXISTS extension_resource_inventory;
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
