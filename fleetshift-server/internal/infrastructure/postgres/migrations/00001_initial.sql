-- +goose Up

-- btree_gist supplies GiST-compatible operator classes for plain
-- scalar types (text, uuid, ...), which resource_aliases' EXCLUDE
-- constraints need below -- EXCLUDE requires a GiST (or SP-GiST)
-- index, which the builtin B-tree operator classes alone can't back.
CREATE EXTENSION IF NOT EXISTS btree_gist;

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
    labels          JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (collection_name, resource_id)
);

CREATE TABLE resource_relationships (
    source_collection_name TEXT NOT NULL,
    source_resource_id     TEXT NOT NULL,
    type                   TEXT NOT NULL,
    target_collection_name TEXT NOT NULL,
    target_resource_id     TEXT NOT NULL,
    source_service         TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL,
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
    management    JSONB,
    inventory     JSONB,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (service_name, type_name)
);

-- collection_name/resource_id mirror platform_resources so a
-- representation can be derived on read by joining the two tables on
-- that pair, rather than maintained as its own reconciled table (see
-- resource_representations' removal below).
CREATE TABLE extension_resources (
    uid             UUID PRIMARY KEY,
    service_name    TEXT NOT NULL,
    type_name       TEXT NOT NULL,
    collection_name TEXT NOT NULL,
    resource_id     TEXT NOT NULL,
    labels          JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (service_name, collection_name, resource_id),
    FOREIGN KEY (service_name, type_name)
        REFERENCES extension_resource_types(service_name, type_name)
);

-- Supports deriving representations for a platform resource: join on
-- (collection_name, resource_id) rather than service_name-prefixed.
CREATE INDEX idx_extension_resources_collection_resource
    ON extension_resources(collection_name, resource_id);

-- Aliases are contributed per extension resource, additive across
-- the (possibly many) extension resources that represent the same
-- platform resource, and replaced independently per contributor --
-- see docs/design/architecture/resource_identity_and_api.md's
-- "Aliases" section for the full contract this table enforces, and
-- extension_resource_repo.go's alias fold-in for how a report is
-- turned into inserts/deletes against it.
--
-- source_extension_resource_uid makes a contributor's claim its own
-- row rather than collapsing every contributor's identical claim
-- into one: many rows can legitimately share (namespace, key, value)
-- -- e.g. two addons agreeing a cluster's instance_id is "i-123" --
-- and the alias only disappears once every contributing row does
-- (whether via an inventory report that stops asserting it, or via
-- that extension resource's own deletion -- ON DELETE CASCADE here
-- covers the latter for free). It's nullable to also accommodate
-- resource_identity_repo.go's reconcileAliases, which attaches an
-- alias directly to a platform resource with no extension resource
-- of its own behind it at all.
--
-- Two EXCLUDE constraints (needing btree_gist -- see this file's
-- CREATE EXTENSION above) replace what used to be a pair of plain
-- UNIQUE indexes, back when (namespace, key, value) alone was the
-- primary key and a row could have only one contributor by
-- construction: unique indexes can't express "these two rows may
-- coexist if source_extension_resource_uid matches, but not
-- otherwise," which is exactly what letting multiple contributors
-- corroborate the same claim, while still rejecting genuine
-- disagreement, requires.
--
--   - The first EXCLUDE keeps (namespace, key, value) a function of
--     resource: rows sharing all three may never disagree on which
--     resource they belong to, regardless of contributor. Needs the
--     collection_name/resource_id pair concatenated into one
--     expression, since EXCLUDE's WITH operators are per-column.
--   - The second EXCLUDE keeps (namespace, key, resource) a function
--     of value: rows sharing all three may never disagree on value,
--     regardless of contributor.
--
-- Both are DEFERRABLE INITIALLY DEFERRED, checked at end of
-- transaction/statement rather than per-row: a contributor legitimately
-- replacing its own value for a key runs as a delete-old-row +
-- insert-new-row pair within the very same statement (see
-- extension_resource_repo.go's del_aliases_replaced), and Postgres
-- doesn't guarantee the delete is visible to the insert's constraint
-- check within one statement unless that check is deferred to the
-- end. (The plain UNIQUE constraint below stays non-deferrable --
-- ON CONFLICT can't target a deferrable constraint as its arbiter --
-- but it never needs to be deferred anyway: re-asserting the exact
-- same (namespace, key, value, source) tuple was never a real
-- conflict in the first place, just a redundant write.)
CREATE TABLE resource_aliases (
    namespace                     TEXT NOT NULL,
    key                           TEXT NOT NULL,
    value                         TEXT NOT NULL,
    platform_collection_name      TEXT NOT NULL,
    platform_resource_id          TEXT NOT NULL,
    source_extension_resource_uid UUID
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    created_at                    TIMESTAMPTZ NOT NULL,
    UNIQUE NULLS NOT DISTINCT (namespace, key, value, source_extension_resource_uid),
    EXCLUDE USING gist (
        namespace WITH =,
        key WITH =,
        value WITH =,
        (platform_collection_name || '/' || platform_resource_id) WITH <>
    ) DEFERRABLE INITIALLY DEFERRED,
    EXCLUDE USING gist (
        namespace WITH =,
        key WITH =,
        platform_collection_name WITH =,
        platform_resource_id WITH =,
        value WITH <>
    ) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (platform_collection_name, platform_resource_id)
        REFERENCES platform_resources(collection_name, resource_id) ON DELETE CASCADE
);

CREATE INDEX idx_resource_aliases_platform ON resource_aliases(platform_collection_name, platform_resource_id);

-- Supports del_aliases_absent/del_aliases_replaced's per-contributor
-- lookups (extension_resource_repo.go), and ON DELETE CASCADE's own
-- lookup when an extension resource is deleted.
CREATE INDEX idx_resource_aliases_source ON resource_aliases(source_extension_resource_uid);

CREATE TABLE extension_resource_managed (
    extension_resource_uid UUID PRIMARY KEY
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    current_version        INTEGER NOT NULL,
    fulfillment_id         TEXT NOT NULL
);

CREATE TABLE resource_intents (
    extension_resource_uid UUID NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    version    INTEGER NOT NULL,
    spec       JSONB NOT NULL,
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
    extension_resource_uid UUID PRIMARY KEY
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    observation JSONB,
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

-- Normalized out of extension_resource_inventory.labels (previously
-- JSONB) so batch label writes can be blind multi-row upserts/deletes
-- against a real table instead of a read-modify-write on a JSON blob.
CREATE TABLE extension_resource_inventory_labels (
    extension_resource_uid UUID NOT NULL
        REFERENCES extension_resource_inventory(extension_resource_uid) ON DELETE CASCADE,
    key                    TEXT NOT NULL,
    value                  TEXT NOT NULL,
    PRIMARY KEY (extension_resource_uid, key)
);

CREATE TABLE extension_resource_inventory_conditions (
    extension_resource_uid UUID NOT NULL
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
    extension_resource_uid UUID NOT NULL
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
    extension_resource_uid UUID NOT NULL
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
DROP TABLE IF EXISTS extension_resource_inventory_labels;
DROP TABLE IF EXISTS extension_resource_inventory;
DROP TABLE IF EXISTS resource_intents;
DROP TABLE IF EXISTS extension_resource_managed;
DROP TABLE IF EXISTS resource_aliases;
DROP TABLE IF EXISTS extension_resources;
DROP TABLE IF EXISTS extension_resource_types;
DROP TABLE IF EXISTS resource_relationships;
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
