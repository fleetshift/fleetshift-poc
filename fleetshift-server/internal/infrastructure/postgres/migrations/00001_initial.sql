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
-- (See design docs.)

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
-- resource_representations' removal below). This name-based
-- derivation is also what makes a no-alias extension resource
-- implicitly accepted as a representation of its platform resource by
-- read semantics alone (see docs/design/architecture/resource_identity_and_api.md's
-- "Aliases" section) -- there is no accepted/pending status column to
-- maintain here for that common case.
--
-- reported_aliases is this extension resource's own complete pending
-- alias payload -- the reporter's assertions, not accepted platform
-- identity. Postgres stores it as a JSONB object keyed by a
-- JSON-encoded [namespace, key] pair, with the alias value as the
-- object value. That repository-local shape lets
-- ApplyInventoryDeltas merge UpsertAliases with a single JSONB `||`
-- operation while read paths still hydrate the domain's
-- Alias/AliasSet snapshots. It is stored with no synchronous
-- cross-resource conflict
-- detection; a future asynchronous reconciliation process is what
-- will eventually decide which reported aliases -- if any conflict --
-- become accepted. Defaults to '{}', never NULL, so "this resource
-- asserts no aliases" is always representable without a NULL special
-- case.
--
-- This deliberately replaces an earlier design that additionally
-- classified aliases against cross-resource resource_alias_claims/
-- resource_alias_contributions state at write time (see those tables'
-- own doc comments below) -- that mechanism is no longer reachable
-- from inventory reporting; see [domain.InventoryReplacement.Aliases]'s
-- doc for the full contract and poc/inventory-identity-reconciliation
-- for the executable reference this schema follows.
CREATE TABLE extension_resources (
    uid               UUID PRIMARY KEY,
    service_name      TEXT NOT NULL,
    type_name         TEXT NOT NULL,
    collection_name   TEXT NOT NULL,
    resource_id       TEXT NOT NULL,
    labels            JSONB NOT NULL DEFAULT '{}',
    reported_aliases  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL,
    UNIQUE (service_name, collection_name, resource_id),
    FOREIGN KEY (service_name, type_name)
        REFERENCES extension_resource_types(service_name, type_name)
);

-- Supports deriving representations for a platform resource: join on
-- (collection_name, resource_id) rather than service_name-prefixed.
CREATE INDEX idx_extension_resources_collection_resource
    ON extension_resources(collection_name, resource_id);

-- Aliases split into two tables, per the validated
-- poc/alias-claims/ prototype -- see
-- docs/design/architecture/resource_identity_and_api.md's "Aliases"
-- section for the full contract.
--
-- Inventory reporting (ReplaceInventory/ApplyInventoryDeltas) no
-- longer writes to either of these tables at all: reported aliases
-- are stored unnormalized on extension_resources.reported_aliases
-- (see that column's own doc comment above) rather than classified
-- into claims/contributions synchronously at write time. These two
-- tables remain reachable only through [ResourceIdentityRepository]'s
-- own platform-owned alias path (resource_identity_repo.go's
-- reconcileAliases, driven by [PlatformResource.AddAlias]), which is
-- independent of inventory reporting. They are kept, rather than
-- dropped, so that path keeps working and so a future asynchronous
-- reconciliation process has a proven schema to promote accepted
-- reported aliases into.
--
-- resource_alias_claims is the canonical (namespace, key, value) ->
-- platform resource mapping, one row per claim regardless of how
-- many extension resources contribute it. resource_alias_contributions
-- tracks which extension resource(s) assert a claim -- many
-- contributors can point at the same claim (e.g. two addons agreeing
-- a cluster's instance_id is "i-123"), and a claim only disappears
-- once every contributing row does.
--
-- Splitting the two concerns this way turns both cross-resource
-- alias invariants into ordinary B-tree uniqueness instead of the
-- GiST EXCLUDE constraints an earlier, single-table design needed:
--
--   - UNIQUE (namespace, key, value) keeps the same (namespace, key,
--     value) from ever mapping to two platform resources.
--   - UNIQUE (namespace, key, platform_collection_name,
--     platform_resource_id) keeps the same platform resource from
--     ever having two different values for the same (namespace,
--     key).
--
-- A contributor legitimately replacing its own value for a key is
-- just an UPDATE of its existing claim row's value (when it's the
-- claim's sole contributor) or a move of its contribution to a
-- different, already-existing claim -- never a same-statement
-- delete-then-insert pair racing a constraint the way the old EXCLUDE
-- design needed, so nothing here is DEFERRABLE.
--
-- platform_owned marks a claim asserted directly by the platform
-- resource itself (resource_identity_repo.go's reconcileAliases),
-- independent of whether it also has contributions: a claim can be
-- platform_owned with zero contributors, contributor-only with
-- platform_owned = false, or both at once if a platform resource
-- corroborates a claim an extension resource already contributed.
-- This is what makes source_extension_resource_uid on
-- resource_alias_contributions NOT NULL and a real primary-key
-- column -- a platform-direct alias needs no contribution row at
-- all, so there's no nullable-contributor case to accommodate.
--
-- No FK to platform_resources, deliberately: platform_resources is
-- virtual by default (see its own doc comment above), and requiring
-- a physical row here would force one into existence on every
-- accepted alias, working against that design. A claim can reference
-- a name with no physical platform_resources row at all, the same
-- way representations already do.
CREATE TABLE resource_alias_claims (
    id                        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace                 TEXT NOT NULL,
    key                       TEXT NOT NULL,
    value                     TEXT NOT NULL,
    platform_collection_name TEXT NOT NULL,
    platform_resource_id      TEXT NOT NULL,
    platform_owned            BOOLEAN NOT NULL DEFAULT false,
    created_at                TIMESTAMPTZ NOT NULL,
    UNIQUE (namespace, key, value),
    UNIQUE (namespace, key, platform_collection_name, platform_resource_id),
    -- Exists purely so resource_alias_contributions' FK below can
    -- also enforce that a contribution's own (namespace, key)
    -- columns agree with its claim's -- a belt-and-suspenders
    -- integrity check, not a performance thing.
    UNIQUE (id, namespace, key)
);

-- source_extension_resource_uid cascades on its extension resource's
-- deletion -- that alone is enough to remove *this* contribution row,
-- but not necessarily the claim it points at (a sibling contributor,
-- or platform_owned, may still need it alive); see
-- ExtensionResourceRepo.Delete's own explicit orphan cleanup for that
-- half.
--
-- The claim_id FK is intentionally restrictive (default NO ACTION),
-- not ON DELETE CASCADE: claim deletion is only ever attempted by
-- the vetted orphan-cleanup logic in extension_resource_repo.go/
-- resource_identity_repo.go, which proves zero contributors remain
-- (and, for resource_identity_repo.go, that the claim also isn't
-- platform_owned) before deleting a claim. If that logic is correct,
-- this FK never fires -- the contributions are already gone by the
-- time the claim delete runs. If it's wrong, a cascading FK would
-- silently delete still-valid contributions along with the claim,
-- corrupting another extension resource's alias state with no error
-- at all; a restrictive FK instead fails the whole statement loudly
-- with a constraint violation. With claim_id indexed below, this
-- costs nothing in the steady state where it never fires.
CREATE TABLE resource_alias_contributions (
    source_extension_resource_uid UUID NOT NULL
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    namespace  TEXT NOT NULL,
    key        TEXT NOT NULL,
    claim_id   BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_extension_resource_uid, namespace, key),
    FOREIGN KEY (claim_id, namespace, key)
        REFERENCES resource_alias_claims(id, namespace, key)
);

-- Supports the alias fold-in's per-claim contributor lookups
-- (sibling checks, refcount baselines) and the orphan-cleanup FK
-- check above.
CREATE INDEX idx_resource_alias_contributions_claim ON resource_alias_contributions(claim_id);

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

-- extension_resource_inventory holds only the *latest* observation,
-- labels, and conditions for an extension resource -- one row per
-- resource, replaced or patched in place on every report. labels and
-- conditions are JSONB rather than normalized out into their own
-- tables (see the now-removed extension_resource_inventory_labels/
-- extension_resource_inventory_conditions tables this replaces):
-- ReplaceInventory's complete-latest-state contract makes a whole-row
-- JSONB replace cheaper than a delete-absent/upsert pair against a
-- normalized table, and ApplyInventoryDeltas's field-level
-- set/upsert-plus-delete semantics map directly onto the `-`
-- (key-removal) and `||` (merge) jsonb operators (see
-- extension_resource_repo.go's applyInventoryDeltasCoreCTEs). The GIN
-- indexes below support future containment/key-existence label and
-- condition search over that latest state; jsonb_ops is the default
-- opclass, chosen because it's the more general one and real query
-- shapes aren't known yet -- jsonb_path_ops could be revisited if a
-- containment-only workload emerges. SQLite's mirror of this table
-- (see that migration) has no equivalent index today; see that
-- file's own doc comment.
--
-- conditions is a JSON object keyed by condition type (not an array)
-- so a delta's per-type upsert/delete can use the same key-removal/
-- merge operators as labels; see ConditionJSON's doc comment in
-- extension_resource_repo.go for the exact per-condition shape.
--
-- Per-condition observed_at/updated_at are deliberately not part of
-- this JSON shape: latest freshness is tracked once, at the inventory
-- row level, by this table's own observed_at/updated_at columns.
--
-- Condition/observation *history* (the extension_resource_inventory_
-- observations/extension_resource_inventory_condition_events tables
-- below) is intentionally not written by ReplaceInventory/
-- ApplyInventoryDeltas in this pass -- seeing this fact requires
-- reading the repository code, not this schema, since the history
-- tables' shape hasn't changed; they're kept, unpopulated by the hot
-- path, for a future asynchronous history writer.
CREATE TABLE extension_resource_inventory (
    extension_resource_uid UUID PRIMARY KEY
        REFERENCES extension_resources(uid) ON DELETE CASCADE,
    observation JSONB,
    labels      JSONB NOT NULL DEFAULT '{}',
    conditions  JSONB NOT NULL DEFAULT '{}',
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX extension_resource_inventory_labels_gin
    ON extension_resource_inventory USING GIN (labels);

CREATE INDEX extension_resource_inventory_conditions_gin
    ON extension_resource_inventory USING GIN (conditions);

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
DROP TABLE IF EXISTS extension_resource_inventory;
DROP TABLE IF EXISTS resource_intents;
DROP TABLE IF EXISTS extension_resource_managed;
DROP TABLE IF EXISTS resource_alias_contributions;
DROP TABLE IF EXISTS resource_alias_claims;
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
