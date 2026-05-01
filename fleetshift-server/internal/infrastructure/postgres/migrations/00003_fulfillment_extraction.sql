-- +goose Up
-- Fulfillment extraction: split deployments into fulfillments + thin deployments.

CREATE TABLE fulfillments (
    id                        TEXT PRIMARY KEY,
    manifest_strategy         TEXT NOT NULL,
    manifest_strategy_version INTEGER NOT NULL DEFAULT 0,
    placement_strategy        TEXT NOT NULL,
    placement_strategy_version INTEGER NOT NULL DEFAULT 0,
    rollout_strategy          TEXT,
    rollout_strategy_version  INTEGER NOT NULL DEFAULT 0,
    resolved_targets          TEXT NOT NULL DEFAULT '[]',
    state                     TEXT NOT NULL DEFAULT 'creating',
    status_reason             TEXT NOT NULL DEFAULT '',
    auth                      TEXT NOT NULL DEFAULT '{}',
    provenance                TEXT,
    generation                INTEGER NOT NULL DEFAULT 1,
    observed_generation       INTEGER NOT NULL DEFAULT 0,
    active_workflow_gen       INTEGER,
    created_at                TEXT NOT NULL DEFAULT NOW(),
    updated_at                TEXT NOT NULL DEFAULT NOW()
);

CREATE TABLE manifest_strategies (
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id) ON DELETE CASCADE,
    version        INTEGER NOT NULL,
    spec           TEXT NOT NULL,
    created_at     TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fulfillment_id, version)
);

CREATE TABLE placement_strategies (
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id) ON DELETE CASCADE,
    version        INTEGER NOT NULL,
    spec           TEXT NOT NULL,
    created_at     TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fulfillment_id, version)
);

CREATE TABLE rollout_strategies (
    fulfillment_id TEXT NOT NULL REFERENCES fulfillments(id) ON DELETE CASCADE,
    version        INTEGER NOT NULL,
    spec           TEXT,
    created_at     TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fulfillment_id, version)
);

INSERT INTO fulfillments (
    id, manifest_strategy, manifest_strategy_version,
    placement_strategy, placement_strategy_version,
    rollout_strategy, rollout_strategy_version,
    resolved_targets, state, status_reason, auth, provenance,
    generation, observed_generation, active_workflow_gen,
    created_at, updated_at
)
SELECT
    id,
    manifest_strategy,
    1,
    placement_strategy,
    1,
    rollout_strategy::text,
    CASE WHEN rollout_strategy IS NOT NULL THEN 1 ELSE 0 END,
    resolved_targets::text,
    state,
    status_reason,
    auth::text,
    provenance::text,
    generation,
    observed_generation,
    active_workflow_gen,
    created_at,
    updated_at
FROM deployments;

INSERT INTO manifest_strategies (fulfillment_id, version, spec, created_at)
SELECT id, 1, manifest_strategy, created_at FROM deployments;

INSERT INTO placement_strategies (fulfillment_id, version, spec, created_at)
SELECT id, 1, placement_strategy, created_at FROM deployments;

INSERT INTO rollout_strategies (fulfillment_id, version, spec, created_at)
SELECT id, 1, rollout_strategy::text, created_at FROM deployments WHERE rollout_strategy IS NOT NULL;

ALTER TABLE delivery_records RENAME COLUMN deployment_id TO fulfillment_id;

CREATE TABLE deployments_new (
    id              TEXT PRIMARY KEY,
    uid             TEXT NOT NULL DEFAULT '',
    fulfillment_id  TEXT NOT NULL REFERENCES fulfillments(id),
    created_at      TEXT NOT NULL DEFAULT NOW(),
    updated_at      TEXT NOT NULL DEFAULT NOW(),
    etag            TEXT NOT NULL DEFAULT ''
);

INSERT INTO deployments_new (id, uid, fulfillment_id, created_at, updated_at, etag)
SELECT id, uid, id, created_at, updated_at, etag FROM deployments;

DROP TABLE deployments;
ALTER TABLE deployments_new RENAME TO deployments;

-- +goose Down
-- Down migration is destructive; prototype only.
DROP TABLE IF EXISTS deployments;
DROP TABLE IF EXISTS rollout_strategies;
DROP TABLE IF EXISTS placement_strategies;
DROP TABLE IF EXISTS manifest_strategies;
DROP TABLE IF EXISTS fulfillments;
