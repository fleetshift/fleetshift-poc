-- +goose Up
-- Convert TEXT columns that store JSON to JSONB. Strategy spec columns
-- were originally TEXT as a workaround for hash computation breaking
-- under JSONB key reordering (see OME-42). The remaining fulfillment
-- columns (resolved_targets, auth) were downgraded from JSONB to TEXT
-- during the fulfillment extraction (00003).
--
-- fulfillments.provenance is intentionally left as TEXT: it contains a
-- content hash computed over the exact bytes of embedded manifest JSON.
-- JSONB normalization (whitespace, key ordering) would alter those
-- bytes and break signature verification.

ALTER TABLE fulfillments
    ALTER COLUMN resolved_targets DROP DEFAULT,
    ALTER COLUMN auth             DROP DEFAULT;

ALTER TABLE fulfillments
    ALTER COLUMN resolved_targets TYPE JSONB USING resolved_targets::jsonb,
    ALTER COLUMN auth             TYPE JSONB USING auth::jsonb;

ALTER TABLE fulfillments
    ALTER COLUMN resolved_targets SET DEFAULT '[]'::jsonb,
    ALTER COLUMN auth             SET DEFAULT '{}'::jsonb;

ALTER TABLE manifest_strategies
    ALTER COLUMN spec TYPE JSONB USING spec::jsonb;

ALTER TABLE placement_strategies
    ALTER COLUMN spec TYPE JSONB USING spec::jsonb;

ALTER TABLE rollout_strategies
    ALTER COLUMN spec TYPE JSONB USING spec::jsonb;

-- +goose Down
ALTER TABLE rollout_strategies
    ALTER COLUMN spec TYPE TEXT USING spec::text;

ALTER TABLE placement_strategies
    ALTER COLUMN spec TYPE TEXT USING spec::text;

ALTER TABLE manifest_strategies
    ALTER COLUMN spec TYPE TEXT USING spec::text;

ALTER TABLE fulfillments
    ALTER COLUMN resolved_targets DROP DEFAULT,
    ALTER COLUMN auth             DROP DEFAULT;

ALTER TABLE fulfillments
    ALTER COLUMN resolved_targets TYPE TEXT USING resolved_targets::text,
    ALTER COLUMN auth             TYPE TEXT USING auth::text;

ALTER TABLE fulfillments
    ALTER COLUMN resolved_targets SET DEFAULT '[]',
    ALTER COLUMN auth             SET DEFAULT '{}';
