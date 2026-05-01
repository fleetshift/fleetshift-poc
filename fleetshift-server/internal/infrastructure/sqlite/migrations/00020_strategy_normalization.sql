-- +goose Up
-- Remove denormalized strategy JSON columns from fulfillments.
-- The strategy version tables are now the sole source of truth for specs.
ALTER TABLE fulfillments DROP COLUMN manifest_strategy;
ALTER TABLE fulfillments DROP COLUMN placement_strategy;
ALTER TABLE fulfillments DROP COLUMN rollout_strategy;

-- +goose Down
ALTER TABLE fulfillments ADD COLUMN manifest_strategy TEXT NOT NULL DEFAULT '{}';
ALTER TABLE fulfillments ADD COLUMN placement_strategy TEXT NOT NULL DEFAULT '{}';
ALTER TABLE fulfillments ADD COLUMN rollout_strategy TEXT;
