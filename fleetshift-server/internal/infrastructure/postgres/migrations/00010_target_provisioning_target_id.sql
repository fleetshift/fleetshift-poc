-- +goose Up
-- Track which seeded target provisioned each emitted target.
ALTER TABLE targets ADD COLUMN provisioning_target_id TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE targets DROP COLUMN provisioning_target_id;
