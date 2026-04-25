-- +goose Up
ALTER TABLE deployments ADD COLUMN status_reason TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE deployments DROP COLUMN status_reason;
