-- +goose Up
ALTER TABLE deployments DROP COLUMN reconciling;
-- +goose Down
ALTER TABLE deployments ADD COLUMN reconciling INTEGER NOT NULL DEFAULT 0;
