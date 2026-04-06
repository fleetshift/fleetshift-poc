-- +goose Up
ALTER TABLE deployments ADD COLUMN provenance TEXT;

-- +goose Down
ALTER TABLE deployments DROP COLUMN provenance;
