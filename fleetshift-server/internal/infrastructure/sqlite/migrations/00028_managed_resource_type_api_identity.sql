-- +goose Up
ALTER TABLE managed_resource_types ADD COLUMN api_service_name TEXT NOT NULL DEFAULT '';
ALTER TABLE managed_resource_types ADD COLUMN api_version      TEXT NOT NULL DEFAULT '';
ALTER TABLE managed_resource_types ADD COLUMN collection_id    TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE managed_resource_types DROP COLUMN collection_id;
ALTER TABLE managed_resource_types DROP COLUMN api_version;
ALTER TABLE managed_resource_types DROP COLUMN api_service_name;
