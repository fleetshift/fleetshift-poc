-- +goose Up
-- Spec validation is now handled at the transport layer via protovalidate
-- (buf.validate annotations in the addon spec proto). The JSON Schema column
-- is no longer needed.
ALTER TABLE managed_resource_types DROP COLUMN spec_schema;

-- +goose Down
ALTER TABLE managed_resource_types ADD COLUMN spec_schema JSONB;
