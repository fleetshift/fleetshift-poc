-- +goose Up
ALTER TABLE targets ADD COLUMN inventory_item_id TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE targets DROP COLUMN inventory_item_id;
