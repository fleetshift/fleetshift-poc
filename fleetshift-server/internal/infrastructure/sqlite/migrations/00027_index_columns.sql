-- +goose Up
-- Index columns for kubernetes resource observations.
ALTER TABLE inventory_items ADD COLUMN target_id TEXT NOT NULL DEFAULT '';
ALTER TABLE inventory_items ADD COLUMN observed_at TEXT;
ALTER TABLE inventory_items ADD COLUMN observed TEXT NOT NULL DEFAULT '{}';
ALTER TABLE inventory_items ADD COLUMN conditions TEXT NOT NULL DEFAULT '[]';

CREATE INDEX idx_inventory_items_target ON inventory_items(target_id);
CREATE INDEX idx_inventory_items_target_type ON inventory_items(target_id, type);

-- +goose Down
DROP INDEX IF EXISTS idx_inventory_items_target_type;
DROP INDEX IF EXISTS idx_inventory_items_target;
ALTER TABLE inventory_items DROP COLUMN conditions;
ALTER TABLE inventory_items DROP COLUMN observed;
ALTER TABLE inventory_items DROP COLUMN observed_at;
ALTER TABLE inventory_items DROP COLUMN target_id;
