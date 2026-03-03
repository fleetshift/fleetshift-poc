-- +goose Up
CREATE TABLE inventory_items (
    id                 TEXT PRIMARY KEY,
    type               TEXT NOT NULL,
    name               TEXT NOT NULL,
    properties         TEXT NOT NULL DEFAULT '{}',
    labels             TEXT NOT NULL DEFAULT '{}',
    source_delivery_id TEXT,
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at         TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_inventory_items_type ON inventory_items(type);

-- +goose Down
DROP TABLE inventory_items;
