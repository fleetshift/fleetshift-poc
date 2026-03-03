-- +goose Up
ALTER TABLE delivery_records ADD COLUMN id TEXT NOT NULL DEFAULT '';
ALTER TABLE delivery_records ADD COLUMN created_at TEXT NOT NULL DEFAULT (datetime('now'));

-- +goose Down
ALTER TABLE delivery_records DROP COLUMN id;
ALTER TABLE delivery_records DROP COLUMN created_at;
