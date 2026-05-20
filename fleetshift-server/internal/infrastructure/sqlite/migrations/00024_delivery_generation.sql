-- +goose Up
ALTER TABLE delivery_records ADD COLUMN generation INTEGER NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE delivery_records DROP COLUMN generation;
