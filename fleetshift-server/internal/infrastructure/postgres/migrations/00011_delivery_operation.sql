-- +goose Up
ALTER TABLE delivery_records ADD COLUMN operation TEXT NOT NULL DEFAULT 'deliver';

-- +goose Down
ALTER TABLE delivery_records DROP COLUMN operation;
