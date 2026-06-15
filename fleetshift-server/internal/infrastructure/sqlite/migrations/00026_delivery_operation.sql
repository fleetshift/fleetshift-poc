-- +goose Up
ALTER TABLE delivery_records ADD COLUMN operation TEXT NOT NULL DEFAULT 'deliver';

-- +goose Down
-- SQLite does not support DROP COLUMN before 3.35; acceptable for POC.
