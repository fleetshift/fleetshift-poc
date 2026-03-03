-- +goose Up
ALTER TABLE targets ADD COLUMN state TEXT NOT NULL DEFAULT 'ready';

-- +goose Down
ALTER TABLE targets DROP COLUMN state;
