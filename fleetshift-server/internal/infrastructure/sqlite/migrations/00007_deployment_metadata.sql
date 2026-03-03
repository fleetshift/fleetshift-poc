-- +goose Up
ALTER TABLE deployments ADD COLUMN uid TEXT NOT NULL DEFAULT '';
ALTER TABLE deployments ADD COLUMN created_at TEXT NOT NULL DEFAULT (datetime('now'));
ALTER TABLE deployments ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));
ALTER TABLE deployments ADD COLUMN etag TEXT NOT NULL DEFAULT '';
UPDATE deployments SET state = 'creating' WHERE state = 'pending';

-- +goose Down
UPDATE deployments SET state = 'pending' WHERE state = 'creating';
ALTER TABLE deployments DROP COLUMN uid;
ALTER TABLE deployments DROP COLUMN created_at;
ALTER TABLE deployments DROP COLUMN updated_at;
ALTER TABLE deployments DROP COLUMN etag;
