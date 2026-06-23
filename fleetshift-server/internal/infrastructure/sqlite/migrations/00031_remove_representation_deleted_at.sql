-- +goose Up

-- Remove any legacy tombstoned representation rows, then drop the
-- obsolete deleted_at column now that representation deletion is a
-- hard delete.
DELETE FROM resource_representations WHERE deleted_at IS NOT NULL;
ALTER TABLE resource_representations DROP COLUMN deleted_at;
