-- +goose Up

-- Rename deployment identity column from id to name and store
-- collection-qualified resource names (e.g. "deployments/my-deploy").
ALTER TABLE deployments RENAME COLUMN id TO name;
UPDATE deployments SET name = 'deployments/' || name WHERE name NOT LIKE '%/%';
