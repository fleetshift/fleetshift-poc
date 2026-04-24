-- +goose Up
ALTER TABLE deployments ADD COLUMN active_workflow_gen INTEGER;

-- +goose Down
ALTER TABLE deployments DROP COLUMN active_workflow_gen;
