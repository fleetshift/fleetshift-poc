-- +goose Up
-- Remove representation-level roles and labels (now derived from the
-- linked extension resource) and make extension_resource_uid required.
DELETE FROM resource_representations;
ALTER TABLE resource_representations DROP COLUMN roles;
ALTER TABLE resource_representations DROP COLUMN labels;
ALTER TABLE resource_representations ADD COLUMN extension_resource_uid UUID NOT NULL;
