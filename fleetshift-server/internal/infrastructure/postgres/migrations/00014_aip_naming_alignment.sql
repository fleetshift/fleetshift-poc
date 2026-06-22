-- +goose Up

-- platform_resources: relative_name → name, drop collection_id (derivable from name)
ALTER TABLE platform_resources RENAME COLUMN relative_name TO name;
ALTER TABLE platform_resources DROP COLUMN collection_id;

-- resource_representations: relative_name → name, drop collection_id (derivable from name)
-- PK changes from (service_name, collection_id, relative_name) to (service_name, name)
ALTER TABLE resource_representations DROP CONSTRAINT resource_representations_pkey;
ALTER TABLE resource_representations DROP COLUMN collection_id;
ALTER TABLE resource_representations RENAME COLUMN relative_name TO name;
ALTER TABLE resource_representations ADD CONSTRAINT resource_representations_pkey PRIMARY KEY (service_name, name);

-- targets: accepted_resource_types → accepted_manifest_types
ALTER TABLE targets RENAME COLUMN accepted_resource_types TO accepted_manifest_types;

-- +goose Down
ALTER TABLE targets RENAME COLUMN accepted_manifest_types TO accepted_resource_types;

ALTER TABLE resource_representations DROP CONSTRAINT resource_representations_pkey;
ALTER TABLE resource_representations RENAME COLUMN name TO relative_name;
ALTER TABLE resource_representations ADD COLUMN collection_id TEXT NOT NULL DEFAULT '';
ALTER TABLE resource_representations ADD CONSTRAINT resource_representations_pkey PRIMARY KEY (service_name, collection_id, relative_name);

ALTER TABLE platform_resources ADD COLUMN collection_id TEXT NOT NULL DEFAULT '';
ALTER TABLE platform_resources RENAME COLUMN name TO relative_name;
