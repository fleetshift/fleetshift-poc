-- +goose Up

-- platform_resources: replace (collection_id, relative_name) with
-- (collection_name, resource_id). collection_name is the full parent
-- collection path (e.g. "clusters" or "publishers/123/books") and
-- resource_id is the leaf segment (e.g. "prod"). This enables exact-match
-- listing by collection without prefix matching, which would over-include
-- descendants in nested collection hierarchies.
--
-- The old collection_id was a flat, single-segment value; relative_name
-- was the full collection-qualified path. We split relative_name at the
-- last '/' to derive the two new columns.
ALTER TABLE platform_resources ADD COLUMN collection_name TEXT;
ALTER TABLE platform_resources ADD COLUMN resource_id TEXT;

UPDATE platform_resources
SET collection_name = SUBSTRING(relative_name FROM 1 FOR LENGTH(relative_name) - LENGTH(SUBSTRING(relative_name FROM '/([^/]+)$')) - 1),
    resource_id = SUBSTRING(relative_name FROM '/([^/]+)$');

ALTER TABLE platform_resources ALTER COLUMN collection_name SET NOT NULL;
ALTER TABLE platform_resources ALTER COLUMN resource_id SET NOT NULL;
ALTER TABLE platform_resources DROP COLUMN collection_id;
ALTER TABLE platform_resources DROP COLUMN relative_name;
ALTER TABLE platform_resources ADD CONSTRAINT uq_platform_resources_identity UNIQUE (collection_name, resource_id);
CREATE INDEX idx_platform_resources_collection ON platform_resources (collection_name);

-- resource_representations: same split.
-- PK changes from (service_name, collection_id, relative_name) to
-- (service_name, collection_name, resource_id).
ALTER TABLE resource_representations ADD COLUMN collection_name TEXT;
ALTER TABLE resource_representations ADD COLUMN resource_id_new TEXT;

UPDATE resource_representations
SET collection_name = SUBSTRING(relative_name FROM 1 FOR LENGTH(relative_name) - LENGTH(SUBSTRING(relative_name FROM '/([^/]+)$')) - 1),
    resource_id_new = SUBSTRING(relative_name FROM '/([^/]+)$');

ALTER TABLE resource_representations ALTER COLUMN collection_name SET NOT NULL;
ALTER TABLE resource_representations ALTER COLUMN resource_id_new SET NOT NULL;
ALTER TABLE resource_representations DROP CONSTRAINT resource_representations_pkey;
ALTER TABLE resource_representations DROP COLUMN collection_id;
ALTER TABLE resource_representations DROP COLUMN relative_name;
ALTER TABLE resource_representations RENAME COLUMN resource_id_new TO resource_id;
ALTER TABLE resource_representations ADD CONSTRAINT resource_representations_pkey PRIMARY KEY (service_name, collection_name, resource_id);

-- platform_resources: drop the deleted_at column. Platform resources are
-- now create/get/list-only; delete is not implemented and this column is
-- unused. The resource_representations.deleted_at column remains
-- temporarily for compatibility at this migration step, but
-- representation deletion no longer uses tombstones and the column is
-- removed in 00017.
ALTER TABLE platform_resources DROP COLUMN deleted_at;

-- targets: accepted_resource_types → accepted_manifest_types
ALTER TABLE targets RENAME COLUMN accepted_resource_types TO accepted_manifest_types;

-- +goose Down
ALTER TABLE platform_resources ADD COLUMN deleted_at TIMESTAMPTZ;

-- NOTE: the rollback is lossy for nested collections. It stores the
-- full collection path (e.g. "publishers/123/books") in the old
-- collection_id column, but pre-migration collection_id was flat
-- (e.g. "books"). Fail fast if nested data exists rather than
-- silently corrupting the collection_id semantics.
--
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM platform_resources
        WHERE collection_name LIKE '%/%'
    ) THEN
        RAISE EXCEPTION 'Cannot roll back: nested collection names exist in platform_resources. '
            'The old collection_id column cannot represent hierarchical paths.';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE targets RENAME COLUMN accepted_manifest_types TO accepted_resource_types;

ALTER TABLE resource_representations DROP CONSTRAINT resource_representations_pkey;
ALTER TABLE resource_representations ADD COLUMN relative_name TEXT;
UPDATE resource_representations SET relative_name = collection_name || '/' || resource_id;
ALTER TABLE resource_representations ALTER COLUMN relative_name SET NOT NULL;
ALTER TABLE resource_representations ADD COLUMN collection_id TEXT NOT NULL DEFAULT '';
UPDATE resource_representations SET collection_id = collection_name;
ALTER TABLE resource_representations DROP COLUMN collection_name;
ALTER TABLE resource_representations DROP COLUMN resource_id;
ALTER TABLE resource_representations ADD CONSTRAINT resource_representations_pkey PRIMARY KEY (service_name, collection_id, relative_name);

DROP INDEX IF EXISTS idx_platform_resources_collection;
ALTER TABLE platform_resources DROP CONSTRAINT uq_platform_resources_identity;
ALTER TABLE platform_resources ADD COLUMN relative_name TEXT;
UPDATE platform_resources SET relative_name = collection_name || '/' || resource_id;
ALTER TABLE platform_resources ALTER COLUMN relative_name SET NOT NULL;
ALTER TABLE platform_resources ADD CONSTRAINT platform_resources_relative_name_key UNIQUE (relative_name);
ALTER TABLE platform_resources ADD COLUMN collection_id TEXT NOT NULL DEFAULT '';
UPDATE platform_resources SET collection_id = collection_name;
ALTER TABLE platform_resources DROP COLUMN collection_name;
ALTER TABLE platform_resources DROP COLUMN resource_id;
