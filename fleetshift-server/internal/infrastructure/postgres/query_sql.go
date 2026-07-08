package postgres

import "fmt"

// buildQueryResourcesSQL assembles QueryResources' single query. It
// runs in two conceptual stages (see the QueryRepository POC plan's
// "Postgres implementation (single SQL stage)" section):
//
//  1. all_rows/filtered_page union the cheap envelope columns for
//     every platform and extension resource, apply predicateSQL
//     (the compiled CEL filter) and keysetSQL (page-token resume),
//     then order and LIMIT down to the page window.
//  2. The outer SELECT LEFT JOIN LATERALs the *same* hydration
//     queries [ExtensionResourceRepo.GetView] (erViewQueryPG) and
//     [ResourceIdentityRepo.GetByName] (platformResourceAggregateSelectPostgres)
//     already use, but only against the already-limited page window
//     -- one data query total, no per-row follow-up read.
//
// predicateSQL and keysetSQL are trusted, pre-built SQL fragments
// (parameterized with $N placeholders only; see querysql's compiler
// for why user input never reaches this function as raw text).
// limitPlaceholder is the $N placeholder index bound to the page's
// row limit.
//
// # Indexing limitations (POC scope)
//
// Per the plan's Non-Goals, this POC does not add indexes tailored to
// query predicates -- it is correct and SQL-pushed down, not
// necessarily fast at scale. query_repo_bench_test.go's
// TestQueryResourcesExplainPlan confirms this with EXPLAIN (ANALYZE,
// BUFFERS) against a seeded ~85k-row corpus (5k platform, 40k
// extension Clusters, 40k extension Nodes); run it with
// FLEETSHIFT_QUERY_BENCH=1 to reproduce. Findings:
//
//   - Every filtered_page query sorts and LIMITs the *entire* matching
//     all_rows set, because there is no index whose key order matches
//     queryResultsOrderBy across the platform_rows/extension_rows
//     UNION ALL branches (that order spans columns computed
//     differently per branch, e.g. resource_type is ” for platform
//     rows and service_name || '/' || type_name for extension rows,
//     so a single btree index can't back it without a matching
//     generated/expression column on both sides). This is the
//     dominant cost in every scenario measured, including the
//     "empty filter" baseline (~63ms, ~2900 buffer hits to sort all
//     85k rows for a 50-row page) -- selective predicates only skip
//     the *filter* step's I/O, not the sort. It also means keyset
//     pagination's second-page query (predicateSQL AND keysetSQL)
//     re-scans and re-sorts the entire remaining set instead of
//     seeking into an index, so deep pages cost the same as page one
//     (~68ms measured for page two of an empty filter, similar to
//     page one's ~63ms). Fixing this needs a materialized or
//     generated ordering key shared by both branches (e.g. a
//     persisted "sort_key" column populated by each source table, or
//     a materialized view refreshed on write) -- deferred; see the
//     plan's Non-Goals and "Future Follow-Ups".
//   - resource_type equality (a common envelope filter, since addons
//     and callers scope queries to one type) used to force a
//     sequential scan re-concatenating service_name || '/' ||
//     type_name per extension_resources row, because that
//     expression had no matching index. Fixed: see
//     idx_extension_resources_resource_type in migrations/00001_initial.sql,
//     which lets Postgres match the plain WHERE predicate against the
//     expression index directly. Confirmed via EXPLAIN: the "selective
//     envelope filter" scenario now uses
//     "Index Scan using idx_extension_resources_resource_type"
//     instead of a Seq Scan, and the "guarded spec filter" scenario
//     (resource_type == ... && resource.spec.provider == ...) dropped
//     from 35398 to 22065 buffer hits once the planner could use the
//     index for whichever side of the join turned out cheaper.
//   - JSONB paths queried via querysql's field resolver
//     (resource_labels, spec, inventory_labels, inventory_observation,
//     inventory_conditions) are read with the ->> text-extraction
//     operator (needed for the safe numeric/bool casts in
//     query_filter.go's safeJSONCast and for exact string match), but
//     the existing GIN indexes on these columns (see
//     migrations/00001_initial.sql) only accelerate containment
//     operators (@>, ?, etc.), not ->>. So
//     resource.labels[...]/resource.spec.*/resource.inventory.*
//     filters fall back to sequential scans regardless of those GIN
//     indexes -- e.g. the "extension label filter" and "inventory
//     label filter" scenarios both sequential-scan their base table.
//     Left as-is for this POC: fixing it means either compiling
//     equality/containment-shaped CEL filters to @> instead of ->> in
//     querysql's compiler/field resolver (only safe for a subset of
//     operators), or dropping the now-confirmed-unused GIN indexes to
//     remove their write overhead. Tracked as a follow-up rather than
//     fixed here since it changes compiled SQL shape, not just
//     schema.
//
// Addon-declared queryable/indexed field configuration -- letting an
// addon say "index resource.spec.provider" -- is intentionally out of
// scope here too: querysql's field set is hard-coded, not
// addon-configurable. Both are tracked as follow-ups (see the plan's
// "Future Follow-Ups": addon-declared queryable/indexed field
// configuration, and SQL index recommendations per registered query
// field).
func buildQueryResourcesSQL(predicateSQL, keysetSQL string, limitPlaceholder int) string {
	return fmt.Sprintf(`
WITH platform_physical AS (
	SELECT collection_name, resource_id, labels, created_at, updated_at
	FROM platform_resources
),
platform_virtual AS (
	SELECT collection_name, resource_id, '{}'::jsonb AS labels,
	       MIN(created_at) AS created_at, MAX(updated_at) AS updated_at
	FROM extension_resources
	GROUP BY collection_name, resource_id
),
platform_base AS (
	SELECT * FROM platform_physical
	UNION ALL
	SELECT v.* FROM platform_virtual v
	WHERE NOT EXISTS (
		SELECT 1 FROM platform_physical p
		WHERE p.collection_name = v.collection_name AND p.resource_id = v.resource_id
	)
),
platform_rows AS (
	SELECT
		'platform'::text AS kind,
		'//fleetshift.io/' || b.collection_name || '/' || b.resource_id AS name,
		b.collection_name || '/' || b.resource_id AS platform_name,
		-- '' (not NULL) so resource_type/api_version predicates behave
		-- like ordinary equality/inequality rather than SQL's
		-- three-valued NULL logic (NULL = '' and NULL != 'x' are both
		-- NULL, i.e. neither true, which would silently drop every
		-- platform row from both a positive and a negative filter).
		-- This also matches QueryResourceResult.ResourceType's
		-- documented "platform resources leave this empty" convention.
		''::text AS resource_type,
		'fleetshift.io'::text AS service_name,
		''::text AS api_version,
		b.collection_name AS collection_name,
		b.resource_id AS resource_id,
		''::text AS type_name,
		b.labels AS resource_labels,
		NULL::jsonb AS spec,
		NULL::integer AS intent_version,
		NULL::text AS fulfillment_state,
		NULL::text AS pause_reason,
		NULL::integer AS generation,
		NULL::jsonb AS inventory_labels,
		NULL::jsonb AS inventory_conditions,
		NULL::jsonb AS inventory_observation,
		NULL::uuid AS extension_uid,
		b.created_at AS created_at,
		b.updated_at AS updated_at
	FROM platform_base b
),
extension_rows AS (
	SELECT
		'extension'::text AS kind,
		'//' || er.service_name || '/' || er.collection_name || '/' || er.resource_id AS name,
		er.collection_name || '/' || er.resource_id AS platform_name,
		er.service_name || '/' || er.type_name AS resource_type,
		er.service_name AS service_name,
		ert.api_version AS api_version,
		er.collection_name AS collection_name,
		er.resource_id AS resource_id,
		er.type_name AS type_name,
		er.labels AS resource_labels,
		ri.spec AS spec,
		erm.current_version AS intent_version,
		f.state AS fulfillment_state,
		f.pause_reason AS pause_reason,
		f.generation AS generation,
		inv.labels AS inventory_labels,
		inv.conditions AS inventory_conditions,
		inv.observation AS inventory_observation,
		er.uid AS extension_uid,
		er.created_at AS created_at,
		er.updated_at AS updated_at
	FROM extension_resources er
	JOIN extension_resource_types ert
		ON ert.service_name = er.service_name AND ert.type_name = er.type_name
	LEFT JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
	LEFT JOIN resource_intents ri
		ON ri.extension_resource_uid = er.uid AND ri.version = erm.current_version
	LEFT JOIN fulfillments f ON f.id = erm.fulfillment_id
	LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
),
all_rows AS (
	SELECT * FROM platform_rows
	UNION ALL
	SELECT * FROM extension_rows
),
filtered_page AS (
	SELECT *
	FROM all_rows
	WHERE (%s)
	  AND (%s)
	ORDER BY %s
	LIMIT $%d
)
SELECT
	fp.kind, fp.name, fp.platform_name, fp.resource_type, fp.service_name, fp.api_version,
	fp.collection_name, fp.resource_id, fp.type_name,

	extv.uid, extv.service_name, extv.type_name, extv.collection_name, extv.resource_id,
	extv.labels, extv.reported_aliases, extv.er_created_at, extv.er_updated_at,
	extv.current_version, extv.fulfillment_id,
	extv.ri_spec, extv.ri_created_at,
	extv.f_id, extv.ms_ver, extv.ms_spec, extv.ps_ver, extv.ps_spec, extv.rs_ver, extv.rs_spec,
	extv.resolved_targets, extv.state, extv.pause_reason, extv.status_reason,
	extv.auth, extv.provenance, extv.attestation_ref,
	extv.generation, extv.observed_generation, extv.active_workflow_gen,
	extv.f_created_at, extv.f_updated_at,
	extv.inv_labels, extv.inv_observation, extv.inv_observed_at, extv.inv_updated_at, extv.inv_conditions,

	plat.collection_name, plat.resource_id, plat.labels, plat.created_at, plat.updated_at,
	plat.representations, plat.aliases, plat.relationships
FROM filtered_page fp
LEFT JOIN LATERAL (
	%s
	WHERE er.uid = fp.extension_uid
) AS extv(
	uid, service_name, type_name, collection_name, resource_id,
	labels, reported_aliases, er_created_at, er_updated_at,
	current_version, fulfillment_id,
	ri_spec, ri_created_at,
	f_id, ms_ver, ms_spec, ps_ver, ps_spec, rs_ver, rs_spec,
	resolved_targets, state, pause_reason, status_reason,
	auth, provenance, attestation_ref,
	generation, observed_generation, active_workflow_gen,
	f_created_at, f_updated_at,
	inv_labels, inv_observation, inv_observed_at, inv_updated_at, inv_conditions
) ON fp.kind = 'extension'
LEFT JOIN LATERAL (
	WITH base AS (
		SELECT fp.collection_name AS collection_name, fp.resource_id AS resource_id,
		       fp.resource_labels AS labels, fp.created_at AS created_at, fp.updated_at AS updated_at
	)
	%s
) AS plat(collection_name, resource_id, labels, created_at, updated_at, representations, aliases, relationships)
	ON fp.kind = 'platform'
ORDER BY %s
`,
		predicateSQL, keysetSQL, queryResultsOrderBy, limitPlaceholder,
		erViewQueryPG,
		platformResourceAggregateSelectPostgres,
		queryResultsOrderByQualified,
	)
}
