package postgres

import "fmt"

// buildQueryResourcesSQL assembles QueryResources' single extension-only
// query. It runs in two conceptual stages:
//
//  1. filtered_page (MATERIALIZED) selects candidate extension_resources
//     rows (plus the LEFT JOINs filter fields may need), applies
//     predicateSQL (the compiled CEL filter) and keysetSQL (page-token
//     resume), then orders and LIMITs down to the page window using a
//     supported order whose columns match a composite B-tree index
//     (see idx_extension_resources_query_order /
//     idx_extension_resources_type_query_order). MATERIALIZED keeps
//     the planner from pulling the limit past the hydration joins.
//  2. The outer SELECT hydrates only that page window by joining
//     extension_resources (and the same managed/intent/fulfillment/
//     inventory/strategy tables [ExtensionResourceRepo.GetView] uses)
//     from filtered_page fp on er.uid = fp.uid -- one data query
//     total, no per-row follow-up read, and no full-table hydration
//     hash join.
//
// Platform aggregate rows are intentionally not part of this query.
// Restoring them via the previous platform_rows / UNION ALL shape
// would reintroduce a derived sort key that no single index can
// satisfy; defer until the platform aggregate model is settled.
//
// TODO: do not restore platform aggregate search by re-adding the
// old platform_physical/platform_virtual/platform_rows CTE union
// here. A future platform query surface needs its own identity model
// and indexes, not a derived approximation over extension_resources.
//
// predicateSQL and keysetSQL are trusted, pre-built SQL fragments
// (parameterized with $N placeholders only; QueryRepo wires
// querysql.DollarParams, and user input never reaches this function
// as raw text). order is a supported order from resolveQueryOrder.
// limitPlaceholder is the $N placeholder index bound to the page's
// row limit.
//
// # Indexing notes
//
// Default empty-filter pagination seeks through
// idx_extension_resources_query_order. The resource_type,name order
// mode seeks through idx_extension_resources_type_query_order.
// resource_type == "service/Type" and resource_type in [...] compile
// to constituent service_name/type_name predicates (see
// query_filter.go), so those filters can use the type-scoped
// composite index rather than an expression index on
// (service_name || '/' || type_name).
//
// Label/condition string equality compiles to JSONB containment
// (@> jsonb_build_object(...)), which is GIN-eligible on the
// jsonb_path_ops indexes over extension_resources.labels and
// extension_resource_inventory labels/conditions. GIN is not
// guaranteed to be chosen: for ORDER BY + LIMIT page queries the
// planner often prefers walking the order B-tree and applying @>
// as a residual filter (especially when the predicate is not
// selective enough that a GIN bitmap plus sort looks cheaper). That
// is acceptable -- containment still preserves correct equality
// semantics and remains available when selectivity favors it.
// Normalized label/condition side tables are intentionally out of
// scope this iteration.
//
// See query_repo_bench_test.go's TestQueryResourcesExplainPlan
// (FLEETSHIFT_QUERY_BENCH=1) for measured plans.
func buildQueryResourcesSQL(predicateSQL, keysetSQL string, order querySupportedOrder, limitPlaceholder int) string {
	return fmt.Sprintf(`
WITH filtered_page AS MATERIALIZED (
	SELECT
		er.uid,
		er.collection_name,
		er.resource_id,
		er.service_name,
		er.type_name
	FROM extension_resources er
	LEFT JOIN extension_resource_managed erm
		ON erm.extension_resource_uid = er.uid
	LEFT JOIN resource_intents ri
		ON ri.extension_resource_uid = er.uid
	   AND ri.version = erm.current_version
	LEFT JOIN fulfillments f
		ON f.id = erm.fulfillment_id
	LEFT JOIN extension_resource_inventory inv
		ON inv.extension_resource_uid = er.uid
	WHERE (%s)
	  AND (%s)
	ORDER BY %s
	LIMIT $%d
)
SELECT
	fp.collection_name,
	fp.resource_id,
	fp.service_name,
	fp.type_name,

	er.uid, er.service_name, er.type_name, er.collection_name, er.resource_id,
	er.labels, er.reported_aliases, er.created_at, er.updated_at,
	erm.current_version, erm.fulfillment_id,
	ri.spec, ri.created_at,
	%s,
	inv.labels, inv.observation, inv.observed_at, inv.updated_at, inv.conditions
FROM filtered_page fp
JOIN extension_resources er ON er.uid = fp.uid
LEFT JOIN extension_resource_managed erm ON erm.extension_resource_uid = er.uid
LEFT JOIN resource_intents ri
	ON ri.extension_resource_uid = er.uid AND ri.version = erm.current_version
LEFT JOIN fulfillments f ON f.id = erm.fulfillment_id
%s
LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
ORDER BY %s
`,
		predicateSQL, keysetSQL, order.OrderBySQL, limitPlaceholder,
		fulfillmentColumnsJoined("f"),
		strategyJoins("f"),
		order.OrderBySQLQualified,
	)
}
