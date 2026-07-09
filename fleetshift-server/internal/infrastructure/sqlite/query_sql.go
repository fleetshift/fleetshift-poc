package sqlite

import "fmt"

// buildQueryResourcesSQL assembles QueryResources' single extension-only
// query. It runs in two conceptual stages, mirroring the Postgres
// sibling (see postgres/query_sql.go):
//
//  1. filtered_page (MATERIALIZED) selects candidate extension_resources
//     rows (plus the LEFT JOINs filter fields may need), applies
//     predicateSQL (the compiled CEL filter) and keysetSQL (page-token
//     resume), then orders and LIMITs down to the page window using a
//     supported order whose columns match a composite B-tree index
//     (see idx_extension_resources_query_order /
//     idx_extension_resources_type_query_order). MATERIALIZED keeps
//     SQLite from pulling the limit past the hydration joins.
//  2. The outer SELECT hydrates only that page window by joining
//     extension_resources (and the same managed/intent/fulfillment/
//     inventory/strategy tables [ExtensionResourceRepo.GetView] uses)
//     from filtered_page fp on er.uid = fp.uid -- one data query
//     total, no per-row follow-up read, and no full-table hydration.
//
// Platform aggregate rows are intentionally not part of this query.
//
// predicateSQL and keysetSQL are trusted, pre-built SQL fragments
// (parameterized with "?" placeholders only; QueryRepo wires
// querysql.QuestionParams, and user input never reaches this function
// as raw text). order is a supported order from resolveQueryOrder.
//
// # Indexing notes
//
// Default empty-filter pagination seeks through
// idx_extension_resources_query_order. The resource_type,name order
// mode seeks through idx_extension_resources_type_query_order.
// resource_type == "service/Type" and resource_type in [...] compile
// to constituent service_name/type_name predicates (see
// query_filter.go), so those filters can use the type-scoped
// composite index.
//
// Label/condition equality uses json_extract / ->> residual filters.
// SQLite has no GIN-equivalent; that is acceptable for the single-pod
// / small-fleet deployment target. Hot-key generated columns can be
// added later if a workload needs them.
func buildQueryResourcesSQL(predicateSQL, keysetSQL string, order querySupportedOrder) string {
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
	LIMIT ?
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
		predicateSQL, keysetSQL, order.OrderBySQL,
		fulfillmentColumnsJoined("f"),
		strategyJoins("f"),
		order.OrderBySQLQualified,
	)
}
