# Advanced Search with CEL

Advanced search lets users express precise fleet-wide resource filters beyond simple name-prefix searches, combining conditions in CEL. It sends the expression to `QueryResources` and displays matching resources in the masthead dropdown, using a raw-expression input with autocomplete, syntax highlighting, and browser-local history; the backend remains the authority on supported expressions and queryable fields.

Simple search is separate: it combines Orama feature search with a limited set of inventory prefix queries. See [Feature Contracts](feature-contracts.md), [Search Result Renderers](search-result-renderer-contract.md), and the [search diagram](diagrams/search/search.c4).

## Opening and using advanced mode

- Click the code icon beside search, or press Ctrl+Shift+F / Command+Shift+F to toggle advanced mode.
- Type an expression; nonempty changes trigger a query after a 400 ms debounce.
- Use arrow keys to select suggestions. Left/right also navigate operator suggestions when those are shown.
- Tab accepts the selected suggestion or history item. Enter executes the expression, executes a selected history entry, or follows a selected result; it does not insert the selected autocomplete suggestion.
- Escape closes the dropdown first. A subsequent Escape exits advanced mode.
- Clicking outside closes the dropdown while keeping the mounted input's expression.

The expression is component state, not a URL query parameter or hash. Exiting advanced mode unmounts that state. Refreshing or reopening does not restore it automatically; saved history can be selected explicitly.

Sources: [FleetSearch](../../client/web/src/components/Search/FleetSearch.tsx), [AdvancedSearchBar](../../client/web/src/components/Search/advanced/AdvancedSearchBar.tsx).

## Query flow and result limits

The UI keeps an expression string, parses cursor context for suggestions, and sends the string directly. It does not maintain a query-builder object or format one with `formatQuery`.

```text
Raw expression input
  → cursor-aware suggestions and preview
  → debounced or explicit execution
  → QueryResources filter parameter
  → inventory result renderers
```

The resource client URL-encodes the filter and uses platform scope `-`:

```text
GET /apis/open-cluster-management.io/v1alpha/-:queryResources?filter=...&pageSize=20
```

Advanced search requests one page of 20 results and does not follow `nextPageToken`. It waits for renderer extension resolution before querying. Results share the inventory renderer path with simple search, including disabled fallbacks for types without a renderer. Referenced field values can be displayed beside results as context; those values are not an explanation of which boolean branch matched.

Query errors are logged and converted into an empty result list. The dropdown does not distinguish an invalid or failed request from a successful query with no matches. The current UI also logs the CEL filter, so it does not provide a filter-content privacy guarantee.

Sources: [resource client](../../sdk/common/src/resourceApi.ts), [inventory search](../../client/web/src/hooks/useInventorySearch.tsx), [field extraction](../../client/web/src/components/Search/advanced/extractMatchFields.ts).

## Examples

These are decoded CEL expressions, not literal unencoded URLs:

```cel
resourceType == "kind.open-cluster-management.io/Cluster"
```

```cel
resourceType == "kubernetes.open-cluster-management.io/Object" && resource.observation.kind == "Pod"
```

```cel
resource.labels["env"] == "production" && resource.state == "ACTIVE"
```

```cel
has(resource.conditions.Ready.status) && resource.conditions.Ready.status == "False"
```

```cel
"node-role" in resource.localLabels
```

```cel
timestamp(resource.localUpdateTime) >= timestamp("2026-06-01T00:00:00Z")
```

Field availability depends on the resource type's managed/inventory capabilities and response shape. A full envelope name starts with `//{service}/...`; `resource.name` is the relative collection/name form.

## Autocomplete

The current suggestion sources are local:

- A hardcoded field tree with conditions, Kubernetes observation fields, resource-type values, namespace values, and shortcuts.
- Type-based operator lists, enum/value suggestions, `has()` path completion, and `&&` / `||` combinators.
- A semantic Orama index mapping search terms such as "clusters" or "running pods" to predefined expressions or values.

Field traversal uses prefix/label matching. Semantic suggestions provide fuzzy matching when direct field matching does not produce results. The separate `fieldSearchIndex.ts` utility is not used by the active advanced-search hook.

These suggestions are not fetched from active backend schemas and are not guaranteed to describe the installed resource types or valid values. Unknown `resource.*` paths can receive generic operator suggestions. Some static suggestions, such as `resource.createTime` and `resource.updateTime`, are not accepted by the current query field resolver. A suggestion is an editing aid, not a validity guarantee.

Sources: [advanced-search hook](../../client/web/src/components/Search/advanced/useAdvancedSearch.ts), [field tree](../../client/web/src/components/Search/advanced/fieldTree.ts), [field registry](../../client/web/src/components/Search/advanced/fieldRegistry.ts), [semantic index](../../client/web/src/components/Search/advanced/semanticIndex.ts), [operator map](../../client/web/src/components/Search/advanced/operatorMap.ts).

## Future work: inventory-driven discovery and caching

This is not implemented; the static autocomplete described above remains the current implementation. The backend should eventually own CEL discovery, producing hints and suggestions from active queryable inventory schemas, resource types, and the current inventory state, respecting the query scope.

The frontend should cache and reuse the discovered CEL tree and hint metadata rather than discover or rebuild the whole tree on every keystroke or search opening. Cached metadata should respect the query scope and be refreshed or invalidated when inventory or schemas change. Cursor handling and suggestion rendering should remain local to the UI.

## Preview and validation limits

The preview tokenizes and highlights the original expression. It does not produce a normalized CEL expression from a query-builder model.

Client validation runs after a short debounce and rejects unmatched double quotes or unequal parenthesis counts. It calls `parseCEL` from `@react-querybuilder/core/parseCEL`, but accepts parser failures and unrecognized forms after those basic checks. A green indicator is therefore best-effort feedback, not proof that the backend accepts the expression.

Neither live queries nor explicit execution are gated by this validation result. Partial or invalid input can reach the backend.

Sources: [validator](../../client/web/src/components/Search/advanced/celValidator.ts), [preview](../../client/web/src/components/Search/advanced/CelPreview.tsx), [execution](../../client/web/src/components/Search/advanced/useAdvancedSearch.ts).

## Backend semantics

Use the [QueryResources CEL reference](../query-resources-cel-filters.md) for the complete supported subset, missing-field behavior, and value rules. Important distinctions for UI authors are:

| Topic | Current semantics |
|-------|-------------------|
| Envelope variables | `name`, `resourceType`, and `resource` |
| Field spelling | Canonical ProtoJSON names; `resourceType`, not `resource_type` |
| Managed counters | `intentVersion` and `generation` are decimal strings, not numeric literals |
| Timestamps | Response strings; wrap both operands in `timestamp()` for instant comparisons |
| Presence | `has(resource.labels.team)` accepts a select; `has(resource.labels["team"])` is not supported |
| Membership | Both value-list membership and string-key container membership are supported |
| Resource fields | Availability depends on capabilities and the projected resource body |

The backend compiles its supported CEL subset into parameterized SQL. Unsupported operations and rejected field paths fail the request. Cross-resource-type filters are supported without choosing a type first. A top-level conjunctive `resourceType == "..."` guard enables descriptor-backed validation of spec/observation paths when a descriptor exists; without that guard or descriptor, those paths can use structural JSON extraction.

Sources: [CEL compiler](../../server/internal/infrastructure/querysql/querysql.go), [schema resolution](../../server/internal/infrastructure/querysql/resolve_context.go), [SQLite field resolver](../../server/internal/infrastructure/sqlite/query_filter.go).

## History and favorites

Explicit execution saves the trimmed expression in IndexedDB database `ocm-fleet-search-history`. Expressions are deduplicated by their text and sorted newest first. Automatic debounced queries do not save every edit.

History retains up to 50 nonfavorite entries. Favorites are retained separately from that cap and can be toggled or removed; they have no custom-label field. With an empty input, the dropdown shows favorites and up to ten recent nonfavorites. Selecting an entry loads its expression.

This is browser-local storage, not identity-scoped backend persistence or a shared-filter service. The current UI has no schema-discovery endpoint integration, schema-change event subscription, or server-provided autocomplete contract.

Sources: [history storage](../../client/web/src/components/Search/advanced/useSearchHistory.ts), [history panel](../../client/web/src/components/Search/advanced/HistoryPanel.tsx), [dropdown behavior](../../client/web/src/components/Search/advanced/AdvancedSearchBar.tsx).
