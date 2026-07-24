# Advanced Search with CEL Expressions

**Jira:** [OME-210](https://redhat.atlassian.net/browse/OME-210)
**Status:** Draft
**Authors:** Martin Marosi

## Problem

The current search is client-side full-text (Orama) over plugin-contributed `fleetshift.search-index` extensions. It handles keyword lookups well but cannot express structured queries like field comparisons, label selectors, or date ranges. Users managing fleets need to filter by status, provider, node count, labels, and other resource attributes — things that require a query language, not a text box.

## Proposal

Add CEL (Common Expression Language) as the query language for advanced search. CEL is already used in the backend for output constraints and auth policy, so extending it to search gives a consistent expression language across the stack.

## Architecture

### Approach: Headless query builder with custom PatternFly UI

Use `@react-querybuilder/core` (Option A — logic only, no vendor UI) as the query manipulation and CEL formatting engine. Build a fully custom PatternFly interface on top.

**Why headless:** The UI must feel native to our PF design system with `ome-` scoped styles. Wrapping a third-party widget with `controlElements` overrides (Option B) still couples us to their component tree and context. The core package gives us everything we need without the UI baggage.

#### `@react-querybuilder/core` provides

| Function | Import | Purpose |
|----------|--------|---------|
| `formatQuery(query, 'cel')` | `@react-querybuilder/core` | Query object → CEL expression string |
| `parseCEL(celString)` | `@react-querybuilder/core/parseCEL` | CEL expression string → query object |
| `add`, `remove`, `update`, `move`, `group` | `@react-querybuilder/core` | Immutable query object manipulation |

These are pure functions — no React dependency. We manage query state in `useState` and build PF `Select`, `TextInput`, `Button`, `ChipGroup` components that call these utilities.

#### Data flow

```
User interacts with PF UI
  → query object updated via add/remove/update
  → formatQuery(query, 'cel') produces CEL string
  → CEL string sent as ?filter= param to QueryResources API
  → Go backend compiles CEL to SQL, returns matching resources
```

### Backend: existing `QueryResources` endpoint

The Go backend already has a `QueryResources` gRPC/REST endpoint that accepts a CEL `filter` string. The CEL expression is compiled to parameterized SQL (not evaluated at runtime) via an AST-to-SQL transpiler in `internal/infrastructure/querysql/querysql.go`.

**CEL environment variables:**

| Variable | Type | Description |
|----------|------|-------------|
| `name` | `string` | Full resource name, e.g. `//fleetshift/clusters/my-cluster` |
| `resourceType` | `string` | Resource type, e.g. `fleetshift/Cluster` |
| `resource` | `dyn` | Dynamic — access `.labels`, `.state`, `.spec.*`, `.conditions`, etc. |

**Supported CEL operators** (built-in only, no custom functions):

`==`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `||`, `!`, `in`, `startsWith`, `timestamp()`

### Resource fields available for filtering

| CEL Path | Type | Description |
|----------|------|-------------|
| `name` | string | Envelope full name |
| `resourceType` | string | `service/type` |
| `resource.name` | string | Relative name (`collection/id`) |
| `resource.uid` | string | UUID |
| `resource.labels["key"]` | string | Managed labels (JSONB) |
| `resource.localLabels["key"]` | string | Inventory labels |
| `resource.intentVersion` | int | Version counter |
| `resource.state` | enum | `CREATING`, `ACTIVE`, `DELETING`, `FAILED` |
| `resource.pauseReason` | string | Pause reason |
| `resource.generation` | int | Generation counter |
| `resource.spec.*` | dynamic | Schema-validated spec fields |
| `resource.observation.*` | dynamic | Schema-validated observation fields |
| `resource.conditions["Type"].status` | string | Condition status |
| `resource.conditions["Type"].reason` | string | Condition reason |
| `resource.conditions["Type"].message` | string | Condition message |
| `resource.conditions["Type"].lastTransitionTime` | timestamp | Condition transition time |
| `resource.localUpdateTime` | timestamp | Local update timestamp |
| `resource.indexUpdateTime` | timestamp | Index update timestamp |

## API Contract

### Filtering (exists today)

The UI sends CEL expressions directly as the `filter` query parameter to the existing `QueryResources` endpoint:

```
GET /v1/resources?filter=resource.state == "ACTIVE" && name.startsWith("clusters/prod")&pageSize=50
```

No new endpoints needed for basic filtering. The backend compiles the CEL to SQL and returns paginated results.

### Schema discovery (new — backend work required)

The backend holds resource type schemas in an in-memory `ActiveResourceRegistry`, populated when addons connect and activate resource types via `DynamicSchemaActivator`. The registry stores compiled proto descriptors per resource type, which the `FieldResolver` already walks to validate CEL field paths. **No API currently exposes this data to the UI.**

New endpoints needed:

```
GET /v1/resourceTypes
→ [
    { "type": "fleetshift/Cluster", "apiVersion": "v1" },
    { "type": "fleetshift/Target",  "apiVersion": "v1" },
    ...
  ]

GET /v1/resourceTypes/{type}/schema
→ {
    "type": "fleetshift/Cluster",
    "fields": [
      { "path": "spec.name",                "type": "string" },
      { "path": "spec.nodes",               "type": "repeated", "itemType": "message" },
      { "path": "spec.networking.podSubnet", "type": "string" },
      ...
    ]
  }
```

These read from the existing `ActiveResourceRegistry` and serialize the proto descriptors into a flat field list the UI can consume. The field `type` maps to CEL types so the UI knows which operators to offer.

### Schema change events (new — backend work required)

When the `ActiveResourceRegistry` mutates — addon installed, schema activated, resource type added/removed/updated — the backend broadcasts a WebSocket event so the UI can refresh its field registry without polling.

This is a **type-level** event, not per-instance. It fires on structural changes to the resource type collection:

- Addon installation / uninstallation
- Schema activation / deactivation
- Resource type collection added / removed / mutated

```
WS /api/ui/events/ws

← { "eventKind": "schema.registry.changed",
    "resourceType": "fleetshift/Cluster",
    "action": "activated",
    "timestamp": 1721474400 }
```

The UI receives this event and re-fetches `/v1/resourceTypes` and any cached per-type schemas. This keeps the field registry in sync without polling — the schema only changes on addon lifecycle events, not on every resource instance write.

## UI Design

### Philosophy: search bar, not form builder

No "nasty clicker" query builder with rows of dropdowns. The primary interface is a **search input with intelligent autocomplete** — feels like a code editor's command palette or a terminal with tab completion.

### Activation

The masthead search bar starts in its current simple text mode. Advanced search activates via:

- **Keyboard shortcut** — `/` + `Space` (or just `/` when search is not focused) opens advanced mode
- **Button** — small filter/advanced icon next to the search input

On activation, the search input **expands to full width** with a smooth animation, and a **results/preview panel** drops open below it.

### Layout (activated state)

```
┌─────────────────────────────────────────────────────────────────────┐
│ 🔍  resource.state == "ACTIVE" && name.startsWith("prod|")          │  ← full-width input
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Suggestions:                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ ▸ resource.state      enum    — Resource lifecycle state    │    │  ← autocomplete list
│  │   resource.generation number  — Generation counter          │    │
│  │   resource.labels     map     — Managed labels              │    │
│  │   resource.name       string  — Relative resource name      │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  CEL preview:                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ resource.state == "ACTIVE" && name.startsWith("prod")       │    │  ← formatted expression
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  [ Search ]  [ Clear ]                                    Esc close │
└─────────────────────────────────────────────────────────────────────┘
```

### Autocomplete behavior

The autocomplete is **context-aware** — it knows where the cursor is in the expression and what's valid next:

| Cursor position | Suggestions shown |
|----------------|-------------------|
| Empty / start of expression | Field names from registry (base + dynamic) |
| After a field name | Valid operators for that field's type |
| After an operator | Valid values (enum list for enums, "type a value" hint for strings/numbers) |
| After a complete clause | Combinators: `&&`, `\|\|` |
| After a combinator | Field names again (start next clause) |

**Tab completion:** pressing `Tab` accepts the first (closest match) suggestion. Arrow keys navigate the list. `Enter` on a suggestion inserts it.

### Autocomplete search index (Orama)

Build a **separate Orama index** for the field registry to power fuzzy autocomplete. Index entries are the available fields with their names, types, descriptions, and aliases. This gives us:

- Fuzzy matching — user types `stat` and gets `resource.state`
- Ranked results — exact prefix matches first, then fuzzy
- Fast client-side — no network round-trip for autocomplete

```ts
const searchIndex = create({
  schema: {
    fieldPath: "string",    // "resource.state"
    label: "string",        // "State"
    description: "string",  // "Resource lifecycle state"
    type: "string",         // "enum"
    aliases: "string[]",    // ["status", "lifecycle"]
  },
});

// Index base fields + dynamic fields from schema
insert(searchIndex, {
  fieldPath: "resource.state",
  label: "State",
  description: "Resource lifecycle state",
  type: "enum",
  aliases: ["status", "lifecycle"],
});
```

When dynamic schemas arrive (phase 2), their fields get inserted into the same index.

### CEL preview panel

Below the input, a **live preview** shows the current expression formatted and syntax-highlighted. As the user types, `parseCEL` attempts to parse the current input:

- **Valid expression** — preview shows the formatted CEL, highlighted. "Ready to search" indicator.
- **Partial expression** — preview shows what's been parsed so far + hint for what's needed next.
- **Invalid expression** — preview shows the error position with a helpful message ("expected operator after field name").

### Interaction flow

1. User presses `/` or clicks advanced search icon
2. Search bar animates to full width, dropdown opens
3. Autocomplete shows available fields immediately
4. User types `res` → autocomplete narrows to `resource.state`, `resource.name`, `resource.labels`, etc.
5. User presses `Tab` → `resource.state` inserted
6. Autocomplete now shows operators for enum type: `==`, `!=`
7. User types `==` or presses `Tab` for `==`
8. Autocomplete shows enum values: `ACTIVE`, `CREATING`, `DELETING`, `FAILED`
9. User selects `"ACTIVE"` → complete clause: `resource.state == "ACTIVE"`
10. Autocomplete offers `&&`, `||` to continue, or user presses `Enter` to search
11. CEL preview panel shows the final expression, search fires against `QueryResources`

### Deactivation

- `Esc` closes advanced search, reverts to normal search bar width
- Clicking outside the dropdown closes it but keeps the expression in the input
- Active filter expression persists as URL query param for shareability

### Search history & favorites

Store search history and user-favorited filters in **IndexedDB** (phase 1, local-only). Show them in the dropdown when the input is empty or as a separate section.

```
┌─────────────────────────────────────────────────────────────────────┐
│ 🔍  |                                                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ★ Favorites:                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ ★ Active clusters        resource.state == "ACTIVE"         │    │
│  │ ★ Prod fleet             name.startsWith("prod")            │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  Recent:                                                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ ↻ resource.state == "FAILED"                         2m ago │    │
│  │ ↻ resource.labels["env"] == "staging"               15m ago │    │
│  │ ↻ name.startsWith("kind-") && resource.state == ...  1h ago │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Phase 1 — IndexedDB (local):**

- History: last N searches (capped at ~50), deduplicated, most recent first
- Favorites: user clicks a star icon to pin a filter expression with a custom label
- Clicking a history/favorite item loads it into the search input
- Stored per-browser, no auth required

**Future — server-persisted (post-RBAC):**

- Saved filters stored per user on the backend
- Shareable filter links (URL with `?filter=` already covers basic sharing)
- Shared team filters — curated filters visible to all users in a workspace/org
- Requires user identity and RBAC — not in scope for phase 1

## Operator & Field Registry

The registry has two layers: **base fields** (static, same for every resource type) and **spec/observation fields** (dynamic, fetched per resource type from the schema endpoint).

### Operators (static)

```ts
const operators = [
  { name: "==",         label: "equals" },
  { name: "!=",         label: "not equals" },
  { name: "startsWith", label: "starts with" },
  { name: "<",          label: "less than" },
  { name: ">",          label: "greater than" },
  { name: "<=",         label: "at most" },
  { name: ">=",         label: "at least" },
  { name: "in",         label: "in" },
];
```

Operator availability per field is derived from the field's type:

| Field type | Available operators |
|------------|-------------------|
| `string` | `==`, `!=`, `startsWith` |
| `number` / `int` | `==`, `!=`, `<`, `>`, `<=`, `>=` |
| `enum` | `==`, `!=` |
| `map` | `in`, `==` (key lookup) |
| `timestamp` | `==`, `!=`, `<`, `>`, `<=`, `>=` |
| `bool` | `==`, `!=` |

### Base fields (static — hardcoded in UI)

These exist on every resource envelope regardless of type:

```ts
const baseFields = [
  { name: "name",                    type: "string" },
  { name: "resourceType",           type: "string" },
  { name: "resource.name",          type: "string" },
  { name: "resource.uid",           type: "string" },
  { name: "resource.state",         type: "enum",
    values: ["CREATING", "ACTIVE", "DELETING", "FAILED"] },
  { name: "resource.generation",    type: "number" },
  { name: "resource.intentVersion", type: "number" },
  { name: "resource.pauseReason",   type: "string" },
  { name: "resource.labels",        type: "map" },
  { name: "resource.localLabels",   type: "map" },
  { name: "resource.localUpdateTime",  type: "timestamp" },
  { name: "resource.indexUpdateTime",  type: "timestamp" },
];
```

### Spec/observation fields (dynamic — fetched from backend)

Fetched from `GET /v1/resourceTypes/{type}/schema` and merged with base fields when the user selects a resource type (or inferred from page context, e.g. "I'm on the clusters page").

The UI caches per-type schemas and invalidates on `schema.registry.changed` WebSocket events.

### Flow

1. User opens advanced search → base fields available immediately
2. User selects resource type (or page context implies it) → fetch schema → merge spec fields into picker
3. Addon installed/changed → WS event → re-fetch affected schemas → update field picker

## Decisions

1. **CEL validation** — client-side via `parseCEL` round-trip. The live preview panel shows parse errors inline before the request is sent. The backend will reject invalid expressions anyway, but the user should never have to wait for a network round-trip to learn their expression is malformed.
2. **Expression persistence** — URL hash fragment (`#filter=resource.state == "ACTIVE"`). Shareable, bookmarkable, doesn't pollute server logs with filter content, survives page refresh.

## Open Questions

1. **Cross-resource queries** — can a single filter span multiple resource types, or does the user pick a resource type first and then filter within it? Needs experimentation — unclear if the backend's CEL-to-SQL compiler handles unscoped queries across heterogeneous spec schemas.
2. **Keyboard shortcut conflict** — `/` is natural but may conflict if the search input is already focused. Need to test with existing masthead behavior.

## Security

CEL is compiled to SQL, not evaluated. The backend's `querysql.Compiler` handles parameterization, preventing SQL injection. The CEL environment has no custom functions and no side effects — only read-only field access and comparison operators.

Execution limits (expression complexity, AST depth) should be enforced server-side.

## Implementation Plan

### Phase 1 — Static search bar with autocomplete (frontend only)

No backend changes. Uses existing `QueryResources` endpoint with `?filter=`.

1. Add `@react-querybuilder/core` dependency — CEL formatting (`formatQuery`) and parsing (`parseCEL`)
2. Define static base field registry in `@fleetshift/common` — fields, types, operators, enum values, descriptions, aliases
3. Build Orama autocomplete index from the field registry — fuzzy field name matching for suggestions
4. Build advanced search components (shell — `packages/gui`):
   - **Search bar expansion** — full-width animation on activation (`/` shortcut or icon button)
   - **Autocomplete dropdown** — context-aware suggestions (fields → operators → values → combinators)
   - **Tab completion** — `Tab` accepts top suggestion, arrow keys navigate
   - **CEL preview panel** — live-parsed expression with syntax highlighting and error hints
5. Build cursor-position parser — determines where in the expression the cursor is and what token type is expected next (field, operator, value, combinator)
6. Wire `formatQuery(query, 'cel')` output to `QueryResources` API calls
7. Client-side validation via `parseCEL` round-trip — preview panel shows valid/partial/error state

8. Search history & favorites (IndexedDB):
   - `useSearchHistory()` hook — stores last ~50 searches, deduped, most recent first
   - `useSearchFavorites()` hook — star/unstar filters with custom labels
   - Show favorites + recent in dropdown when input is empty

**Scope:** base fields only (`name`, `resourceType`, `resource.state`, `resource.labels`, `resource.generation`, etc.). No `resource.spec.*` fields — those require dynamic schema from phase 2.

### Phase 2 — Dynamic schema discovery (requires backend work)

Backend discussion needed before starting.

1. **Backend:** expose `GET /v1/resourceTypes` and `GET /v1/resourceTypes/{type}/schema` from `ActiveResourceRegistry`
2. **Backend:** broadcast `schema.registry.changed` WS events on addon lifecycle mutations
3. **Frontend:** `useResourceTypeSchema(type)` hook — fetches and caches per-type field schemas, invalidates on WS event
4. **Frontend:** merge dynamic spec/observation fields into query builder field picker based on selected resource type
5. **Frontend:** resource type selector in query builder (or infer from page context)
