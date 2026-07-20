# QueryResources CEL filters

This is the FleetShift-specific reference for the `filter` argument on
`queryResources` (CLI: `fleetctl resource query --filter`).

It assumes familiarity with [CEL](https://cel.dev/). This document
covers only FleetShift’s supported subset and the fields and value
rules that apply to query results.

## What the filter evaluates

Each candidate result is the same shape returned in the
`queryResources` response (and by typed Get/List for that extension
resource):

- Envelope identity: `name`, `resourceType`
- Resource body: `resource` — the ProtoJSON form of the extension
  resource for that type’s capabilities (managed fields, inventory
  fields, or both)

Filters should read like that JSON response. Copy field names from a
response payload when unsure.

An empty filter matches every resource in scope (subject to the
platform’s activation and authorization boundaries).

`order_by` is **not** CEL. It is a separate request field with its own
conventions (see [Ordering](#ordering)).

## Supported CEL subset

Only the following forms are accepted. Anything else fails the request
as invalid (for example unsupported macros, arithmetic, regex, or
string methods other than `startsWith`).

### Boolean structure

| Form | Meaning |
|------|---------|
| `a && b` | Both must match |
| `a \|\| b` | Either may match |
| `!a` | Negation |

Short-circuiting matters for missing fields: a left-hand `true \|\| …`
still matches even when the right-hand side would not (see
[Missing fields](#missing-fields)).

### Comparisons and membership

| Form | Meaning |
|------|---------|
| `a == b`, `a != b` | Equality / inequality |
| `a < b`, `a <= b`, `a > b`, `a >= b` | Ordering (when both sides are comparable) |
| `a in […]` | Value membership in a list of literals |
| `"key" in field` | Container membership (object key or list value; see below) |
| `has(field.path)` | Presence of a select path on the projected resource body |
| `field.startsWith("prefix")` | String prefix match |

Operands on each side of a comparison must be consistent with the
field’s value rules (see [Value types](#value-types)). Ordered
comparisons across incompatible types are rejected as invalid.

### Presence with `has()`

`has(path)` is true when `path` appears on the projected ProtoJSON
`resource` body. It follows standard CEL: the argument must be a
**select** (`has(resource.labels.team)`), not an index
(`has(resource.labels["team"])` is invalid).

Message-shaped containers such as `resource.spec` and
`resource.observation` remain present when the projected value is an
empty object `{}`. Map-shaped fields (`labels`, `localLabels`,
`conditions`) are omitted when empty, so `has(resource.labels)` is
false when there are no labels.

For free-form or non-identifier keys, use container membership instead
of trying to force `has` with brackets:

```text
has(resource.labels.team)              # identifier-safe key
"node-role" in resource.localLabels    # free-form key (use in, not has(index))
has(resource.conditions.Ready.status)
"status" in resource.conditions.Ready  # same meaning as the has() line above
has(resource.spec)                     # empty {} spec still present
"spec" in resource                     # same as has(resource.spec)
has(resource.pauseReason)              # false when omitted as ""
```

Presence matches the response body, not raw storage defaults. For
example, empty condition `reason` / `message` and a zero
`lastTransitionTime` are omitted from ProtoJSON and therefore fail
`has(...)` even if they exist in storage.

### Container membership with `"key" in field`

`"key" in <field path>` tests membership in a container field:

- If the field is an **object** (map, condition entry, message, or JSON
  object), the key is present when that object member exists (including
  a JSON `null` value).
- If the field is a **JSON array**, the key is present when a string
  element equal to `key` exists.
- If the field is missing, JSON `null`, or a scalar, the membership
  test is unknown and the row does not match (including under `!`).

For object-valued parents, `"key" in parent` has the same meaning as
`has(parent.key)` when both can be spelled. Prefer `"key" in …` when
the key is not a legal CEL identifier.

Value-list membership (`field in ["a", "b"]`) is unchanged and
distinct. Container membership requires a **string literal** on the
left and a **field path** on the right; forms such as `1 in
resource.labels` or `"x" in resource.pauseReason` are rejected.

### `timestamp()`

Use standard CEL `timestamp()` for **instant** comparisons:

```text
timestamp(resource.localUpdateTime) == timestamp("2026-06-01T08:00:00-04:00")
timestamp(resource.conditions["Ready"].lastTransitionTime) < timestamp("2026-06-01T13:00:00Z")
```

Rules:

- Apply `timestamp()` to **both** sides of the comparison (or both
  sides of each membership element).
- The argument may be a string-valued field path, or an RFC 3339 /
  RFC 3339 Nano string literal.
- Offset-equivalent literals denote the same instant
  (`…T12:00:00Z` and `…T08:00:00-04:00`).
- Prefer `timestamp()` whenever chronological / instant semantics are
  intended. Direct string comparison on timestamp fields matches the
  **exact ProtoJSON spelling** in the response, not “same instant”
  (see [Timestamps](#timestamps)).

## Field names

### Message fields: canonical JSON names only

Message fields use their **canonical ProtoJSON JSON names** (normally
lowerCamelCase, or an explicit `json_name` on the schema). There are
**no** proto-name / snake_case aliases.

| Use | Do not use |
|-----|------------|
| `resourceType` | `resource_type` |
| `resource.intentVersion` | `resource.intent_version` |
| `resource.pauseReason` | `resource.pause_reason` |
| `resource.localUpdateTime` | `resource.local_update_time` |
| `resource.conditions["Ready"].lastTransitionTime` | `…last_transition_time` |

### Map and Struct keys: exact literals

Keys in maps and `google.protobuf.Struct`-shaped JSON (`labels`,
`localLabels`, condition type keys, open `spec` / `observation`
paths without a typed message field) are **exact, case-sensitive data
keys**. They are never rewritten to camelCase.

| Equivalent when the key is the same string | Distinct keys |
|--------------------------------------------|---------------|
| `resource.labels.team` and `resource.labels["team"]` | `node_role` vs `nodeRole` |
| `resource.localLabels["node-role"]` | `Ready` vs `ready` (condition type) |

Use bracket form when the key is not a legal CEL identifier (hyphens,
dots, leading digits, quotes, and similar):

```text
resource.labels["node-role.kubernetes.io/worker"] == ""
resource.labels["has\"quote"] == "x"
```

Select and string-index syntax with the **same raw key** are
equivalent.

### Spec and observation paths

Under `resource.spec` and `resource.observation`:

- When the type has a known message schema for that path, segments
  must be the message’s JSON field names (again: no proto-name
  aliases). Invalid fields or illegal nesting are rejected.
- When there is no typed schema for a path, segments are treated as
  exact JSON object keys (same literal rules as maps).

Always copy keys from the response JSON for the type you are querying.

## Envelope fields

| Field | Type | Notes |
|-------|------|--------|
| `name` | string | Full resource name: `//{service}/{collection}/{id}` |
| `resourceType` | string | Type identity: `{service}/{TypeName}` |

Examples:

```text
name == "//kind.fleetshift.io/clusters/managed"
name.startsWith("//kind.fleetshift.io/")
resourceType == "kind.fleetshift.io/Cluster"
resourceType in ["kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"]
```

Top-level identity fragments such as `service_name`, `collection_name`,
or `resource_id` are **not** filter fields; use `name` / `resourceType`
instead. Discriminators that are not part of the public response
envelope (for example an internal `kind`) are not filterable.

## Resource body fields

Availability depends on the resource type’s capabilities (managed,
inventory, or both). Filtering a field that is not present for a given
row behaves like a missing path (non-match), unless the expression is
rejected as unsupported for the field set.

### Common / managed

| Field | Type in filters | Notes |
|-------|-----------------|--------|
| `resource.name` | string | Relative name: `{collection}/{id}` |
| `resource.uid` | string | Resource UID as a string |
| `resource.labels` | map\<string, string\> | User / extension labels on the managed resource |
| `resource.intentVersion` | string | ProtoJSON int64 as a **decimal string** |
| `resource.generation` | string | ProtoJSON int64 as a **decimal string** |
| `resource.state` | string | Fulfillment state **enum name** (e.g. `"ACTIVE"`, `"CREATING"`) |
| `resource.pauseReason` | string | Pause reason text |
| `resource.spec` | typed or open JSON | Addon-defined intent; see naming rules above |

`resource.state` matches the API / ProtoJSON enum spelling only (for
example `"CREATING"`). Storage or lowercase spellings such as
`"creating"` do not match.

### Inventory / observed state

| Field | Type in filters | Notes |
|-------|-----------------|--------|
| `resource.localLabels` | map\<string, string\> | Reporter-local labels |
| `resource.conditions` | map of condition objects | Keyed by **condition type** |
| `resource.conditions["T"].status` | string | e.g. `"True"`, `"False"`, `"Unknown"` |
| `resource.conditions["T"].reason` | string | |
| `resource.conditions["T"].message` | string | |
| `resource.conditions["T"].lastTransitionTime` | string | ProtoJSON timestamp string |
| `resource.observation` | open / typed JSON | Latest observation payload |
| `resource.localUpdateTime` | string | ProtoJSON timestamp of last local observation |
| `resource.indexUpdateTime` | string | ProtoJSON timestamp of last index update |

Condition type keys are case-sensitive (`"Ready"` ≠ `"ready"`).

Nested legacy paths such as `resource.inventory.…` are not supported;
use the flattened inventory fields above.

Platform-only body concepts that are not on the extension query
envelope (for example `effective_labels`, platform `nxt`,
`aliases`, `relationships` as filter roots) are rejected as
unsupported.

## Value types

Filters follow ProtoJSON / CEL typing. There is **no** silent
string↔number or string↔bool coercion.

| Stored JSON / field form | Literal that matches `==` |
|--------------------------|---------------------------|
| JSON string `"5"` | `"5"` only |
| JSON number `5` | `5` (numeric) only |
| JSON boolean `true` | `true` only |
| ProtoJSON int64 string field (`intentVersion`, `generation`) | `"1"` (string), not `1` |
| Known string fields (`state`, `pauseReason`, labels, …) | string literals |

For a **present** value of an incompatible type:

- `==` is false
- `!=` is true

Examples:

```text
resource.observation.cpu == 8          # JSON number
resource.observation.cpu == "8"        # JSON string "8"
resource.intentVersion == "1"          # matches
resource.intentVersion == 1            # never matches (heterogeneous)
resource.state == "ACTIVE"             # matches enum spelling
resource.state == "active"             # does not match
```

## Missing fields

A path that does not resolve (missing map key, missing nested field,
absent inventory on a managed-only row, and similar) is a **non-match**
for comparisons and value-list membership — including `!=`.

Use `has(...)` or `"key" in …` when you need to select rows where a
field or key is present regardless of its value:

```text
resource.observation.absent != 5       # does not select the row
has(resource.observation.absent)       # true only if that key exists
!has(resource.observation)             # managed-only / not-yet-reported rows
true || resource.observation.absent != 5   # still matches (short-circuit)
false && resource.observation.absent != 5  # does not match
has(resource.observation) || resourceType == "kind.fleetshift.io/Cluster"
```

Invalid `timestamp()` inputs (non-string JSON, non-RFC 3339 text) are
likewise non-matches for that comparison.

## Timestamps

Timestamp-valued response fields are **strings** in ProtoJSON form
(Z-normalized; 0, 3, 6, or 9 fractional digits).

### Direct string comparison

Matches only the exact response spelling:

```text
resource.localUpdateTime == "2026-06-01T12:00:00Z"                 # OK if that is the response
resource.conditions["Ready"].lastTransitionTime == "2026-06-01T12:00:00.500Z"
resource.conditions["Ready"].lastTransitionTime == "2026-06-01T12:00:00.5Z"     # no (different spelling)
resource.conditions["Ready"].lastTransitionTime == "2026-06-01T08:00:00-04:00" # no (same instant, different string)
```

### Instant comparison with `timestamp()`

Compares the underlying instant across spellings and offsets:

```text
timestamp(resource.conditions["Ready"].lastTransitionTime)
  == timestamp("2026-06-01T08:00:00.5-04:00")

timestamp(resource.localUpdateTime)
  == timestamp("2026-06-01T08:00:00-04:00")

timestamp(resource.labels["seenAt"])
  < timestamp("2026-06-01T13:00:00Z")
```

`timestamp()` may wrap any string-valued path (including ordinary
label or observation strings that hold RFC 3339 text).

## Ordering

`order_by` is a non-CEL request parameter:

| Value | Meaning |
|-------|---------|
| _(empty)_ | Server default stable order |
| `resource_type,name` | Order by resource type, then name |

Unsupported values fail the request. Page tokens are bound to the
filter and `order_by` used when they were issued.

## Examples

```text
# Kind clusters in a region that are active
resourceType == "kind.fleetshift.io/Cluster"
  && resource.spec.region == "us-east-1"
  && resource.state == "ACTIVE"

# Worker nodes by inventory label
resourceType == "kubernetes.fleetshift.io/Node"
  && resource.localLabels["node-role"] == "worker"

# Ready condition
resource.conditions["Ready"].status == "True"

# Names under a service prefix
name.startsWith("//kind.fleetshift.io/")

# Observation numeric threshold (JSON number)
resourceType == "kubernetes.fleetshift.io/Node"
  && resource.observation.capacity.cpu > 4

# Ready condition present (any status)
has(resource.conditions.Ready)
"Ready" in resource.conditions

# Hyphenated local-label key exists
"node-role" in resource.localLabels

# Nested observation key (object) or string in a JSON array field
"k" in resource.observation.tags
"k" in resource.observation.roles

# Instant: observed at or after noon UTC on 2026-06-01
timestamp(resource.localUpdateTime) >= timestamp("2026-06-01T12:00:00Z")

# Instant: Ready transitioned before a deadline (any equivalent literal)
timestamp(resource.conditions["Ready"].lastTransitionTime)
  < timestamp("2026-06-01T13:00:00Z")
```

## Quick checklist

1. Copy JSON names from a real `queryResources` / Get response.
2. Use camelCase message fields; never rely on snake_case aliases.
3. Treat map / Struct / condition-type keys as exact literals.
4. Compare ProtoJSON int64 and enum fields as strings / enum names.
5. Do not expect string↔number coercion.
6. Use `timestamp()` on both sides for chronological comparisons.
7. Remember missing paths do not match — including under `!=`.
8. Use `has(select)` or `"key" in field` for presence; `has(map["key"])` is invalid.
