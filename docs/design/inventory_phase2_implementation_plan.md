# Phase 2 Implementation Plan: Canonical Platform Resource Identity

## Goal

Implement the canonical platform resource identity layer described by the
two-layer API design.

After this phase, the server should be able to materialize, correlate, and read
platform resources independently of managed-resource CRUD and independently of
inventory reporting:

- one platform identity for `clusters/prod`
- multiple extension representations attached to that identity
- globally unique alias claims for the prototype
- user labels on the platform resource
- namespaced extension-contributed labels in `effective_labels`
- semantic relationships between platform identities

This phase creates the domain, repository, and application service foundation.
It intentionally does not expose the platform resource API over dynamic gRPC or
HTTP. That belongs to Phase 3.

## Non-Goals

- No dynamic platform service at `/apis/fleetshift.io/v1/{collection}`.
- No changes to managed-resource create/get/list/delete behavior yet.
- No inventory report API, observation history, or condition-event history.
- No search API.
- No workspace, tenant, or organization scoping.
- No workspace-relative resource names.
- No unresolved identity inbox for alias-only reports.
- No deletion policy for extension-observed deletion of pre-created platform
  resources.
- No migration or removal of the existing `InventoryItem` model.
- No UI or CLI behavior changes.

## Definition of Done

- Domain value types exist for service names, API versions, collections,
  relative names, full resource names, aliases, representation roles, and
  relationships.
- `PlatformResource` is a domain aggregate with snapshot support.
- `ResourceIdentityRepository` is defined in the domain layer.
- Shared repository contract tests cover identity, representations, aliases,
  labels, and relationships.
- SQLite and Postgres have migrations and repository implementations that pass
  the same contract.
- `domain.Store` / `domain.Tx` expose the identity repository.
- `ResourceIdentityService` supports claim/get, representation attachment,
  alias conflict detection, label updates, and relationship attachment.
- All behavior uses a single global identity domain for the prototype.
- The broader server test suite passes after formatting.

## Model Decisions for This Phase

### Global Prototype Scope

Tenant and workspace scoping remains out of scope. Do not add `tenant_id`,
`workspace_id`, workspace hierarchy columns, or scope-aware authorization in
this phase.

Identity uniqueness is global:

- one platform resource per relative name, for example `clusters/prod`
- one alias owner per `(namespace, key, value)`
- one extension representation per `(service_name, collection_id,
  relative_name)`

Use names and interfaces that could accept scope later, but keep the stored
schema and behavior global for now.

### Resource Names

Use these concepts consistently:

- `ServiceName`: versionless AIP service name, for example
  `kind.fleetshift.io` or `fleetshift.io`.
- `APIVersion`: URL/API version segment, initially `v1`.
- `CollectionID`: AIP collection identifier, for example `clusters`.
- `RelativeResourceName`: collection-qualified relative name, for example
  `clusters/prod`.
- `FullResourceName`: service-qualified name, for example
  `//kind.fleetshift.io/clusters/prod`.

`RelativeResourceName` includes the collection segment. Store `collection_id`
separately anyway because it is useful for indexes, service registration, and
future platform service reference counting.

Do not reuse the existing `domain.ResourceName` for platform identity. Today it
means the leaf managed-resource name, not the AIP relative name.

### Platform Resource Shape

The platform resource is the canonical identity record. It is generic and does
not include addon-specific spec or observation payloads.

For this phase it should model:

- `uid`
- `collection_id`
- `relative_name`
- user `labels`
- computed `effective_labels`
- extension representations
- aliases
- relationships
- `created_at`
- `updated_at`
- optional `deleted_at`

`effective_labels` should be computed for reads from platform labels plus
representation labels. It does not need to be stored as a separate column in
Phase 2.

### Representations

An extension representation is an extension API resource that models the same
platform identity through a typed service.

Representations should store:

- platform UID
- API service name, for example `kind.fleetshift.io`
- API version, for example `v1`
- collection ID
- relative resource name
- one or more roles:
  - `managed`
  - `inventory`
  - `target`
- extension-contributed labels
- `created_at`
- `updated_at`
- optional `deleted_at`

The full extension resource name is derived:

```text
//{service_name}/{relative_name}
```

For Phase 2, role validation should be conservative:

- at least one role is required
- role values must be from the known enum
- `managed` and `inventory` should not be combined yet
- `target` may combine with either `managed` or `inventory`

### Labels

Platform labels are user-writable and unprefixed.

Representation labels are extension-contributed. When included in
`effective_labels`, prefix them with the representation's service name:

```text
{service_name}/{key}
```

Example:

```text
labels["env"] = "prod"
representation labels from kind.fleetshift.io: {"provider": "kind"}
effective_labels["env"] = "prod"
effective_labels["kind.fleetshift.io/provider"] = "kind"
```

This is a prototype stand-in for the design's addon label namespace. If a later
capability-registration record gives a better label namespace, change the
service-layer merge helper in one place rather than spreading label prefixing
through repository code.

### Aliases

Aliases are namespaced key/value facts used for correlation.

Model each alias as:

- `namespace`
- `key`
- `value`

Examples:

- namespace: `gcp`, key: `cluster-id`, value: `abc123`
- namespace: `sig-multicluster`, key: `cluster-id`, value:
  `550e8400-e29b-41d4-a716-446655440000`

An alias can point at only one platform resource. Re-adding the same alias to
the same platform UID is idempotent. Adding an existing alias to a different
platform UID is a conflict and should return an error wrapping
`domain.ErrAlreadyExists`.

Do not implement alias-only unresolved reports in this phase.

### Relationships

Relationships are semantic links between different platform resources. They are
not extension representations.

Model each relationship as:

- source platform UID
- relationship type
- target platform UID
- optional source service name
- `created_at`

Use UID foreign keys in storage. Expose relative names and full resource names
in views when useful.

For Phase 2, relationships are simple directed edges. Relationship schema
extension and alias-based relationship references are deferred.

### Deletion

Keep deletion user-driven and conservative:

- deleting or tombstoning a representation must not delete the platform
  resource
- deleting a platform resource can be implemented as a soft delete if needed by
  tests, but platform API delete semantics are Phase 3+
- extension-observed deletion only tombstones the representation in this phase

## Work Plan

### Step 1 - Add Failing Domain and Service Tests

Start with tests that describe the identity behavior without involving gRPC,
HTTP, CLI, or inventory reporting.

New test areas:

- `fleetshift-server/internal/domain/resource_identity_test.go`
- `fleetshift-server/internal/domain/resourceidentityrepotest/contract.go`
- `fleetshift-server/internal/application/resource_identity_service_test.go`

Domain tests:

- `TestRelativeResourceName_ValidatesCollectionQualifiedName`
  - accepts `clusters/prod`
  - rejects empty names
  - rejects names without a collection segment
  - rejects empty path segments
- `TestFullResourceName_ConstructsAndParses`
  - constructs `//kind.fleetshift.io/clusters/prod`
  - extracts `kind.fleetshift.io` and `clusters/prod`
- `TestRepresentationRoles_Validate`
  - accepts `managed`
  - accepts `target + inventory`
  - rejects `managed + inventory`
  - rejects unknown roles
- `TestPlatformResource_EffectiveLabels`
  - user labels remain unprefixed
  - representation labels are service-prefixed

Repository contract tests:

- create/get platform resource by UID
- get platform resource by relative name
- duplicate relative name returns `ErrAlreadyExists`
- list by collection returns stable ordering
- update platform labels preserves creation time
- attach Kind and GCP HCP representations to the same platform UID
- attaching the same representation to the same UID is idempotent or updates in
  place
- attaching the same representation identity to a different UID returns
  `ErrAlreadyExists`
- add and resolve aliases
- re-adding an alias to the same UID is idempotent
- adding an alias to a different UID returns `ErrAlreadyExists`
- add and list relationships
- tombstoned representations are omitted from the active representation view
  unless explicitly requested

Application service tests:

- claiming `clusters/prod` twice returns the same UID
- claiming `clusters/prod` and attaching Kind/GCP HCP representations yields
  one platform resource with two representations
- contradictory alias claim fails and does not partially write
- setting platform labels updates `effective_labels`
- attaching representation labels namespaces them in `effective_labels`
- marking a representation gone leaves the platform resource readable

Keep these tests isolated from existing managed-resource CRUD. Phase 3 will
wire managed-resource create/delete through this identity service.

### Step 2 - Add Domain Value Types and Validation Helpers

Files:

- `fleetshift-server/internal/domain/resource_identity.go`
- `fleetshift-server/internal/domain/resource_identity_test.go`

Add value types:

```go
type ServiceName string
type APIVersion string
type CollectionID string
type RelativeResourceName string
type FullResourceName string
type PlatformResourceUID string
type AliasNamespace string
type AliasKey string
type AliasValue string
type RepresentationRole string
type RelationshipType string
```

Add structured values:

```go
type Alias struct {
    Namespace AliasNamespace
    Key       AliasKey
    Value     AliasValue
}

type Relationship struct {
    Type          RelationshipType
    TargetUID     PlatformResourceUID
    SourceService ServiceName
}
```

Add role constants:

```go
const (
    RepresentationRoleManaged   RepresentationRole = "managed"
    RepresentationRoleInventory RepresentationRole = "inventory"
    RepresentationRoleTarget    RepresentationRole = "target"
)
```

Validation rules:

- `ServiceName` must be non-empty and must not contain `/`.
- `APIVersion` must be non-empty and should start with `v`.
- `CollectionID` must be non-empty, lower-case, and path-safe.
- `RelativeResourceName` must be collection-qualified and path-safe.
- `FullResourceName` must use the `//{service}/{relative_name}` form.
- aliases must have non-empty namespace, key, and value.
- relationship type must be non-empty.

Implementation notes:

- Centralize parsing and construction helpers. Do not spread ad hoc string
  slicing through services or repositories.
- Avoid over-validating DNS or proto naming rules in Phase 2. The goal is to
  prevent malformed storage keys, not to fully implement AIP linting.
- Keep helpers allocation-light and easy to use in repository tests.

Suggested helpers:

```go
func NewRelativeResourceName(collection CollectionID, id string) (RelativeResourceName, error)
func (n RelativeResourceName) CollectionID() CollectionID
func (n RelativeResourceName) ID() string
func NewFullResourceName(service ServiceName, name RelativeResourceName) FullResourceName
func (n FullResourceName) ServiceName() ServiceName
func (n FullResourceName) RelativeName() RelativeResourceName
```

### Step 3 - Add PlatformResource Aggregate and Snapshots

Files:

- `fleetshift-server/internal/domain/resource_identity.go`
- `fleetshift-server/internal/domain/snapshot.go`

Add aggregate:

```go
type PlatformResource struct {
    uid          PlatformResourceUID
    collectionID CollectionID
    relativeName RelativeResourceName
    labels       map[string]string
    createdAt    time.Time
    updatedAt    time.Time
    deletedAt    *time.Time
}
```

Add companion values:

```go
type ResourceRepresentation struct {
    PlatformUID  PlatformResourceUID
    ServiceName  ServiceName
    Version      APIVersion
    CollectionID CollectionID
    RelativeName RelativeResourceName
    Roles        []RepresentationRole
    Labels       map[string]string
    CreatedAt    time.Time
    UpdatedAt    time.Time
    DeletedAt    *time.Time
}

type ResourceRelationship struct {
    SourceUID     PlatformResourceUID
    Type          RelationshipType
    TargetUID     PlatformResourceUID
    SourceService ServiceName
    CreatedAt     time.Time
}

type PlatformResourceView struct {
    Resource        PlatformResource
    Representations []ResourceRepresentation
    Aliases         []Alias
    Relationships   []ResourceRelationship
    EffectiveLabels map[string]string
}
```

Add constructors and reconstitution helpers:

- `NewPlatformResource`
- `PlatformResourceFromSnapshot`
- `ResourceRepresentationFromSnapshot`
- `ResourceRelationshipFromSnapshot`

Add aggregate methods:

- `UID()`
- `CollectionID()`
- `RelativeName()`
- `Labels()`
- `SetLabels(labels map[string]string, now time.Time)`
- `CreatedAt()`
- `UpdatedAt()`
- `DeletedAt()`
- `Snapshot()`

Add snapshot DTOs:

- `PlatformResourceSnapshot`
- `ResourceRepresentationSnapshot`
- `ResourceAliasSnapshot`
- `ResourceRelationshipSnapshot`

Implementation notes:

- Follow the existing snapshot pattern in `docs/domain.md`.
- Return defensive copies for maps and slices.
- Keep representation and alias mutation mostly service/repository-owned for
  Phase 2. The platform resource aggregate owns its own labels and identity
  fields.
- Compute effective labels in a helper such as
  `EffectiveLabels(platformLabels, representations)`.

### Step 4 - Define Repository Interface and Read Model Boundaries

Files:

- `fleetshift-server/internal/domain/repository.go`
- `fleetshift-server/internal/domain/store.go`

Add repository interface:

```go
type ResourceIdentityRepository interface {
    CreatePlatformResource(ctx context.Context, r *PlatformResource) error
    GetPlatformResourceByUID(ctx context.Context, uid PlatformResourceUID) (*PlatformResource, error)
    GetPlatformResourceByName(ctx context.Context, name RelativeResourceName) (*PlatformResource, error)
    ListPlatformResourcesByCollection(ctx context.Context, collection CollectionID) ([]PlatformResource, error)
    UpdatePlatformResourceLabels(ctx context.Context, r *PlatformResource) error

    PutRepresentation(ctx context.Context, rep ResourceRepresentation) error
    TombstoneRepresentation(ctx context.Context, service ServiceName, collection CollectionID, name RelativeResourceName, now time.Time) error
    ListRepresentationsByPlatformUID(ctx context.Context, uid PlatformResourceUID) ([]ResourceRepresentation, error)
    GetRepresentation(ctx context.Context, service ServiceName, collection CollectionID, name RelativeResourceName) (ResourceRepresentation, error)

    PutAlias(ctx context.Context, uid PlatformResourceUID, alias Alias, now time.Time) error
    ResolveAlias(ctx context.Context, alias Alias) (PlatformResourceUID, error)
    ListAliasesByPlatformUID(ctx context.Context, uid PlatformResourceUID) ([]Alias, error)

    PutRelationship(ctx context.Context, rel ResourceRelationship) error
    ListRelationshipsBySourceUID(ctx context.Context, uid PlatformResourceUID) ([]ResourceRelationship, error)
}
```

Adjust exact method names if implementation pressure suggests a simpler shape,
but keep these boundaries:

- repositories persist snapshots and child records
- repositories translate unique violations into domain errors
- repositories do not own cross-operation orchestration
- application services own multi-step claim/attach flows inside transactions

Update `domain.Tx`:

```go
ResourceIdentities() ResourceIdentityRepository
```

Update store implementations in SQLite and Postgres to return the new
repository.

### Step 5 - Add Shared Repository Contract Tests

File:

- `fleetshift-server/internal/domain/resourceidentityrepotest/contract.go`

Factory options:

- Prefer `Factory func(t *testing.T) domain.ResourceIdentityRepository` for
  repository-only behavior.
- Add a second `StoreFactory` only if transaction behavior needs to be tested
  at this layer.

Contract scenarios:

1. `CreateAndGetByUID`
2. `GetByRelativeName`
3. `DuplicateRelativeName`
4. `ListByCollection`
5. `UpdateLabels`
6. `PutRepresentation`
7. `PutRepresentationUpdatesSameIdentity`
8. `PutRepresentationConflictingPlatformUID`
9. `TombstoneRepresentation`
10. `PutAndResolveAlias`
11. `PutAliasIdempotentForSameUID`
12. `PutAliasConflictsForDifferentUID`
13. `PutRelationship`
14. `GetNotFoundCases`

Add the contract to:

- `fleetshift-server/internal/infrastructure/sqlite/repos_test.go`
- `fleetshift-server/internal/infrastructure/postgres/repos_test.go`

Also add a `storetest` assertion that `tx.ResourceIdentities()` is available
and participates in commit/rollback.

### Step 6 - Add Storage Migrations

SQLite file:

- `fleetshift-server/internal/infrastructure/sqlite/migrations/00026_resource_identity.sql`

Postgres file:

- `fleetshift-server/internal/infrastructure/postgres/migrations/00011_resource_identity.sql`

SQLite stores JSON as `TEXT`; Postgres stores JSON as `JSONB`.

Suggested SQLite schema:

```sql
-- +goose Up
CREATE TABLE platform_resources (
    uid           TEXT NOT NULL PRIMARY KEY,
    collection_id TEXT NOT NULL,
    relative_name TEXT NOT NULL UNIQUE,
    labels        TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    deleted_at    TEXT
);

CREATE INDEX idx_platform_resources_collection
    ON platform_resources(collection_id, relative_name);

CREATE TABLE resource_representations (
    service_name  TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    relative_name TEXT NOT NULL,
    platform_uid  TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    api_version   TEXT NOT NULL,
    roles         TEXT NOT NULL,
    labels        TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    deleted_at    TEXT,
    PRIMARY KEY (service_name, collection_id, relative_name)
);

CREATE INDEX idx_resource_representations_platform_uid
    ON resource_representations(platform_uid);

CREATE TABLE resource_aliases (
    namespace    TEXT NOT NULL,
    key          TEXT NOT NULL,
    value        TEXT NOT NULL,
    platform_uid TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (namespace, key, value)
);

CREATE INDEX idx_resource_aliases_platform_uid
    ON resource_aliases(platform_uid);

CREATE TABLE resource_relationships (
    source_uid     TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    type           TEXT NOT NULL,
    target_uid     TEXT NOT NULL REFERENCES platform_resources(uid) ON DELETE CASCADE,
    source_service TEXT,
    created_at     TEXT NOT NULL,
    PRIMARY KEY (source_uid, type, target_uid)
);

CREATE INDEX idx_resource_relationships_target_uid
    ON resource_relationships(target_uid);

-- +goose Down
DROP TABLE resource_relationships;
DROP TABLE resource_aliases;
DROP TABLE resource_representations;
DROP TABLE platform_resources;
```

Postgres should use the same shape, with `JSONB NOT NULL` for `labels` and
`roles`.

Migration notes:

- Do not backfill existing managed resources in Phase 2. Backfill or
  create-on-managed-resource-create belongs to Phase 3.
- Do not add workspace columns.
- Keep `relative_name` globally unique for the prototype.
- Use text timestamps to match existing repository conventions.

### Step 7 - Implement SQLite Repository

Files:

- `fleetshift-server/internal/infrastructure/sqlite/resource_identity_repo.go`
- `fleetshift-server/internal/infrastructure/sqlite/store.go`
- `fleetshift-server/internal/infrastructure/sqlite/repos_test.go`

Implementation requirements:

- Marshal label maps and role slices with `encoding/json`.
- Use `time.RFC3339` UTC timestamps, matching nearby repositories.
- Translate unique violations:
  - duplicate platform relative name -> `ErrAlreadyExists`
  - duplicate alias on another UID -> `ErrAlreadyExists`
  - duplicate representation on another UID -> `ErrAlreadyExists`
- Make re-putting the same alias for the same UID idempotent.
- Make re-putting the same representation for the same UID update roles,
  labels, API version, `updated_at`, and clear `deleted_at`.
- `ListRepresentationsByPlatformUID` should return active representations by
  default (`deleted_at IS NULL`).
- `TombstoneRepresentation` should set `deleted_at` and `updated_at`; it should
  not delete the platform resource.

Suggested implementation detail for alias idempotency:

1. Try `INSERT`.
2. On unique violation, `SELECT platform_uid`.
3. If it matches the requested UID, return nil.
4. If it differs, return `ErrAlreadyExists`.

Do not depend on SQLite-only UPSERT semantics for behavior that will be hard to
mirror in Postgres. Prefer explicit logic when it keeps the two repositories
consistent.

### Step 8 - Implement Postgres Repository

Files:

- `fleetshift-server/internal/infrastructure/postgres/resource_identity_repo.go`
- `fleetshift-server/internal/infrastructure/postgres/store.go`
- `fleetshift-server/internal/infrastructure/postgres/repos_test.go`

Implementation requirements:

- Use `$1` style placeholders.
- Use `JSONB` columns for labels and roles.
- Match SQLite domain error behavior exactly.
- Keep ordering stable in list methods.
- Add the missing identity repo contract runner to Postgres tests.

Postgres test note:

- Existing Postgres tests use fresh databases and `t.Parallel`. Keep the new
  contract compatible with that pattern.

### Step 9 - Add ResourceIdentityService

Files:

- `fleetshift-server/internal/application/resource_identity_service.go`
- `fleetshift-server/internal/application/resource_identity_service_test.go`

Service shape:

```go
type ResourceIdentityService struct {
    Store domain.Store
    Now   func() time.Time
    NewID func() domain.PlatformResourceUID
}
```

Default behavior:

- `Now` defaults to `time.Now().UTC`.
- `NewID` defaults to `uuid.New().String()` converted to
  `domain.PlatformResourceUID`.

Suggested inputs:

```go
type ClaimPlatformResourceInput struct {
    CollectionID  domain.CollectionID
    RelativeName  domain.RelativeResourceName
    Labels        map[string]string
    Aliases       []domain.Alias
}

type AttachRepresentationInput struct {
    PlatformUID  domain.PlatformResourceUID
    ServiceName  domain.ServiceName
    Version      domain.APIVersion
    CollectionID domain.CollectionID
    RelativeName domain.RelativeResourceName
    Roles        []domain.RepresentationRole
    Labels       map[string]string
}

type SetPlatformResourceLabelsInput struct {
    PlatformUID domain.PlatformResourceUID
    Labels      map[string]string
}

type AddRelationshipInput struct {
    SourceUID     domain.PlatformResourceUID
    Type          domain.RelationshipType
    TargetUID     domain.PlatformResourceUID
    SourceService domain.ServiceName
}
```

Service operations:

- `ClaimOrGet(ctx, in) (domain.PlatformResourceView, error)`
- `GetByUID(ctx, uid) (domain.PlatformResourceView, error)`
- `GetByRelativeName(ctx, name) (domain.PlatformResourceView, error)`
- `AttachRepresentation(ctx, in) (domain.PlatformResourceView, error)`
- `SetLabels(ctx, in) (domain.PlatformResourceView, error)`
- `AddAliases(ctx, uid, aliases) (domain.PlatformResourceView, error)`
- `TombstoneRepresentation(ctx, service, collection, name) error`
- `AddRelationship(ctx, in) (domain.PlatformResourceView, error)`

Implementation requirements:

- Every mutating operation opens one write transaction.
- `ClaimOrGet` is idempotent by relative name.
- Alias conflict checks happen in the same transaction as identity creation or
  update.
- If alias attachment fails, do not leave a newly created platform resource
  behind.
- `AttachRepresentation` verifies the platform resource exists before writing
  the representation.
- `AttachRepresentation` rejects a representation identity already attached to
  a different platform UID.
- Reads assemble `PlatformResourceView` by loading resource, representations,
  aliases, and relationships, then computing effective labels.

Validation should happen at the service boundary, even if constructors also
validate. Return `ErrInvalidArgument` for malformed inputs.

### Step 10 - Keep Existing Runtime Wiring Unchanged

Phase 2 should compile without changing the current dynamic transport behavior.

Do not yet:

- call `ResourceIdentityService` from `ManagedResourceService.Create`
- call `ResourceIdentityService` from dynamic HTTP handlers
- register platform resource dynamic services
- expose platform identity over CLI
- write identity rows from inventory processing

It is acceptable to instantiate the service in tests only. Production wiring can
wait until Phase 3 introduces the first caller.

### Step 11 - Update Documentation Pointers if Needed

Files:

- `docs/design/inventory_implementation_plan.md`
- optionally `docs/design/architecture/resource_identity_and_api.md`

Required:

- Link this detailed plan from the Phase 2 section of the parent inventory
  implementation plan.

Optional:

- If implementation chooses a different exact table shape, update the Phase 2
  parent plan after the code lands.
- Do not rewrite architecture docs as part of implementation unless the design
  changes. The phase plan is allowed to be more implementation-specific.

### Step 12 - Verification Commands

Run focused tests first:

```sh
cd fleetshift-server
go test ./internal/domain
go test ./internal/application
go test ./internal/infrastructure/sqlite
go test ./internal/infrastructure/postgres
```

Then run the default server suite:

```sh
cd fleetshift-server
go test ./...
```

Run formatting:

```sh
cd fleetshift-server
go fmt ./...
```

If sandboxed execution cannot write to the default Go build cache, set
`GOCACHE` to a writable temp directory for local verification.

No CLI test run is required for Phase 2 unless the implementation unexpectedly
touches CLI code.

## Suggested Commit Slices

1. Add domain value types, aggregate, snapshots, and domain tests.
2. Add `ResourceIdentityRepository`, `Tx` accessor, and failing shared contract
   tests.
3. Add SQLite migration and repository implementation.
4. Add Postgres migration and repository implementation.
5. Add `ResourceIdentityService` and application tests.
6. Run full verification and update docs if implementation details shifted.

Each slice should keep the focused package tests either failing for the next
missing implementation or passing before moving on.

## Risk Checklist

- **Scope creep into Phase 3**: do not register platform resource APIs yet.
- **Tenant/workspace leakage**: do not add placeholder scope columns that create
  ambiguous semantics.
- **Name confusion**: keep leaf managed-resource names separate from
  collection-qualified relative names.
- **Alias atomicity**: contradictory alias claims must not leave partial writes.
- **Representation collisions**: the same extension full resource name cannot
  attach to two platform UIDs.
- **Label collisions**: extension labels must be namespaced before entering
  `effective_labels`.
- **JSON drift between backends**: SQLite `TEXT` and Postgres `JSONB` must round
  trip the same Go maps/slices under shared contract tests.
- **Timestamp churn**: update `updated_at` only on writes; preserve `created_at`
  on idempotent reads and label updates.
- **Relationship ambiguity**: relationships link platform resources, not
  extension representations.
- **Old inventory coupling**: do not route existing `InventoryRepository` writes
  into the new identity repository until Phase 4.

## Phase 2 Exit Tests

At the end of this phase, these scenarios should pass:

1. `ClaimOrGet(clusters/prod)` creates a platform resource with a stable UID.
2. Calling `ClaimOrGet(clusters/prod)` again returns the same UID.
3. Kind and GCP HCP representations can both attach to `clusters/prod`.
4. A representation full resource name cannot attach to two platform resources.
5. An alias can resolve back to the platform resource that owns it.
6. A contradictory alias claim fails atomically.
7. Platform user labels appear unprefixed in `effective_labels`.
8. Extension representation labels appear with a service-name prefix in
   `effective_labels`.
9. A relationship from one platform resource to another can be stored and read.
10. SQLite and Postgres pass the same identity repository contract.
