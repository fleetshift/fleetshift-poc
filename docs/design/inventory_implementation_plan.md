# Resource Identity, Extension API, and Inventory Implementation Plan

## Purpose

This plan describes how to implement the design changes introduced on the
`inventory` branch:

- two-layer resource identity and extension APIs
- addon-specific proto packages and HTTP API prefixes
- platform resources as canonical identity records
- extension representations with `managed`, `inventory`, and `target` roles
- inventory reporting with properties, observations, conditions, aliases, and
  relationships
- fleet-wide search over the indexed projection

The plan is intentionally phased so each increment can land as working code with
tests, without requiring the entire design to be implemented at once.

## Prototype Scope

Tenant and workspace scoping is intentionally out of scope for this prototype.
The implementation should use a single global identity domain:

- no `tenant_id`, `workspace_id`, or workspace hierarchy enforcement in new
  tables
- no workspace-relative resource names
- no workspace authorization model beyond the existing authentication checks
- no workspace-scoped search

Where future scoping is expected, use names and interfaces that can accept a
scope later, but keep the prototype behavior global. Do not block the prototype
on tenancy decisions.

## Current State

The current implementation already has several useful pieces:

- `domain.ManagedResource` and `resource_intents` provide versioned managed
  resource intent.
- `Fulfillment` is already the kernel primitive and managed resources drive a
  fulfillment through `RegisteredSelfTarget`.
- `DynamicSchemaActivator`, `DynamicServiceMux`, `DynamicHTTPMux`, and dynamic
  reflection already prove runtime API activation.
- `InventoryItem` and `InventoryRepository` exist, but are still a simple
  target-output catalog keyed by opaque `inventory_items.id`.
- `TargetRegistrar` and `ProcessDeliveryOutputs` currently write target
  inventory directly.

Known gaps against the branch design:

- `domain.ManagedResourceSchema` does not carry `ServiceName` or
  `ProtoPackage`.
- `DynamicSchemaActivator` hardcodes `fleetshift.v1`.
- Dynamic HTTP routes are under `/v1/{collection}` rather than
  `/apis/{service_name}/{version}/{collection}`.
- The current dynamic API assumes a resource type maps to one service and one
  collection. It cannot model multiple addons sharing `clusters/foo`.
- There is no platform resource aggregate, alias table, representation table,
  or identity claiming service.
- Inventory does not distinguish platform identity from extension
  representations and does not keep observation or condition history.

## API Design Guardrails

Use the AIP guidance already referenced by `docs/api-design.md`:

- AIP-122 resource names
- AIP-123 resource types
- AIP-127 HTTP and gRPC transcoding
- AIP-128 declarative-friendly interfaces
- AIP-129 server-modified values and effective labels
- AIP-136 custom methods
- AIP-154 freshness validation
- AIP-160 filtering

For this prototype:

- Extension APIs use full HTTP paths:
  `/apis/{service_name}/{version}/{collection}/{id}`.
- The platform API uses:
  `/apis/fleetshift.io/{version}/{collection}/{id}`.
- The existing `/v1/{collection}` dynamic path can remain temporarily as a
  compatibility alias while CLI and tests are migrated, but it should not be
  treated as the canonical API.
- Resource names returned inside messages remain relative names such as
  `clusters/prod`. Full names such as `//gcphcp.fleetshift.io/clusters/prod`
  are used for cross-service references and representation records.

## Cross-Cutting Implementation Rules

- Follow TDD per phase. Add failing domain/application/transport contract tests
  before implementation whenever practical.
- Keep domain model changes in `internal/domain`; repositories implement domain
  interfaces in SQLite/Postgres.
- Keep proto compilation, descriptor construction, HTTP transcoding, and
  reflection concerns in `internal/transport/managedresource`.
- Keep application services protocol-agnostic.
- Keep SQLite and Postgres repository contracts aligned.
- Run `go fmt ./...` from `fleetshift-server` after implementation work.
- Run at least `go test ./...` from `fleetshift-server` for server phases; add
  `task test:cli` when CLI behavior changes.

## Phase 1 - Namespace Dynamic Extension APIs

Goal: make addon-provided APIs transport-distinct before changing identity.

Detailed plan: [inventory_phase1_implementation_plan.md](inventory_phase1_implementation_plan.md).

### Work

1. Extend `domain.ManagedResourceSchema` with:
   - `ServiceName`, for example `kind.fleetshift.io`
   - `ProtoPackage`, for example `kind.fleetshift.v1`
   - `Version`, initially `v1`
   - an explicit collection identifier if needed to avoid overloading the
     current PascalCase `Plural`
2. Update `kindaddon.Schema()` and `gcphcpaddon.Schema()` with service names,
   proto packages, version, and shared collection IDs. For example:
   - Kind extension service: `kind.fleetshift.io`, proto package
     `kind.fleetshift.v1`, service `ClusterService`, collection `clusters`
   - GCP HCP extension service: `gcphcp.fleetshift.io`, proto package
     `gcphcp.fleetshift.v1`, service `ClusterService`, collection `clusters`
   - provider-specific names remain in the service name and spec message, not
     in the shared platform collection
3. Update `DynamicSchemaActivator` to build service names from
   `ProtoPackage + "." + Singular + "Service"`.
4. Include `ServiceName`, `ProtoPackage`, `Version`, and collection ID in the
   schema content hash.
5. Update descriptor file names and dynamic file registry keys so two addons
   can both have `ClusterService` without descriptor path collisions.
6. Change `DynamicHTTPMux` to key handlers by the full canonical prefix:
   `/apis/{service_name}/{version}/{collection}`.
7. Keep the old `/v1/{collection}` route only as a temporary compatibility
   alias, guarded by tests that make it clear it is not canonical.
8. Update CLI dynamic discovery so `ResourceType` includes service name,
   proto package, version, and collection. If two services expose the same
   collection, CLI commands must either require `--service` or fail with a clear
   ambiguity error.

### Tests

- Activating a schema registers `kind.fleetshift.v1.ClusterService`, not
  `fleetshift.v1.KindClusterService`.
- HTTP requests route at `/apis/kind.fleetshift.io/v1/clusters/{id}`.
- Two schemas with the same collection but different service names can both be
  active.
- Reflection lists both extension services.
- Schema hashes change when service name or proto package changes.
- CLI resource discovery displays service name and collection separately.

### Exit Criteria

Existing managed-resource create/get/list/delete flows still work through the
new canonical extension paths for Kind and GCP HCP resources.

## Phase 2 - Add Canonical Platform Resource Identity

Goal: introduce the platform resource identity layer without changing all
managed-resource CRUD behavior at once.

### Work

1. Add domain value types:
   - `ServiceName`
   - `APIVersion`
   - `CollectionID`
   - `RelativeResourceName`
   - `FullResourceName`
   - `Alias`
   - `RepresentationRole`
   - `Relationship`
2. Add a `PlatformResource` aggregate with snapshot support. It should model:
   - relative name, for example `clusters/prod`
   - UID
   - user labels
   - effective labels
   - aliases
   - relationships
   - representations
   - create/update/delete timestamps
3. Add a `ResourceIdentityRepository` interface and contract tests. Initial
   storage can use these tables:
   - `platform_resources`
   - `resource_representations`
   - `resource_aliases`
   - `resource_relationships`
4. Use global uniqueness for the prototype:
   - unique platform resource by relative name
   - unique alias by `(namespace, key, value)`
   - unique representation by `(service_name, collection, relative_name)`
5. Add `ResourceIdentityService` with operations:
   - claim or get platform identity by relative name
   - attach an extension representation
   - add aliases with conflict detection
   - set user labels on the platform resource
   - read platform resource views with representations
6. Return a conflict error when an alias already points at another platform UID.
7. Keep deletion user-driven in the prototype. Extension-reported deletion can
   mark a representation gone, but must not delete a pre-existing platform
   resource until that design is settled.

### Tests

- Claiming `clusters/prod` twice returns the same platform UID.
- A Kind representation and a GCP HCP representation can both attach to
  `clusters/prod`.
- Contradictory alias claims fail and do not partially write.
- Labels written on the platform resource appear in `effective_labels`.
- Extension labels are namespaced before entering `effective_labels`.
- SQLite and Postgres pass the same identity repository contract.

### Exit Criteria

The server can materialize and read platform resources independently of
managed-resource CRUD.

## Phase 3 - Dual Register Extension and Platform APIs

Goal: expose both typed extension APIs and generic platform resource APIs for
the same identity domain.

### Work

1. Change schema activation to return a richer `SchemaHandle` containing all
   transport registrations:
   - extension gRPC service name
   - extension HTTP prefix
   - platform gRPC service name
   - platform HTTP prefix
   - descriptor file paths
2. Add platform resource dynamic service generation. It should use generic
   platform resource messages with:
   - `name`
   - `uid`
   - `labels`
   - `effective_labels`
   - `conditions`
   - `representations`
   - `aliases`
   - `relationships`
3. Register platform services under the platform package and service name:
   - gRPC package `fleetshift.v1` for now
   - HTTP service name `fleetshift.io`
   - path `/apis/fleetshift.io/v1/{collection}`
4. Reference count platform services by collection. If two addons both register
   `clusters`, the platform `clusters` service should be registered once and
   shared.
5. Update managed-resource create flow:
   - create or claim the platform identity for the relative name
   - attach the extension representation with role `managed`
   - then start the existing create workflow
6. Update managed-resource delete flow:
   - delete or tombstone the managed representation
   - leave platform identity deletion as explicit platform API behavior
7. Add platform API Create/Get/List/Delete for the generic resource. In the
   prototype, Create should support pre-creating an identity with labels.
8. Keep platform Update/Patch for labels as a follow-up unless needed by tests.

### Tests

- Creating `POST /apis/kind.fleetshift.io/v1/clusters?cluster_id=prod` creates:
  - managed resource intent
  - derived fulfillment
  - platform resource `clusters/prod`
  - representation `//kind.fleetshift.io/clusters/prod`
- `GET /apis/fleetshift.io/v1/clusters/prod` returns the platform resource and
  its representation list.
- Registering Kind and GCP HCP cluster schemas does not collide on the platform
  `clusters` service.
- Disabling one addon removes its extension service but leaves the shared
  platform service if another addon still references the collection.
- gRPC reflection includes extension and platform services.

### Exit Criteria

The two-layer API model works end to end for managed resources, even before
inventory reporting is complete.

## Phase 4 - Replace Opaque Inventory with Extension Projections

Goal: evolve inventory from a target-output catalog into extension resource
projections linked to platform identity.

### Work

1. Replace or migrate `InventoryItem` toward an `InventoryProjection` model:
   - full extension resource name
   - service name
   - relative resource name
   - platform resource UID
   - representation roles
   - target ID, fulfillment ID, delivery ID, and optional manifest key
   - labels
   - properties
   - latest observation payload
   - latest conditions
   - observed timestamp
2. Add history tables:
   - `inventory_observations`
   - `inventory_condition_events`
3. Add repository contract tests for:
   - idempotent upsert
   - latest projection read
   - observation history append
   - condition transition deduplication
   - stale source cleanup by delivery ID
4. Add `InventoryReportService.ReportBatch` in the application layer. It should:
   - validate the reporting addon owns the service name
   - resolve or claim platform identity by relative name or aliases
   - attach/update the extension representation
   - write labels/properties/latest observations/latest conditions
   - append observation and condition event history
5. For unresolved alias-only reports, keep the prototype simple:
   - auto-create identity if the addon is authorized for that identity domain
   - reject reports for unregistered domains
   - defer an unresolved inbox and operator assignment UI
6. Update `TargetRegistrar` and `ProcessDeliveryOutputs` to write through the
   new inventory/reporting path instead of directly creating
   `target:<id>` inventory items.
7. Preserve delete-time cleanup by keeping delivery-owned output correlation.
   The cleanup code should remove projections and targets produced by a deleted
   delivery without depending on the old `target:` ID convention.

### Tests

- Reporting by relative name creates an inventory representation and linked
  platform identity.
- Reporting by alias attaches to an existing platform identity.
- Reporting a contradictory alias fails atomically.
- Re-reporting the same resource updates the latest projection and appends
  observation history.
- Condition event history records only real transitions.
- Delivery output cleanup removes delivery-owned inventory projections and
  targets.
- SQLite and Postgres contracts pass.

### Exit Criteria

Provisioned targets and addon observations are stored as extension projections
linked to platform resources, with no new code depending on the old opaque
inventory ID shape.

## Phase 5 - Add Search API Over the Projection

Goal: expose fleet-wide discovery over platform identity and extension
projections.

### Work

1. Add a static platform search service in `proto/fleetshift/v1`.
2. Implement the canonical HTTP form under the platform service prefix:
   `/apis/fleetshift.io/v1/{scope}:searchResources`.
3. Because workspace scoping is out of scope, accept only a global prototype
   scope. Use `-` as the global scope placeholder:
   `/apis/fleetshift.io/v1/-:searchResources`.
4. Add `SearchResources` application service over the inventory projection
   repository.
5. Use CEL for filters. Start with a small environment:
   - `name`
   - `service`
   - `collection`
   - `labels`
   - `effective_labels`
   - `conditions`
   - `properties`
   - `observations`
6. Implement pagination from the start, even if cursor encoding is simple.
7. Enforce existing authentication. Do not implement workspace RBAC in this
   prototype.

### Tests

- Search without a filter returns platform, managed, inventory, and target
  shapes visible in the global scope.
- Label filters work against `effective_labels`.
- Service and collection filters distinguish `kind.fleetshift.io/clusters` from
  `gcphcp.fleetshift.io/clusters`.
- Property and condition filters work for reported inventory projections.
- Invalid CEL returns `InvalidArgument`.
- Pagination is stable across pages.

### Exit Criteria

Users and tests can discover resources across extension APIs through one
platform search endpoint.

## Phase 6 - Project Inventory into Managed Resource Reads

Goal: make managed resource reads reflect the new inventory model.

### Work

1. Extend dynamic managed-resource envelope descriptors with:
   - `labels`
   - `effective_labels`
   - `properties`
   - `observations`
   - `conditions`
   - `representations` if useful for debugging
2. Update `dynamicHandler.viewToResource` so managed resource responses are
   projected from:
   - current spec from `resource_intents`
   - lifecycle state from `Fulfillment`
   - platform labels/effective labels from `PlatformResource`
   - properties/observations/conditions from the matching inventory projection
3. Add delivery condition persistence:
   - delivery-level latest conditions
   - delivery condition event history
4. Add fulfillment condition aggregation:
   - single-target pass-through first
   - CEL aggregation for multi-target later
5. Add domain support for `manifest_results`, since the proto already exposes
   it but `domain.DeliveryResult` currently does not.
6. Decide which delivery outputs remain delivery-local and which become
   inventory properties. For the prototype, provisioned target connection
   values such as `api_url`, `console_url`, and vault refs should be properties
   on the target representation.

### Tests

- Managed resource GET includes properties reported by the addon.
- Managed resource GET merges fulfillment operational conditions with inventory
  resource-health conditions.
- Delivery condition reports update delivery and fulfillment condition views.
- `manifest_results` are persisted and replaced on each apply attempt.
- Existing create/delete/resume tests continue to pass.

### Exit Criteria

The consumer-facing managed resource API no longer needs a separate status
table or ad hoc target-output lookup to show useful state.

## Phase 7 - Cleanup, Migration, and Hardening

Goal: remove temporary compatibility and harden the prototype.

### Work

1. Backfill existing inventory and target rows into platform resources and
   extension projections in migrations where feasible.
2. Remove code paths that assume:
   - `fleetshift.v1` for all dynamic extension services
   - `/v1/{collection}` as the canonical dynamic route
   - `target:<id>` as the inventory identity convention
3. Keep a compatibility route only if the CLI or demos still need it, and mark
   it clearly as temporary.
4. Update docs:
   - `docs/design/architecture/resource_identity_and_api.md` if implementation
     choices differ from design notes
   - `docs/design/architecture/resource_indexing.md` with the concrete
     prototype search/filter subset
   - `docs/design/architecture/open_questions.md` for remaining scoping,
     deletion, and versioning questions
5. Add observability probes for identity claim conflicts, report batch sizes,
   search latency, and condition-event writes.
6. Add load-oriented tests for the SQLite path to preserve the single-pod
   viability invariant.

### Tests

- Full server suite: `go test ./...` from `fleetshift-server`.
- CLI suite after discovery/path changes: `task test:cli`.
- Migration tests for SQLite and Postgres.
- Reflection tests after dynamic service cleanup.
- End-to-end managed resource flow through canonical `/apis/...` paths.

### Exit Criteria

The old inventory and dynamic API assumptions are no longer required for normal
operation.

## Deferred Design Areas

These should not block the prototype:

- tenant and workspace resource scoping
- workspace RBAC and relationship-based access control
- multi-version extension API coexistence
- short-form platform API aliases such as `/v1/clusters/foo`
- unresolved inventory inbox and operator assignment workflow
- extension-initiated deletion of pre-created platform identities
- generic semantic relationship query language
- federation and recursive platform search
- config-only resource types
- campaign APIs

## Suggested PR Breakdown

1. Schema metadata and `/apis/{service_name}/v1/...` routing.
2. Platform resource identity domain and repositories.
3. Dual registration of extension and platform resource APIs.
4. Managed-resource create/delete integration with platform identity.
5. Inventory projection repositories and report service.
6. Delivery output migration to inventory reporting.
7. Search API.
8. Managed-resource read projection from inventory.
9. Cleanup and compatibility removal.

Each PR should include focused contract tests and avoid combining storage,
transport, and CLI changes unless the slice cannot work without all three.
