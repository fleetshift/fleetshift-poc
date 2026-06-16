# Phase 1 Implementation Plan: Namespaced Dynamic Extension APIs

## Goal

Implement the first slice of the inventory branch design: addon-provided APIs
must be transport-distinct by service name, proto package, version, and HTTP
prefix.

After this phase, two addons should be able to expose the same resource
collection, such as `clusters`, without colliding:

- `kind.fleetshift.v1.ClusterService`
- `gcphcp.fleetshift.v1.ClusterService`
- `/apis/kind.fleetshift.io/v1/clusters/...`
- `/apis/gcphcp.fleetshift.io/v1/clusters/...`

This phase does not introduce canonical platform resources or shared identity
records. It only makes the extension API layer correctly namespaced so the next
phases have a clean transport foundation.

## Non-Goals

- No platform resource service at `/apis/fleetshift.io/v1/...`.
- No platform identity aggregate, alias correlation, or representation table.
- No inventory schema migration.
- No tenant or workspace scoping.
- No removal of the old `/v1/{collection}` route unless all callers are already
  migrated. It may remain as a temporary compatibility alias.
- No multi-version service coexistence beyond carrying `Version: "v1"` in
  schema metadata and HTTP routing.

## Definition of Done

- Addon schemas carry API service name, proto package, API version, and explicit
  collection ID.
- Dynamic gRPC services are registered under addon proto packages, not
  `fleetshift.v1`.
- Dynamic HTTP services are registered at canonical `/apis/{service}/{version}`
  prefixes.
- Two active dynamic services may use the same collection ID when their service
  names differ.
- gRPC reflection exposes both dynamic services.
- CLI dynamic discovery and invocation no longer assumes `fleetshift.v1`.
- Existing Kind and GCP HCP managed-resource flows work through canonical
  namespaced routes.

## Naming Decisions for This Phase

Use separate names for separate concepts:

- `APIServiceName`: versionless AIP service name used in full resource names and
  HTTP prefixes, for example `kind.fleetshift.io`.
- `ProtoPackage`: versioned proto package used by gRPC, for example
  `kind.fleetshift.v1`.
- `GRPCServiceName`: fully qualified gRPC service name derived from proto
  package and singular service type, for example
  `kind.fleetshift.v1.ClusterService`.
- `Version`: HTTP API version segment, initially `v1`.
- `CollectionID`: AIP collection identifier, for example `clusters`.
- `Singular` and `Plural`: PascalCase RPC/message naming inputs, for example
  `Cluster` and `Clusters`.

Do not use `ResourceType` as the transport routing key. Keep it as the current
managed-resource/domain discriminator until Phase 2 decides how to map resource
types to identity domains.

## Compatibility Rule

For Phase 1, preserve the existing lowerCamel route as a compatibility alias
only if needed:

- canonical: `/apis/kind.fleetshift.io/v1/clusters/prod`
- temporary alias: `/v1/kindClusters/prod`

New tests and docs should use the canonical route. Compatibility aliases should
be easy to delete in Phase 7.

## Work Plan

### Step 1 - Add Failing Transport and CLI Tests

Add or update tests before implementation so the expected behavior is pinned
down.

Server tests:

- `TestDynamicSchemaActivator_UsesAddonProtoPackage`
  - activate Kind schema
  - expect handle gRPC service `kind.fleetshift.v1.ClusterService`
  - assert `fleetshift.v1.KindClusterService` is absent
- `TestDynamicSchemaActivator_RegistersCanonicalHTTPRoute`
  - expect `/apis/kind.fleetshift.io/v1/clusters/test-id` to route
  - expect this test to avoid `/v1/kindClusters`
- `TestDynamicSchemaActivator_AllowsSameCollectionAcrossServices`
  - activate Kind and GCP HCP schemas with `CollectionID: "clusters"`
  - expect both gRPC services in `DynamicServiceMux.ServiceInfo`
  - expect both HTTP prefixes to route independently
- `TestSchemaContentHash_IncludesTransportIdentity`
  - changing `APIServiceName`, `ProtoPackage`, `Version`, or `CollectionID`
    changes the hash
- `TestDynamicHTTPMux_KeyedByFullPrefix`
  - registering two services with collection `clusters` but different API
    service names does not return duplicate-route errors
- `TestCompositeReflection_ListsNamespacedDynamicServices`
  - reflection returns both namespaced dynamic services

Application tests:

- update `recordingActivator` and `addon_manager_test.go` to use richer schema
  handles and to verify deactivate uses the same handle returned by activate.

CLI tests:

- `TestClient_ListResourceTypes_NamespacedPackages`
  - discovery includes dynamic services outside `fleetshift.v1`
- `TestClient_ResolveType_AmbiguousCollection`
  - two services expose collection `clusters`
  - resolving by collection alone fails with a clear ambiguity error
- resource command tests should include a way to pick a service explicitly when
  a collection is ambiguous.

### Step 2 - Extend Domain Schema Metadata

File: `fleetshift-server/internal/domain/addon.go`

Extend `ManagedResourceSchema` with:

```go
APIServiceName string
ProtoPackage   string
Version        string
CollectionID   string
```

Implementation notes:

- Prefer typed aliases only if they simplify validation without spreading
  conversion noise through transport code.
- Validate empty fields in the activator first; domain constructors can come in
  a later cleanup if needed.
- Keep `Singular` and `Plural` as PascalCase inputs for generated RPC and
  message names.
- Keep `ResourceType` unchanged for existing managed resource repository and
  fulfillment relation behavior.

Minimum validation rules:

- `APIServiceName` must be non-empty.
- `ProtoPackage` must be non-empty.
- `Version` must be non-empty and should start with `v`.
- `CollectionID` must be non-empty and lower-case path-safe for first
  implementation.
- `Singular` and `Plural` remain PascalCase.

### Step 3 - Update Built-In Addon Schemas

Files:

- `fleetshift-server/internal/addon/kind/descriptor.go`
- `fleetshift-server/internal/addon/gcphcp/descriptor.go`

Change the schema definitions:

Kind:

```go
APIServiceName: "kind.fleetshift.io",
ProtoPackage:   "kind.fleetshift.v1",
Version:        "v1",
Singular:       "Cluster",
Plural:         "Clusters",
CollectionID:   "clusters",
SpecMessage:    "addons.kind.v1.KindClusterSpec",
```

GCP HCP:

```go
APIServiceName: "gcphcp.fleetshift.io",
ProtoPackage:   "gcphcp.fleetshift.v1",
Version:        "v1",
Singular:       "Cluster",
Plural:         "Clusters",
CollectionID:   "clusters",
SpecMessage:    "addons.gcphcp.v1.GCPHCPClusterSpec",
```

Rationale:

- Provider-specific naming belongs in `APIServiceName`, `ProtoPackage`, and the
  spec message.
- The collection remains `clusters` so Phase 2 can attach both extensions to
  the same platform identity domain.

### Step 4 - Refactor ResourceTypeConfig

File: `fleetshift-server/internal/transport/managedresource/config.go`

Add fields mirroring schema metadata:

```go
APIServiceName string
Version        string
CollectionID   string
ProtoPackage   string
```

Add helper methods:

- `GRPCServiceName() string`
- `CanonicalHTTPPrefix() string`
- `LegacyHTTPPrefix() string`
- `ResourceCollection() string`

Update existing call sites:

- use `GRPCServiceName()` where code currently calls `ServiceName()`
- use `CollectionID` directly instead of deriving lowerCamel from `Plural`
- keep a fallback from `Plural` to lowerCamel only for tests or temporary
  compatibility fixtures

Avoid keeping two methods named `ServiceName`; the design now has API service
names and gRPC service names, and ambiguity will create bugs.

### Step 5 - Update Descriptor Generation

Files:

- `fleetshift-server/internal/transport/managedresource/descriptor_builder.go`
- descriptor builder tests

Required changes:

- Generate the dynamic proto file with `Package: cfg.ProtoPackage`.
- Generate the service named `{Singular}Service`.
- Generate message names from `Singular` and `Plural`, now usually `Cluster`
  and `Clusters`.
- Use `CollectionID` for list response fields and resource name prefixes.
- Make synthesized descriptor file paths unique by package, for example:
  `dynamic/kind/fleetshift/v1/cluster_service.proto`.
- Keep spec descriptors imported from addon proto files as they are today.

Important test updates:

- `BuildServiceDescriptors` should work for two configs with the same
  `Singular`, `Plural`, and `CollectionID` when `ProtoPackage` differs.
- Descriptor lookup in tests should use `cfg.ProtoPackage + "." + message`.
- Existing validation tests should add missing `APIServiceName`, `Version`, and
  `CollectionID` cases.

### Step 6 - Update Dynamic Schema Activation

File: `fleetshift-server/internal/transport/managedresource/schema_activator.go`

Required changes:

- Build `ResourceTypeConfig` from schema metadata; remove hardcoded
  `fleetshift.v1`.
- Compute the activation handle from the gRPC service name and canonical HTTP
  prefix.
- Change hash key from old service name to gRPC service name.
- Include these in `schemaContentHash`:
  - `APIServiceName`
  - `ProtoPackage`
  - `Version`
  - `CollectionID`
  - `Singular`
  - `Plural`
  - `SpecMessage`
  - proto file contents
- Replace `dynamicFilePath(handle.ServiceName)` with a handle-carried
  descriptor path or a deterministic helper that does not assume
  `fleetshift.v1`.
- On HTTP or file-registry failure, deregister using the namespaced gRPC service
  and canonical HTTP prefix.

Update `application.SchemaHandle` in
`fleetshift-server/internal/application/addon_manager.go`:

```go
type SchemaHandle struct {
    GRPCServiceName string
    APIServiceName  string
    Version         string
    CollectionID    string
    HTTPPrefixes    []string
    DescriptorPaths []string
}
```

If that feels too broad for Phase 1, the minimum is:

```go
type SchemaHandle struct {
    GRPCServiceName string
    HTTPPrefix      string
    DescriptorPath  string
}
```

Prefer the richer form if compatibility aliases are registered because
deactivation needs to remove every handler installed by activation.

### Step 7 - Rework DynamicHTTPMux Prefixing

Files:

- `fleetshift-server/internal/transport/managedresource/dynamic_mux.go`
- `fleetshift-server/internal/transport/managedresource/http.go`

Required changes:

- Register handlers by canonical prefix:
  `/apis/{APIServiceName}/{Version}/{CollectionID}`.
- Key `handlers` by exact prefix, not collection.
- Let `Register`, `Replace`, and `Deregister` accept prefix information from
  `RegisteredService` or `SchemaHandle`.
- `buildHTTPHandler` should trim the canonical prefix, not `/v1/{collection}`.
- Add optional alias registration:
  `/v1/{legacyCollection}`.

Routing behavior:

- `POST /apis/kind.fleetshift.io/v1/clusters?cluster_id=prod`
  invokes `CreateCluster`.
- `GET /apis/kind.fleetshift.io/v1/clusters/prod`
  invokes `GetCluster` with name `clusters/prod`.
- `GET /apis/gcphcp.fleetshift.io/v1/clusters/prod`
  routes to a different handler and gRPC service.

The request ID query parameter should derive from `Singular`, so
`Cluster` yields `cluster_id`.

### Step 8 - Update Dynamic gRPC Handler Assumptions

File: `fleetshift-server/internal/transport/managedresource/registrar.go`

Required changes:

- Use `cfg.GRPCServiceName()` in full method names.
- Use `cfg.ProtoPackage` when resolving generated message descriptors.
- Use `cfg.CollectionID + "/"` for `parseName`.
- Keep application service calls using `cfg.ResourceType` for now.

Potential pitfall:

- If both Kind and GCP HCP use `ResourceType` values that differ from
  collection, create/list/get must still call the existing managed-resource
  service by the old resource type. That is expected in Phase 1.

### Step 9 - Update File Registry and Reflection

Files:

- `fleetshift-server/internal/transport/managedresource/dynamic_file_registry.go`
- `fleetshift-server/internal/transport/managedresource/composite_resolver.go`
- reflection tests

Required changes:

- Registry keys must be descriptor paths that include proto package or a
  sanitized gRPC service name.
- Reflection should list namespaced gRPC services.
- `FileContainingSymbol` for `kind.fleetshift.v1.ClusterService` must return
  the Kind dynamic descriptor, not the GCP HCP one.
- Deactivating one service must not remove another service with the same
  `Singular` and `CollectionID`.

### Step 10 - Update AddonManager Test Doubles

Files:

- `fleetshift-server/internal/application/addon_manager.go`
- `fleetshift-server/internal/application/addon_manager_test.go`

Required changes:

- Update `recordingActivator` to compute handles and hashes with new schema
  metadata.
- Ensure stale schema deactivation passes the exact handle returned by
  activation.
- Add a test where two schemas have the same `CollectionID` but distinct gRPC
  service names.

No storage changes should be needed in this phase.

### Step 11 - Update CLI Dynamic Discovery

Files:

- `fleetshift-cli/internal/dynamic/client.go`
- `fleetshift-cli/internal/cli/resource_types.go`
- `fleetshift-cli/internal/cli/resource_describe.go`
- `fleetshift-cli/internal/cli/resource_create.go`
- `fleetshift-cli/internal/cli/resource_get.go`
- `fleetshift-cli/internal/cli/resource_list.go`
- `fleetshift-cli/internal/cli/resource_delete.go`
- CLI tests

Required changes:

- Stop filtering dynamic services by `strings.HasPrefix(svcName,
  "fleetshift.v1.")`.
- Exclude known static services by exact name as today.
- Detect dynamic resource services by descriptor shape:
  - service name ends in `Service`
  - has `Create{Singular}`, `Get{Singular}`, `List{Plural}`, and
    `Delete{Singular}` methods
  - resource message has a `spec` field
- Store proto package from the service descriptor.
- Find generated messages using proto package:
  - `{proto_package}.{Singular}`
  - `{proto_package}.Create{Singular}Request`
  - `{proto_package}.List{Plural}Response`
- Use the list response field name to infer `CollectionID` instead of deriving
  lowerCamel from `Plural`.
- Add service disambiguation:
  - `fleetctl resource list clusters --service kind.fleetshift.v1.ClusterService`
  - if no service is supplied and multiple services match `clusters`, return an
    ambiguity error listing candidates.

CLI can use the gRPC service name as the disambiguator in this phase. Mapping
that to the AIP API service name can wait until a typed discovery endpoint or
resource annotations exist.

### Step 12 - Update Server Bootstrap Comments and Routing

File: `fleetshift-server/internal/cli/serve.go`

Required changes:

- Update comments that describe dynamic routes as `/v1/...`.
- Ensure the top-level mux does not route `/apis/...` to a static catch-all
  before `DynamicHTTPMux` can handle it.
- Keep grpc-gateway static routes working at `/v1/...`.

### Step 13 - Verification Commands

Run focused tests first:

```sh
cd fleetshift-server
go test ./internal/transport/managedresource
go test ./internal/application
```

Then run server-wide tests:

```sh
cd fleetshift-server
go test ./...
```

Run CLI tests after the discovery changes:

```sh
task test:cli
```

Run formatting:

```sh
cd fleetshift-server
go fmt ./...
cd ../fleetshift-cli
go fmt ./...
```

If sandboxed execution cannot write to the default Go build cache, set
`GOCACHE` to a writable temp directory for local verification.

## Suggested Commit Slices

1. Add schema metadata fields and update builtin addon schema values.
2. Update descriptor builder/config tests and implementation.
3. Update schema activator, file registry, and reflection.
4. Update HTTP mux canonical prefixing and compatibility alias.
5. Update CLI discovery and service disambiguation.
6. Run full verification and clean up obsolete test expectations.

Each slice should preserve a passing focused test package before moving to the
next one.

## Risk Checklist

- Descriptor path collisions: avoid by including proto package or gRPC service
  in the dynamic file path.
- HTTP route collisions: avoid by keying handlers by full prefix.
- CLI ambiguity: fail explicitly rather than silently picking the first
  `clusters` service.
- Hidden `fleetshift.v1` assumptions: search for `fleetshift.v1.` in
  `internal/transport/managedresource` and `fleetshift-cli/internal/dynamic`.
- Resource name parsing: ensure `clusters/prod` remains the in-message relative
  name even though HTTP is service-prefixed.
- Compatibility aliases: keep tests clear about which routes are canonical and
  which routes are temporary.

## Phase 1 Exit Tests

At the end of this phase, these scenarios should pass:

1. Kind schema activation registers `kind.fleetshift.v1.ClusterService`.
2. GCP HCP schema activation registers `gcphcp.fleetshift.v1.ClusterService`.
3. Both services expose collection `clusters` without gRPC, HTTP, or descriptor
   collisions.
4. `POST /apis/kind.fleetshift.io/v1/clusters?cluster_id=dev` creates a Kind
   managed resource.
5. `POST /apis/gcphcp.fleetshift.io/v1/clusters?cluster_id=dev` creates a GCP
   HCP managed resource.
6. `fleetctl resource types` lists both cluster services distinctly.
7. `fleetctl resource list clusters` fails with an ambiguity error when both
   services are active.
8. `fleetctl resource list clusters --service kind.fleetshift.v1.ClusterService`
   targets the Kind service.
