# Addon Integration

## What this doc covers

The platform's primary extension surface:

- addon lifecycle: defined, enabled, connected
- the capability model and schema reconciliation
- why addons are separate trust and execution boundaries
- how addons extend manifest, placement, and rollout behavior
- strategy transport options
- `InvalidateManifests`
- the `ManifestGenerator` contract
- managed resources as a structural bridge
- UI and API extensibility — plugin model and dynamic gRPC/HTTP

## When to read this

Read this when you are designing or implementing an addon, extending the strategy model, or reasoning about how addon-owned APIs and UIs plug into the platform.

## What is intentionally elsewhere

- Core strategy vocabulary and target model: [core_model.md](core_model.md)
- Orchestration semantics: [orchestration.md](orchestration.md)
- Fleetlet channels and request routing: [fleetlet_and_transport.md](fleetlet_and_transport.md)
- Full managed-resource design: [../managed_resources.md](../managed_resources.md)
- Full authentication and signing model: [../authentication.md](../authentication.md)
- Two-layer API model, resource identity, and extension API packages: [resource_identity_and_api.md](resource_identity_and_api.md)

## Related docs

- [../architecture.md](../architecture.md)
- [tenancy_and_permissions.md](tenancy_and_permissions.md)
- [../addon-ui-architecture.md](../addon-ui-architecture.md) — addon bundle model, OCI artifact distribution, shell integration, and UI plugin capability

## Addons as trust boundaries

Addons are separate processes not just for modularity but for trust. Targets may trust addon-produced manifests or placement decisions in ways they do not trust the platform itself. This is one reason addon logic is out-of-process rather than an in-process kernel plugin.

In practice, that means:

- addons can sign outputs with their own identities
- delivery agents can verify addon-produced material independently
- the platform remains an orchestrator and courier rather than the only trusted authority

The full signing model lives in [../authentication.md](../authentication.md).

## Addon lifecycle

An addon is a single unit — not "a backend addon" or "a frontend addon." Each addon declares one or more capabilities of different types. The lifecycle phases do different things depending on what capabilities are present. An addon can be purely backend (managed resources, delivery agents), purely frontend (UI plugin), or a combination.

Addons follow a three-phase lifecycle managed by the `AddonManager`:

1. **Defined** — the addon descriptor has been loaded into the catalog, but no authorization or trust configuration exists yet.
2. **Enabled** — an admin has authorized the addon and configured its trust policy. Capability expectations are recorded. For capabilities that don't require runtime workload interaction (e.g. UI plugins), Enable is sufficient — the capability is active. For capabilities that require workload-provided assets (schemas, delivery agents), the capability is recorded but not yet activated.
3. **Connected** — a workload has connected and provided runtime assets for capabilities that need them. Managed resource schemas are compiled, delivery agents are registered, targets are seeded. This phase is only relevant for capabilities that require workload interaction; a frontend-only addon never transitions to Connected.

The transitions are:

- **Enable** (Defined → Enabled): records the addon's declared capabilities. This is the authorization gate — the admin decides which addons can participate. For UI capabilities, the plugin metadata (manifest URL, asset base URL) is available immediately from the descriptor. For backend capabilities, the runtime assets come later at Connect.
- **Connect** (Enabled → Connected): the addon workload provides its runtime assets — delivery agents, managed resource schemas (inline proto sources), and target definitions. The platform compiles schemas, registers dynamic gRPC/HTTP services, wires delivery agents into the router, and seeds targets. Connect is idempotent on reconnection: schemas that haven't changed are left in place (content-hashed by the activator), stale schemas are deactivated, and new schemas are compiled and registered. Connect is irrelevant for addons that only declare capabilities without runtime workload assets (e.g. UI-only addons).
- **Disconnect** (Connected → Enabled): the delivery agent is deregistered, but the API surface (gRPC/HTTP for managed resources) stays live so consumers can still CRUD resources. This reflects the design intent that addon unavailability should not prevent users from submitting or reading managed resources — delivery is paused, not the API.
- **Disable** (any → Defined): full teardown. Schema activations are torn down, delivery agents removed, managed resource type definitions deleted, UI plugin entries removed. The addon returns to catalog-only state.

### Capability model

Each addon declares capabilities in its descriptor. The set of capabilities determines which lifecycle phases are meaningful and what each phase activates:

- **`ManagedResourceCapability`**: declares that the addon will provide a managed resource type (e.g. `clusters`). The full schema — inline proto files, spec message name, singular/plural, proto package, and service name — comes from the workload at connect time via `ManagedResourceSchema`. Activation implies dual registration: both the extension resource type (in the addon's own package) and the corresponding platform resource type (for canonical identity). The platform validates at connect time that every schema matches a declared capability. Requires Connect.
- **`DeliveryCapability`**: declares that the addon provides a `DeliveryAgent` for a target type (e.g. `kind`, `ocp`, `kubernetes`). The concrete agent is provided at connect time and registered in the delivery router. Requires Connect.
- **UI plugin capability** (not yet implemented): declares that the addon ships a UI plugin. The manifest URL and asset base URL are provided in the descriptor. Active at Enable — no Connect needed. See [addon-ui-architecture.md](../addon-ui-architecture.md) for the distribution model.

An addon can declare multiple capabilities of different types. For example, a cluster management addon might declare a `ManagedResourceCapability` for `clusters`, a `DeliveryCapability` for its provisioning target type, and a UI plugin capability for its management dashboard — all in a single addon descriptor.

### Schema reconciliation on reconnection

When an addon reconnects (after a disconnect), `Connect` receives the addon's current truth — the full set of schemas and agents it now provides. The manager reconciles against the previous state:

1. Schemas that were active but are absent from the new input are deactivated (stale removal). This deactivates both the extension service and the corresponding platform resource type service, should that type no longer be referenced by any addon. (TODO: possibly revisit this last note)
2. Every schema in the new input is passed to the `SchemaActivator`. The activator uses content hashing (SHA-256 over proto files, spec message, singular, plural, proto package, and service name) to determine whether the schema changed. Unchanged schemas are left in place (no recompilation). Changed schemas are atomically replaced — both the extension service and platform service gRPC and HTTP mux entries are swapped without a deregister/register gap.

The connect-time schema specifies the target package (`ProtoPackage`) and service name (`ServiceName`), which the activator uses for extension service registration. The platform resource type service is derived automatically from the resource type registration.

This design pushes content-change detection into the transport layer (`DynamicSchemaActivator`), keeping the application layer (`AddonManager`) free of proto or hash concerns.

### In-process POC model

In the current proof-of-concept, addon descriptors and schemas are compiled into the server binary (e.g. `kindaddon.Descriptor()`, `kindaddon.Schema()` with `go:embed` for the proto source). Enable and Connect happen at startup. In a production deployment, addons would register dynamically via API and provide schemas over their connect channel.

## Addon surface

All three strategy axes are addon-extensible through the same integration model.

### Manifest strategies

For manifest generation, an addon can:

1. register a capability with a `ManifestGenerator`
2. signal manifest recomputation through `InvalidateManifests`
3. respond to `Generate` requests from the platform

### Placement strategies

For placement, an addon implements `Resolve(pool) -> targets`. A scored placement addon can maintain its own data sources and signal the platform when changing scores should cause re-resolution.

### Rollout strategies

For rollout, an addon implements `Plan(delta) -> steps`. Steps can include tasks, and the platform calls back to the addon when those tasks need evaluation.

The platform's executor stays agnostic to how the strategy was produced. Addons own the strategy logic; the platform owns the common execution shell.

## Where strategy implementations run

Strategy implementations run wherever the addon runs. The primary production model is a fleetlet channel:

- the addon connects to its local fleetlet
- the fleetlet connects to the platform
- the platform sends requests over the registered channel

This keeps addon execution location independent from platform hosting choices.

## Transport options

Three transport shapes exist for reaching addon strategy implementations:

1. **In-process**: the platform receives a Go strategy implementation directly during registration
2. **Fleetlet channel**: the platform talks to the addon through a fleetlet channel
3. **Direct HTTP**: the addon exposes a callback URL

The fleetlet-channel model is the primary production path because it preserves the same zero-coupling story as the rest of the architecture.

## `InvalidateManifests`

`InvalidateManifests` is the direct signal that an addon's internal state changed and its manifests should be regenerated for affected fulfillments.

The platform responds by advancing the manifest strategy version on each affected fulfillment (bumping its generation), which triggers reconciliation:

1. re-running the addon's `ManifestGenerator`
2. diffing new output against previously stored output
3. delivering only where manifests changed

This replaces more indirect signaling patterns such as updating an intermediate CR only to trigger another controller to re-render.

## `ManifestGenerator` contract

Addon implementers should treat `ManifestGenerator` with at-least-once semantics in mind.

Requirements:

- **Idempotent side effects.** Repeated calls must not leak or duplicate external resources.
- **Replay safety.** Durable execution may re-run a step after a crash or retry.
- **Dry-run safety.** `Generate` may be invoked speculatively without resulting in delivery.
- **`OnRemoved` idempotency.** Removal callbacks must tolerate retries and duplicates.

The platform uses the latest successful output. If addon state changed between calls, different outputs are legitimate. What must stay safe is the side-effect behavior around those calls.

## Managed resources as a structural bridge

Managed resources are addon-driven, consumer-facing resource types such as clusters, VMs, or ArgoCD instances.

The important architectural relationship here is structural:

- the user submits a managed-resource spec
- the platform stores that signed spec
- the platform derives a fulfillment that targets the addon itself
- the addon fulfills that resource through the normal orchestration pipeline

Managed resources, like deployments, are user-facing concepts that own a fulfillment. The fulfillment is the kernel primitive that drives orchestration. This gives managed resources provenance continuity and reuse of the same delivery shell without hard-coding a different control path in the platform.

The full managed-resource API shape, lifecycle, and open questions live in [../managed_resources.md](../managed_resources.md).

## UI and API extensibility

The platform's API model is polymorphic enough for CLIs and APIs, but the UI cannot stay generic forever. Addons therefore own much of the domain-specific UI and API surface.

### Three extension points

**Manifest strategy UI** answers "what are you deploying?" The platform can offer built-in editors for simple cases, while addon strategies can provide richer configuration or topology UIs.

**Target type UI** answers "what does this target look like?" Kubernetes targets, platform targets, and addon-defined targets each need different views.

**Platform shell** remains platform-owned. Navigation, authentication, workspace scoping, rollout status, and target inventory stay in the common shell rather than in plugins.

### Plugin model

The UI plugin model follows the Scalprum dynamic-plugin pattern: the platform provides the shell and plugin registry, addons provide domain-specific content via federated modules. Addon UI components are packaged as OCI artifacts and served from an external asset host — the platform stores metadata and URLs, not JS bytes. The full design is in [addon-ui-architecture.md](../addon-ui-architecture.md).

### API extensibility — dynamic gRPC and HTTP

Addons extend the platform's gRPC and HTTP API surface at runtime. Each managed resource schema activation performs **dual registration** in the two-layer API model (see [resource_identity_and_api.md](resource_identity_and_api.md)):

1. **Extension service**: registered under the addon's own proto package (e.g. `kind.fleetshift.v1.ClusterService`), providing the full typed API with addon-defined spec, observation schema, and extension-specific fields. HTTP routes are at `/apis/{service_name}/{version}/{resource_path}`, where `resource_path` uses the shared collection identifier for that identity domain (for example `clusters/{id}`).
2. **Platform service**: registered under the platform package for the corresponding platform resource type, providing the generic canonical identity surface (labels, effective_labels, conditions, representations, aliases). HTTP routes are at `/apis/fleetshift.io/{version}/{resource_path}` for the same collection identifier.

This means a single `ManagedResourceCapability` results in two dynamic gRPC services and two HTTP path prefixes. The extension service is the primary API for consumers; the platform service provides identity, aggregation, and cross-extension correlation.

The key components:

These components are split across three packages under `internal/transport/`:

- **`dynamicapi`** (shared leaf): `DynamicServiceMux`, `DynamicHTTPMux`, `DynamicFileRegistry`, the proto compiler, composite reflection, and shared helpers (field builders, timestamp marshaling, HTTP utilities). This package has no knowledge of specific resource types.
- **`managedresource`** (extension + activator): service builder and gRPC/HTTP handlers for addon-defined extension APIs, plus the `DynamicSchemaActivator` that orchestrates schema compilation and registration.
- **`platformresource`** (platform): service builder and gRPC/HTTP handlers for platform-canonical resource APIs (`fleetshift.v1.Platform{Singular}Service`).

`dynamicapi` is a pure leaf — both `managedresource` and `platformresource` import it, and `managedresource` imports `platformresource` (the activator registers platform services as a side-effect of extension activation). There are no cycles.

Key runtime components:

- **`DynamicServiceMux`** (`dynamicapi`): wired as the gRPC server's `UnknownServiceHandler`. Requests to services that were not registered at server creation time are routed here. Services can be added, replaced (atomically), or removed at any time. Composite reflection merges dynamic services with statically registered ones so they are discoverable via `grpcurl` and similar tools.
- **`DynamicHTTPMux`** (`dynamicapi`): wraps an `http.ServeMux` with handler indirection. A stable dispatcher function is registered once per URL prefix; the actual handler is stored in an internal map and swapped atomically on replacement. This avoids Go 1.22's panic on duplicate `ServeMux` pattern registration and provides zero-downtime replacement. HTTP path routing uses the `/apis/{service_name}/{version}/` prefix to differentiate between extension services sharing resource type names.
- **`DynamicSchemaActivator`** (`managedresource`): the `SchemaActivator` implementation in the transport layer. It compiles inline proto, builds the service, and manages registration in both muxes. Content hashing (SHA-256) ensures that unchanged schemas skip recompilation and that changed schemas are atomically replaced rather than deregistered-then-registered. The activator uses the addon-provided `ProtoPackage` and `ServiceName` rather than hardcoding `fleetshift.v1`.

Proto schemas are transmitted as inline source content at addon connect time (see [addon lifecycle](#addon-lifecycle)). The connect-time schema now specifies the target package (`ProtoPackage`) and service name (`ServiceName`) for the extension service registration. The platform's compiler combines inline sources with a built-in resolver for well-known imports (`google/protobuf/*`, `buf/validate/*`), so addon-defined specs can use `protovalidate` annotations that the platform enforces at the API boundary.
