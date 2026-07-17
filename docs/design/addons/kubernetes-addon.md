# Kubernetes Addon

## What this doc covers

The built-in addon for direct Kubernetes cluster management:

- addon descriptor and capabilities (`DeliveryCapability`, `IndexCapability`)
- the Agent as the per-target unit of work
- deployment models: in-process and external agent
- the delivery pipeline: attested and passthrough modes, server-side apply, removal
- the indexing pipeline: LIST+WATCH informers, event batching, edge computation, inventory writes
- two-tier extraction: base tier for all resources, enriched tier with schema-driven fields and hooks
- resource filtering: allow/deny lists, default deny list, namespace filtering
- graph model: edges, recursive ownership, NodeStore

## When to read this

Read this when you are working on the kubernetes addon, extending its delivery or indexing behavior, adding indexed resource types, configuring resource filtering, or understanding how a Kubernetes cluster becomes a managed target.

## What is intentionally elsewhere

- Core delivery contract and target model: [../architecture/target_delivery_contract.md](../architecture/target_delivery_contract.md)
- Delivery authorization, attestation, and the trust model: [../authentication.md](../authentication.md)
- Fleet-wide indexing model and the inventory item shape: [../architecture/resource_indexing.md](../architecture/resource_indexing.md)
- Addon lifecycle and capability model: [../architecture/addon_integration.md](../architecture/addon_integration.md)
- Core vocabulary (fulfillment, strategies, delivery agents): [../architecture/core_model.md](../architecture/core_model.md)

## Related docs

- [../architecture.md](../architecture.md)
- [../architecture/addon_integration.md](../architecture/addon_integration.md)
- [../architecture/resource_indexing.md](../architecture/resource_indexing.md)

## Overview

The kubernetes addon is a delivery agent and indexer for Kubernetes clusters. It implements the delivery contract via server-side apply and the indexing contract via LIST+WATCH against the cluster's API server.

The core invariant is one Agent per target cluster. Each Agent holds shared Kubernetes clients and delegates to two independent delegates: a delivery delegate that applies and removes manifests, and an indexer delegate that watches resources and writes inventory. The two delegates operate independently.

The indexer watches all non-denied resources on the cluster by default, collecting base metadata and conditions for every resource. Resources with enriched schema entries get additional field extractions, computed properties via hooks, and edge computation. The indexer produces both inventory items and edges — relationships between items that enable topology queries.

The addon supports two deployment models: in-process within the platform server and as an external agent attached to a fleetlet. The Agent and its delegates are the reusable core across both models. What changes between models is the wiring — how the Agent is created, how delivery requests reach it, and how inventory writes reach the platform.

## Addon descriptor and capabilities

The addon declares two capabilities for the `kubernetes` target type: delivery and indexing.

```go
AddonDescriptor{
    ID:   "kubernetes",
    Name: "Kubernetes Agent",
    Capabilities: []Capability{
        DeliveryCapability{TargetType: "kubernetes"},
        IndexCapability{TargetType: "kubernetes"},
    },
}
```

`DeliveryCapability` declares that the addon provides a `DeliveryAgent` for Kubernetes targets — it can apply and remove manifests via server-side apply. `IndexCapability` declares that the addon provides an `IndexAgent` — it watches cluster resources and writes observed inventory to the platform. These are independent capabilities: an addon could declare one without the other. For example, an observation-only addon could declare only `IndexCapability`, while a simple apply-only addon could declare only `DeliveryCapability`.

At Connect time, the addon provides both a `DeliveryAgent` and an `IndexAgent` through `ConnectInput`. The platform validates each against the corresponding capability declaration. `DeliveryCapability` drives delivery agent registration in the delivery router. `IndexCapability` drives `IndexAgent` registration in the `AddonManager`, which dispatches `StartIndexing`/`StopIndexing` calls when targets become ready or are terminated.

In the current POC, the descriptor is compiled into the server binary. Enable and Connect happen at startup. In a future external agent model, the descriptor would be provided dynamically at addon registration time, and the Agent would connect through a fleetlet channel rather than being wired in-process.

The addon does not declare a managed resource capability. Kubernetes targets are registered through the platform's target management API, not through addon-owned managed resources.

## Agent

The Agent is the primary abstraction — the per-target unit that does all delivery and indexing work for a single Kubernetes cluster. It holds shared Kubernetes clients and two delegates:

- a **delivery delegate** for manifest apply and removal
- an **indexer delegate** for resource watching and inventory writes

When started, the Agent runs the indexer delegate continuously. Delivery operations are dispatched to the delivery delegate when delivery requests arrive. The Agent stops when its context is cancelled.

### Target properties

A kubernetes target provides its connection details through properties:

| Property | Required | Description |
| --- | --- | --- |
| `api_server` | yes | Kubernetes API server URL |
| `ca_cert` | no | PEM-encoded CA certificate for TLS verification |
| `service_account_token` | no | Direct bearer token for authentication |
| `service_account_token_ref` | no | Vault secret reference resolved at connect time |

One of `service_account_token` or `service_account_token_ref` must be set. Direct tokens take precedence. In the external agent model, the Agent may use in-cluster credentials instead, making these properties unnecessary.

## Deployment models

The Agent is the reusable core. How it gets created and wired depends on the deployment model.

### In-process

In the in-process model, the platform server hosts multiple Agents — one per registered kubernetes target. An AgentPool handles this:

- **Agent registry**: tracks running Agents by target ID
- **Delivery routing**: implements `DeliveryAgent` by dispatching to the correct Agent
- **Indexing lifecycle**: implements `IndexAgent` — `StartIndexing` creates an Agent when a target becomes ready, `StopIndexing` stops it when terminated
- **Client construction**: builds Kubernetes client configuration from target properties and vault-backed service account tokens

The AgentPool does not manage inventory cleanup directly. When a target is terminated, `StopIndexing` stops the Agent; the platform handles inventory cleanup (edges and items) in the orchestration's cleanup transaction or via the `AddonManager` on addon disable.

Agent creation is idempotent — concurrent `StartIndexing` calls for the same target do not create duplicates.

The AgentPool receives target lifecycle events through the `AddonManager`, which implements the orchestration's `TargetObserver` interface and dispatches to the AgentPool's `IndexAgent` methods based on `IndexCapability` target type matching. The AgentPool does not receive target events directly from the orchestration.

The AgentPool is in-process infrastructure, not part of the addon's core model. The key interfaces it passes to each Agent — `InventoryWriter` and `DeliveryReporter` — are the modularity boundaries that enable the external deployment model.

### External agent

In the external model, the Agent runs as its own process on or near the target cluster — one Agent per cluster. There is no AgentPool. The process creates a single Agent directly, using in-cluster credentials or configuration flags for the Kubernetes client, and wiring the delegates to fleetlet channel adapters.

The `InventoryWriter` and `DeliveryReporter` interfaces are the key modularity boundaries. In-process, these are backed by application-layer services (direct function calls). In the external model, they would be backed by fleetlet channel adapters implementing the same interfaces. The Agent and its delegates are identical in both models — only the interface implementations change.

| Concern | In-process | External agent |
| --- | --- | --- |
| Agent creation | AgentPool builds from target properties + vault | Process builds from in-cluster config |
| Delivery requests | Direct calls from delivery router via AgentPool | Fleetlet delivery channel |
| Inventory writes | Direct InventoryWriteService | Fleetlet index channel adapter |
| Delivery reporting | Direct DeliveryReportService | Fleetlet delivery channel adapter |
| Lifecycle | AddonManager dispatches `StartIndexing`/`StopIndexing` via `IndexAgent` | Process start/stop IS the lifecycle |

The actual fleetlet channel integration is not yet built, but no structural changes to the Agent or its delegates should be required.

## Delivery pipeline

The delivery delegate handles manifest apply and removal. It supports two delivery modes based on whether an attestation is present.

### Attested delivery

When an attestation is provided:

1. Verify the attestation against the target's trust bundle and the expected generation
2. On success: apply using the platform's service account
3. On failure: report `AuthFailed` to the delivery reporter

The trust bundle is a JSON array of trusted issuers stored as a target property. Each entry specifies an issuer URL, JWKS URI, audience, and key claim mappings. Verifiers are cached per target to avoid rebuilding on every delivery.

### Passthrough delivery

When no attestation is provided, the delivery falls through to passthrough mode. The caller must provide a bearer token. The delivery uses the caller's credentials directly, so the caller's own Kubernetes RBAC governs what gets applied. The platform's service account is never used.

### Server-side apply

Both modes use the same apply path. Manifests are applied sequentially via server-side apply with field manager `fleetshift`. Progress events are reported per manifest. A failure on any manifest stops the delivery. Kubernetes 401/403 errors map to `AuthFailed`; all other errors map to `Failed`.

### Removal

Removal follows the same attestation-or-passthrough flow as delivery, then deletes each resource. Resources that are already gone (404) are silently treated as success, making removal idempotent.

### Async dispatch

Validation and attestation verification happen synchronously. The actual apply or delete is dispatched asynchronously so that the operation completes even if the caller's context is cancelled.

## Resource filtering

Resource filtering controls which Kubernetes resources the indexer watches. Three layers participate: a default deny list, user-configured allow/deny lists, and namespace filtering.

### Default deny list

The following resources are excluded by default. They are either too high-volume, sensitive, or problematic for indexing:

| Resource | Group | Reason |
| --- | --- | --- |
| `events` | `""` | Transient status messages, thousands per minute on busy clusters |
| `events` | `events.k8s.io` | Newer Event API, same volume problem |
| `leases` | `coordination.k8s.io` | Node heartbeats, update every ~10 seconds per node |
| `endpoints` | `""` | Derived from Services, changes with every pod scale event |
| `endpointslices` | `discovery.k8s.io` | Same as endpoints, newer API |
| `componentstatuses` | `""` | Deprecated in modern Kubernetes |
| `oauthaccesstokens` | `oauth.openshift.io` | Sensitive + high volume |
| `oauthauthorizetokens` | `oauth.openshift.io` | Sensitive |
| `projects` | `project.openshift.io` | Same UID as namespaces, creates duplicate inventory |
| `packagemanifests` | `packages.operators.coreos.com` | OLM catalog metadata, read-only, very large |

Non-OpenShift clusters will not have the OpenShift GVRs — they are silently skipped during discovery.

Default deny entries can be overridden by user-configured allow lists.

### Allow/deny configuration

Allow/deny lists are configured at two levels:

1. **Addon-level**: default configuration applied to all targets
2. **Per-target override**: target properties can override addon defaults

Both use the same structure — lists of `{apiGroups, resources}` entries with wildcard support. `"*"` matches any API group or resource.

### Precedence model

The system operates in one of two modes depending on whether an allow list is configured:

**Watch-all mode** (default — no allow list configured):

1. In user deny? → DENY
2. In default deny? → DENY
3. Default → ALLOW (watch everything)

**Watch-selected mode** (allow list is configured):

1. In user deny? → DENY
2. In user allow? → ALLOW
3. Default → DENY (only explicitly allowed resources are watched)

In both modes, if a resource appears in both user allow and user deny, deny wins.

Watch-all is the default behavior — index everything the cluster supports minus noise. Watch-selected is for constrained environments where the agent should be limited to specific resource types. The mode is determined per-target from the effective configuration: if the merged addon-level + per-target config contains any allow entries, that target operates in watch-selected mode.

### Namespace filtering

Namespace filtering is applied at the informer level during LIST and WATCH. Resources in excluded namespaces are dropped before reaching the event channel.

**Namespace include/exclude** — glob patterns controlling which namespaces are watched. Include patterns whitelist matching namespaces. Exclude patterns remove matching namespaces from the included set. Glob patterns are evaluated directly on each event via `filepath.Match` — no caching or pre-resolution is needed because pattern matching is a pure string operation with negligible cost.

**Cluster-scoped resources** always pass the namespace filter — they don't have a namespace, so namespace patterns don't apply to them. Whether cluster-scoped resource types are watched is controlled entirely by the allow/deny lists at the GVR selection layer. To exclude cluster-scoped resources, deny them via the deny list or omit them from the allow list (in watch-selected mode).

## Graph model

The indexing pipeline produces inventory items and edges. Edges are relationships between inventory items that enable topology queries — "show me everything connected to this Deployment" or "which pods run on this node."

### Edge types

Core edge types shipped with the kubernetes addon:

| Edge Type | Source | Destination | Discovery |
| --- | --- | --- | --- |
| `ownedBy` | any resource | its controlling owner | controlling `ownerReference`, walked recursively up the controller chain |
| `runsOn` | Pod | Node | `spec.nodeName` |
| `attachedTo` | Pod | Secret, ConfigMap, PVC | scanning `spec.volumes`, `spec.containers[].env`, `spec.containers[].envFrom` |
| `attachedTo` | PVC | PV | `spec.volumeName` |
| `selects` | Service | Pod | label selector matching against known Pods |

### Recursive owner traversal

The `ownedBy` edge type follows the controller ownership chain, not just the direct owner. A Pod owned by a ReplicaSet owned by a Deployment produces two edges: Pod→ReplicaSet and ReplicaSet→Deployment. This enables queries like "everything owned by this Deployment" without the caller needing to know intermediate types. The traversal includes cycle detection to handle malformed owner references.

Only the controlling ownerReference (`controller: true`) is followed — non-controlling owners are ignored. This matches the Kubernetes lifecycle model where exactly one controller manages each resource's garbage collection and scaling. Non-controlling owners (e.g., sidecar injectors) would produce misleading topology edges.

Recursive owner traversal is computed automatically for all resources via the controlling `ownerReference` — no schema entry or hook is needed.

### Edge type extensibility

Edge types are strings, not a closed enum. Future addons or schema extensions can register custom edge types (e.g., a GCP addon could add `hostedIn`) without modifying the core model.

### NodeStore

A dual-indexed view of current inventory items, built at edge computation time:

- `ByUID`: `map[string]InventoryItem` — O(1) lookup by UID
- `ByKindNamespaceName`: `map[string]map[string]map[string]InventoryItem` — O(1) lookup by Kind→Namespace→Name

Edge computation closures receive the NodeStore so they can efficiently find related resources. For example, the `selects` edge builder for a Service looks up Pods by kind+namespace, then matches label selectors. Without the ByKindNamespaceName index, this would require scanning all items.

### Edge computation timing

Edges are computed lazily, at flush time in the Writer, not on every event or on resync. When a resource arrives (via event or resync), its edge computation closure is stored alongside it. At flush, the Writer builds the NodeStore from all known resources across all GVRs, runs all closures, and diffs edges against the previous set to produce add/delete edge deltas. This matches search-collector's pattern — edges are recomputed from scratch each cycle because any resource change can affect edges involving other resources.

The flush path is the single owner of edge computation because it has a complete view of all resources. Cross-GVR edges — Pod→Node (`runsOn`), Service→Pod (`selects`), PVC→PV (`attachedTo`) — require the NodeStore to contain resources from multiple GVRs simultaneously. The resync path operates on a single GVR at a time and cannot guarantee that dependent GVRs have been processed yet, so it delegates edge management to flush.

### Edge persistence

Edges are written through the `InventoryWriter` interface alongside inventory items, but persisted in a separate table rather than embedded on items. This separation reflects that edges describe relationships between items — embedding them on one side creates update anomalies (deleting item B requires updating item A's edges even though A itself is unchanged) and makes incoming-edge queries expensive (scanning all items to find edges pointing to a given destination).

The edge table is keyed by target ID, source UID, destination UID, and edge type. A source can have multiple edge types to the same destination. A secondary index on destination UID enables efficient incoming-edge queries such as "which pods run on this node."

`ApplyDelta` includes edge add and edge delete parameters alongside item upserts and deletes, all within a single transaction. `Resync` atomically replaces items only — it does not touch edges. Edge management is handled exclusively by the `ApplyDelta` path.

Edge persistence is handled by a dedicated `EdgeRepository`, separate from the `InventoryRepository` that handles items. Both repositories are accessed through the same transaction, ensuring atomicity. The `EdgeRepository` supports creating or updating edges in batch, deleting specific edges, deleting edges by source UIDs (for resync scoping), and deleting all edges for a target (for termination cleanup).

Inventory and edge cleanup on target termination is the platform's responsibility, not the addon's. The orchestration's `CleanupDeliveryData` deletes edges and items atomically in the same transaction that removes the target record. On addon disable, the `AddonManager` uses the `InventoryCleanup` interface to delete edges and items per target. The addon's `StopIndexing` only stops the Agent — it does not touch inventory data.

## Indexing pipeline

The indexing pipeline watches Kubernetes resources on the target cluster, extracts inventory items in two tiers, computes edges between items, and writes the results to the platform. It is a multi-stage system: CRD watching triggers informer reconciliation, informers produce events, a writer batches events and computes edges, and the inventory writer persists items and edges.

```text
CRDWatcher
  |
  +-- triggers reconciliation on CRD add/update/delete
  |
InformerManager
  |
  +-- discovers supported GVRs via API server
  +-- applies resource filtering (default deny + user allow/deny)
  +-- applies namespace filtering (include/exclude patterns)
  |
  +-- GenericInformer (pods)  --+
  +-- GenericInformer (nodes) --+--> eventCh / resyncCh --> Writer --> InventoryWriter
  +-- GenericInformer (...)   --+
                                         |
                                    edge computation
                                    (at flush time)
```

### CRD watching

The InformerManager watches the `CustomResourceDefinition` resource itself. When a CRD is created, updated, or deleted, the manager triggers a reconciliation cycle: re-discovers supported GVRs, applies filtering, and starts or stops informers to match the effective set. Reconciliation is throttled — a minimum delay between cycles prevents API server spam when multiple CRDs change rapidly (e.g., during an operator install).

### Informer manager

The InformerManager reconciles running informers against the effective set of resources to watch:

1. Discover all WATCH-capable resources via `ServerPreferredResources()`
2. Apply resource filtering (default deny list + user allow/deny config)
3. Apply namespace filtering if configured
4. Diff against running informers — stop removed, start new
5. Serialized startup with initialization timeout to avoid memory spikes

CRD-triggered reconciliation re-runs this from step 1. The desired GVR set is determined by discovery and filtering, not by the schema. The schema only controls enrichment at extraction time.

### GenericInformer

Each `GenericInformer` implements LIST+WATCH for a single GVR with minimal memory overhead. It tracks only UID-to-resourceVersion mappings, not full objects.

**LIST phase**: paginated LIST, sending an `Add` event for each resource. Resources in excluded namespaces are dropped before being sent to the event channel. Stale resources (UIDs in the previous index but absent from the LIST) produce `Delete` events. After the LIST completes, a `ResyncEvent` with the full resource set is sent, and the list's resourceVersion is saved for watch continuity.

**WATCH phase**: WATCH from the saved list resourceVersion. `Add`, `Update`, and `Delete` events are dispatched from the watch stream, with namespace filtering applied before dispatch. On watch error or channel close, the informer returns to the LIST phase.

**Shutdown**: when the context is cancelled, the informer sends `Delete` events for all tracked resources. This cleans up the Writer's internal state (pending maps, sent versions).

**Retry**: on LIST or WATCH errors, the informer retries with exponential backoff. Retries reset on a successful LIST+WATCH cycle.

### Writer

The Writer batches informer events, performs two-tier extraction, computes edges, and flushes results to the `InventoryWriter` at a configurable interval.

**Event handling**:

- `Add` / `Update`: buffer the resource in pending upserts keyed by UID, store edge computation closure
- `Delete`: add the UID to pending deletes and remove any pending upsert for that UID
- Late-delete protection: if a UID was deleted in the current batch, subsequent `Add`/`Update` events for that UID are dropped

**Flush** (on batch timer):

1. For each pending upsert, skip if the resourceVersion is unchanged since the last flush (deduplication)
2. Run two-tier extraction: base extraction for all resources, enriched extraction for resources with a schema entry
3. Build a NodeStore from current inventory state
4. Run edge computation: recursive owner traversal for all resources, type-specific edge closures for enriched resources
5. Diff edges against the previous edge set to produce add/delete edge deltas
6. Call `InventoryWriter.ApplyDelta` with item upserts, deleted IDs, edge adds, and edge deletes

**Resync** (on `ResyncEvent`): extract all resources for the GVR, merge into the Writer's internal state, purge any tracked nodes for that GVR absent from the resync set, and atomically replace all items for the target+type via `InventoryWriter.Resync`. Retries with exponential backoff on failure. Resync is items-only — edges are left untouched and recomputed on the next flush cycle. Purging stale nodes ensures edge computation does not reference items removed by the resync, even if the corresponding Delete events have not yet been consumed from the event channel.

**Error recovery**: on write failure, the Writer retries with exponential backoff. If all retries are exhausted, the failed batch remains pending and is retried on the next batch tick. Internal deduplication and edge-diff state is only advanced after a successful write, so failed items and edges are never silently lost. Memory during extended outages is bounded by unique UIDs on the cluster.

**Heartbeat**: if no changes occur within a fixed interval (60 seconds), the Writer sends an empty heartbeat to signal liveness. In-process, the receiver can use this to distinguish "no changes" from "agent is dead." In the external model, it keeps the fleetlet channel alive.

All event processing is serialized on a single goroutine for ordering safety. Late-delete protection and resourceVersion deduplication ensure correct behavior under interleaved events.

### InventoryWriter

The `InventoryWriter` interface models the addon-to-platform direction of the indexing protocol:

- **ApplyDelta**: upserts and deletes items, adds and deletes edges, in a single transaction. This is the incremental update path and the sole path for edge persistence.
- **Resync**: atomically replaces all items for a target+type. Does not touch edges — edge computation requires a cross-GVR view that the single-GVR resync path cannot provide. Edges are managed exclusively by the flush/`ApplyDelta` path.

In-process, the `InventoryWriteService` wraps each operation in a store transaction. In the external model, a fleetlet channel adapter would implement the same interface.

## Two-tier extraction

The indexer extracts inventory items in two tiers. The base tier runs for every watched resource. The enriched tier runs only for resources with a schema entry.

### Base tier

Every watched resource gets the following fields extracted, with no schema entry required:

| Field | Source |
| --- | --- |
| `name` | `metadata.name` |
| `namespace` | `metadata.namespace` (empty for cluster-scoped) |
| `uid` | `metadata.uid` |
| `creationTimestamp` | `metadata.creationTimestamp` |
| `deletionTimestamp` | `metadata.deletionTimestamp` (present only during graceful deletion) |
| `labels` | `metadata.labels` |
| `controllingOwnerUID` | controlling `metadata.ownerReferences` entry (used for recursive edge building) |
| `generation` | `metadata.generation` |
| GVR + Kind | From the discovery context (group, version, resource, kind) |
| `status.conditions` | `status.conditions` (tolerant of missing — no-op if absent) |

No annotations are collected in the base tier.

### Enriched tier

Resources with a `SchemaEntry` get additional extraction:

- **JSONPath field extractions**: named fields with JSONPath expressions and data type coercion (unchanged from current behavior)
- **ComputeExtra hook**: optional function for computed properties that JSONPath cannot express
- **BuildEdges hook**: optional function for type-specific edge building (core `ownedBy` edges are automatic)
- **Annotation extraction**: opt-in via `ExtractAnnotations` flag, with a configurable size cap per value (default 64 chars). `kubectl.kubernetes.io/last-applied-configuration` is always stripped.

## Schema and field extraction

### Schema model

The indexing schema defines how to enrich extraction beyond the base tier. It does not control which resources are watched — that is determined by resource discovery and filtering.

- **SchemaEntry**: a single resource type, identified by GVR and Kind. Contains field extractions, condition and annotation extraction flags, and optional hooks for computed properties and edge building.
- **FieldExtraction**: a named field with a JSONPath expression and a data type for coercion.
- **IndexSchema**: the complete set of entries, keyed by GVR. Looked up at extraction time — if an entry exists for a resource's GVR, enriched extraction runs.

### Data types

| Type | Coercion |
| --- | --- |
| `string` | Default. Value returned as-is. |
| `number` | Coerced to float64. Accepts int, int64, float64, and string. |
| `bytes` | Parsed as a Kubernetes quantity (e.g. `1Gi`) and converted to bytes as float64. |
| `slice` | All JSONPath results collected into a flat list. |
| `mapString` | Map values coerced to strings via `fmt.Sprint`. |

### Extraction

Two-tier extraction converts an unstructured Kubernetes resource into a domain `InventoryItem`. Base and enriched steps are interleaved in a single pass — enriched steps are no-ops when no schema entry exists:

1. Build the inventory type from `apiVersion` and `kind` (e.g. `apps/v1/Deployment`, `v1/Pod`)
2. Extract `status.conditions` if present
3. *(enriched)* Evaluate JSONPath expressions for each schema field, coercing by data type
4. Copy curated metadata: namespace, controlling ownerReference UID, generation, deletionTimestamp
5. *(enriched)* Extract annotations if `ExtractAnnotations` is true, with size cap and noise stripping
6. *(enriched)* Run `ComputeExtra` hook if present (e.g., pod status computation, container lists)
7. Build the item with ID `targetID/UID`, name, labels, the computed inventory type, and the extracted fields
8. *(enriched)* Store `BuildEdges` closure for edge computation at flush time

### Default schema

The default schema provides enriched extraction for core Kubernetes resource types: workloads (Deployments, StatefulSets, DaemonSets, ReplicaSets, Jobs, CronJobs), pods, services, nodes, namespaces, storage (PVCs, PVs), and configuration (ConfigMaps, Secrets). For most types, key status fields and replica counts are extracted. Nodes and pods include `ComputeExtra` hooks for computed properties (node addresses, pod status). Pods, services, and PVCs include `BuildEdges` hooks for type-specific edges (`runsOn`, `attachedTo`, `selects`). ConfigMaps and Secrets are indexed for identity and labels only — no enriched fields are extracted.

All other resources discovered on the cluster receive base extraction only — metadata, GVR, labels, conditions — without any schema entry.

## Open questions & todo

### Drift detection

The indexing pipeline observes cluster state but does not compare it against delivery intent. Drift detection — identifying resources that have diverged from their delivered manifests — is not yet implemented. See [../architecture/target_delivery_contract.md](../architecture/target_delivery_contract.md) for the design-level discussion.

### Runtime schema extensibility

The two-tier model and allow/deny configuration address the core extensibility gap for controlling what is watched. However, enriched extraction rules (JSONPath fields, hooks) are compiled into the binary. Whether enriched schema entries should be configurable at runtime — via target properties, a configuration API, or a custom resource — remains open. Allow/deny lists and namespace filters are runtime-configurable; enriched extraction rules are not.

### Edge querying

Edges are persisted in a dedicated table alongside inventory items, but the platform's search API does not yet expose edge-aware queries. How edges are queried — traversal API, filter joins, or topology endpoints — is platform-level design work outside this addon doc. The persistence model (separate table with destination index) is designed to support both outgoing and incoming edge lookups efficiently.

### Resync write amplification on watch reconnection

The informer sends a full Resync after every LIST — both on initial startup and on every watch reconnection. Watch reconnections are routine (API server timeouts, network blips, 410 Gone errors). Each Resync replaces all items for the affected GVR, even when nothing on the cluster has changed. Edges are not affected — the resync passes nil edges, leaving edge management to the flush path. The informer also sends individual Add and Delete events from the same LIST, which would produce a correct ApplyDelta diff on their own — the Resync is a redundant safety net for items.

For a single target with SQLite the item-only Resync is negligible. At scale with Postgres and frequent watch reconnections across many targets, the write amplification could matter. The mitigation is for the Writer to distinguish first-sync Resyncs (which must hit the database) from watch-reconnection Resyncs (which can be skipped, letting the individual events flow through ApplyDelta as normal diffs). The Resync path would only hit the database on initial startup and after repeated ApplyDelta failures.

### Informer startup serialization at scale

The current serialized startup (one informer at a time, with initialization timeout) works for a bounded set of GVRs. With watch-all mode on a large cluster (200-400+ GVRs), serialized startup could take significant time. A bounded-parallelism approach (e.g., start N informers concurrently) may be needed, balanced against memory spike risk during initial LIST phases.

### Resync fallback as defense-in-depth

The Writer's error recovery relies on retaining failed batches for retry on the next tick. This handles all known failure modes — transient DB errors, extended outages, and partial state divergence. A full resync fallback (triggering a re-LIST of all GVRs after N consecutive failures) could be layered on as defense-in-depth against unknown state corruption or bugs in the retry logic. This is not currently needed — the retry mechanism is correct by construction and self-healing — but could be added if operational experience reveals failure modes that per-batch retry does not cover. Such a fallback would require a back-channel from the Writer to the InformerManager, inverting the current one-directional dependency.

### Observation and condition history

The implementation stores only the latest observation and current conditions per inventory item — a single `observed` JSON blob, a single `conditions` array, and a single `observed_at` timestamp. There are no history tables or condition transition event tracking. Adding historical observations (e.g., a time-series of observed state) or condition transition logs (e.g., "Available went from True to False at T") would require new persistence tables and a retention policy. The current single-snapshot model is sufficient for point-in-time inventory queries but cannot answer "when did this resource last change state" or "how long has this condition been degraded."

### Static metadata in the observed JSON

The `observed` blob carries both volatile runtime state (replica counts, pod phase) and static identity-like metadata (`namespace`, `controllingOwnerUID`) that never changes after resource creation. These static fields don't fit cleanly into any inventory bucket — `namespace` is closer to identity (like `name`, which has a dedicated column), and `controllingOwnerUID` is closer to a relationship (already captured as `ownedBy` edges) — but adding dedicated columns would embed Kubernetes-specific concepts into the platform's addon-agnostic model.

Storing them in `observed` is correct by elimination (`properties` is for addon-produced delivery outputs like `api_url`, not observed metadata), but has a query performance tradeoff: namespace filtering requires JSON extraction rather than an indexed column lookup. Postgres can add a functional index on `observed->>'namespace'`; SQLite cannot. If the search API requires high-performance namespace filtering at fleet scale, the platform may need a generic indexed-dimensions mechanism available to any addon that needs to promote specific observed fields for efficient querying.

### Index resumption after reconnection

All indexing state — `listRV` (the Kubernetes resourceVersion for watch continuity), `sentVersions` (resourceVersion deduplication), `previousEdges` (edge diff baseline), and `currentNodes` (NodeStore source) — is held in memory and discarded when `StopIndexing` cancels the Agent. When `StartIndexing` runs again for the same target, a fresh Agent performs a full LIST of every watched GVR, triggers a ResyncEvent per GVR (atomically replacing all items in the database), and rebuilds all edge state from scratch. The existing inventory data survives in the database, but the synchronization checkpoint is lost.

If the platform persisted a resumption checkpoint — the last successfully processed resourceVersion per GVR per target — a reconnecting Agent could skip the full LIST and resume WATCH from where it left off. This would eliminate the resync write amplification on routine reconnections (API server restarts, network blips, addon upgrades) and reduce the time-to-incremental for large clusters. The checkpoint would need to be written atomically alongside each successful `ApplyDelta` or `Resync`, and the platform would pass it to the addon at `StartIndexing` time — likely as part of `TargetInfo` or a dedicated resumption token.

### Addon-to-addon communication for schema and edge extensibility

Each addon currently operates in complete isolation. The `IndexSchema` is hardcoded at compile time via `DefaultKubernetesSchema()` and injected directly into `IndexConfig` during `StartIndexing`. There is no mechanism for one addon to influence another addon's indexing behavior — no cross-addon messaging, no shared schema registry, and no hook points in the addon lifecycle for inter-addon coordination.

A concrete use case: a VM management addon knows that certain Kubernetes resources (e.g., VirtualMachine CRDs, specific fields on Pods) are relevant to its domain but is not itself an indexing addon. It needs to tell the kubernetes indexing addon "watch these additional resource types and extract these fields." This requires either a schema contribution API (addons register supplementary `SchemaEntry` sets that the indexing addon merges into its effective schema) or a platform-level schema registry that aggregates entries from multiple addons and feeds the merged result to whichever addon handles indexing for that target type.

### Configurable edge and relationship discovery

The current edge types (`ownedBy`, `runsOn`, `attachedTo`, `selects`) and their discovery logic are compiled into the binary as `BuildEdges` closures on `SchemaEntry`. Adding a new relationship type — for example, a `managedBy` edge from a Kubernetes resource to a VM, or a `dependsOn` edge between services discovered by a service mesh addon — requires code changes to `edge.go` and `schema_default.go`.

This is closely related to addon-to-addon communication: if an external addon can contribute schema entries, it could also contribute `BuildEdges` closures that define new edge types and their discovery logic. A lighter-weight alternative is a declarative edge configuration — edge definitions expressed as data (source kind, destination kind, edge type, and a field path or label selector for discovery) rather than compiled closures. Declarative edges could cover common patterns (field-based reference lookups, label selector matching) without requiring Go code, while the closure-based `BuildEdges` mechanism would remain available for complex edge logic that declarative rules cannot express.
