# Kubernetes Addon

## What this doc covers

The built-in addon for direct Kubernetes cluster management:

- addon descriptor and capabilities (delivery and inventory reporting)
- delivery and per-target indexing as separate units of work
- deployment models: in-process and external agent
- the delivery pipeline: attested and passthrough modes, server-side apply, removal
- the indexing pipeline: LIST+WATCH informers, event batching, edge computation, inventory reports
- two-tier extraction: base tier for all resources, enriched tier with schema-driven fields and hooks
- resource filtering: allow/deny lists, default deny list, namespace filtering
- graph model: edges, recursive ownership, NodeStore (computed in memory; persistence disabled)

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

The kubernetes addon is a delivery agent and indexer for Kubernetes clusters. It implements the delivery contract via server-side apply and the indexing contract via LIST+WATCH against the cluster's API server, reporting observed objects into the platform inventory model under one generic extension resource type: `kubernetes.fleetshift.io/Object`.

Delivery and indexing are independent. A stateless delivery agent handles apply/remove. Per-target indexing runs as a long-lived indexer hosted by an in-process indexing runtime. The two do not share a long-lived per-target "Agent" process: delivery is request-scoped, indexing is long-lived per target when started.

The indexer watches all non-denied resources on the cluster by default, collecting base metadata and conditions for every resource. Resources with enriched schema entries get additional field extractions, computed properties via hooks, and edge computation. Topology edges (`ownedBy`, `runsOn`, `attachedTo`, `selects`) remain part of the indexing core, but the in-process path disables edge persistence — edges are not written to the platform in the current integration.

The addon supports two deployment models: in-process within the platform server and as an external agent. The reusable indexing core (discovery, filtering, informers, extraction, writer, inventory reporting) is shared across both. What changes between models is the wiring — who starts indexers, how credentials are supplied, and how inventory reports reach the platform.

## Addon descriptor and capabilities

The addon declares two capabilities: delivery for the `kubernetes` target type, and inventory reporting for the generic Kubernetes object resource type.

```go
AddonDescriptor{
    ID:   "kubernetes.fleetshift.io",
    Name: "Kubernetes Agent",
    Capabilities: []Capability{
        DeliveryCapability{TargetType: "kubernetes"},
        InventoryResourceCapability{ResourceType: "kubernetes.fleetshift.io/Object"},
    },
}
```

Delivery capability means the addon can apply and remove manifests via server-side apply. Inventory resource capability means the addon is authorized to report inventory for `kubernetes.fleetshift.io/Object`. These are independent: an addon could declare one without the other.

At Connect time, the addon provides its delivery agent and the inventory-only schema for `Object`. The platform validates delivery and registers the schema for reporting authorization. Indexing runtime lifecycle is **not** part of Connect: the addon manager does not start or stop indexers.

In the current POC, the descriptor is compiled into the server binary. Enable and Connect happen at startup. The in-process indexing runtime is composed in server wiring and injected into producers (Kind, GCP HCP) that start and stop indexers. In a future external agent model, the same inventory report contract would be used over a transport, and an agent pod on the target cluster would own its own lifecycle rather than being hosted in-process.

The addon does not declare a managed resource capability for watched cluster objects. Kubernetes delivery targets are registered through the platform's target management / delivery-output path. Watched objects are inventory-only extension resources under `kubernetes.fleetshift.io/Object`.

## Agent

Delivery and indexing are separate surfaces that share target connection properties, not a single per-target Agent with two delegates.

- a **delivery agent** for manifest apply and removal (stateless; builds clients per delivery)
- an **indexing runtime** that hosts a per-target **indexer** for resource watching and inventory reports

When a producer (or startup replay) successfully ensures an indexer, the host runs discovery readiness and then starts the indexer under a long-lived context. Delivery operations are handled independently when delivery requests arrive. Indexers stop when explicitly stopped, on server shutdown, or when an unexpected exit exhausts local restart attempts.

### Target properties

A kubernetes target provides its connection details through properties:

| Property | Required | Description |
| --- | --- | --- |
| `api_server` | yes | Kubernetes API server URL |
| `ca_cert` | no | PEM-encoded CA certificate for TLS verification |
| `service_account_token` | no | Direct bearer token for authentication |
| `service_account_token_ref` | no | Vault secret reference resolved at connect / indexer start / unexpected-exit restart |
| `cluster_resource_name` | yes (for indexing) | Managed cluster resource name (e.g. `clusters/c1`) whose ID is the inventory object-name parent segment |

For indexing, one of `service_account_token` or `service_account_token_ref` must resolve to a non-empty credential. Direct tokens take precedence. `cluster_resource_name` is distinct from the kubernetes target ID when the target is prefixed (e.g. target `k8s-c1` with cluster resource `clusters/c1`). In the external agent model, the agent runs as a pod on the target cluster and can use in-cluster credentials, making some of these properties unnecessary.

## Deployment models

The reusable indexing core is shared. How indexers get created and wired depends on the deployment model.

### In-process

In the in-process model, the platform server hosts multiple per-target indexers — one per Kubernetes target that a producer (or startup replay) has ensured:

- **Indexer registry**: tracks running indexers by target ID (generation, fingerprint, readiness, restart metadata)
- **Ensure / start**: starts or replaces an indexer after discovery readiness; joins in-flight matching starts; generation-fenced
- **Stop**: idempotent; does not delete inventory
- **Client construction**: builds Kubernetes clients from API server, CA, and bearer credential
- **Unexpected-exit restart**: when a vault-backed credential ref is present, a small number of local restarts with backoff; re-resolves the credential from vault (raw token bytes are not retained after start handoff)

Producers own start/stop for provisioned clusters: Kind and GCP HCP ensure an indexer before reporting delivery success, and stop it at cluster teardown. After addon Connect, a one-shot startup replay lists persisted kubernetes targets with resolvable credentials and starts indexers so server restart recovers indexing. There is no store-driven reconcile loop and no orchestration hooks for indexing.

The host does not manage inventory cleanup. Stopping an indexer only stops watching; target-scoped indexed inventory cleanup after target deletion is currently deferred (stale inventory can remain until a future cleanup path lands).

Ensure is idempotent for the same generation and fingerprint — concurrent matching calls join rather than creating duplicates. A higher generation or changed fingerprint (API server, CA, credential identity, index-config digest) stop-and-replaces. A lower generation is rejected as stale.

The key modularity boundaries are the inventory reporter and delivery reporter interfaces. Those seams enable the external deployment model.

### External agent

In the external model, the indexer runs as an agent pod on the Kubernetes target cluster — one agent per cluster. There is no in-process host registry driven by producers. The pod creates a single indexer directly, using in-cluster credentials for the Kubernetes client, and wiring reports to a transport inventory reporter.

The inventory reporter is the key modularity boundary for inventory. In-process, a direct adapter writes through the platform inventory report service. In the external model, a fleetlet/channel or HTTP/gRPC adapter would implement the same report shape. The discovery/filter/informer/extraction/writer core is identical — only the reporter transport and process lifecycle change.

| Concern | In-process | External agent |
| --- | --- | --- |
| Indexer creation | Producers + startup replay ensure indexers on the in-process host | Agent pod on the target cluster, in-cluster credentials |
| Delivery requests | Direct calls from delivery router to the delivery agent | Fleetlet delivery channel (future) |
| Inventory writes | Direct adapter to the platform inventory report path | Transport inventory reporter |
| Delivery reporting | Direct delivery reporter | Fleetlet delivery channel adapter |
| Lifecycle | Producer start/stop; one-shot startup replay; stop-all on shutdown | Agent pod lifecycle IS the indexing lifecycle |

The actual fleetlet channel integration is not yet built, but no structural changes to the indexing core should be required beyond a transport reporter.

## Delivery pipeline

The delivery agent handles manifest apply and removal. It supports two delivery modes based on whether an attestation is present.

### Attested delivery

When an attestation is provided:

1. Verify the attestation against the target's trust bundle and the expected generation
2. On success: apply using the platform's service account (from target properties / vault)
3. On failure: report `AuthFailed` to the delivery reporter

The trust bundle is a JSON array of trusted issuers stored as a target property. Each entry specifies an issuer URL, JWKS URI, audience, and key claim mappings. Verifiers are cached per target (by trust-bundle content) to avoid rebuilding on every delivery.

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

Allow/deny lists are configured at two levels conceptually:

1. **Addon-level defaults**: applied when producers and startup replay start an indexer
2. **Per-target override**: target properties can override defaults (config fields exist; producers currently start with the default schema only — deny/allow/namespace wiring from properties is not yet connected)

Both use the same structure — lists of `{apiGroups, resources}` entries with wildcard support. `"*"` matches any API group or resource.

### Precedence model

The system operates in one of two modes depending on whether an allow list is configured:

**Watch-all mode** (default — no allow list configured):

1. In user deny? → DENY
2. In default deny? → DENY
3. Default → ALLOW (watch everything)

**Watch-selected mode** (allow list is configured):

1. In user deny? → DENY
2. In user allow? → ALLOW (user allow overrides default deny)
3. Default → DENY (only explicitly allowed resources are watched)

In both modes, if a resource appears in both user allow and user deny, deny wins.

Watch-all is the default behavior — index everything the cluster supports minus noise. Watch-selected is for constrained environments where the agent should be limited to specific resource types. The mode is determined per-target from the effective config: if the config contains any allow entries, that target operates in watch-selected mode. An explicit non-empty allow list that filters to zero watchable GVRs fails discovery readiness immediately.

### Namespace filtering

Namespace filtering is applied at the informer level during LIST and WATCH. Resources in excluded namespaces are dropped before reaching the event channel.

**Namespace include/exclude** — glob patterns controlling which namespaces are watched. Include patterns whitelist matching namespaces. Exclude patterns remove matching namespaces from the included set. Glob patterns are evaluated directly on each event — no caching or pre-resolution is needed because pattern matching is a pure string operation with negligible cost.

**Cluster-scoped resources** always pass the namespace filter — they don't have a namespace, so namespace patterns don't apply to them. Whether cluster-scoped resource types are watched is controlled entirely by the allow/deny lists at the GVR selection layer. To exclude cluster-scoped resources, deny them via the deny list or omit them from the allow list (in watch-selected mode).

## Graph model

The indexing pipeline produces inventory object reports and, in memory, topology edges. Edges are relationships between Kubernetes objects that enable topology queries — "show me everything connected to this Deployment" or "which pods run on this node." In the current integration, edges are **not persisted**; the in-process path uses a disabled edge sink, and the writer skips edge diff work when that sink is installed.

### Edge types

Core edge types shipped with the kubernetes addon:

| Edge Type | Source | Destination | Discovery |
| --- | --- | --- | --- |
| `ownedBy` | any resource | its controlling owner | controlling `ownerReference`, walked recursively up the controller chain |
| `runsOn` | Pod | Node | `spec.nodeName` |
| `attachedTo` | Pod | Secret, ConfigMap, PVC | scanning `spec.volumes` and `spec.containers[].env` valueFrom refs |
| `attachedTo` | PVC | PV | `spec.volumeName` |
| `selects` | Service | Pod | label selector matching against known Pods |

### Recursive owner traversal

The `ownedBy` edge type follows the controller ownership chain, not just the direct owner. A Pod owned by a ReplicaSet owned by a Deployment produces two edges from the original resource as source: Pod→ReplicaSet and Pod→Deployment. This enables queries like "everything owned by this Deployment" without the caller needing to know intermediate types. The traversal includes cycle detection to handle malformed owner references.

Only the controlling ownerReference (`controller: true`) is followed — non-controlling owners are ignored. This matches the Kubernetes lifecycle model where exactly one controller manages each resource's garbage collection and scaling. Non-controlling owners (e.g., sidecar injectors) would produce misleading topology edges.

Recursive owner traversal is computed automatically for all resources via the controlling `ownerReference` — no schema entry or hook is needed. Type-specific edges use optional edge-building hooks on schema entries.

### Edge type extensibility

Edge types are strings, not a closed enum. Future addons or schema extensions can register custom edge types (e.g., a GCP addon could add `hostedIn`) without modifying the core model.

### NodeStore

A dual-indexed view of current inventory nodes, built at edge computation time:

- by UID — O(1) lookup
- by Kind → Namespace → Name — O(1) cross-resource lookup

Edge computation closures receive the NodeStore so they can efficiently find related resources. For example, the `selects` edge builder for a Service looks up Pods by kind+namespace, then matches label selectors. Without the kind/namespace/name index, this would require scanning all items. Cluster-scoped resources use a sentinel empty-namespace key.

### Edge computation timing

Edges are computed lazily, at flush time in the writer, not on every event or on resync — and only when a real edge sink is configured. When a resource arrives (via event or resync), its edge computation closure and membership are retained in memory. At flush, the writer can build the NodeStore from all known nodes across all GVRs, run all closures plus recursive `ownedBy`, and diff edges against the previous set to produce add/delete deltas for the edge sink.

With the default disabled sink, that diff/delivery step is skipped: inventory reporting proceeds without edge work. This matches the first-integration decision to keep topology computation available in the addon while avoiding a premature platform edge store.

The flush path remains the owner of edge computation because it has a complete view of all resources. Cross-GVR edges — Pod→Node (`runsOn`), Service→Pod (`selects`), PVC→PV (`attachedTo`) — require the NodeStore to contain resources from multiple GVRs simultaneously. Resync updates in-memory membership for later edge computation but does not itself write edges.

## Indexing pipeline

The indexing pipeline watches Kubernetes resources on the target cluster, extracts inventory object reports in two tiers, optionally computes edges between items, and reports results through the inventory reporter. It is a multi-stage system: CRD watching triggers informer reconciliation, informers produce events, a writer batches events and acknowledges persistence, and the reporter maps to the platform inventory write path (mixed upserts and exact-name deletes).

```text
CRD watcher
  |
  +-- triggers reconciliation on CRD add/update/delete
  |
Informer manager
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

The informer manager watches CustomResourceDefinitions. When a CRD is created, updated, or deleted, the manager triggers a reconciliation cycle: re-discovers supported GVRs, applies filtering, and starts or stops informers to match the effective set. Reconciliation is throttled — a minimum delay between cycles (10s) prevents API server spam when multiple CRDs change rapidly (e.g., during an operator install).

### Informer manager

The informer manager reconciles running informers against the effective set of resources to watch:

1. Discover all WATCH-capable resources from the API server (partial discovery results are accepted with a warning; partial discovery is not treated as authoritative GVR removal)
2. Apply resource filtering (default deny list + user allow/deny config)
3. Apply namespace filtering if configured
4. Diff against running informers — stop removed, start new
5. Serialized startup with initialization timeout (10s per informer) to avoid memory spikes

CRD-triggered reconciliation re-runs this from step 1. The desired GVR set is determined by discovery and filtering, not by the schema. The schema only controls enrichment at extraction time. Each started informer is tagged with a monotonic GVR process generation so the writer can fence late events after a generation closes.

### Informer

Each informer implements LIST+WATCH for a single GVR with minimal memory overhead. It tracks only UID-to-resourceVersion mappings, not full objects. Separately, it maintains a watch cursor for clean watch resume.

**LIST phase**: paginated LIST, sending an add event for each resource. Resources in excluded namespaces are dropped before being sent to the event channel. UIDs that disappeared from the local index are dropped locally without emitting deletes — omission deletes are the writer's responsibility via its acknowledged-UID baseline on resync. After the LIST completes, a resync snapshot with the full resource set is sent; the informer waits for the writer's persistence ack before starting WATCH from the LIST resourceVersion.

**WATCH phase**: WATCH from the saved cursor, requesting bookmarks. Add, update, and delete events are dispatched from the watch stream, with namespace filtering applied before dispatch. Bookmarks advance only the cursor. On clean channel close with a cursor, the informer resumes WATCH without LIST. On expired/unsafe cursor (`410 Gone` and unclassified watch failures), it returns to the LIST phase.

**Shutdown**: when the context is cancelled, the informer stops without emitting delete events. Shutdown is a runtime lifecycle event, not a Kubernetes source-of-truth delete. The writer may flush real pending object work; it must not turn local cache eviction into persisted deletes. Stopping all informers likewise does not treat shutdown as GVR removal.

**Retry**: on LIST or WATCH errors, the informer retries with exponential backoff. Retries reset on a successful watch start.

### Writer

The writer batches informer events, performs two-tier extraction, computes edges, and flushes results to the inventory reporter at a configurable interval (default 5s).

**Event handling**:

- Add / update: buffer the resource in pending upserts keyed by UID
- Delete: add the UID to pending deletes and remove any pending upsert for that UID
- Late-delete protection: if a UID was deleted in the current batch, subsequent add/update events for that UID are dropped
- Generation fencing: events tagged with a closed GVR generation are rejected

**Flush** (on batch timer):

1. For each pending upsert, skip if the resourceVersion is unchanged since the last flush (deduplication)
2. Run two-tier extraction into an inventory object report (plus in-memory node / edge closures)
3. Build delete reports as exact-name whole-resource deletes from cluster + GVR + UID
4. Send upserts and deletes in one mixed inventory report batch
5. On success, advance the acknowledged-UID set and version dedup state; then flush edges to the edge sink (skipped when disabled)

**Resync** (on LIST snapshot): extract all resources for the GVR, compute deletes as acknowledged UIDs minus LIST UIDs for the **current process/GVR generation only**, and send one mixed report batch. On success, replace the acknowledged-UID baseline with the LIST set and ack the informer so WATCH can start. On failure, retain the batch for ticker retry; do not advance the baseline or ack. A first LIST after process start has an empty acknowledged set, so it upserts only — it does not prune database-only rows left by an earlier process.

**Error recovery**: on write failure, the writer retries with exponential backoff (up to 3 attempts: 1s, 2s, 4s). If all retries are exhausted, the failed batch remains pending and is retried on the next batch tick. Deduplication and the acknowledged-UID baseline advance only after a successful write, so failed items are never silently lost. Memory during extended outages is bounded by unique UIDs on the cluster.

**No heartbeat**: idle empty flushes are not sent. An empty report is a no-op and is not used as a liveness signal. In-process runtime health is owned by the indexing host / producer lifecycle, not inventory report no-ops.

All event processing is serialized on a single goroutine for ordering safety. Late-delete protection and resourceVersion deduplication ensure correct behavior under interleaved events.

### Inventory reporter

The inventory reporter is the addon-to-platform direction of the indexing protocol:

- one mixed batch of complete-object upserts and exact-name deletes — the sole live write path for Kubernetes object inventory

There are no separate platform resync/collection-delete/subtree-delete methods on this boundary. Same-process LIST omission reconciliation is expressed as ordinary deletes computed from the in-memory acknowledged-UID set. Target-wide subtree cleanup is deferred and not exposed here.

In-process, a direct adapter always reports under `kubernetes.fleetshift.io/Object` and maps both upserts and deletes into one platform replacement batch. In the external model, a transport adapter would implement the same report shape.

Object identity uses:

```text
ResourceType: kubernetes.fleetshift.io/Object
ResourceName: clusters/{clusterResourceID}/apiResources/{gvrKey}/objects/{uid}
```

where `gvrKey` is `{groupKey}~{version}~{resource}` (`groupKey` is `core` for the core API group), and `clusterResourceID` is the ID segment of the managed cluster resource name.

## Two-tier extraction

The indexer extracts inventory object reports in two tiers. The base tier runs for every watched resource. The enriched tier runs only for resources with a schema entry that defines fields or hooks.

### Base tier

Every watched resource gets the following projected into the inventory report, with no enriched schema fields required:

| Projection | Source |
| --- | --- |
| Resource name | `clusters/{clusterResourceID}/apiResources/{gvrKey}/objects/{uid}` |
| Labels | Kubernetes `metadata.labels` (complete latest set; empty map clears) |
| Conditions | Kubernetes `status.conditions` projected to FleetShift conditions (True/False/Unknown only; empty type dropped; missing transition time falls back to observed-at) |
| Observation GVR | group, version, resource, scope |
| Observation apiVersion / kind | from the object |
| Observation metadata | uid, namespace, name, resourceVersion, generation, creationTimestamp, deletionTimestamp, annotations, ownerReferences |
| Observation extracted | empty object unless enriched tier fills it |
| Controlling owner UID (in-memory) | controlling `metadata.ownerReferences` entry (used for recursive edge building) |

Secret `data` is not stored in observation — only metadata/identity and safe extracted fields.

### Enriched tier

Resources with a schema entry get additional extraction into `observation.extracted`:

- **JSONPath field extractions**: named fields with JSONPath expressions and data type coercion
- **Compute-extra hook**: optional function for computed properties that JSONPath cannot express
- **Build-edges hook**: optional function for type-specific edge building (core `ownedBy` edges are automatic)
- **Annotation extraction**: opt-in, with a configurable size cap per value (default 64 chars). `kubectl.kubernetes.io/last-applied-configuration` is always stripped. When enabled, the capped map is stored under `extracted.annotations`. This part needs to be rethought - see TODO section.

## Schema and field extraction

### Schema model

The indexing schema defines how to enrich extraction beyond the base tier. It does not control which resources are watched — that is determined by resource discovery and filtering. It is also distinct from the platform extension resource schema registered at Connect (the inventory-only marker for `kubernetes.fleetshift.io/Object`).

- **Schema entry**: enrichment rules for one watched GVR — field extractions, annotation extraction flags, and optional hooks for computed properties and edge building.
- **Field extraction**: a named field with a JSONPath expression and a data type for coercion.
- **Index schema**: the complete set of entries, keyed by GVR. Looked up at extraction time — if an entry exists for a resource's GVR, enriched extraction runs; otherwise base extraction alone applies.

### Data types

| Type | Coercion |
| --- | --- |
| `string` | Default. Value returned as-is. |
| `number` | Coerced to a floating-point number. Accepts int, float, and string. |
| `bytes` | Parsed as a Kubernetes quantity (e.g. `1Gi`) and converted to bytes as a number. |
| `slice` | All JSONPath results collected into a flat list. |
| `mapString` | Map values coerced to strings. |

### Extraction

Two-tier extraction converts an unstructured Kubernetes resource into an inventory object report plus an in-memory node for edge computation. Base and enriched steps run in one pass — enriched steps are no-ops when no schema fields/hooks exist:

1. Build the resource name from cluster resource ID, GVR key, and UID
2. Project conditions from `status.conditions` when present
3. *(enriched)* Evaluate JSONPath expressions for each schema field, coercing by data type into `extracted`
4. *(enriched)* Extract annotations if enabled, with size cap and noise stripping
5. *(enriched)* Run the compute-extra hook if present (e.g., pod status computation, node roles)
6. Build observation (`gvr`, `apiVersion`, `kind`, `metadata`, `extracted`) and labels from Kubernetes labels
7. *(enriched)* Store the build-edges closure for edge computation when a real sink is active

### Default schema

The default schema provides enriched extraction for core Kubernetes resource types: workloads (Deployments, StatefulSets, DaemonSets, ReplicaSets, Jobs, CronJobs), pods, services, nodes, namespaces, storage (PVCs, PVs), and configuration (ConfigMaps, Secrets). For most types, key status fields and replica counts are extracted. Nodes and pods include compute-extra hooks for computed properties (node roles, pod status). Pods, services, and PVCs include build-edges hooks for type-specific edges (`runsOn`, `attachedTo`, `selects`). ConfigMaps and Secrets are indexed for identity and labels only — no enriched fields are extracted; Secret payload data is not stored.

All other resources discovered on the cluster receive base extraction only — identity, labels, conditions, observation metadata — without enriched fields.

## Open questions & todo

### Runtime schema extensibility

The two-tier model and allow/deny configuration address the core extensibility gap for controlling what is watched. However, enriched extraction rules (JSONPath fields, hooks) are compiled into the binary. Whether enriched schema entries should be configurable at runtime — via target properties, a configuration API, or a custom resource — remains open. Allow/deny lists and namespace filters are modeled in index config, but producers currently start with the default schema only, so runtime allow/deny/namespace overrides from target properties are not yet wired. Also consider whether CEL should be used in addition to JSONPath (platform already utilizes CEL), or whether we should completely replace JSONPath with CEL.

### Edge querying / persistence

Topology edges are computed in the addon but not persisted. The platform has no inventory edge query API and does not accept edge reports through the inventory reporter. Whether edges become platform relationships, a separate topology store, or remain unpersisted is an open platform decision. Until then, topology queries over persisted inventory cannot be answered from edge tables.

### Pod `attachedTo` coverage gaps

Pod `attachedTo` edges currently come from `spec.volumes` and `spec.containers[].env` valueFrom refs (secretKeyRef / configMapKeyRef). They do not yet cover `spec.containers[].envFrom` (configMapRef / secretRef), which is a common way to inject whole ConfigMaps and Secrets and is a real attachment dependency. `initContainers` are similarly unscanned. If attachment-graph completeness matters once edges are persisted or queried, extend the Pod edge builder to include those sources.

### Watch continuity and absence reconciliation across restart

Watch continuity (resourceVersion cursor) and persistence acknowledgement (acknowledged UID set) are held in memory per process/GVR generation and discarded when the indexer stops. Within one generation, LIST/resync deletes only previously acknowledged UIDs absent from the LIST, and clean watch disconnects resume from the cursor without LIST.

After process restart that state is gone: a fresh indexer performs a full LIST per GVR against an empty acknowledged baseline (upsert-only), rebuilds in-memory edge state from scratch, and does not remove database-only rows left by an earlier process. Objects deleted while the indexer was down can remain until a future cleanup path or manual operation removes them. Existing inventory survives in the database, but the synchronization checkpoint is lost.

A platform-persisted checkpoint per GVR per target — last successfully processed resourceVersion plus an acknowledged membership baseline — could address both sides: resume or narrow WATCH without a full LIST, and prune cross-process absences. Clean in-process watch resume already avoids LIST on ordinary disconnects; this is about stop/restart and the correctness gap that upsert-only recovery leaves behind.

### CRD / GVR removal and target inventory cleanup

When a GVR leaves the desired set, or when an indexer is stopped / a target is deleted, persisted inventory for that scope is not deleted. GVR removal drops in-memory state only. Target-scoped indexed inventory cleanup after target deletion is deferred. Restoring offline, owner-validated subtree cleanup (or an equivalent) remains open work.

### Informer startup serialization at scale

The current serialized startup (one informer at a time, with initialization timeout) works for a bounded set of GVRs. With watch-all mode on a large cluster (200-400+ GVRs), serialized startup could take significant time. A bounded-parallelism approach (e.g., start N informers concurrently) may be needed, balanced against memory spike risk during initial LIST phases.

### Resync fallback as defense-in-depth

The writer's error recovery relies on retaining failed batches for retry on the next tick. This handles all known failure modes — transient DB errors, extended outages, and partial state divergence within a process generation. A full resync fallback (triggering a re-LIST of all GVRs after N consecutive failures) could be layered on as defense-in-depth against unknown state corruption or bugs in the retry logic. This is not currently needed — the retry mechanism is correct by construction and self-healing within a generation — but could be added if operational experience reveals failure modes that per-batch retry does not cover. Such a fallback would require a back-channel from the writer to the informer manager, inverting the current one-directional dependency.

### Observation and condition history

The implementation stores only the latest observation and current conditions per inventory object — a single observation payload, a conditions set, and freshness timestamps. Platform observation/condition history tables exist but are not populated synchronously by inventory reports. The current single-snapshot model is sufficient for point-in-time inventory queries but cannot answer "when did this resource last change state" or "how long has this condition been degraded" without additional work.


### Annotation collection policy

Annotation handling needs a deliberate decision. Today the base observation stores the object's annotations uncapped in metadata, while an unused schema opt-in can also store a size-capped, noise-stripped copy under `extracted`. That dual path should not stand. Options to choose among include: collect no annotations at all; or keep annotations with a size limit and always drop known-large noise such as `kubectl.kubernetes.io/last-applied-configuration`. Revisit storage cost, sensitivity, and query value before locking a policy.

### Addon-to-addon communication for schema and edge extensibility

Each addon currently operates in complete isolation. The index schema is hardcoded at compile time and injected when an indexer starts. There is no mechanism for one addon to influence another addon's indexing behavior — no cross-addon messaging, no shared schema registry, and no hook points in the addon lifecycle for inter-addon coordination.

A concrete use case: a VM management addon knows that certain Kubernetes resources (e.g., VirtualMachine CRDs, specific fields on Pods) are relevant to its domain but is not itself an indexing addon. It needs to tell the kubernetes indexing addon "watch these additional resource types and extract these fields." This requires either a schema contribution API (addons register supplementary schema entries that the indexing addon merges into its effective schema) or a platform-level schema registry that aggregates entries from multiple addons and feeds the merged result to whichever addon handles indexing for that target type.

### Configurable edge and relationship discovery

The current edge types (`ownedBy`, `runsOn`, `attachedTo`, `selects`) and their discovery logic are compiled into the binary as edge-building hooks on schema entries. Adding a new relationship type — for example, a `managedBy` edge from a Kubernetes resource to a VM, or a `dependsOn` edge between services discovered by a service mesh addon — requires code changes to the kubernetes addon.

This is closely related to addon-to-addon communication: if an external addon can contribute schema entries, it could also contribute edge-building hooks that define new edge types and their discovery logic. A lighter-weight alternative is a declarative edge configuration — edge definitions expressed as data (source kind, destination kind, edge type, and a field path or label selector for discovery) rather than compiled hooks. Declarative edges could cover common patterns (field-based reference lookups, label selector matching) without requiring Go code, while hook-based edge building would remain available for complex edge logic that declarative rules cannot express.
