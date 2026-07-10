# Resource Indexing

## What this doc covers

The fleet-wide inventory and observation indexing model:

- inventory scope
- what gets indexed
- how index data reaches the platform
- index schemas and indexer agents
- the inventory item shape
- condition history at a high level
- scale assumptions
- the relationship between observed and intended state
- the search API shape

## When to read this

Read this when you need the model for fleet-wide search, drift detection, target observation, or how observations become queryable platform inventory.

## What is intentionally elsewhere

- The index channel itself: [fleetlet_and_transport.md](fleetlet_and_transport.md)
- Core delivery and target contracts: [core_model.md](core_model.md)
- Detailed target delivery protocol, reporting, and journaling: [target_delivery_contract.md](target_delivery_contract.md)
- Cross-instance federation on top of search: [platform_hierarchy.md](platform_hierarchy.md)
- Managed-resource projection details and fuller condition-history commentary: [../managed_resources.md](../managed_resources.md)

## Related docs

- [../architecture.md](../architecture.md)
- [resource_identity_and_api.md](resource_identity_and_api.md)
- [target_delivery_contract.md](target_delivery_contract.md)
- [orchestration.md](orchestration.md)
- [../managed_resources.md](../managed_resources.md)

## Overview

The platform continuously projects observations into a fleet-wide inventory and search system. Managed targets are the most common source of observations, but the model is broader than target-local search. Inventory can also represent managed resources, discovered resources, sub-resources, and side-effect resources associated with deliveries.

This enables cross-target discovery and aggregation such as:

- all degraded deployments across the fleet
- all targets with VMs in error state
- pod counts by namespace across production targets

The platform owns the indexing infrastructure:

- the built-in index channel
- index storage
- the search API

Inventory is a projection of what reporters extract from source objects, not a literal copy of those objects. Schemas define that extraction. Once on the platform, the same fields are available via typed Get/List and via `queryResources`.

## How indexing works

For Kubernetes targets, an indexer agent watches the local Kubernetes API server and streams deltas through the fleetlet's built-in index channel.

```text
Indexer Agent -> watches local K8s API server
              -> batches deltas to Fleetlet
              -> Platform Index Service
              -> Index Store
```

The indexer agent is itself deployed through the normal delivery pipeline. It is not built into the fleetlet. This preserves zero infrastructure coupling for the fleetlet while still letting the platform manage indexing as ordinary deployment infrastructure.

Other target types follow the same pattern:

- **platform targets**: platform-internal status can be indexed without an external agent
- **addon-defined targets**: the addon defines both the schema and the indexer agent

Inventory items are typically associated with a fulfillment and target, with an optional manifest correlation key when the observed resource maps back to delivered intent. Some observed resources are side effects and therefore have no direct manifest correlation.

## Index schemas

Schemas define:

- which resource types are indexed
- which fields are extracted
- how agents are configured
- which fields are queryable

For addon-defined targets, addons own the schema. The platform still stores and queries indexed data uniformly.

When a schema changes, the platform re-delivers the affected indexer-agent configuration through the normal delivery path.

## Inventory item shape

The inventory shape is designed to balance well-structured data the platform and addons can depend on, with free-form data open to extension. It also balances history-keeping, but only where it matters, to keep the data from getting bloated.

- **Identity**: resource type and name, following AIP resource names. The full resource name includes the extension's service name (e.g. `//kubernetes.fleetshift.io/clusters/foo/namespaces/bar/objects/apps.v1.Deployment.nginx`). The relative resource name links to the platform resource identity.
  - This necessarily includes the **Parent** resource, if any.
- **Aliases**: namespaced key:value pairs for alternate identifiers. These solve the problem of relating to a resource where you do not know the canonical platform-defined resource name. Inventory reporting may contribute aliases, but the linked platform resource remains the canonical owner of the aggregated, *accepted* alias set used for cross-extension identity correlation (see [resource_identity_and_api.md](resource_identity_and_api.md#aliases)).
- **Relationships**: semantic links between different platform resource identities (e.g. Pod → Service, Node → Cluster). These are modeled on the platform resource and follow the relationship model described in [resource_identity_and_api.md](resource_identity_and_api.md#semantic-relationships). Inventory reporting may contribute relationship facts, but the platform resource remains the canonical owner of the aggregated relationship graph.
  - In the future these may drive relationship-based access control, as well as general-purpose relational queries.
  - Relationships should be able to be defined using aliases.
- **Labels**: All items in inventory should have labels, for queries & placement. On the typed extension API these are reporter `local_labels`, distinct from user-writable extension `labels` on management-capable types. The index projection may still treat both as queryable label spaces; do not collapse them on the wire.
  - Be careful about attestation. Need local tracking of "who am I" (what am I labelled) which requires either (a) signatures for proof, (b) label "delivery" with authorization, or (c) the target itself being an authority of its own labels. For reporting inventory, (c) works.
- **Properties**: stable-ish values (e.g. api_url) produced once and rarely changed. Not historical. Lives on the inventory item because a single Fulfillment can target many objects, each with its own properties. Older notes sometimes called this bucket "outputs"; the canonical term in the current design is `properties`. Properties may also be a part of objects not managed by a Fulfillment. Should be able to align with SIG Multicluster ClusterProperty and/or OCM's ClusterClaim API on the managed clusters.
  - We might want to consider how else properties can be used and if they need to be signed in any way, if they want to participate in placement.
  - If we want to lean into SIG use cases, then we should consider indexing these by default.
  - TODO: Consider alternatives:
    - Option 1: Remove it entirely and collapse with Observations / Aliases
    - Option 2: Keep it as a distinct stable-value bucket, but tighten its intended use and signing model
    - Regardless we can project various fields onto SIG objects
- **Observations**: opaque, addon-defined. Potentially volatile runtime state as seen by the observer. Only the latest observation is stored today; historical observations are a planned future asynchronous writer, not something the synchronous inventory write path maintains (see [../managed_resources.md](../managed_resources.md)).
- **Conditions**: structured, platform-queryable health and progress signals. Only the latest condition set is stored today, as a single JSON object keyed by condition type; a history of condition transition events is the same planned future asynchronous work as observation history.

This gives the platform a uniform query surface without requiring the platform to understand every domain-specific observation payload. The platform identity layer owns aliases and semantic relationships; the per-extension projection owns labels, properties, observations, and conditions. There are several different structured types here over the more basic Kubernetes shape, mainly for supporting secondary indexes (which etcd cannot support). Specifically we have:

- Relationships: In kube these are up to spec/status fields. Here, they are well defined to support a graph traversal of relationships. This is likely to be used for access control, search, and navigation.
- Aliases: These are well defined to support correlation across extensions. ACS may already scan a cluster. If that cluster is later imported into the management plane, ACS won't already know the resource's platform name. Aliases must be used to correlate (kube system namespace uid, cloud platform identifier, ACS's own ID, SIG multicluster ID, ...).
- Conditions: Conditions are elevated to first class, out of status. This is so they can be intentionally indexed, as well as used for condition aggregation (though condition aggregation may still want to be possibly based on other data, not just child conditions)
- Properties: I am less sure about these. There are two intentions here, which possibly shouldn't be combined: possible alignment with multicluster SIG, and elevating certain status (or spec?) fields for reliable consumption. For example, a cluster control plane URL. It also gives operators a place to put fields where the cost of story history is not worth it.

Observations are for everything else (observed spec, status), sans what is transformed to the other field types.

Condition transitions are to be modeled as historical condition events, but that history is not populated by the current synchronous write path.

## Resource identity and child resources

Resource identity for inventoried resources follows the two-layer API model defined in [resource_identity_and_api.md](resource_identity_and_api.md).

Inventoried resources are extension resources in their addon's own package, with a corresponding platform resource for canonical identity. For example, when the ACS addon reports cluster status, it creates an extension resource at `//acs.fleetshift.io/clusters/foo` which links to the same platform identity as `//gcphcp.fleetshift.io/clusters/foo` or `//kubernetes.fleetshift.io/clusters/foo`.

Identity correlation across extensions works through:

1. **Shared relative name**: if an extension registers into the same platform identity domain and uses the same relative name (e.g. `clusters/foo`), it links to the existing identity automatically. 
2. **Alias correlation**: if an extension doesn't know the canonical name, it can only resolve a report to an *existing* identity today by asserting an alias that identity has already accepted (see [resource_identity_and_api.md](resource_identity_and_api.md#aliases)). Newly reported aliases are stored as pending assertions on the reporting extension resource, not synchronously checked against or folded into the platform resource's accepted alias set -- accepting or conflict-flagging a pending alias is deferred to a future asynchronous reconciliation process.

### What if there is no matching resource?

If an addon reports about a resource and no matching platform identity exists by name, the platform creates the platform resource implicitly (if the addon is authorized to establish identity in that domain). If an addon reports only aliases and none of them match an already-accepted identity, the resource should be held in some kind of "inbox," to be resolved later (by user or later report).

Whether the assigning operator is the provider or a tenant depends on who is importing the resource. Trusted addons can offer a tenant association; otherwise it defaults to the provider tenant.



## What gets indexed

The indexed projection can represent more than direct target-native objects. It can include managed resources themselves, discovered resources, sub-resources, and side-effect resources, as long as a schema defines the extracted fields.

For Kubernetes targets, the default is medium-depth indexing:

- kind and API version
- name and namespace
- labels
- selected annotations
- owner references
- status conditions
- key spec fields

That covers the common fleet-wide query cases without reporting full source-object bodies to the platform.

Default schema categories:

- **Core types**: Pods, Deployments, StatefulSets, DaemonSets, Services, Nodes, Namespaces, PVCs
- **Extended types**: VirtualMachines, Routes, Ingresses, CRDs
- **Events**: opt-in with aggressive TTL

For the full live object on a target (beyond what was reported into inventory), the platform uses direct API proxying or addon-specific APIs — not a deeper read of the index.

## Scale characteristics

> NOTE: Possibly made up

Representative scale assumptions for a typical production Kubernetes target:

- around 11,000 indexed core resources
- around 100 events per minute in steady state
- roughly 500 B to 1 KB per indexed representation


| Fleet size    | Indexed resources | Index storage | Write rate | Per-fleetlet bandwidth |
| ------------- | ----------------- | ------------- | ---------- | ---------------------- |
| 50 targets    | 550K              | ~550 MB       | ~80/sec    | ~1.6 KB/s              |
| 500 targets   | 5.5M              | ~5.5 GB       | ~780/sec   | ~1.6 KB/s              |
| 2,000 targets | 22M               | ~22 GB        | ~3,100/sec | ~1.6 KB/s              |


The steady-state fleetlet bandwidth is modest. The platform-side index service is the real bottleneck consideration rather than the fleetlet link.

SQLite remains viable for smaller instances, while Postgres is the expected production choice for larger fleets.

Initial syncs stay manageable as well. After an agent restart, a full resource dump is roughly 11 MB per target. Even a worst-case rolling restart across 500 targets is about 5.5 GB over 5 minutes, and in practice restarts can be staggered or prioritized for high-value resource types first.

## Indexing capability and ownership model

Addons define a resource type. Should other addons be able to report about other resource types?

For aliases, definitely. This is partly why we might want aliases at all; for different services to contribute and align on keys. It also lets them reference resources without having to know the platform's canonical name.

For labels, this is nuanced. We might want to namespace labels. In other words, perhaps indexed metadata is a shared space. There are fleetshift-contributed labels (by the user). Then there might be other addons that want to contribute labels of their own, from their own users. The tricky part about labels (or anything that should be leveragable in placement decisions) is attestation. There must be verifiable provenance for a placement decision, which includes the state involved in that decision. So if an addon contributes labels, it must tell OME about them with an authorized token, which we can then publish to other addons; or it must be signed by that addon.

> [!NOTE]
> Signing is not a silver bullet here; it might not always fit the trust model. For example, signatures from a cluster-side agent might not be verifiable outside of that one agent. This is because to be verifiable, we have to know who issued those keys. On a spoke cluster, I don't think there's a need otherwise for us to know about signing key issuers, there. We do need to know about the spoke workloads _identity_ issuer, though. But that is complex enough before introducing signing into the mix.

## Relationship to fulfillment intent

The platform knows what it intended through fulfillments and their delivery records. Inventory holds observations of what actually exists, including resources that may not map 1:1 to delivered manifests. Joining those two views enables:

- intent-aware search (which fulfillment delivered what to where)
- drift detection between delivered manifests and observations
- richer status views for user-facing concepts (deployments, managed resources)
- impact analysis for placement changes

This is one of the reasons indexing belongs in the core architecture rather than as an addon-only concern.

## Search API shape

Fleet-wide discovery needs a filterable search surface over inventory. That surface is `queryResources`: a custom GET on the platform API over a hierarchy scope, with a CEL filter over the result envelope. Coverage starts with activated extension resources — the same types already addressable via typed Get/List — and expands as more platform-held shapes are included.

```
GET /apis/fleetshift.io/v1/{scope}:queryResources?filter={cel_expression}
```

- **Scope**: a collection path in the resource hierarchy, or `-` for the whole platform. Narrower scopes (cluster, workspace, and other levels) are part of the same method as coverage grows; the URI keeps `{scope=**}` for that.
- **Filter**: a CEL expression over the query result envelope — identity (`name`, `resource_type`) and the resource body as returned by typed Get/List for that type's capabilities (labels, managed fields, observed-state fields such as local labels, conditions, observation, and timestamps). Filters should read like the response shape, not like storage columns. Results follow the same activation boundary as the typed API surface.
- **Pagination / ordering**: AIP-158 page tokens; deterministic ordering suitable for keyset pagination.
- **Response**: each hit is `name`, `resource_type`, and a body matching the dynamic extension-resource Get/List envelope for that type — the same platform-held fields, not a reduced search DTO.

Fully realized, the same method covers a broader set of platform-held resource shapes (platform aggregates, inventoried and managed resources, and other inventory-held types), still filtered with CEL and still scoped through the hierarchy. Filters may limit results to specific resource types or extensions. RBAC is enforced by the platform, so users only see resources they are authorized to access.


`queryResources` returns the same platform-held resource data as typed Get/List for those hits — it queries the resources. Selectivity lives in what reporters extract and send to the platform (schemas define that extraction); once data is on the platform, query and direct get see the same thing. Reaching beyond what was reported (full live objects on a target) is a different path: API proxying or addon-specific APIs, not a richer variant of this search surface. See [resource_identity_and_api.md](resource_identity_and_api.md#inventory-search-api) for how this fits into the overall API model.

## Open questions

### How strongly typed are inventoried resource schemas?

Should we accept observations as opaque, and only define indexes?

Inventoried resources have extension-defined observation schema (the extension chooses what fields to extract and make queryable). The common metadata surface (labels, conditions) is generic and platform-defined. The observation payload itself may remain schema-defined but stored as a structured type (Struct or equivalent) for flexibility.

### Do inventoried resources materialize as resources in the normal resource hierarchy?

**Resolved: yes.** Inventoried resources are extension API resources in their addon's package, with a corresponding platform resource for canonical identity. For example:

- Platform identity: `clusters/foo` (at `//fleetshift.io/clusters/foo`)
- Extension resource: `//kubernetes.fleetshift.io/clusters/foo/namespaces/bar/objects/apps.v1.Deployment.nginx`

This means inventoried resources get AIP-compliant names, are addressable via typed extension APIs, and are also discoverable through the search API. The dynamic gRPC surface area grows with inventoried object types, but this is handled by the existing `DynamicServiceMux` / `DynamicHTTPMux` infrastructure.

See [resource_identity_and_api.md](resource_identity_and_api.md) for the full two-layer model that makes this possible without name collisions.

### How this maps to server-side drift correction (if we do that)

We'd have to correlate when a manifest in a fulfillment maps to an observed resource. Then, we'd have to define equivalence between observations and the fulfillment's manifest.

Manifests can have keys, which are unique within the scope of its target.

A target may map to a resource.

That doesn't necessarily make the manifests direct child resources of the target resource.

The saving grace here is that the thing handling fulfillments is also the thing reporting about resources, in the common case. It just needs to tell the platform that they are related so it can compare and reconcile, if needed.