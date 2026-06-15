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

Inventory is a projection, not a literal copy of source objects. Schemas define what is extracted and made queryable.

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

- **Identity**: resource type and name, following AIP resource names.
  - This necessarily includes the **Parent** resource, if any.
- **Aliases**: namespaced key:value pairs for alternate identifiers. These can be used in correlation or referencing a resource by another name. Aliases solve the problem of relating to a resource where you don't know the canonical, platform-defined resource name.
- **Relationships**: resource associations, following the related AIP
  - In the future these may drive relationship-based access control, as well as general-purpose relational queries.
  - Relationships should be able to be defined using aliases.
- **Labels**: All items in inventory should have labels, for queries & placement.
  - Be careful about attestation. Need local tracking of "who am I" (what am I labelled) which requires either (a) signatures for proof, (b) label "delivery" with authorization, or (c) the target itself being an authority of its own labels. For reporting inventory, (c) works.
- **Properties**: stable-ish values (e.g. api_url) produced once and rarely changed. Not historical. Lives on the inventory item because a single Fulfillment can target many objects, each with its own properties. Properties may also be a part of objects not managed by a Fulfillment (hence the prior term, "output," is no longer proposed). Should be able to align with SIG Multicluster ClusterProperty and/or OCM's ClusterClaim API on the managed clusters.
  - We might want to consider how else properties can be used and if they need to be signed in any way, if they want to participate in placement.
  - If we want to lean into SIG use cases, then we should consider indexing these by default.
  - TODO: Consider alternatives:
    - Option 1: Remove it entirely and collapse with Observations / Aliases
    - Option 2: Make it only a concern for managed resources and go back to "outputs"
    - Regardless we can project various fields onto SIG objects
- **Observations**: opaque, addon-defined. Potentially volatile runtime state as seen by the observer. Historical observations are kept over time.
- **Conditions**: structured, platform-queryable health and progress signals. A history of condition transition events is kept over time.

This gives the platform a uniform query surface without requiring the platform to understand every domain-specific observation payload. There are several different structured types here over the more basic Kubernetes shape, mainly for supporting secondary indexes (which etcd cannot support). Specifically we have:

- Relationships: In kube these are up to spec/status fields. Here, they are well defined to support a graph traversal of relationships. This is likely to be used for access control, search, and navigation.
- Aliases: These are well defined to support correlation across extensions. ACS may already scan a cluster. If that cluster is later imported into the management plane, ACS won't already know the resource's platform name. Aliases must be used to correlate (kube system namespace uid, cloud platform identifier, ACS's own ID, SIG multicluster ID, ...).
- Conditions: Conditions are elevated to first class, out of status. This is so they can be intentionally indexed, as well as used for condition aggregation (though condition aggregation may still want to be possibly based on other data, not just child conditions)
- Properties: I am less sure about these. There are two intentions here, which possibly shouldn't be combined: possible alignment with multicluster SIG, and elevating certain status (or spec?) fields for reliable consumption. For example, a cluster control plane URL. It also gives operators a place to put fields where the cost of story history is not worth it.

Observations are for everything else (observed spec, status), sans what is transformed to the other field types.

Condition transitions are retained historically as condition events. This document focuses on the current queryable projection and search surface; the fuller managed-resource discussion of observations and condition-event history lives in [../managed_resources.md](../managed_resources.md).

## Resource identity and child resources

Within a single extension, resource type and identity is simple (ish–see [managed_resources.md](../managed_resources.md) for a discussion of type collisions). The extension defines the name.

But when another extension wants its resource identity to correlate to the same resource, how does it do it?

Even if the inventory resources are directly URL-addressable independently to start (only searchable), we still need resource names to uniquely identify them. We might as well follow the AIPs.

Examples:

- ACS reports its cluster status, it should be about a cluster

It reports by aliases it knows about.

### What if there is no matching resource?

Then it is either rejected or goes to an "inbox," awaiting a matching alias with a real cluster name. This may manually be assigned by an operator, or perhaps defaulted based on some rules.

Is that operator the provider, or a tenant?

I think it depends who is importing the cluster. So perhaps when ACS is reporting, it can offer a tenant, if trusted. Otherwise it must go to the provider tenant.

### 

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

That covers the common fleet-wide query cases without storing full resource bodies.

Default schema categories:

- **Core types**: Pods, Deployments, StatefulSets, DaemonSets, Services, Nodes, Namespaces, PVCs
- **Extended types**: VirtualMachines, Routes, Ingresses, CRDs
- **Events**: opt-in with aggressive TTL

For full-fidelity object access, the platform uses direct API proxying or addon-specific APIs rather than the index.

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

> NOTE: This is purely an example and not at all a suggestion.

The platform exposes a fleet-wide search endpoint shaped roughly like:

```json
GET /{scope}:searchResources?filter=...
```

...where the filter is a CEL expression. Pagination, etc, follow the AIPs.

Responses include inventory identity, resource metadata, and any requested aggregation summaries. RBAC is still enforced by the platform, so users only see resources they are authorized to access.

For full resource details, the platform falls back to the Kubernetes API proxy or addon-specific APIs. The inventory/search projection is for fast fleet-wide discovery and observation queries, not full object fidelity.

## Open questions

### How strongly typed are inventoried resource schemas?

Should we accept observations as opaque, and only define indexes?

This somewhat depends on the next open question.

### Should inventoried resources materialize as resources in the normal resource hierarchy, or only through inventory search?

An API could be extended, e.g.

`workspaces/{...}/gcpClusters/{...}/namespaces/{...}/objects/{...}`

(or by type, like `namespaces/{...}/deployments)

This would result in a lot more dynamic gRPC surface area as inventoried object types grew. However it would make the API potentially more usable, faster, and natural for use cases outside of raw searching. As inventory becomes an ubiquitous backend for multicluster extensions, that may be valuable.

Note the AIPs say the correlation of names to URLs is "convention," and not a requirement. But it is nice to be able to take a resource name and mechanically turn that into a "GET".

Also note that if we treat the inventoried objects as resources, then they should get AIP names regardless.

### How this maps to server-side drift correction (if we do that)

We'd have to correlate when a manifest in a fulfillment maps to an observed resource. Then, we'd have to define equivalence between observations and the fulfillment's manifest.

Manifests can have keys, which are unique within the scope of its target.

A target may map to a resource.

That doesn't necessarily make the manifests direct child resources of the target resource.

The saving grace here is that the thing handling fulfillments is also the thing reporting about resources, in the common case. It just needs to tell the platform that they are related so it can compare and reconcile, if needed.