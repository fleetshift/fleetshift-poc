# Resource Indexing

## What this doc covers

The fleet-wide inventory and observed-state indexing model:

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

Read this when you need the model for fleet-wide search, drift detection, target observation, or how observed state becomes queryable platform inventory.

## What is intentionally elsewhere

- The index channel itself: [fleetlet_and_transport.md](fleetlet_and_transport.md)
- Core delivery and target contracts: [core_model.md](core_model.md)
- Cross-instance federation on top of search: [platform_hierarchy.md](platform_hierarchy.md)
- Managed-resource projection details and fuller condition-history commentary: [../managed_resources.md](../managed_resources.md)

## Related docs

- [../architecture.md](../architecture.md)
- [orchestration.md](orchestration.md)
- [../managed_resources.md](../managed_resources.md)

## Overview

The platform continuously projects observed state into a fleet-wide inventory and search system. Managed targets are the most common source of observations, but the model is broader than target-local search. Inventory can also represent managed resources, discovered resources, sub-resources, and side-effect resources associated with deliveries.

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

The shared inventory model is intentionally small:

- **Identity**: resource type, name, and source association
- **State**: opaque, addon-defined runtime or observed properties
- **Conditions**: structured, platform-queryable health or lifecycle signals

This gives the platform a uniform query surface without requiring the platform to understand every domain-specific state payload.

Condition transitions can also be retained historically as condition events. This document focuses on the current queryable projection and search surface; the fuller managed-resource discussion of condition-event history lives in [../managed_resources.md](../managed_resources.md).

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

## Relationship to fulfillment intent

The platform knows what it intended through fulfillments and their delivery records. Inventory knows what is actually observed, including resources that may not map 1:1 to delivered manifests. Joining those two views enables:

- intent-aware search (which fulfillment delivered what to where)
- drift detection between delivered manifests and observed state
- richer status views for user-facing concepts (deployments, managed resources)
- impact analysis for placement changes

This is one of the reasons indexing belongs in the core architecture rather than as an addon-only concern.

## Search API shape

> NOTE: This is purely an example and not at all a suggestion.

The platform exposes a fleet-wide search endpoint shaped roughly like:

```json
POST /search
{
  "resourceTypes": ["apps/v1/Deployment", "kubevirt.io/v1/VirtualMachine"],
  "targets": ["target-a", "target-b"],
  "namespaces": ["production"],
  "labelSelector": "app=frontend",
  "fieldSelector": "status.phase=Running",
  "query": "frontend",
  "aggregations": ["countByTarget", "countByStatus"],
  "limit": 100,
  "offset": 0
}
```

Responses include inventory identity, resource metadata, and any requested aggregation summaries. Workspace scoping is still enforced by the platform, so users only see resources they are authorized to access.

For full resource details, the platform falls back to the Kubernetes API proxy or addon-specific APIs. The inventory/search projection is for fast fleet-wide discovery and observed-state queries, not full object fidelity.