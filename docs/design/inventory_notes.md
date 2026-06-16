# Inventory Design Notes

> [!NOTE]
> The canonical design for resource identity, inventory resource materialization, and the search API now lives in:
> - [architecture/resource_identity_and_api.md](architecture/resource_identity_and_api.md) — two-layer API model, resource identity, platform resource, extension representations
> - [architecture/resource_indexing.md](architecture/resource_indexing.md) — fleet-wide indexing, search API shape, scale characteristics
>
> This document retains exploratory notes and open questions not yet formalized into those documents.

---

## Resolved questions (formalized elsewhere)

The following questions from these notes have been resolved in the canonical design:

- **Resource identity across extensions**: resolved via the two-layer API model. Multiple extensions unify on the same platform resource identity through shared relative names or alias correlation. See [architecture/resource_identity_and_api.md](architecture/resource_identity_and_api.md#resource-identity-model).
- **Should inventoried resources materialize as resources?**: yes, as extension API resources in their addon's package with a corresponding platform resource. See [architecture/resource_indexing.md](architecture/resource_indexing.md#do-inventoried-resources-materialize-as-resources-in-the-normal-resource-hierarchy).
- **API namespacing for extensions**: each addon defines services in its own proto package; no collisions. See [architecture/resource_identity_and_api.md](architecture/resource_identity_and_api.md#extension-resources-and-api-packages).
- **Labels across multiple providers**: aggregated on the platform resource with namespace prefixes. See [architecture/resource_identity_and_api.md](architecture/resource_identity_and_api.md#aggregation-semantics).

---

## Reporting

- A target often knows which platform resource identity it corresponds to.
    - Bootstrap or registration can provide the canonical identity directly (for example `//fleetshift.io/clusters/bar`) or provide aliases that correlate to that identity.
    - Workspace or tenant association is separate from the resource name itself.
- An addon reports about its own extension representation, not a shared cross-addon record.
    - Managed resources, inventoried resources, and target resources can all produce inventory projections.
    - Cross-addon aggregation happens on the linked platform resource, where labels and conditions are merged with source attribution.
- Resource identity is shared across addons when they model the same logical resource.
    - Multiple addons may legitimately report `clusters/foo` when they participate in the same identity domain.
    - Conflicts are contradictory alias claims, unauthorized identity claims, or attempts to bind one extension resource to multiple platform identities.
- The platform resource is the canonical identity layer, not just another inventoried resource.
    - Managed, inventory, and target representations attach to it.
    - Import-first then managed should work through alias correlation, though bad matches may still require admin intervention.

## Scoping and removal detection

- Reporting can be done in context of a _scope_ within a target. A target is itself a kind of scope, but arbitrary, opaque scopes within that are also possible. Reported resources are associated with a scope. This is for a few reasons:
    - We can use it in part to help addons efficiently query for what's left to report about, by allowing addons to store an opaque cursor, per target and optionally scope. A cursor can be retrieved to then pick up where the addon last left off.
    - Detecting omissions. If an addon doesn't report about a resource for a particular full sweep, is that because it was just missed, not reported yet, or because it is gone? Allow to signal completion of an entire "scope" (whether target or subset) let's the platform know it can consider untouched resources in that scope as stale. (This assumes there is some protocol for saying, "I can't pick up where I left off, so I'm going to tell you about the entirety of this scope.")

See [architecture/target_delivery_contract.md](architecture/target_delivery_contract.md) for the related cursor/resume discussion.

## Multi-provider cluster view

The high-level answer now lives in [architecture/resource_identity_and_api.md](architecture/resource_identity_and_api.md): each provider or addon owns its own extension API, while the platform resource supplies the shared identity and aggregated metadata. The remaining notes here are about read-projection ergonomics:

- how closely the cross-provider search/read model should align with ACM Search or SIG ClusterProfile
- which fields belong on the shared platform projection versus extension-specific properties and observations
- whether a higher-level profile-style cluster API is worthwhile on top of search, platform resources, and provider APIs

Parity with ACM search would be:

- name: got it
- created: got it
- label: need to figure out how to handle labels managed at the shared identity level vs provider resource level; each may have labels
- derived addon map: not sure what this is exactly but should be able to discover
- cpu, memory: multiple sources potentially – provider vs cluster agent
- kubernetesVersion: multiple sources potentially – provider vs cluster agent
- all ManagedCluster.status.conditions[*] copied as top-level properties: provider only?
- apiEndpoint
- consoleURL
- nodes

What about SIG clusterprofile? We'd like to be able to be basically compatible with that.

---

The canonical answer to "who owns what API?" is now:

- use the extension API for provider-specific full-fidelity CRUD
- use the platform resource API for shared identity and aggregated metadata
- use search for cross-provider discovery and reporting

The remaining open question is whether we also want a higher-level profile API optimized for cluster-centric reads.

