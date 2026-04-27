# Platform Hierarchy

## What this doc covers

How the architecture extends upward into recursive platforms and outward into provisioning and federation:

- platforms as targets
- recursive instantiation
- multi-platform placement and rollout
- lifecycle and identity boundaries
- cross-instance query federation
- infrastructure provisioning as deployment
- bootstrap and pivot

## When to read this

Read this when you need the model for parent and child platforms, regional or tenant isolation, federated queries, or bootstrap flows that create and pivot to new instances.

## What is intentionally elsewhere

- Core target and delivery vocabulary: [core_model.md](core_model.md)
- Orchestration execution details: [orchestration.md](orchestration.md)
- Fleetlet proxy transport and platform routing: [fleetlet_and_transport.md](fleetlet_and_transport.md)
- Provider/factory topology proposals layered on top of this model: [../provider_consumer_model.md](../provider_consumer_model.md)

## Related docs

- [../architecture.md](../architecture.md)
- [../managed_resources.md](../managed_resources.md)

## Platforms as targets

Recursive instantiation is a direct consequence of the target model. A parent platform can register child platform instances as targets of type `platform` and deliver to them using the same deployment model used for Kubernetes targets.

```text
MSP Platform
  -> deploys "Acme" platform instance
     -> which may manage targets directly
     -> or deploy further regional platform instances
```

A child instance is operationally the same kind of system as the parent:

- same binary
- same APIs
- same addon contracts
- its own target fleet
- its own state and health

This makes recursion part of the main architecture rather than a special-purpose federation subsystem.

## Multi-platform placement and rollout

Placement can resolve to multiple platform targets. A parent-level deployment can therefore fan out to many child platforms the same way it fans out to many Kubernetes targets.

Two rollout layers then apply:

- the parent rollout decides which platform instances receive the deployment and when
- each child platform runs its own internal rollout across its own targets

This gives the hierarchy a uniform execution model without inventing a second orchestration system for "federation."

## Lifecycle and identity boundaries

Child platforms are managed as normal deployments to Kubernetes targets. Upgrades, configuration changes, and health observation all use the ordinary deployment pipeline.

Each platform instance also keeps its own identity boundary. Three modes are important:

1. **Inherited**: child and parent share the same OIDC issuer
2. **Isolated**: child has its own issuer and the parent cannot authenticate to its API
3. **Federated**: child trusts its own issuer plus explicitly allowed external issuers

The parent can still manage the child's infrastructure lifecycle without requiring direct API access to the child's platform surface.

The single-pod viability invariant matters here: it keeps additional child instances cheap enough that stronger authority, blast-radius, or sovereignty boundaries are practical rather than premium.

## Provider, consumer, and factory topology

Provider and factory patterns compose with this hierarchy model but are not fully specified here. They combine:

- managed resources
- pools
- delivery authorization
- provider and tenant boundaries

The deeper exploration lives in [../provider_consumer_model.md](../provider_consumer_model.md).

## Cross-instance query federation

When multiple platform instances exist in a hierarchy, users still need aggregated views across them.

The key design principle is: **no persistent global aggregate**.

Each instance owns its own data. Cross-instance queries fan out on demand and merge results in flight rather than replicating all child state into a single global store.

### Platform-native federation

For platform-owned APIs such as search, deployment status, and target listing, the platform owns the response schema and can therefore implement federation directly:

1. fan out the query to relevant child instances
2. let each child execute it locally
3. merge results in the parent

This is the Thanos Query model applied to Kubernetes resources and deployment state. It works well for list responses, counts, sums, and deduplicated resource views.

### Addon API federation

Addon APIs are different because the platform does not own their response schema. The addon therefore owns the merge logic.

Two patterns exist.

#### Addon-provided aggregator

The addon deploys an aggregator component at the parent level. That component fans out to child-level addon backends and merges results using addon-specific logic.

```text
User query
  -> global platform
  -> addon API channel
  -> addon-owned aggregator
  -> child addon backends
  -> merged response
```

The platform helps with instance discovery; the addon owns the merge semantics.

#### Merge hints

For simple list-style endpoints, the addon can provide merge metadata that lets the platform run a generic federator:

```json
{
  "path": "/api/v1/findings",
  "merge": "concat",
  "deduplicateBy": "id",
  "sortBy": "severity"
}
```

This is suitable for simple concatenation, deduplication, and sorting. Complex analytics still require addon-owned aggregation.

### Graduated path for addon authors

Addon federation can therefore evolve in layers:

1. no federation support
2. generic federation through merge hints
3. full custom federation through an addon-owned aggregator

An addon can mix these levels across different endpoints.

## Infrastructure provisioning as deployment

Creating new targets is itself a deployment to an existing target. The platform does not hardcode a single provisioner. Provisioning is just another combination of manifest strategy, placement, rollout, and target delivery.

Examples include:

- CAPI
- RKE2-style direct provisioners
- Terraform-based flows
- cloud-provider-specific APIs

The delivery agent applies the provisioning manifests to a management target. The provisioner creates infrastructure, and newly created targets appear when their fleetlets connect to the platform.

### Target correlation

The platform correlates a provisioning deployment to the targets it creates. That lets it:

- roll up health from created targets back to the provisioning deployment
- cascade delete through deprovisioning
- show lifecycle lineage between provisioning deployments and resulting targets

## Bootstrap and pivot

The bootstrap story is intentionally ambitious: a local ephemeral distribution can create and pivot to a self-managing production platform.

### Bootstrap sequence

1. A local platform runs with a built-in Kind target
2. It deploys a provisioning addon such as CAPI
3. It provisions a real cluster and obtains kubeconfig
4. It deploys a proxy fleetlet so the new cluster can be managed before it can connect back on its own
5. It deploys infrastructure management, FleetShift, configuration, and a co-located fleetlet to the remote cluster
6. The co-located fleetlet connects to the production FleetShift
7. The local bootstrap instance is shut down

This is a pivot from bootstrap control to self-management rather than a throwaway evaluation install.

### Provisioner pluggability

The bootstrap flow is not tied to CAPI. Other provisioners can participate as long as they fit the same deployment-based model.

### Why bootstrap uses proxy delivery

Bootstrap often begins from a topology where the new remote target cannot connect directly back to the bootstrap host. Proxy delivery bridges that gap temporarily until the co-located fleetlet on the real target comes online.

The fleetlet details of that proxy path live in [fleetlet_and_transport.md](fleetlet_and_transport.md).
