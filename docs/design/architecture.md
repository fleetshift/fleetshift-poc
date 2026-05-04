# Management Plane Architecture

This document is the entry point for the architecture. It is intentionally short: it gives the system's core mental model, names the major subsystems, and points to the deeper design documents that describe each area in detail.

## What this doc covers

- What the management plane is and is not
- The core deployment abstraction
- The delivery-authorization model
- The major moving parts and how they relate
- Where to read next for detailed design work

## System model

The management plane is a URL-addressable service, not a cluster. It provides:

- the orchestration pipeline
- the fleetlet, a transport-agnostic, channel-based message broker
- the platform APIs and contracts used by addons, targets, and users

Zero infrastructure coupling remains a core property: no addon or fleetlet inherently depends on Kubernetes or any other specific infrastructure just to integrate with the platform. An addon may be a Kubernetes operator. It may be a standalone application. Kubernetes is the primary built-in target type, but the system is target-agnostic. Any endpoint that satisfies the delivery contract is a valid target.

At the center of the model is a three-axis deployment abstraction:

```text
Deployment = ManifestStrategy × PlacementStrategy × RolloutStrategy
```

- `ManifestStrategy`: what to deploy
- `PlacementStrategy`: where it goes
- `RolloutStrategy`: how fast and in what order it changes

These are the orchestration axes. They describe how the platform computes and executes delivery. They are intentionally separate from delivery authorization.

Internally, the orchestration axes live on the **Fulfillment** kernel primitive — a separate aggregate from the user-facing Deployment. A Deployment holds a `FulfillmentID` reference; orchestration operates on Fulfillment directly. This separation enables multiple user-facing concepts (deployments, managed resources, campaigns) to drive the same orchestration pipeline. See [managed_resources.md](managed_resources.md#architectural-layering) for the layering model.

Delivery authorization is a first-class, cross-cutting concern:

```text
Delivery authorization = CredentialPresentation × Provenance
```

- `CredentialPresentation`: whose credential applies resources at the target
- `Provenance`: cryptographic proof of who authorized the operation

It governs delivery authority across targets and transports rather than orchestration behavior inside a deployment plan.

## Major moving parts

### Targets and delivery

A target is any endpoint that can:

1. Accept a declarative payload
2. Apply it
3. Continuously report health

Target types can be things like:

- `kubernetes`: manifests are applied against a Kubernetes API server
- `platform`: manifests are FleetShift API objects delivered to a child platform
- `local`: the platform itself, addressed in-process

Addons can register their own target types and delivery agents while still participating in the same orchestration pipeline.

The detailed model for targets, delivery agents, and the delivery contract lives in [docs/design/architecture/core_model.md](architecture/core_model.md).

### Orchestration spine

Every fulfillment follows the same high-level execution flow:

1. Resolve placement against the fulfillment's pool
2. Compute the delta from the previous target set
3. Plan rollout steps
4. Generate manifests per target
5. Deliver to the target through its delivery agent
6. Diff and apply
7. Evaluate rollout tasks and continue or pause

User-facing concepts (deployments, managed resources) trigger this pipeline by mutating their owned Fulfillment — advancing its generation and triggering reconciliation.

The detailed orchestration semantics, including pools, invalidation, durable execution, rollout planning, and `DeploymentGroup`, live in [docs/design/architecture/orchestration.md](architecture/orchestration.md).

### Fleetlet and transport

> NOTE: This design is unproven and may change arbitrarily!

All addon-platform communication goes through the fleetlet. Local processes connect to a local fleetlet over UDS or TCP; the fleetlet connects outward to the platform. Channels are the unit of multiplexing, routing, and access control.

This same model covers:

- control traffic such as registration, delivery, status, access, and indexing
- addon-defined channels such as API proxying, generation, and strategy-specific traffic
- proxy delivery for remote targets
- peer-mesh routing between platform replicas
- the choice between addon-managed and fleetlet-channeled data paths

The detailed design lives in [docs/design/architecture/fleetlet_and_transport.md](architecture/fleetlet_and_transport.md).

### Kernel constraints

The platform kernel must remain correct as a single pod with an embedded database. Single-pod viability is a design invariant, not a degraded mode. Multi-replica deployments with external Postgres are important for larger instances, but new kernel features should not require extra services or multiple replicas just to function.

This invariant is specified in [docs/design/architecture/core_model.md](architecture/core_model.md) and shapes the design of orchestration, routing, and indexing.

### Addons and extension points

Addons are the primary extension surface for non-trivial strategy logic, additional target types, managed-resource fulfillment, addon APIs, and UI extensions. They are separate processes not just for modularity, but also for trust and signature boundaries.

The addon contract lives in [docs/design/architecture/addon_integration.md](architecture/addon_integration.md). The full managed-resource design lives in [docs/design/managed_resources.md](managed_resources.md).

### Tenancy and permissions

The platform is natively multi-tenant. Tenants operate within a workspace hierarchy; targets belong to workspaces; users are granted access to workspaces rather than being members of them. Addons consume generic permission lookups and translate those results into domain-specific filtering.

The organizational model and permission boundary live in [docs/design/architecture/tenancy_and_permissions.md](architecture/tenancy_and_permissions.md).

### Indexing and search

The platform continuously indexes observed state from managed targets into a fleet-wide search index. The platform owns the indexing infrastructure; target types define what is indexable through schemas and agents.

The indexing design lives in [docs/design/architecture/resource_indexing.md](architecture/resource_indexing.md).

### Platform hierarchy, provisioning, and federation

Platforms can themselves be targets. This makes recursive instantiation, hierarchical rollout, cross-instance federation, provisioning-as-deployment, and bootstrap/pivot flows part of the same architecture rather than a separate subsystem.

That design lives in [docs/design/architecture/platform_hierarchy.md](architecture/platform_hierarchy.md). The provider/factory variant is explored separately in [docs/design/provider_consumer_model.md](provider_consumer_model.md).

## Reading guide

Start here when you need a fast map of the system. Then continue with the smallest document that matches your question:

- Read [docs/design/architecture/core_model.md](architecture/core_model.md) for the core vocabulary, strategy axes, target model, delivery contract, and single-pod invariant.
- Read [docs/design/architecture/orchestration.md](architecture/orchestration.md) for how fulfillments execute, re-evaluate, and roll out over time.
- Read [docs/design/architecture/fleetlet_and_transport.md](architecture/fleetlet_and_transport.md) for fleetlets, channels, proxying, routing, and data-path choices.
- Read [docs/design/architecture/tenancy_and_permissions.md](architecture/tenancy_and_permissions.md) for the provider/tenant/workspace model and the generic permission boundary.
- Read [docs/design/architecture/addon_integration.md](architecture/addon_integration.md) for capability registration, addon strategy contracts, managed-resource bridging, and UI/API extension points.
- Read [docs/design/architecture/resource_indexing.md](architecture/resource_indexing.md) for the fleet-wide indexing and search model.
- Read [docs/design/architecture/platform_hierarchy.md](architecture/platform_hierarchy.md) for recursive platforms, federation, provisioning, bootstrap, and pivot.
- Read [docs/design/architecture/open_questions.md](architecture/open_questions.md) for unresolved design areas that still need dedicated decisions.

## Related design documents

- [docs/design/authentication.md](authentication.md): full delivery-authorization model, including credential presentation, provenance, trust anchors, and `PausedAuth`
- [docs/design/managed_resources.md](managed_resources.md): consumer-facing managed resources and their structural relationship to fulfillments
- [docs/design/provider_consumer_model.md](provider_consumer_model.md): provider/consumer/factory topology built on top of the core architecture
- [docs/design/mcoa_migration.md](mcoa_migration.md): migration notes from the current MCOA architecture to this model
- [docs/design/security.md](security.md): redirect to the authentication design

