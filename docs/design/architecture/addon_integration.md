# Addon Integration

## What this doc covers

The platform's primary extension surface:

- why addons are separate trust and execution boundaries
- how addons extend manifest, placement, and rollout behavior
- strategy transport options
- `InvalidateManifests`
- the `ManifestGenerator` contract
- managed resources as a structural bridge
- UI and API extensibility

## When to read this

Read this when you are designing or implementing an addon, extending the strategy model, or reasoning about how addon-owned APIs and UIs plug into the platform.

## What is intentionally elsewhere

- Core strategy vocabulary and target model: [core_model.md](core_model.md)
- Orchestration semantics: [orchestration.md](orchestration.md)
- Fleetlet channels and request routing: [fleetlet_and_transport.md](fleetlet_and_transport.md)
- Full managed-resource design: [../managed_resources.md](../managed_resources.md)
- Full authentication and signing model: [../authentication.md](../authentication.md)

## Related docs

- [../architecture.md](../architecture.md)
- [tenancy_and_permissions.md](tenancy_and_permissions.md)

## Addons as trust boundaries

Addons are separate processes not just for modularity but for trust. Targets may trust addon-produced manifests or placement decisions in ways they do not trust the platform itself. This is one reason addon logic is out-of-process rather than an in-process kernel plugin.

In practice, that means:

- addons can sign outputs with their own identities
- delivery agents can verify addon-produced material independently
- the platform remains an orchestrator and courier rather than the only trusted authority

The full signing model lives in [../authentication.md](../authentication.md).

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

The intended model follows the dynamic-plugin pattern used by products such as Grafana and OpenShift: the platform provides the shell and plugin registry, while addons provide domain-specific content.

The exact loading and isolation mechanism is still open. Candidate shapes include:

- micro-frontends
- module federation
- web components
- iframe isolation

The architecture establishes the extension points. The concrete SDK and runtime mechanism remain a separate design exercise.
