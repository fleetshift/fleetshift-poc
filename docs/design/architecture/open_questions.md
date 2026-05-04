# Open Questions

## What this doc covers

Unresolved design areas grouped by topic rather than by their original position in the monolithic architecture document.

## When to read this

Read this when you are working in an area that still has intentional design uncertainty, or when you need to know whether a behavior is settled architecture or still an open trade-off.

## What is intentionally elsewhere

- Stable architecture overview: [../architecture.md](../architecture.md)
- Full authentication design: [../authentication.md](../authentication.md)
- Provider/factory proposals: [../provider_consumer_model.md](../provider_consumer_model.md)

## Related docs

- [core_model.md](core_model.md)
- [orchestration.md](orchestration.md)
- [fleetlet_and_transport.md](fleetlet_and_transport.md)
- [tenancy_and_permissions.md](tenancy_and_permissions.md)
- [addon_integration.md](addon_integration.md)
- [resource_indexing.md](resource_indexing.md)
- [platform_hierarchy.md](platform_hierarchy.md)

## Resolved decision retained for context

### Rollout strategy

This is resolved and retained here only because it previously lived in the open-questions section: rollout is a first-class third strategy axis on the fulfillment kernel primitive (user-facing deployments compose these three axes through their owned fulfillment).

```text
Fulfillment = ManifestStrategy × PlacementStrategy × RolloutStrategy
```

The built-in default is `immediate`. Non-trivial rollout remains addon-driven through the same integration model as manifest and placement strategies.

## Fleetlet and transport

### Certificate provisioning

**Question:** How should mTLS certificates be provisioned for fleetlet-to-platform communication?

**Options:** platform-managed mTLS as part of the fleetlet channel, addon-bundled certificates in generated manifests, or a dedicated `CertService`.

**Partial progress:** [authentication.md](../authentication.md) already covers user and addon key distribution, including key binding bundles, external key sources, and addon-oriented approaches such as SPIFFE/SPIRE and cert-manager. Fleetlet-to-platform mTLS provisioning remains open as a separate concern.

### Proxy delivery mechanics

**Question:** How should kubeconfig lifecycle work for proxy fleetlets?

**Context:** A proxy fleetlet uses a kubeconfig to reach a remote cluster. The main sub-questions are:

- who rotates the kubeconfig when credentials expire
- how handoff works from a proxy fleetlet to a co-located fleetlet on the target
- whether the 1:N fleetlet model could eventually absorb proxy delivery, or whether one proxy fleetlet per target should remain the default

### Target capability negotiation

**Question:** Should targets advertise delivery capabilities at registration?

**Context:** When a fleetlet registers a target, it always declares the target type. But capabilities may vary within a type: one Kubernetes target may support CRDs that another does not, or an addon target may accept some manifest schemas but not others. The question is whether registration should include a capabilities declaration for placement filtering and manifest validation, or whether runtime rejection is enough.

**Leaning:** Start with runtime rejection because it is simpler. Add opt-in capability advertisement only if placement strategies need to filter by capability.

## Orchestration and rollout

### Deployment defaults for registered capabilities

**Question:** When an addon registers a capability but no deployment exists yet, should the platform auto-create a default deployment (and thus a fulfillment) or stay fully explicit?

**Context:** Some existing systems, such as OCM's `global` Placement, default to deploying everywhere. The explicit-opt-in model, where no deployment means nothing happens, is cleaner long term. This is primarily a UX decision rather than an architectural one.

### Invalidation granularity

**Question:** Should `InvalidateManifests` support per-target invalidation, or is whole-capability invalidation plus diffing sufficient?

**Trade-off:** Whole-capability invalidation is simpler for addon authors. Per-target invalidation, such as when only one target's secret rotated, is more efficient at scale. A middle ground is to keep invalidation broad but always diff generated output against the previous version and skip delivery when unchanged, making whole-capability invalidation efficient in practice.

**Context (Fulfillment split):** Invalidation now bumps the manifest strategy version on each affected fulfillment, advancing its generation. The orchestration pipeline diffs after re-generation, so unchanged targets are never re-delivered regardless of invalidation scope.

### `ManifestGenerator` error handling

**Question:** If generation fails for one target, should the platform retry that target, skip it, or fail the whole invalidation batch?

**Leaning:** Retry the failing target with exponential backoff and surface failures through status. The remaining design work is around retry budget, backoff ceiling, and whether persistent failure should alert the addon.

### Rollback pacing

**Question:** Should rollback use the fulfillment's rollout strategy or bypass it with immediate rollback to all targets?

**Context:** KubeFleet has no built-in rollback; operators manually stop the update run and fix the CRP. FleetShift's `Rollback` endpoint is an explicit addition. The trade-off is safety versus speed: staged rollback is safer if the rollback itself is risky, but rollback also exists to recover quickly. The current leaning is immediate rollback by default with an optional `"paced": true` flag that reuses the fulfillment's rollout strategy.

### Delivery-agent rollback mechanics

**Question:** Does the delivery contract need a target-side rollback concept, or is re-delivering the previous manifest set sufficient for all target types?

**Context:** The existing rollback discussion focuses on orchestration: which targets should roll back and how fast. It does not fully answer what happens on a single target when the platform says "roll back." The simplest model is declarative re-delivery of the previous manifest set. For Kubernetes targets using Server-Side Apply with pruning, that is probably sufficient: the old state converges again, and resources unique to the newer manifest set are pruned away.

The open question is whether addon-defined targets with external side effects, such as provisioning, certificate issuance, or SaaS registration, need something richer so the delivery agent can capture what it did during apply and later reverse it with the platform's help.

**One possible shape, rollback ticket:** The delivery acknowledgment could optionally include an opaque agent-produced token, a rollback ticket, that the platform stores alongside the delivery record. On rollback, the platform passes the ticket back to the delivery agent, which interprets it and performs the reversal. The platform never inspects the ticket; the agent owns its semantics.

Sub-questions if that direction is worthwhile:

- **Validation and rejection:** Can the agent refuse a rollback because the target drifted, reversal is unsafe, or the target type does not support that operation? If so, does the platform surface the rejection, fall back to re-delivery, or fail the rollback step?
- **Ticket lifecycle:** Does every delivery produce a new ticket and invalidate the old one? Can intermediate deliveries make older tickets stale? Is there a stack of rollback points or only the most recent one?
- **Whether this belongs in the delivery contract at all:** The platform always knows the previous manifest set and can re-deliver it. The remaining question is whether realistic target types exist where that knowledge is insufficient and agent cooperation is required to reverse delivery safely.

### Task failure contract

**Question:** When a rollout strategy task fails, should the platform pause, roll back automatically, or let the strategy specify failure policy per task?

**Leaning:** Let the strategy specify the failure policy per task, such as pause or rollback, and have the platform enforce it. That keeps failure semantics addon-driven while preserving a generic platform contract.

### Delivery status signals for rollout strategies

**Question:** What delivery status signals should rollout strategies be able to gate on beyond baseline delivery availability?

**Context:** `DeliveryStatusService.Status` already reports `Applied` and `Available` conditions based on the fleetlet's local observation of manifest application. That covers cases like "the Deployment exists and has ready pods" but not broader notions of application correctness. Richer signals, such as addon-reported health, HTTP probe results, or metric thresholds, would widen what rollout strategies can gate on. The current leaning is to keep `Available = True` as the baseline while allowing addon-reported signals for strategies that need them.

### In-flight rollout collision

**Question:** What happens when manifest invalidation fires while a rollout is already in progress?

**Context:** The architecture already says that if the user changes the rollout strategy spec, the change governs the next rollout rather than retroactively changing the one in flight. Manifest invalidation during an active rollout is harder: does the platform supersede the current rollout and restart, push new manifests only to remaining steps, or do something else entirely?

**Partial resolution:** The fulfillment's generation-based convergence provides a partial answer. Any mutation (including invalidation) bumps the fulfillment's generation. The orchestration workflow checks generation between rollout steps: if the generation advanced and the `VersionConflictPolicy` is `restart`, the current reconciliation completes early and a new pass starts from scratch with the latest state. The remaining design work is around non-restart policies and whether partial step completion should be preserved.

### Generation failures inside rollout batches

**Question:** How should per-target generation failures propagate through a rollout batch, especially in relation to task evaluation and batch progression?

**Context:** `Generate` runs per target inside the batch loop. If generation fails for a target, that target never enters the delivery pipeline. The missing design work is whether the platform should skip that target and continue the batch, block the batch until retry succeeds, or fail the batch entirely, and how any of those choices interact with rollout-strategy task evaluation.

**Current behavior:** The orchestration workflow currently treats any generation failure as a terminal error for the fulfillment, transitioning it to `Failed` state. This is conservative but may be too aggressive for multi-target rollouts where one target has a transient issue.

### `DeploymentGroup` controller design

**Question:** How should the `DeploymentGroup` controller manage child deployment lifecycle, sequencing, and partial failure?

**Context:** The controller creates child deployments from the group spec and manages their choreography. The main sub-questions are whether it should be a durable workflow or a reconciliation loop, how it handles partial failure, how it handles spec changes, and what state machine it exposes for group status.

## Indexing and federation

### Resource index design

**Question:** How should the index schema be managed?

**Context:** The indexer agent's configuration determines what is searchable. One option is for an administrator to define a global schema. Another is to let workspace admins extend the schema for their own workspaces, for example to include custom CRDs. A third is to make the schema itself a declarative platform resource whose changes trigger indexer-agent re-delivery. The current leaning is toward a global schema with workspace-level extension for CRD-style cases.

**Sub-questions:**

1. **Event indexing policy:** Events are high churn and dominate index volume. Should they be opt-in with a short TTL, or indexed by default with aggressive cleanup? The indexer agent could also filter events, such as taking only `Warning` events, to reduce volume.
2. **Staleness and disconnection:** When a fleetlet disconnects, index entries for that target become stale. Options include marking them stale after a timeout, automatically tombstoning them after a TTL, or leaving them untouched until resync. The current leaning is to mark them stale and surface that state in results.
3. **Index storage:** The single-pod invariant requires embedded SQLite to remain a first-class option. Larger instances still want Postgres, likely with JSONB and GIN-style indexing. The storage interface needs to abstract both cleanly so SQLite remains viable for small and medium deployments while Postgres handles large fleets.
4. **Deployment correlation:** The platform can correlate observed resources with its own deployment intent. One option is for the indexer or delivery path to stamp resources with deployment metadata. Another is to infer correlation from owner references or manifest hashes. The current leaning is explicit stamping because the delivery path already controls the manifests.

### Query federation mechanics

**Question:** What is the right format for addon merge hints, and how expressive should the generic federator be?

**Context:** The generic federator handles simple list-style APIs with concatenate, deduplicate, and re-sort behavior. That covers many addon endpoints, but the boundary between generic merge and "needs a custom aggregator" is still fuzzy.

1. **Merge hint expressiveness:** Should merge hints support simple numeric combiners such as sum or count, or only list operations? The current leaning is to support simple combiners and leave anything more complex to custom aggregators.
2. **Pagination semantics:** Federated pagination across many child instances is a distributed pagination problem. Cursor-based pagination likely scales better than offset-based pagination, but the platform has not yet decided whether to require it for federable endpoints.
3. **Partial failure handling:** If only some child instances respond, should the federator return degraded partial results or fail the whole query? The current leaning is to return partial results with explicit availability or staleness metadata per instance.

### Instance discovery for addon aggregators

**Question:** How should addon-provided aggregators discover peer instances?

**Context:** An aggregator deployed at a parent platform needs the endpoints of child instances that host the same capability. The main options are a discovery API, injected configuration through manifest generation, or fleetlet-pushed discovery updates. Injected configuration is simple but requires re-delivery when instance membership changes; a pushed channel is more reactive but adds another channel type. The current leaning is a discovery API as the primary mechanism, with injected configuration as a convenience for static cases.

## Tenancy and permissions

### Authorization model

The unified grant and inheritance model remains a separate design area, potentially with SpiceDB- or Zanzibar-style relationships behind the generic `LookupPermissions` API.

### Tenancy model and isolation primitives

The exact provider-versus-tenant relationship and the non-authorization isolation primitives between tenants remain unresolved.

### Permission schema language

How should addons declare their permission schema to the platform: SpiceDB ZED, CEL, or a custom DSL?

## Platform hierarchy, provisioning, and bootstrap

### Cycle detection in platform targets

**Question:** How should the platform prevent cycles in the platform-target graph?

**Context:** If platform A targets platform B and platform B targets platform A, a deployment could loop indefinitely. The platform therefore needs to detect and reject cycles in the transitive target set. The main options are maintaining a global graph and validating on registration, detecting cycles lazily during placement resolution, or limiting recursion depth. The current leaning is a combination of global validation plus a depth limit.

### Infrastructure provisioning correlation

**Question:** How should the platform correlate a provisioning deployment to the targets it creates?

**Context:** When a provisioner such as CAPI creates a cluster and that cluster's fleetlet later connects, the platform needs to know which provisioning deployment created it. Candidate approaches include labeling the resulting cluster with a deployment ID that the fleetlet reports at registration, matching through provisioner-specific metadata, or having the provisioning addon call an explicit correlation API. The current leaning is label-based correlation because it fits best when the delivery path controls the labels.

### Bootstrap pivot mechanics

**Question:** How should infrastructure state transfer, such as `clusterctl move`, work within the bootstrap sequence?

**Context:** During bootstrap, infrastructure state may need to move from the local bootstrap environment into the real cluster. The open questions are whether `clusterctl move` should run as a subprocess or be reimplemented with client libraries, what state beyond CAPI objects needs to transfer, whether ordinary `DeploymentGroup` sequencing is enough, and how recovery should work if the move fails mid-pivot.

## Addon integration and UI

### UI plugin mechanism

**Question:** How are UI plugins registered, loaded, and isolated?

**Context:** The architecture already establishes that manifest strategies and target types may provide UI plugins. The remaining design work is how plugins register with the platform, how they load at runtime, how they are isolated so a bad plugin does not destabilize the shell, and what contract exists between the shell and a plugin for props, events, and lifecycle. OpenShift dynamic console plugins and similar systems remain useful prior art.

This remains a separate SDK and runtime design.
