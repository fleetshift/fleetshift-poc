# Target delivery contract

A fleetshift *Target* is a logical "location" that can fulfill manifest delivery, report inventory, or both. The output of a placement decision is targets. A rollout is a (potentially complex) sequencing of targets. Targets are supported by addons. An addon can support many targets. The properties and scope of a target are arbitrary and addon-defined. With the properties of a target alone, an addon SHOULD (for efficiency; it is not a must) be able to query what resources it is managing under the scope of that target (ignoring whether it has the authority on its own to run that query).

Examples:

- A managed cluster
- A VM
- An Ansible instance
- A GCP project

Targets have to handle two main things:

1. Fulfillment (eventually converging on latest fulfillment intent)
2. Reporting back (status, inventory)

> Note: See [./managed_resources.md](../managed_resources.md)  description of condition events and inventory

This contract is intentionally Kubernetes-like. It is also intentionally not 1-1 defined and coupled to the kubernetes API. A kubernetes cluster is, and should be, a natural target. However, to the extent that other targets may fit that shape, they can also be targets, without forcing them to be fronted by a Kubernetes resource and controller (and thus API server semantics like the security and tenancy model). 

## Fulfillment

There are two triggers for fulfillment reconciliation. For each, we have to look at how they **guarantee** eventual consistency:

1. Changes platform-side (new, altered, or removed fulfillment)
2. Changes target-side (aka "drift")

Detecting drift can have some overlap with reporting inventory. They both involve scanning resources.

The targets interact with the platform through two directional interfaces:

- **Platform → Addon (DeliveryService / DeliveryAgent):** The platform pushes delivery instructions — `Deliver` to apply manifests and `Remove` to tear them down. These are dispatched by the orchestration workflow through a routing service that delegates to the appropriate addon agent.
- **Addon → Platform (DeliveryReporter):** The addon pushes delivery lifecycle updates back to the platform: `ReportEvent` for non-terminal progress events, `ReportResult` for terminal outcomes, and `ListActiveDeliveries` to recover in-progress work after a restart.

In-process addons receive the application-layer `DeliveryReportService` directly as their `DeliveryReporter`. Remote addons (via fleetlet) will receive a gRPC client stub implementing the same interface, backed by a bidirectional stream. This layering decouples addon code from platform internals: the addon interacts with client-style interfaces regardless of transport.

### Guarantees

Fulfillments are guaranteed by virtue of durable workflows, idempotent delivery, and acknowledgements. Even if a delivery target restarts, it will keep getting fulfillment deliveries until it acks them. "Ack" must mean enough work is done that it can guarantee the work can finish with the information and APIs it has available.

This is in contrast to a kube controller, like in OCM, which cannot guarantee reliably deliver of removal events from the hub. When its watch is not connected, it may miss them. Thus OCM must sync both ways: from work it knows it has locally, and from work it knows the hub has. In fleetshift this is unnecessary because the server ensures any changes are delivered reliably. So the targets never miss work add or removal.

#### Base protocol for reliable fulfillment

When an addon starts up, it should:

1. Connect to the fleetshift (in process or through a to-be-built fleetlet)
2. Wait, asynchronously, for delivery requests and ONLY "ack" once enough there is just enough state, and no more, to guarantee progress (see next steps). The quicker the ack the more responsive progression will be. Acks should generally be quick (i.e. within a typical response time of a single synchronous RPC).
3. Ask for what fulfillments are in progress ("deliveries" not yet terminal)
  - The platform returns `ActiveDelivery` values: the delivery record enriched with full `TargetInfo`, caller `DeliveryAuth`, and (when signed) a re-assembled `Attestation`. This gives the addon everything `Deliver` would have provided.
  - Stale deliveries — where the fulfillment's generation has advanced past the delivery's — are excluded from the response. Their auth and attestation cannot be correctly reconstructed. The orchestration will re-dispatch with the current generation.
  - Due to the platform's guarantees, an addon will never receive a follow-up delivery for a fulfillment that is still in progress. So races between 3 and 2 should never happen. However, a `Pending` delivery may appear in both the list response (step 3) and arrive via `Deliver` (step 2) if the addon starts while a `DeliverToTarget` activity is in flight. Addons must deduplicate by `DeliveryID` across both paths.

> NOTE: Somewhere the addon needs to discover new targets that come over time. Does this just come as part of deliveries from step 2?

#### Generation ordering and stale delivery prevention

Generation is a **first-class delivery protocol field** — every `DeliveryRequest` carries it unconditionally, independent of attestation. This is the single source of truth for fencing: the addon uses this field to detect stale deliveries regardless of whether attestation is present.

When attestation *is* present, the platform's verification step asserts that the delivery-level generation matches the signed `expected_generation` in the attestation. This is a consistency gate: attestation does not introduce a second generation value; it validates the one carried by the protocol. The addon never chooses between two values — attestation either passes or rejects the whole delivery.

The addon must never apply a delivery whose generation is older than one it has already acked. This is a safety property, not just a consistency mechanism: a stale delivery can visibly regress a customer's environment (e.g. reverting a cluster upgrade).

The platform enforces a concurrency limit of one in-flight delivery per fulfillment. Under normal operation, the addon never sees two deliveries for the same fulfillment at once. However, there is an edge case: a platform process may lose its orchestration lock (crash, partition, workflow task timeout) without knowing it. A second process acquires the lock and dispatches a newer generation. The stale process's delivery may still arrive at the addon after the newer one.

This decomposes into two independent subproblems:

1. **Platform-side stale delivery.** The addon receives a delivery with an older generation than one it has already processed for the same fulfillment. Solutions:
  - **Resource metadata.** Store the generation as target-side metadata (Kubernetes annotation, cloud resource tag, etc.). Before applying, check the stored generation; skip if the delivery's generation is not newer.
  - **Platform-provided journal.** If the addon uses the platform journal (see [Journaling](#journaling) below), the journal write enforces generation ordering: the platform rejects a journal entry for an older generation when a newer one is already recorded. The journal becomes the generation fence — the addon writes before acting, and the write itself is the high-water mark check. The platform also discards stale acks (the workflow eventually realizes its lock is lost.)
2. **Addon-side concurrency.** The addon itself may have concurrent processes (e.g. one still alive when another connects), causing read-modify-write races against the target. Solutions:
  - **At-most-one leaseholder per target.** A Kubernetes `Lease` or equivalent ensures only one active process per target. This is the same pattern Kubernetes controllers use via leader election.
  - **Optimistic concurrency.** Use the target's native concurrency control (Kubernetes `resourceVersion`, Azure ARM etags) to detect and retry conflicting writes.

> NOTE: "The workflow eventually realizes its lock is lost" – we should be careful that within an activity, these races are detected. For example, we don't want an old ack to overwrite a more recent one.

These compose naturally: pick one approach from each. In practice, Kubernetes targets often merge both into a single read-check-write: read the resource (getting `resourceVersion`), check the generation annotation, and apply with the `resourceVersion` for atomic compare-and-swap. For targets without native optimistic concurrency (e.g. AWS EKS), at-most-one process per target is the practical choice.

This is not more complex than what a Kubernetes controller already handles. A standard spoke controller must manage informer caches, leader election, periodic requeues, orphan detection, and `resourceVersion`-based conflict resolution. The FleetShift addon skips most of that (no informers, no requeue timers, no orphan detection) and adds one integer comparison for generation ordering.

#### Delivery authorization

Delivery authorization and attestation verification apply as defined in [core_model.md](core_model.md) and [authentication.md](../authentication.md). The delivery carries auth credentials and an optional attestation. The addon verifies attestation before applying; if verification fails, it reports an auth failure rather than acking. The platform transitions the fulfillment to `PausedAuth` until an authorized user resumes it.

> NOTE: We may be able to offload attestation to the common fleetlet. We should try to. At the very least, it would have to be reusable via libraries.

#### Idempotent removal

`Remove` must be idempotent. If a removal partially completes (3 of 5 resources deleted) and the addon crashes before acking, the platform retries the full removal. (This assumes the target only acks after doing the deleting work, which it doesn't have to.) The addon must handle "delete something already gone" as a no-op. For Kubernetes targets, delete of a missing resource returns 404 which is treated as success. For cloud provider APIs, the same pattern applies (delete of a nonexistent resource returns success or a "not found" error that is safe to ignore).

#### Journaling

A goal of this delivery protocol is to minimize the amount of state that an addon needs to keep track of to participate as a high quality target. However, fulfillment guarantees MAY still require a journal of some kind (like AppliedMW in OCM) to inventory what is happening in the target in more detail as a result of a delivery. Fleetshift could potentially provide APIs to keep track of such a journal as a service to addon developers.

There are two cases where this may be needed:

1. Non self identifying resources
2. Spec shrink

This is the key question:

> after a restart, can the target take the retried delivery alone and deterministically finish convergence?

The platform can handle guaranteed delivery. But the target needs to know what to do with that deterministically for idempotency and correctness (as required by step 2 above). If you create a bunch of things, how do you uniquely identify those things for deletion or reconciliation later?

That is, journaling may be necessary to make progress at all, regardless of whether it is a removal.

Sometimes identifying a resource is easy–it's "self-identifying." A kube manifest itself is self-identifying. In other cases, the identity of resources resulting may be sourced from input not contained within that manifest itself. This could be other state known only to the addon at runtime, obtained via I/O like syscalls or service calls or otherwise. In this case, identifiers must be stored so they can be retrieved later.

Spec shrink refers to the need to act on now-missing information. There are two kinds of spec shrink: 

1. Within a manifest, there is an implicit removal requested. The absence of information in a manifest implies something should be removed. Acting on this requires seeing what was there before. The target must be able to get this from somewhere.
2. Within a fulfillment with more than one manifest, one or more manifests are removed. This is again an absence of information that implies something to be removed. Acting on this requires knowing what was there before.

It is possible the platform could eliminate the need for addon journaling due to spec shrink if delivery includes the previously ack'd manifests for that target. Then, a target can detect the absence in both cases.

> TODO: Revisit the journal contract – something has to write to it BEFORE doing work as if it waits until after, it can crash before writing. It is essentially just saga / durable worfklow / event sourcing over again.

**IMPORTANT:** If we do provide a Journal service, solving these use cases requires addon-signed entries. Likewise, including previously ack'd manifests can only be trusted if we provide the attestation chain for those manifests. Alternatively, perhaps we could consider giving the fleetlet its own storage.

## Reporting: drift, state, and conditions

After fulfilling, there is work to do to consider what's happened since. This is looking at current state to:

1. Report back mostly-static results (e.g. computed IDs or URLs)
2. Report back more dynamic conditions or events
3. Detect drift from intent

None of these are strictly required. They are just very useful. Still, trivially simplistic addons may forego them.

Detecting drift means detecting changes for objects still managed by the platform. Detecting drift is not necessarily required, but it is a common feature of management products. If we detect drift, we have to detect changes, somehow, on any resources still managed.

Then, something has to know how to diff the state it sees against the platform intents.

For OCM, drift is simply reprocessing manifests periodically and querying against desired state. For fleetshift, we could consider the same. Give addons a way to query what should be present at their targets (needed for fresh start sync). They may cache that as needed.

These should all be achievable, generally, by adding two more steps to the protocol above:

1. Ask for what active fulfillments (those not creating, deleting, or terminal) it should watch for state (drift) and conditions.
  - This can be done in one or more ways: either by asking for active fulfillments directly, or by asking for what targets the addon is currently fulfilling.
2. Watch the implied resources for changes.
  - How this is done is implementation dependent. A target may have native, efficient change detection. Or, it may just poll.
  - The scope of what is reported may vary based on configuration TBD. For example, it is unlikely we'd index all state about all resources in a managed kubernetes clusters, but some subset.

## Open questions / notes

### Delivery lifecycle state mapping

The codebase models deliveries with explicit lifecycle states: `pending`, `accepted`, `progressing`, `delivered`, `failed`, `partial`, and `auth_failed`. This document's protocol uses "ack" without mapping it to those states. The mapping between the protocol concepts (ack, auth failure, partial apply) and the delivery entity states needs to be defined. Similarly, `DeliveryResult` carries structured outputs (`ProvisionedTargets`, `ProducedSecrets`) that the protocol should account for — when and how these are reported back during the protocol flow.

### Should manifests be able to opt out of drift repair?

Drift detection is optional (an addon may skip it entirely). But if an addon does detect drift, should it always repair it? Some resources are intentionally modified after initial delivery (e.g. a resource the target customizes post-apply, or a `CreateOnly` resource that should be applied once and then left alone). OCM models this with per-manifest update strategies (`Update`, `ServerSideApply`, `CreateOnly`, `ReadOnly`). FleetShift may need a similar mechanism — either as a platform-level manifest annotation or as an addon-defined policy.

### Should the platform ever diff? What does that buy us? What does it cost us?

The platform indexes things. It need not be full resources. If there is a mapping between that observation and intent, it can diff and redeliver. Should it?

### Should the platform optionally track target-specific "cursors" to aid in addon watch protocols?

Do we give an addon a way to say, "Hi, what did I last tell you about? I'll pick it up from there."

Think: a kubernetes delivery addon wakes up, wants to use a watch, and so queries for a resource version. If one is found, it can attempt to watch from that position, rather than rescanning everything it is watching.

Technically, an addon could make an RPC to store an opaque "cursor" about some opaque "scope" within a target. Revisit this as target implementations mature.

Native watches (which might just be polling under the hood) aren't always strictly better than polling. It depends on the cardinality of resources and staleness requirements.