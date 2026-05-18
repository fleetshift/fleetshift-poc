## Core model

OCM workload delivery is a **spoke-side, eventually consistent controller system**.

- **Desired state** lives on the hub as `ManifestWork`
- **Actual state** lives on the managed cluster as ordinary Kubernetes objects
- **Local inventory and cleanup state** lives on the managed cluster as `AppliedManifestWork`
- The **hub does not directly apply manifests**; the spoke `work-agent` does

The important mental model is:

- `ManifestWork` is the current desired bundle for this cluster
- `AppliedManifestWork` is a durable local checkpoint of what the spoke believes it applied and may later need to clean up
- controllers reconcile by replay, not by transaction

## Hub vs spoke responsibilities

On the hub, OCM mostly stores and fans out desired work. The hub-side `work-manager` creates and manages `ManifestWork` objects, but it does not reconcile the leaf `Deployment`, `ConfigMap`, or other resource directly on the managed cluster.

On the spoke, the `work-agent`:

- watches a cluster-scoped view of hub `ManifestWork`
- applies manifests locally
- reads back local status and feedback
- tracks local applied inventory
- finalizes and evicts local work when the hub work changes or disappears

The actual leaf-object convergence loop lives on the spoke.

## Spoke runtime

`RunWorkloadAgent()` in `reference/ocm/pkg/work/spoke/spokeagent.go` builds:

- spoke clients against the managed cluster API
- a hub `ManifestWork` client/informer
- a spoke `AppliedManifestWork` client/informer
- local controllers for apply, status, finalization, and orphan cleanup

By default, the hub source is the hub Kubernetes API. The hub informer is namespace-scoped to the spoke cluster name, so each spoke only watches its own `ManifestWork` namespace on the hub.

The main controllers are:

- `AddFinalizerController`
- `ManifestWorkController`
- `AvailableStatusController`
- `ManifestWorkFinalizeController`
- `AppliedManifestWorkFinalizeController`
- `UnManagedAppliedWorkController`

These controllers mostly read from informer/lister caches, not repeated live GETs to the hub.

## Main reconciliation flow

The core apply loop is `ManifestWorkController`.

At a high level, one reconcile pass does this:

1. read the current `ManifestWork` from the hub informer cache
2. ensure the `ManifestWork` cleanup finalizer is present
3. ensure a corresponding local `AppliedManifestWork` exists
4. parse the desired manifests, derive concrete resource identities, and apply them locally
5. update `ManifestWork.Status` on the hub
6. update `AppliedManifestWork.Status.AppliedResources` on the spoke
7. requeue for another pass later

That means OCM is always reconciling:

- **desired** = cached hub `ManifestWork`
- **actual** = live local resource state
- **local inventory** = `AppliedManifestWork`

This is not a cross-object transaction. Individual API writes are durable, but the overall flow is replayed and retried until it converges.

## What triggers reconciliation

OCM uses three trigger families:

1. **Hub `ManifestWork` events**
2. **Selected local resource events**
3. **Periodic requeues**

### 1. Hub events

The main apply loop listens to the hub `ManifestWork` informer. New work, finalizer addition, spec changes, and label changes enqueue the work immediately.

This is the primary "desired state changed" trigger.

### 2. Selected local resource events

OCM also has a spoke-local feedback path. `AvailableStatusController` reads local objects and, when `FeedbackScrapeType == Watch`, registers dynamic informers for those specific resources.

Those local resource add/update events are mapped back to the owning `ManifestWork` and requeued.

Important nuance: this watch path is mainly for **availability, feedback, and condition freshness**. It is not a global "watch everything on the cluster and re-apply it" mechanism.

### 3. Periodic requeues

OCM also has timer-based safety nets:

- the main `ManifestWorkController` requeues each work on a slower interval (default base ~4 minutes)
- the `AvailableStatusController` requeues each work on a faster interval for status/feedback (default 10 seconds)

So OCM is not purely event-driven. It also periodically reruns reconciliation even if no event was observed.

## How OCM knows what to query on the spoke

For resources that are still part of the **current desired work**, OCM does not need `AppliedManifestWork` to know what to query.

When applying a manifest, OCM parses the desired object and derives a concrete `ManifestResourceMeta` using the REST mapper:

- group
- version
- kind
- resource
- namespace
- name

Those concrete resource identities are then used for:

- apply and drift repair
- availability checks
- status feedback
- condition rule evaluation

`AvailableStatusController` reads those identities from `ManifestWork.Status.ResourceStatus.Manifests[*].ResourceMeta` and asks `objectReader` to `Get` or `Watch` the corresponding local objects.

So for the **current desired set**, OCM can derive what to query from the work itself.

## Why `AppliedManifestWork` exists

`AppliedManifestWork` is not the desired state for reconciliation. It is the local inventory and cleanup checkpoint.

Its main jobs are:

- link local state to the hub work (`hubHash`, `manifestWorkName`, `agentID`)
- record which concrete resources were applied
- record enough information to safely delete them later
- carry deletion/eviction progress across retries and restarts

This matters because the **current desired spec is not enough** to answer all cleanup questions.

Examples:

- a `ManifestWork` spec shrinks, so the current desired set no longer names some previously applied resources
- a `ManifestWork` is deleted while the agent is down
- a local cleanup is in progress and some resources are already deleting or blocked on their own finalizers
- the hub work is gone, but the spoke still has local applied inventory

So the real distinction is:

- `ManifestWork` tells OCM what **should exist now**
- `AppliedManifestWork` tells OCM what it **believes it applied previously and may still need to clean up**

## How cleanup works

OCM has three important cleanup paths.

### 1. Spec shrink: resource removed from a still-existing `ManifestWork`

When the desired work still exists but now contains fewer resources, OCM compares:

- the new applied set derived from the current work
- the previous applied set recorded in `AppliedManifestWork.Status.AppliedResources`

Any previously applied resource that is no longer tracked by the current work is deleted.

This is why some local applied inventory is needed: the current `ManifestWork.Spec` no longer names the removed resources.

### 2. Hub `ManifestWork` delete: normal finalization path

`AddFinalizerController` makes sure the `ManifestWork` cleanup finalizer is present before normal apply proceeds.

When a `ManifestWork` is deleted on the hub:

1. `ManifestWorkFinalizeController` sees the deleting hub work
2. it marks the work deleting and deletes the corresponding local `AppliedManifestWork`
3. `AppliedManifestWorkFinalizeController` deletes the tracked local resources from `AppliedManifestWork.Status.AppliedResources`
4. once cleanup completes, finalizers are removed and deletion finishes

This is the normal "hub work is terminating, clean up the spoke" path.

### 3. Hub work missing: orphaned local work path

Reprocessing known hub `ManifestWork`s is not enough to detect all cleanup cases.

If the agent is down while the hub work is deleted, then on restart:

- the hub informer only sees the works that still exist
- there is no retroactive delete event for the already-missing work
- the spoke may still have a local `AppliedManifestWork`

That case is handled by `UnManagedAppliedWorkController`.

This controller watches **both**:

- hub `ManifestWork`
- local `AppliedManifestWork`

and explicitly checks:

- "I have this local applied work, does the corresponding hub `ManifestWork` still exist?"

If the hub work is missing, or the local record points at a different hub hash, the controller starts an eviction timer and eventually deletes the orphaned `AppliedManifestWork`. That in turn triggers the normal local finalizer cleanup.

This is the "check both directions" path that catches orphaned local work after missed history or restart.

## Drift detection vs cleanup inventory

It helps to separate two questions:

### How does OCM detect drift for resources that are still desired?

From the current `ManifestWork`, derived `ManifestResourceMeta`, local reads, and periodic requeues.

For the current desired set, OCM mostly does not need `AppliedManifestWork` to know what to query.

### How does OCM know what to remove when resources are no longer desired?

From `AppliedManifestWork.Status.AppliedResources`.

That inventory is what lets OCM say:

- "this used to be managed"
- "it no longer appears in the current work"
- "I still need to delete or finish deleting it"

So `AppliedManifestWork` is mainly about **cleanup and replay of previously managed state**, not about querying the current desired set.

## Watch-based feedback

`FeedbackScrapeType == Watch` is an optimization for **status and availability freshness**, not the main apply mechanism.

Without watch-based feedback, OCM already has periodic status polling/requeue.

With watch-based feedback, OCM:

- registers a dynamic informer for the specific local resource
- maps local add/update events back to the owning `ManifestWork`
- refreshes feedback and conditions sooner than the normal status interval

This is useful for:

- faster availability updates
- faster condition-rule evaluation
- quicker feedback when another controller mutates the resource's status

It is not required for correctness. It is a responsiveness improvement, and OCM caps the number of such watches and falls back to poll if the cap is exceeded.

## Failure and consistency model

OCM does **not** provide ACID atomicity across:

- creating or updating `AppliedManifestWork`
- mutating local resources
- patching `ManifestWork.Status`
- patching `AppliedManifestWork.Status`

Instead, it relies on:

- durable individual API writes
- idempotent apply and delete behavior
- finalizers as completion barriers
- `AppliedManifestWork` as a durable local checkpoint
- informer events plus periodic requeues to replay reconciliation after failures

So the guarantee is best understood as:

- **replayable, eventually consistent convergence**

not:

- **all-or-nothing transaction**

## Update strategies and repair behavior

The update strategy on a manifest affects what "drift repair" means:

- `Update` and `ServerSideApply`: local drift is corrected on later reconcile passes
- `CreateOnly`: create once, then stop trying to modify the object
- `ReadOnly`: do not apply corrective changes at all

So "reconcile" in OCM does not always mean "force the live object back to spec." It depends on the update strategy chosen for that manifest.

## Transport does not change the contract

OCM can source desired work from different transports:

- default Kubernetes API (`WorkloadSourceDriver = kube`)
- CloudEvents-backed drivers like `grpc`, `mqtt`, and `kafka`

But the controller contract stays the same:

- a `ManifestWork` client/informer for desired state
- spoke clients for actual state
- the same local convergence, feedback, and cleanup logic

So transport changes where the desired work view comes from, not the core reconciliation model.

## Adaptation takeaway

The most important lessons for adapting OCM-style behavior elsewhere are:

- current desired-state drift detection can often be driven from the current work itself
- cleanup of previously managed resources needs some durable notion of prior concrete inventory
- in OCM, `AppliedManifestWork` is that local inventory and cleanup checkpoint

Another system could replace `AppliedManifestWork` with a thinner local journal or a stronger replay contract, but it still needs to preserve the same essential information:

- what the current desired set is
- what previously applied resources may still need cleanup
- enough identity to delete them safely and resume after restart

## Bottom line

OCM workload reconciliation is best understood as:

- **spoke-side convergence** driven by a watched view of hub desired state
- **current-resource drift detection** derived from the current work
- **local cleanup and orphan handling** driven by `AppliedManifestWork`
- **eventual convergence** using informer events, periodic requeues, and replay

`AppliedManifestWork` is not the source of desired state. It is the durable local record that makes cleanup, eviction, and restart recovery work.