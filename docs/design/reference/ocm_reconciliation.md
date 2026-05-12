## Core model

In OCM, workload reconciliation is primarily a **spoke-side convergence loop** driven by **hub-side desired state**.

- **Desired state** lives on the hub as `ManifestWork`
- **Actual state** lives on the managed cluster as normal Kubernetes objects
- **Spoke-side bookkeeping** lives on the managed cluster as `AppliedManifestWork`
- The **hub does not directly apply manifests to the managed cluster**; the spoke `work-agent` does

The API contract says this explicitly:

```13:18:github.com/open-cluster-management-io/ocm/vendor/open-cluster-management.io/api/work/v1/types.go
// ManifestWork represents a manifests workload that hub wants to deploy on the managed cluster.
// A manifest workload is defined as a set of Kubernetes resources.
// ManifestWork must be created in the cluster namespace on the hub, so that agent on the
// corresponding managed cluster can access this resource and deploy on the managed
// cluster.
```

So the high-level split is:

- **Hub**: store desired work, place/fan it out, accept status
- **Spoke**: apply, observe, and report

## Hub-side responsibilities

The hub-side `work-manager` does **not** reconcile the delivered `ConfigMap` or `Deployment` on the managed cluster.

Its main work is in `pkg/work/hub/manager.go`:

- build a hub-side `ManifestWork` client/informer
- run `ManifestWorkReplicaSet` reconciliation
- optionally garbage-collect completed `ManifestWork`

That means hub-side work reconciliation is mostly:

- `ManifestWorkReplicaSet` -> per-cluster `ManifestWork`
- cleanup of hub-side work objects

The actual leaf-object convergence loop lives on the spoke.

## Spoke runtime

The spoke `work-agent` is started by `pkg/cmd/spoke/work.go`, which calls `RunWorkloadAgent()` in `pkg/work/spoke/spokeagent.go`.

At startup it builds:

- **spoke clients** against the managed cluster API
- a **hub `ManifestWork` client/informer** against the hub source of truth
- local controllers for apply, status, finalization, eviction

By default the hub source is the hub Kubernetes API via a mounted kubeconfig:

```220:269:github.com/open-cluster-management-io/ocm/pkg/work/spoke/spokeagent.go
if o.workOptions.WorkloadSourceDriver == "kube" {
	config, err := clientcmd.BuildConfigFromFlags("", o.workOptions.WorkloadSourceConfig)
	// ...
	workClient, err = workclientset.NewForConfig(config)
	// ...
} else {
	// For cloudevents drivers, we build ManifestWork client that implements the
	// ManifestWorkInterface and ManifestWork informer based on different driver configuration.
	// ...
}
factory := workinformers.NewSharedInformerFactoryWithOptions(
	workClient,
	24*time.Hour,
	workinformers.WithNamespace(o.agentOptions.SpokeClusterName),
)
informer := factory.Work().V1().ManifestWorks()
```

Important details:

- the informer is scoped to `WithNamespace(o.agentOptions.SpokeClusterName)`
- so each spoke only watches **its own namespace on the hub**
- controllers mostly read desired state from the informer/lister cache, not repeated live GETs

The main controllers started by the work agent are:

- `ManifestWorkController`
- `AvailableStatusController`
- `AddFinalizerController`
- `ManifestWorkFinalizeController`
- `AppliedManifestWorkFinalizeController`
- `UnManagedAppliedWorkController`

## The main spoke-side reconciliation loop

The core apply loop is `ManifestWorkController` in `pkg/work/spoke/controllers/manifestcontroller/manifestwork_controller.go`.

Its own comment is the best summary:

```125:128:github.com/open-cluster-management-io/ocm/pkg/work/spoke/controllers/manifestcontroller/manifestwork_controller.go
// sync is the main reconcile loop for manifest work. It is triggered in two scenarios
// 1. ManifestWork API changes
// 2. Resources defined in manifest changed on spoke
```

Inside that loop:

1. it reads the current `ManifestWork` from the **hub lister cache**
2. it ensures there is a corresponding local `AppliedManifestWork`
3. it runs reconcilers that:
   - apply manifests locally
   - update `AppliedManifestWork.Status.AppliedResources`
4. it patches `ManifestWork.Status` back to the hub
5. it requeues itself for another pass later

So the spoke is constantly doing:

- **desired** = cached hub `ManifestWork`
- **actual** = live/local resource state
- **action** = apply/patch/delete/record status

## Trigger path 1: immediate reconcile on hub `ManifestWork` changes

The spoke adds event handlers to the **hub `ManifestWork` informer**. New works or spec/label changes get queued immediately:

```236:280:github.com/open-cluster-management-io/ocm/pkg/work/spoke/controllers/manifestcontroller/manifestwork_controller.go
func onAddFunc(queue workqueue.TypedRateLimitingInterface[string]) func(obj interface{}) {
	return func(obj interface{}) {
		// ...
		if commonhelper.HasFinalizer(accessor.GetFinalizers(), workapiv1.ManifestWorkFinalizer) {
			queue.Add(accessor.GetName())
		}
	}
}

func onUpdateFunc(queue workqueue.TypedRateLimitingInterface[string]) func(oldObj, newObj interface{}) {
	return func(oldObj, newObj interface{}) {
		// enqueue when finalizer is added, spec or label is changed.
		// ...
		if !apiequality.Semantic.DeepEqual(newWork.Spec, oldWork.Spec) ||
			!apiequality.Semantic.DeepEqual(newWork.Labels, oldWork.Labels) {
			queue.Forget(newWork.GetName())
			queue.Add(newWork.GetName())
		}
	}
}
```

This is the “hub desired state changed, reconcile now” path.

## Trigger path 2: local requeues when watched local resources change

OCM also has a spoke-local path for status/feedback.

`AvailableStatusController` reads local objects and, for manifests configured with `FeedbackScrapeType == Watch`, registers local informers on those actual resources:

```122:139:github.com/open-cluster-management-io/ocm/pkg/work/spoke/controllers/statuscontroller/availablestatus_controller.go
for index, manifest := range manifestWork.Status.ResourceStatus.Manifests {
	obj, availableStatusCondition, err := c.objectReader.Get(ctx, manifest.ResourceMeta)
	// ...
	option := helper.FindManifestConfiguration(manifest.ResourceMeta, manifestWork.Spec.ManifestConfigs)
	if option != nil && option.FeedbackScrapeType == workapiv1.FeedbackWatchType {
		if err := c.objectReader.RegisterInformer(ctx, manifestWork.Name, manifest.ResourceMeta, controllerContext.Queue()); err != nil {
			// ...
		}
	} else {
		if err := c.objectReader.UnRegisterInformer(manifestWork.Name, manifest.ResourceMeta); err != nil {
			// ...
		}
	}
}
```

Those local resource events are then mapped back to the owning `ManifestWork` and requeued:

```200:207:github.com/open-cluster-management-io/ocm/pkg/work/spoke/objectreader/reader.go
registration, err := informer.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
	AddFunc: o.queueWorkByResourceFunc(ctx, gvr, queue),
	UpdateFunc: func(old, new interface{}) {
		o.queueWorkByResourceFunc(ctx, gvr, queue)(new)
	},
})
```

```259:279:github.com/open-cluster-management-io/ocm/pkg/work/spoke/objectreader/reader.go
func (o *objectReader) queueWorkByResourceFunc(ctx context.Context, gvr schema.GroupVersionResource, queue workqueue.TypedRateLimitingInterface[string]) func(object interface{}) {
	return func(object interface{}) {
		// ...
		objects, err := o.indexer.ByIndex(byWorkIndex, key)
		// ...
		for _, obj := range objects {
			work := obj.(*workapiv1.ManifestWork)
			queue.Add(work.Name)
		}
	}
}
```

This is the “actual local state changed, revisit the owning work” path.

One nuance: this local watch path is mainly used for **availability/status feedback**. It is not a global “watch everything always” mechanism.

## Trigger path 3: periodic requeues even without events

OCM also has timer-based safety nets.

### Main apply loop
The main `ManifestWorkController` requeues every work after each reconcile. The base interval is 4 minutes:

```37:40:github.com/open-cluster-management-io/ocm/pkg/work/spoke/controllers/manifestcontroller/manifestwork_controller.go
var (
	// ResyncInterval defines the base interval for periodic reconciliation via AddAfter.
	ResyncInterval = 4 * time.Minute
)
```

```169:202:github.com/open-cluster-management-io/ocm/pkg/work/spoke/controllers/manifestcontroller/manifestwork_controller.go
var requeueTime = wait.Jitter(ResyncInterval, 0.5)
// ...
logger.V(2).Info("Requeue manifestwork", "requeue time", requeueTime)
controllerContext.Queue().AddAfter(manifestWorkName, requeueTime)
```

This is the drift-repair safety net even if neither hub events nor local watch events fired.

### Status loop
The status controller also requeues periodically. The default status sync interval is 10 seconds:

```30:37:github.com/open-cluster-management-io/ocm/pkg/work/spoke/options.go
return &WorkloadAgentOptions{
	MaxJSONRawLength:                       1024,
	StatusSyncInterval:                     10 * time.Second,
	AppliedManifestWorkEvictionGracePeriod: 60 * time.Minute,
	WorkloadSourceDriver:                   "kube",
	WorkloadSourceConfig:                   "/spoke/hub-kubeconfig/kubeconfig",
```

And it uses that interval to requeue:

```97:104:github.com/open-cluster-management-io/ocm/pkg/work/spoke/controllers/statuscontroller/availablestatus_controller.go
err = c.syncManifestWork(ctx, controllerContext, manifestWork)
if err != nil {
	return fmt.Errorf("unable to sync manifestwork %q: %w", manifestWork.Name, err)
}

// requeue with a certain jitter
controllerContext.Queue().AddAfter(manifestWorkName, wait.Jitter(c.syncInterval, 0.9))
```

So there is a separate fast periodic loop for status/feedback.

## What “requeue” means in OCM

“Requeue” does **not** mean “update local state now.”

It means:

- put the `ManifestWork` key back on the controller queue
- when a worker picks it up, run `sync()` again
- that new pass reads the **current** desired state from the hub cache and the **current** actual state from the spoke

So it schedules another convergence pass.

## What is actually authoritative

For a delivered resource like a `ConfigMap`:

- **authoritative desired state**: `ManifestWork.Spec.Workload.Manifests` on the hub
- **authoritative actual state**: the live `ConfigMap` in the spoke cluster API
- **supporting tracking state**: `AppliedManifestWork`

`AppliedManifestWork` is not where the `ConfigMap` reconciles from. It is used for:

- naming/ownership linkage (`hubHash`, `manifestWorkName`, `agentID`)
- inventory of applied resources
- deletion/finalization/eviction

That is why editing `AppliedManifestWork` does not redefine the desired `ConfigMap`, but it can interfere with cleanup logic.

## How apply itself works

When the main reconcile runs, `manifestworkReconciler` iterates the manifests from the hub `ManifestWork`, validates executor permissions, picks an update strategy, and applies locally.

The update strategy matters for drift:

- `Update` or `ServerSideApply`: local drift is repaired on the next reconcile
- `CreateOnly`: no automatic correction after initial create
- `ReadOnly`: no corrective apply at all

So “spoke-side reconciliation” in OCM really means:

- keep an eventually consistent cache of desired work from the hub
- react to hub events
- react to selected local events
- run periodic reconciliation anyway
- use that to make the local cluster converge toward desired state over time

## What changes in alternate deployments

Two big things can vary, but the logical model stays the same:

- **transport**
  - default: hub Kubernetes API (`WorkloadSourceDriver = kube`)
  - alternate: CloudEvents-backed drivers like `grpc`, `mqtt`, `kafka`
- **process placement**
  - hosted/singleton modes move where the agent process runs

But in all of those, the controllers still see the same abstraction:

- a `ManifestWork` client/informer for desired state
- spoke clients for actual state
- the same local convergence logic

So the contract does not change even if the transport does.

## Bottom line

What we learned about OCM reconciliation is:

- it is **not hub-direct apply**
- it is **not purely polling**
- it is a **spoke-side controller system** fed by a watched/cached view of hub desired state
- it converges local resources using:
  - immediate hub-driven enqueue
  - local watch-driven enqueue for status/feedback
  - periodic requeue safety nets
- it reports status back to the hub through `ManifestWork.Status`
- it uses `AppliedManifestWork` as local inventory/ownership/cleanup state, not as desired state

If useful, I can turn this into a single end-to-end timeline for one `ConfigMap`:
hub create -> informer event -> local apply -> local drift -> local/hybrid requeue -> repair -> status patch back.