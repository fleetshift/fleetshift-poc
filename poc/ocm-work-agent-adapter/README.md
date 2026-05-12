# OCM Work-Agent Adapter POC

This is a standalone Go proof of concept for one narrow question:

Can FleetShift project its delivery output into an OCM-shaped compatibility view and then reuse the spoke-side `work-agent` shape, without adopting the whole `klusterlet` or persisting extra OCM state?

The answer from this POC is "probably yes, at the `ManifestWork` seam."

## What this demonstrates

- A tiny FleetShift-shaped delivery envelope is translated into an OCM `ManifestWork` shape.
- Desired work is exposed through a **virtual**, target-scoped informer/lister view that matches the namespace-scoped model used by OCM spoke agents.
- No `ManifestWork` objects are persisted to a Kubernetes API server, locally or remotely.
- A small target-local reconcile loop watches that scoped compatibility view, keeps a **minimal local journal**, and projects a real OCM-shaped `AppliedManifestWork` only when needed.
- The resulting desired-work view and projected local state are turned back into a small FleetShift-shaped feedback struct.
- A focused integration test also shows that the projected `ManifestWork` shape can drive OCM's real `manifestcontroller.NewManifestWorkController` with no controller fork, if we accept OCM's client/informer assumptions.

## What is real reuse

- Real OCM API types:
  - `ManifestWork`
  - `AppliedManifestWork`
  - `ManifestConfigOption`
  - OCM finalizer and update-strategy constants
- The same target-scoped informer idea used in `reference/ocm/pkg/work/spoke/spokeagent.go`:
  - one desired-work view per target
  - one lister view per target
- The same event-driven queue idea used by OCM's `ManifestWorkController`:
  - enqueue on add when the finalizer is present
  - enqueue on spec or label change
- The same broad spoke-side split of state:
  - desired work is external to the local reconciler
  - local bookkeeping is separate from desired state
- One real OCM controller path:
  - `pkg/work/spoke/controllers/manifestcontroller.NewManifestWorkController`
  - OCM's apply pipeline under `pkg/work/spoke/apply`
  - OCM's status patching and `AppliedManifestWork` bookkeeping behavior

## What the direct reuse path requires

The `ocm_manifestcontroller_reuse_test.go` experiment is intentionally narrow, but it shows the minimum shape needed to reuse OCM's real manifest controller without forking it:

- a typed `ManifestWorkInterface`
- a generated `ManifestWorkInformer` and namespace lister
- a typed `AppliedManifestWorkInterface`
- a generated `AppliedManifestWorkInformer` and lister
- a spoke `dynamic.Interface`, `kubernetes.Interface`, and `RESTMapper`
- OCM's `sdk-go` controller/factory/patcher stack

That is meaningful reuse, but it also shows why "reuse as-is" conflicts with the stricter minimal-state goal: the controller wants a real `AppliedManifestWork` API shape and informer/lister cache, not just a tiny local journal.

## What a fork could simplify

If FleetShift wants to keep the in-memory `ManifestWork` view and the minimal journal from this POC, the clearest fork points are:

- replace `AppliedManifestWork` client/lister/patcher dependencies with a narrow journal interface
- split the manifest apply reconciler from the `AppliedManifestWork` inventory reconciler so apply can be reused without the full local CR shape
- make `objectreader` and feedback watches optional or pluggable
- drop or collapse the extra finalizer and unmanaged-work controllers if FleetShift owns deletion semantics centrally

## What is intentionally stubbed

- The main virtual/journal adapter path still has no real Kubernetes apply logic; only the focused reuse test exercises OCM's real apply path against fake clients
- No OCM registration agent
- No CloudEvents workload driver
- No full OCM status scraping or availability watching
- No Kubernetes API server for `ManifestWork`
- No direct imports from `fleetshift-server/internal/...`

That last point is intentional: this POC lives under top-level `poc/`, so it cannot legally import FleetShift internal packages. Instead it uses a tiny copied FleetShift-shaped interface and payload model as demonstration glue.

## Files

- `adapter.go`: translator, synthetic desired-work source, minimal journal, and simplified spoke reconcile loop
- `adapter_test.go`: end-to-end tests for virtual desired-work projection, informer cache updates, journal-backed `AppliedManifestWork` projection, and feedback
- `ocm_manifestcontroller_reuse_test.go`: focused proof that the projected `ManifestWork` can drive OCM's real manifest controller when backed by OCM-shaped clients and informers

## Run

```bash
go test ./...
```

## Takeaway

The promising boundary is not "run klusterlet as-is." It is:

1. FleetShift orchestration emits a delivery envelope.
2. An adapter projects that envelope into an **in-memory `ManifestWork` compatibility view** for one target.
3. A target-local spoke loop consumes that view and manages local apply/inventory/status state.
4. The local checkpoint stores only the minimal data needed for restart/cleanup and projects `AppliedManifestWork` on demand.

That suggests a realistic next step would be a FleetShift fleetlet-side adapter that reuses more of OCM's spoke reconciliation internals while keeping FleetShift's own transport, identity, and routing model, and without introducing a second persisted desired-state store.
