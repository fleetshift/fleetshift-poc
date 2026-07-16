# FleetShift Controller-Runtime POC

Standalone proof of concept: run **controller-runtime** reconcilers against
**FleetShift delivery targets** instead of kube-apiserver, using
[sig-multicluster/multicluster-runtime](https://github.com/kubernetes-sigs/multicluster-runtime)
and the same manager-swap idea as
[postgres-controller-backend](https://github.com/jmelis/postgres-controller-backend).

## Question

Can addon authors keep the controller-runtime mental model (reconcile loops,
informers, `SetupWithManager`, optimistic concurrency) while the objects they
reconcile are **FleetShift deliveries** — with placement, rollout, attestation,
and `DeliveryReporter` feedback — rather than Kubernetes API resources?

This POC says **yes, at the `cluster.Cluster` / Provider seam**.

## What the in-memory store is (and is not)

The store is a **projection shim**, not a second source of truth.

FleetShift already owns durable desired state and pushes work via
`Deliver` / `Remove` (with restart recovery via `ListActiveDeliveries`).
Controller-runtime still needs a Kubernetes-shaped list/watch surface.
The local store is only that surface: hydrate on deliver, drop on
completion/restart rehydrate from the platform.

It is **not** a stand-in for Postgres/etcd. Bolting on a durable local
object DB would reintroduce a second desired-state store and defeat the
point of the delivery contract. Optional local state that *is* fine:
generation fencing / a small journal — not a full object database.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  DeliveryReconciler (standard controller-runtime shape)         │
│  Reconcile → Get Delivery CR → work → Status().Update           │
│            → DeliveryReporter.ReportEvent / ReportResult        │
└──────────────────────────┬──────────────────────────────────────┘
                           │ mcbuilder / mcreconcile.Request
┌──────────────────────────▼──────────────────────────────────────┐
│  multicluster-runtime Manager                                   │
│  Provider.Engage(targetID, fsruntime.Cluster) per target        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   fsruntime.Cluster  fsruntime.Cluster  ...
   (in-memory store)  (in-memory store)
          ▲
          │ projects Deliver/Remove into Delivery CRs
┌─────────┴───────────────────────────────────────────────────────┐
│  provider.Provider                                              │
│  implements multicluster.Provider + contract.DeliveryAgent      │
└─────────┬───────────────────────────────────────────────────────┘
          │ Deliver / Remove / Report*
┌─────────▼───────────────────────────────────────────────────────┐
│  platform.Fake (stand-in for FleetShift orchestration)          │
│  same API surface as domain.DeliveryAgent / DeliveryReporter    │
└─────────────────────────────────────────────────────────────────┘
```

| Layer | Role | Inspiration |
|-------|------|-------------|
| `fsruntime` | Drop-in `cluster.Cluster` / `manager.Manager` over an in-memory list/watch store | `pgruntime.NewManager` |
| `provider` | Discovers targets, engages clusters, implements `DeliveryAgent` | multicluster-runtime providers + gcphcp agent |
| `contract` | FleetShift-shaped delivery types (no `internal/` imports) | `poc/ocm-work-agent-adapter` |
| `controllers` | Ordinary reconciler; reports via `DeliveryReporter` | greeting controller / gcphcp reconcile |
| `platform` | Fake control plane for tests | recording delivery + DeliveryReportService |

## List/Watch → stock Reflector

Client-side integration matches ordinary kube controllers:

```
store (projection + RV journal) 
  → cache.ListerWatcher (metav1.ListOptions, watch.Interface, 410 Gone)
  → SharedIndexInformer / stock Reflector
  → workqueue → Reconcile against informer cache
```

- `store` is the POC analogue of postgres-controller-backend `internal/reader`:
  commit-ordered seq, journaled watch, compaction → `ResourceExpired` (410).
- `store.NewListerWatcher` is the only custom kube API surface.
- `fsruntime` uses unmodified `toolscache.NewSharedIndexInformer` (Reflector
  owns relist, watch restart, and indexer population). WatchList streaming
  is opted out via `IsWatchListSemanticsUnSupported` — classic List+Watch.
- Reconcile `GetClient()` reads the Reflector cache and writes the store.
  Delivery projection uses `DirectClient()` so writes are not blocked on
  informer lag.

## What stays the same

- `Reconcile(ctx, req) (Result, error)`
- `mcbuilder.ControllerManagedBy(mgr).For(&Delivery{}).Complete(r)`
- SharedInformer → workqueue → reconcile against the informer cache
- `client.Get` / `Status().Update` / `apierrors.IsNotFound`
- Generation fencing and async report-back (same contract as gcphcp)

## What changes

| Standard CR | This POC |
|-------------|----------|
| `ctrl.NewManager(kubeconfig, …)` | `fsruntime.NewManager` + `mcmanager.WithMultiCluster` |
| Cluster = kube API server | Cluster = FleetShift **target** (fsruntime store) |
| Desired state from etcd watch | Desired state from `DeliveryAgent.Deliver` → projected CR |
| Status stays in etcd | Status mirrored to platform via `DeliveryReporter` |
| Leader election | Not used (POC); production would use target leases / buckets |

## Run

```bash
cd poc/fleetshift-controller-runtime
go test ./...
go run ./example
```

## Mapping to real FleetShift

| POC | Production |
|-----|------------|
| `contract.DeliveryAgent` / `DeliveryReporter` | `fleetshift-server/internal/domain` interfaces |
| `platform.Fake` | orchestration + `DeliveryReportService` |
| `provider.Provider` | in-process addon or fleetlet Delivery channel adapter |
| `fsruntime` store | could be Postgres (pgruntime), SQLite, or a thin cache over fleetlet streams |
| Example target type `gcphcp` | real `fleetshift-server/internal/addon/gcphcp` |

The reconciler in this POC only simulates apply. A next step would replace
the simulated work with the same phase machine gcphcp uses, while keeping
the controller-runtime watch/reconcile loop.

## Files

- `contract/` — delivery protocol types
- `store/` — in-memory list/watch object store + `ListerWatcher`
- `fsruntime/` — Cluster/Client/Cache/Manager (SharedIndexInformer-backed cache)
- `provider/` — multicluster Provider + DeliveryAgent
- `apis/delivery/v1alpha1/` — Delivery CR
- `controllers/` — Delivery reconciler
- `platform/` — fake FleetShift control plane
- `example/` — runnable wiring
- `e2e_test.go` — end-to-end deliver → reconcile → report
