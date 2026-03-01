# MCOA Migration Guide

This document covers migrating the existing MCOA (Multicluster Observability Addon) codebase to the new platform. For the platform's architecture, see [architecture.md](architecture.md).

## 1. OCM Adapter Mapping

> **Note:** This section describes one possible implementation of the platform interfaces -- an adapter layer that bridges to OCM. This is an implementation detail, not an architectural concern. The platform contracts are infrastructure-agnostic; OCM is one way to fulfill them during an incremental migration. Other backends (direct Kubernetes, cloud-native APIs, etc.) are equally valid. This section is preserved for migration planning and can be revisited later.

New package: `[operators/pkg/platform/ocm/](operators/pkg/platform/ocm/)`. This is the only place OCM imports appear.


| Platform Interface                      | OCM Adapter Wraps                                                                                | Current MCOA Code Location                                                                                                                                                                                                                                                                                                                            |
| --------------------------------------- | ------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CapabilityService.Register`            | Creates `ClusterManagementAddOn` CR + `AddOnDeploymentConfig` CR + deploys addon-manager         | `[clustermanagementaddon.go](operators/multiclusterobservability/pkg/util/clustermanagementaddon.go)`, `[renderer_mcoa.go](operators/multiclusterobservability/pkg/rendering/renderer_mcoa.go)`, `[cluster_management_addon.yaml](operators/multiclusterobservability/manifests/base/multicluster-observability-addon/cluster_management_addon.yaml)` |
| `CapabilityService.InvalidateManifests` | Updates `AddOnDeploymentConfig` → addon-framework re-renders → updates ManifestWorks per cluster | `[renderer_mcoa.go::renderAddonDeploymentConfig()](operators/multiclusterobservability/pkg/rendering/renderer_mcoa.go)` updates `CustomizedVariables`, addon-framework watches and cascades                                                                                                                                                           |
| `ClusterService.Labels`                 | Aggregates labels from ManagedCluster cache                                                      | `[informer.go::GetManagedClusterLabelList()](proxy/pkg/informer/informer.go)`                                                                                                                                                                                                                                                                         |
| `DeliveryStatusService.Status`          | Reads `ManagedClusterAddOn.Status.Conditions` + `ManifestWork.Status.Conditions`                 | Addon-framework populates `ManagedClusterAddOn` status; ManifestWork status is per-cluster                                                                                                                                                                                                                                                            |
| `AccessService.ResolveIdentity`         | OpenShift `users/~` API + cache                                                                  | `[util.go::GetUserName()](proxy/pkg/util/util.go)`, `[user_project.go](proxy/pkg/cache/user_project.go)`                                                                                                                                                                                                                                              |
| `AccessService.LookupPermissions`       | `rbac-api-utils` + OpenShift Projects + merge → generic attrs                                    | `[modifier.go::getUserMetricsACLs()](proxy/pkg/metricquery/modifier.go)` lines 140-213. The OCM adapter performs the existing 3-source merge and converts to `ResourcePermission` tuples.                                                                                                                                                             |


**What disappears from the adapter layer:**

- The `ClusterManagementAddOn` CR, `AddOnDeploymentConfig` CR, and `Placement` CR chain -- the platform resolves targets and drives manifest generation directly
- The external `multicluster-observability-addon-manager` Deployment -- the platform calls `ManifestGenerator` itself
- Per-cluster `ManagedClusterAddOn` CRs -- the platform tracks per-cluster state internally

**Addon-framework retention:** The addon-framework is currently used for two purposes: (1) manifest delivery (via the external addon-manager), and (2) CSR signing for mTLS certs (via `[cert_controller.go](operators/multiclusterobservability/pkg/certificates/cert_controller.go)` and `[cert_agent.go](operators/multiclusterobservability/pkg/certificates/cert_agent.go)`). The platform replaces (1) entirely. For (2), the addon-framework dependency is retained until certificate provisioning is redesigned (see open question on certificate provisioning in Part 7).

**Platform co-location:** When the platform and a fleet member happen to run in the same environment, the delivery implementation can use direct-apply instead of remote delivery. The platform calls the manifest strategy's `Generate(localGenerateContext)` and applies directly. No special-casing is needed in the platform contracts or in addon code -- this is a delivery optimization, not an architectural concern.

## 2. Rightsizing special case

The Policy/Placement/PlacementBinding/ConfigurationPolicy chain in `[rs-utility/](operators/multiclusterobservability/controllers/analytics/rightsizing/rs-utility/)` collapses completely. The rightsizing controller:

1. Registers `"rightsizing-rules"` capability with a `ManifestGenerator` that returns the PrometheusRule
2. When the PrometheusRule content changes, calls `InvalidateManifests("rightsizing-rules")`
3. The platform re-calls `Generate` for all matching clusters and delivers

The four-CR indirection (Policy, ConfigurationPolicy, Placement, PlacementBinding) AND the ManifestWork path both disappear.

## 3. Reconciler flow: before and after

### Before (current MCOA path)

The MCOA path uses the addon-framework, which is itself an inverted model (the framework calls the addon for manifests). But the indirection is through multiple CRs:

```
MCO operator reconciles MCO CR
  -> renders ClusterManagementAddOn CR (with installStrategy: Placements, ref: global)
  -> renders AddOnDeploymentConfig CR (with CustomizedVariables from MCO CR capabilities)
  -> renders multicluster-observability-addon-manager Deployment
  -> applies all to the platform deployment

Addon-framework (running in addon-manager) watches CMA + Placement:
  -> resolves Placement "global" -> all managed clusters
  -> for each cluster:
       creates ManagedClusterAddOn CR
       calls addon agent's Manifests() -> renders spoke resources
       creates/updates ManifestWork

MCO CR config changes:
  -> MCO operator updates AddOnDeploymentConfig CustomizedVariables
  -> addon-framework watches ADC -> re-renders for all clusters
  -> updates ManifestWorks
```

This involves: 3 platform-side CRs rendered by MCO + 1 external Deployment + per-cluster MCA + per-cluster ManifestWork, all coordinated through CR watches.

### After (strategy-based: platform orchestrates, strategies are pluggable)

**Platform orchestration pipeline (same for all strategy types):**

```
for each deployment in workspace:
    targets = deployment.PlacementStrategy.Resolve(workspace_cluster_pool)
    delta = diff(targets, previous_targets)

    // Removals happen immediately -- rollout strategy does not gate removals
    for gone_cluster in delta.removed:
        deployment.ManifestStrategy.OnRemoved(gone_cluster)
        remove(gone_cluster)

    // Additions and updates are paced by the rollout strategy
    clusters_needing_delivery = delta.added
    if manifest_invalidated:
        clusters_needing_delivery = clusters_needing_delivery + delta.unchanged

    if len(clusters_needing_delivery) > 0:
        plan = deployment.RolloutStrategy.Plan(
            TargetDelta{Added: delta.added, Unchanged: delta.unchanged},
            current_delivery_state,
        )
        for batch in plan.batches:
            await_tasks(batch.beforeTasks)   // approval, health check
            for cluster in batch.clusters (up to batch.maxConcurrency):
                gctx = GenerateContext{Cluster: cluster, Namespace: ..., Config: ...}
                manifests = deployment.ManifestStrategy.Generate(gctx)
                if manifests != stored_previous(cluster):
                    deliver(cluster, manifests)  // request/response: blocks until fleetlet ACK
            await_tasks(batch.afterTasks)    // timed wait, health check, approval
```

When the rollout strategy is `immediate` (the default), `Plan` returns a single batch containing all `clusters_needing_delivery` with no before/after tasks and maxConcurrency equal to the total -- collapsing to parallel delivery with no gating.

For a staged rollout, each batch corresponds to a stage. The platform persists rollout state (current batch index, gate status, per-cluster delivery status) so that rollouts survive controller restarts. Gate execution is non-blocking: when a gate is pending (e.g., approval), the platform records the deployment as `state: awaitingGate` and moves on to other work. The gate clears when the external signal arrives (approval POST, health check passes, timer expires), resuming execution.

**Durable execution.** The orchestration pipeline uses durable execution semantics (e.g., DBOS, Temporal, or equivalent). Each step -- Resolve, Plan, Generate, deliver, gate evaluation -- is persisted before execution. If the platform crashes mid-rollout, execution resumes from the last completed step.

All strategy interfaces must be **safe for at-least-once invocation**. The durable engine may replay any step. This does NOT mean all interfaces must be idempotent in the strict sense (same input produces same output). `Generate` and `Resolve` may return different results on repeated calls if the underlying state changed -- that's correct behavior, and the platform uses whatever the latest call returns. The constraint is narrower: **any side effects must be idempotent.** If `Generate` mutates external state (registers in a SaaS database, issues a certificate), repeating that mutation must be safe. `OnRemoved` must be traditionally idempotent -- calling it twice for the same cluster must not double-delete or error. `Plan` and `Resolve` are typically side-effect-free and naturally safe.

**Delivery is request/response, not fire-and-forget.** Each delivery step (sending manifests to a cluster via the fleetlet) is a correlated request/response interaction. The fleetlet acknowledges receipt and application (or returns an error). The orchestration step blocks until the ACK arrives. If the connection drops mid-delivery, the RPC fails and the durable engine retries the step. Server-Side Apply makes repeated application idempotent. This gives at-least-once delivery guarantees without a separate acknowledgment protocol -- the guarantee falls out of RPC semantics + durable execution + idempotent apply. The separate status channel remains for ongoing health reporting (Available/Degraded over time), which is distinct from the one-time delivery ACK.

**MCOA addon startup (manifest strategy = addon):**

```go
generator := &MetricsCollectionGenerator{platformCtx: buildPlatformContext()}
platform.Capabilities().Register(ctx, metricsCollectionCap, generator)
```

**MCOA addon reconciler (watches MCO CR, secrets, configmaps):**

```go
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
    r.generator.UpdatePlatformContext(r.buildPlatformContext(ctx))
    return reconcile.Result{}, r.platform.Capabilities().InvalidateManifests(ctx, metricsCollectionCap)
}
```

**ManifestGenerator (called by platform via AddonManifestStrategy):**

```go
func (g *MetricsCollectionGenerator) Generate(ctx context.Context, cap CapabilityName, gctx GenerateContext) ([]Manifest, error) {
    return g.renderManifests(gctx, g.platformCtx)
}

func (g *MetricsCollectionGenerator) OnRemoved(ctx context.Context, cap CapabilityName, cluster ClusterID) error {
    return g.cleanup(cluster)
}
```

The addon code is unchanged from the previous design. What changed is that the platform wraps the registered `ManifestGenerator` in an `AddonManifestStrategy` and pairs it with whatever `PlacementStrategy` the user chose in the deployment spec. The platform handles: target resolution via the placement strategy, calling Generate via the manifest strategy, diffing, delivery, cleanup, retries.

**Raw workload example (manifest strategy = inline, no addon involved):**

A user deploys a static set of manifests to 3 specific clusters. No capability, no addon, no generator. The platform's `InlineManifestStrategy` returns the manifests from the deployment spec. The `StaticPlacementStrategy` filters the workspace pool by the explicit cluster list. The platform delivers and reconciles.

**Transport agnosticism:** Addon code is identical regardless of where the addon runs. The `platform` variable holds a `Platform` interface whose concrete implementation determines the transport. If the addon runs on a remote fleet member, `InvalidateManifests` flows through the local fleetlet to the platform, and Generate requests flow back through the same channel.

**What the strategy model eliminates:**

- Indirect signaling through chains of CRs -- the platform orchestrates strategies directly
- External addon-manager deployments -- the platform calls `ManifestGenerator` itself
- Per-cluster tracking CRs -- the platform tracks delivery state internally
- The inability to deploy raw manifests without creating an addon
- The inability to control rollout pacing -- changes now flow through the rollout strategy instead of hitting all fleet members simultaneously

## 4. Proxy flow: before and after

**Before** (3 external API calls, manual merge):

```
token -> GetUserName (OpenShift) -> GetMetricsAccess (K8s RBAC)
      -> FetchUserProjectList (OpenShift) -> merge both
      -> filter by cluster list (ManagedClusterInformer) -> rewrite query
```

**After** (1 or 2 platform calls + addon-side translation):

```
credential -> platform.Access().LookupPermissions(
                  AccessSubject{Credential: &cred}, "cluster", "observe") -> []ResourcePermission
           -> permissionTranslator.TranslatePermissions(perms) -> QueryFilter
           -> rewrite query using QueryFilter
cluster labels -> platform.Clusters().Labels() -> label list for Grafana
```

The platform returns generic permission tuples. The addon's `PermissionTranslator` (MCOA-provided code) converts `ResourcePermission{id: "cluster-b", attrs: {namespace_scope: ["default"]}}` into the PromQL matchers that `rewrite.InjectClusterLabels` and `AddNamespaceFilters` currently produce.

## 5. File impact summary

> This section is specific to migrating the MCOA codebase. For greenfield implementations, only the `operators/pkg/platform/` package and its interfaces are relevant.

**New files** (~27 files):

- `operators/pkg/platform/types.go` -- all value types including strategy specs (ManifestStrategySpec, PlacementStrategySpec, ScoringSpec, PrioritizerSpec, RolloutStrategySpec, StageSpec, StageTask, RolloutStatus, etc.)
- `operators/pkg/platform/generator.go` -- `ManifestGenerator` interface (addon implements)
- `operators/pkg/platform/strategy.go` -- `ManifestStrategy`, `PlacementStrategy`, and `RolloutStrategy` interfaces (platform-internal)
- `operators/pkg/platform/strategy_addon.go` -- `AddonManifestStrategy` (wraps ManifestGenerator)
- `operators/pkg/platform/strategy_inline.go` -- `InlineManifestStrategy` (returns deployment-spec manifests)
- `operators/pkg/platform/strategy_selector.go`, `strategy_static.go`, `strategy_all.go` -- placement strategy implementations (stateless filters)
- `operators/pkg/platform/strategy_scored.go` -- `ScoredPlacementStrategy` (stateful: scoring pipeline, ranked selection, recompute signaling)
- `operators/pkg/platform/transport.go` -- `Transport` and `Channel` interfaces (platform-facing transport abstraction)
- `operators/pkg/platform/transport_grpc.go` -- gRPC transport implementation (multiple bidirectional streams on one HTTP/2 connection, connection classes)
- `operators/pkg/platform/fleetlet.go` -- fleetlet broker logic: local gRPC/UDS listener, channel registration, platform-side stream mapping, reconnection isolation, fan-in/sharding
- `operators/pkg/platform/strategy_immediate.go` -- `ImmediateRolloutStrategy` (single batch, no gates)
- `operators/pkg/platform/strategy_rolling.go` -- `RollingRolloutStrategy` (batches of N with health-check gates)
- `operators/pkg/platform/strategy_staged.go` -- `StagedRolloutStrategy` (named stages with selectors, concurrency, and gates)
- `operators/pkg/platform/capability.go`, `cluster.go`, `delivery.go`, `deployment.go`, `access.go`, `platform.go` -- service interfaces
- `operators/pkg/platform/workspace.go` -- admin-facing interface
- `operators/pkg/platform/fake/*.go` (10-11 files: CapabilityService, DeliveryStatusService, AccessService, ManifestGenerator, ManifestStrategy, PlacementStrategy, ScoredPlacementStrategy, RolloutStrategy, ClusterService, Clock, Transport, Channel)
- `operators/pkg/platform/ocm/*.go` (3 files: capability+delivery orchestrator, cluster labels, access adapter)

**Major refactors** (~5 files):

- `operators/multiclusterobservability/pkg/rendering/renderer_mcoa.go` -- the MCOA platform-side CR rendering (ClusterManagementAddOn, AddOnDeploymentConfig, addon-manager Deployment) is replaced by a single `Register` call with a `ManifestGenerator` callback
- `operators/multiclusterobservability/controllers/multiclusterobservability/multiclusterobservability_controller.go` -- MCO operator reconciler collapses to: build PlatformContext, register generator, call `InvalidateManifests` on config changes
- `proxy/pkg/metricquery/modifier.go` -- replace AccessReviewer + UPI with AccessService.LookupPermissions + PermissionTranslator
- `proxy/pkg/proxy/proxy.go` -- replace preCheckRequest with AccessService.ResolveIdentity
- `proxy/pkg/informer/informer.go` -- replace with ClusterService.Labels()

**Eventually removed:**

- `operators/multiclusterobservability/manifests/base/multicluster-observability-addon/` -- the ClusterManagementAddOn, AddOnDeploymentConfig, and addon-manager Deployment manifests (replaced by platform API registration)
- `operators/multiclusterobservability/controllers/placementrule/` -- the legacy placement controller and manifestwork code (no longer needed when MCOA uses the platform)
- OCM imports from ~15 files -- move into `operators/pkg/platform/ocm/` exclusively
- The external `multicluster-observability-addon-manager` Deployment (no longer needed)

## 6. Expanded MCOA migration context

This section collects all "MCOA depends on this for" annotations from the API endpoints, organized by MCOA subsystem.

### MCO operator

- **Capability registration:** The platform calls MCOA's `ManifestGenerator` for clusters in workspaces where the `metrics-collection` capability is deployed.

- **POST /capabilities/{name}/invalidate:** Called on every state change -- MCO CR update, cert rotation, allowlist change, image update. Replaces the indirect signaling through `AddOnDeploymentConfig` updates and the addon-framework's watch-based re-rendering.

- **DELETE /capabilities/{name}:** Operator uninstall / MCO CR deletion cleanup path.

- **GET /workspaces/{ws}/deployments/{id}:** The MCO status controller (`[status.go](operators/pkg/status/status.go)`) could use this to report fleet-wide health on the `MultiClusterObservability` CR status subresource.

- **Manifest strategy: addon:** MCO CR updates, cert rotation, allowlist changes, image updates. Replaces the `AddOnDeploymentConfig` → addon-framework watch → re-render chain.

- **GET /delivery/{deployment}/{cluster}/status:** Populating the `MultiClusterObservability` CR status with per-cluster health.

### Addon-manager

- **POST /capabilities:** Today in MCOA, this declaration is split across multiple CRs with no unified schema (`ClusterManagementAddOn`, `AddOnDeploymentConfig`, `cluster_management_addon.yaml`). The MCO operator renders these CRs, which the addon-framework consumes. With the proposed model, this is replaced by a single `Register` call, and the separate addon-manager process is no longer needed.

- **PUT /workspaces/{ws}/clusters/{cluster}:** This determines which clusters the platform calls `ManifestGenerator.Generate` for. The addon doesn't see workspace structure, but the platform uses it to resolve which clusters are valid targets for each deployment.

- **GET /capabilities:** Not directly. MCOA might use it defensively to check whether its registration succeeded.

### Proxy

- **GET /clusters/labels:** The `acm_label_names` synthetic metric endpoint for Grafana label dropdowns (`[proxy.go](proxy/pkg/proxy/proxy.go)` `handleManagedClusterLabelQuery`)

- **POST /access/resolve:** The `preCheckRequest` function in `[proxy.go](proxy/pkg/proxy/proxy.go)` lines 154-214 which resolves the caller's identity from their credential before every query. Replaces the `X-Forwarded-User` header fallback chain and the `util.GetUserName` call.

- **POST /access/lookup:** The `Modifier.Modify()` function in `[modifier.go](proxy/pkg/metricquery/modifier.go)` lines 51-119 uses the lookup result (via the addon's `PermissionTranslator`) to inject `cluster` and `namespace` label matchers into PromQL queries. If the lookup returns all clusters with no attribute restrictions, the proxy skips query rewriting entirely (replacing `canAccessAll` in `[modifier.go](proxy/pkg/metricquery/modifier.go)` lines 280-300).

### Rightsizing

- **POST /capabilities/{name}/invalidate:** Called when PrometheusRule content changes.

- **Manifest strategy: addon:** PrometheusRule content changes. Replaces the Policy/Placement/PlacementBinding/ConfigurationPolicy chain.

### Other (usage, deployment)

- **GET /usage:** Not directly consumed by addon code. The platform computes this from delivery records. However, MCOA could optionally report custom usage metrics (e.g., time series ingestion rate per cluster) through an addon telemetry extension.

- **GET /capabilities/{name}:** Not directly. Platform-facing.

