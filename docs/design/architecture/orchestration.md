# Orchestration

## What this doc covers

How deployments execute over time:

- pool and placement semantics
- the orchestration pipeline
- durable execution and diffing
- re-evaluation and invalidation
- rollout planning
- `DeploymentGroup`

## When to read this

Read this when you need to understand how the platform computes target sets, plans changes, executes deliveries, and advances rollouts.

## What is intentionally elsewhere

- Core vocabulary, targets, and delivery contract: [core_model.md](core_model.md)
- Fleetlets, channels, and request routing: [fleetlet_and_transport.md](fleetlet_and_transport.md)
- Addon contracts such as `ManifestGenerator` and `InvalidateManifests`: [addon_integration.md](addon_integration.md)
- Platform hierarchy and provisioning flows: [platform_hierarchy.md](platform_hierarchy.md)

## Related docs

- [../architecture.md](../architecture.md)
- [../mcoa_migration.md](../mcoa_migration.md)

## Orchestration pipeline

The orchestration pipeline is always the same regardless of which concrete strategies are plugged in:

1. Resolve the placement strategy against the deployment's pool
2. Compute the delta against the previous target set
3. Plan the rollout from that delta
4. Generate manifests per target in each batch
5. Deliver manifests through the target's delivery agent
6. Diff against previous state and apply changes
7. Evaluate rollout tasks before continuing

## Pool and placement

A **pool** produces a target set from nothing: `Pool() -> Target[]`.

A **placement** produces a target set from a target set: `Resolve(Target[]) -> Target[]`.

These are orthogonal. The pool determines which targets exist as candidates; placement selects among those candidates. Every deployment has a pool. Placement never fetches targets on its own.

Pools can be:

- a static list of target IDs
- a label query over targets
- a richer future resource that includes provisioning correlation, scaling policy, and health aggregation

### Placement view vs full target state

Placement receives a minimal placement view of each target, such as ID, name, and labels. The platform stores more target state than that, but it does not pass all of it to placement.

This has two important effects:

- placement cannot accidentally depend on delivery-only state
- the platform only needs to re-run placement when the placement view changes

### Why pool is a first-class input

- **Persistent authority.** A deployment keeps running after the original request. The pool fixes its scope even when there is no current user in memory.
- **Composition.** Placement can keep the uniform shape `Resolve(ctx, pool) -> targets`, which enables chaining or layering of placement logic.
- **Re-evaluation.** When pool membership or target labels change, the platform reloads the pool and re-runs placement without changing the placement contract.

## Execution model

```text
for each deployment in workspace:
    targets = deployment.PlacementStrategy.Resolve(deployment.pool)
    delta = diff(targets, previous_targets)

    plan = deployment.RolloutStrategy.Plan(delta)
    for step in plan.steps:
        if step.remove:
            for target in step.remove.targets:
                deployment.ManifestStrategy.OnRemoved(target)
                remove(target)
            evaluate(step.remove.afterTasks)
        if step.deliver:
            evaluate(step.deliver.beforeTasks)
            for target in step.deliver.targets:
                manifests = deployment.ManifestStrategy.Generate(gctx)
                if manifests != stored_previous(target):
                    deliver(target, manifests)
            evaluate(step.deliver.afterTasks)
```

The rollout strategy returns an ordered sequence of `remove` and `deliver` steps. Each step can include tasks. When the platform encounters a task, it calls back to the rollout strategy to evaluate it.

Removals are not a separate subsystem. They are part of the rollout plan and can be paced or gated the same way as deliveries.

## Two-phase diffing and delivery semantics

Manifest diffing is two-phase:

1. The platform compares newly generated output to the stored previous output per deployment and target.
2. If the output changed, the platform sends the full new payload to the target's delivery agent.

This allows the platform to skip unchanged deliveries entirely.

Delivery is request/response rather than fire-and-forget. The delivery agent acknowledges receipt and apply success or failure. If a connection drops mid-delivery, the durable engine retries the step. Idempotent apply semantics make at-least-once delivery safe.

## Durable execution

The orchestration pipeline uses durable execution semantics. Each step is persisted before execution. If the platform crashes mid-rollout, execution resumes from the last completed step.

This implies at-least-once invocation for strategy callbacks. Strategy implementations must therefore treat side effects as idempotent:

- repeated `Generate` calls must be safe
- repeated `OnRemoved` calls must be safe
- any external writes initiated by strategy code must tolerate replay

## Re-evaluation and invalidation

Placement and manifest strategies can become stale. Re-evaluation happens in two broad ways:

1. **User input changes.** The user PATCHes the deployment spec.
2. **Strategy state changes.** The strategy's own state changes and it signals recompute.

### Downstream effects by axis

- **Placement changes.** The platform re-runs `Resolve`, computes a new delta, removes departed targets, and feeds the new delta through rollout.
- **Manifest changes.** The platform re-runs `Generate` for currently placed targets, diffs the results, and delivers only what changed.
- **Rollout progress.** A task completion advances the rollout plan. This is normal temporal progression rather than recomputation.

A strategy-triggered signal only re-runs that strategy and its downstream effects. Manifest invalidation does not force placement to re-run. A score change does not force regeneration for unchanged targets.

Rollout is categorically different from manifest or placement output. A rollout task completing is not invalidation; it is the rollout moving forward in time.

## Rollout strategies

`Plan(delta)` returns an ordered sequence of steps. Each step is either:

- **remove**: remove from departed targets
- **deliver**: generate and apply to targets

Each step can include before-tasks and after-tasks. The platform executes steps in order and delegates task evaluation back to the rollout strategy.

The built-in `immediate` strategy emits one remove step followed by one deliver step, both ungated. Addon-provided rollout strategies can implement batching, approvals, time delays, disruption policies, health gates, and other sequencing logic without changing the platform's executor.

## DeploymentGroup

Complex applications often consist of multiple components with ordering dependencies. `DeploymentGroup` is a platform-native manifest kind that orchestrates these multi-component applications while still using the standard deployment model.

### `DeploymentGroup` as a manifest

A `DeploymentGroup` is delivered to the platform itself via the `local` placement strategy, or to a child platform through a `platform` target. The platform's `DeploymentGroup` controller creates child deployments, manages their lifecycle, and reports aggregate health upward.

```text
Parent Deployment
  manifestStrategy: { type: "deploymentGroup", spec: { ... } }
  placementStrategy: { type: "local" }
  rolloutStrategy: { type: "immediate" }
```

### Sequencing and health rollup

A `DeploymentGroup` contains a sequence of steps. Each step references one or more child deployments. A step does not begin until the previous step is healthy.

```yaml
deploymentGroup:
  deployments:
    - name: database
      manifestStrategy: { type: "addon", capability: "postgres-operator" }
      placementStrategy: { type: "selector", targetSelector: { tier: "data" } }
      rolloutStrategy: { type: "rolling", batchSize: 1 }
    - name: api-server
      manifestStrategy: { type: "addon", capability: "api-server" }
      placementStrategy: { type: "selector", targetSelector: { tier: "app" } }
      rolloutStrategy: { type: "staged", ... }
  sequence:
    - step: [database]
    - step: [api-server]
```

The parent deployment's health rolls up from its children. It is in progress while any child is deploying, healthy when all are healthy, and degraded if any child is degraded.

### Cascading delete

Deleting the parent deployment deletes its child deployments. Child deployments are owned by the group and are recreated if deleted independently.

### Example

```json
POST /deployments
{
  "name": "my-app",
  "manifestStrategy": {
    "type": "deploymentGroup",
    "spec": {
      "deployments": [
        { "name": "postgres", "..." : "..." },
        { "name": "api", "..." : "..." },
        { "name": "frontend", "..." : "..." }
      ],
      "sequence": [
        { "step": ["postgres"] },
        { "step": ["api"] },
        { "step": ["frontend"] }
      ]
    }
  },
  "placementStrategy": { "type": "local" },
  "rolloutStrategy": { "type": "immediate" }
}
```

One deployment, one status surface, and one delete path. The platform handles the choreography internally.
