# Spike: Pool & Neighborhood

**Issue**: FM-34
**Status**: Draft
**Date**: 2026-03-31

## What we're trying to understand

Today, placement sees every target in the system. There's no concept of "these clusters do one job, those clusters do another job." There's no way to know when a group of clusters is running out of room, or to automatically grow that group.

The design doc (`provider_consumer_model.md`, sections 1 and 8) introduces Pool and Neighborhood as the concepts to solve this. This spike explores what those concepts mean, what questions they raise, and where they touch the existing code.

## Pool

A Pool is a group of clusters that do the same job.

Example: you run 5 management clusters that host customer control planes. Together they are your "HCP management pool." Each one can hold about 80 control planes.

### What a Pool needs to know

- **Who's in it.** Some way to say "these targets belong to this pool." Could be a label selector, an explicit list, or both.
- **How full each member is.** A way to track utilization so the pool knows when it's running out of room.
- **When to grow.** Some signal that says "we need more clusters." Could be automatic, could just be a warning to a human.
- **What a new member looks like.** If the pool can grow, it needs to know how to create a new cluster.

### How targets join a pool

We see two paths:

1. **Label match** — you label an existing cluster and it matches the pool's selector. This is for bringing existing infrastructure into a pool.
2. **Provisioned from template** — the pool creates a new cluster from its template. The template includes labels that match the selector, so the new cluster joins automatically.

Both paths can work together. The template is the "grow" path, the selector is the "adopt" path.

**Open question: can a target be in multiple pools?** A cluster might match selectors for both a "region: us-east" pool and an "hcp-management" pool. This could be useful (regional pools AND workload-type pools), but it means capacity tracking has to be per-pool. Worth thinking about whether this adds more complexity than value.

### Capacity

Each pool tracks how full its members are. The simplest version is a single number per member — "this cluster is running 60 out of 80 control planes."

The pool doesn't reach into targets to check this. Some external system (an addon, Prometheus, a cloud API) reports utilization. The pool just stores it and computes an overall state.

**Pool states we've been thinking about:**

- **Healthy** — plenty of room across members.
- **Nearing capacity** — average utilization crossed a threshold (e.g., 80%). This is the "heads up, you might want to grow" signal. Important because provisioning a new cluster takes time — you want to start before you're actually full.
- **Full** — no member can take the next workload.
- **Expanding** — a new member is being provisioned.

**Open questions around capacity:**

- **Single metric vs multiple?** We've been thinking single (e.g., just "control plane count"). But some workloads might care about multiple things (CPU, memory, GPU count). Is one number enough, or does that fall apart for certain use cases?
- **How does utilization data arrive?** Something needs to push updates. An addon? A Prometheus scraper? A new fleetlet status channel? We know where the data needs to live, but not how it gets there.
- **What are the right default thresholds?** 80% for expand, 20% for contract sounds reasonable, but this probably varies a lot by workload type.

### Expansion and contraction

When a pool is nearing capacity, it can either:

- **Automatically provision** a new cluster from its template.
- **Just signal** that someone should act (advisory mode).

This should be configurable per pool. A dev/test pool might auto-scale freely. A production pool might need a human to approve.

**Open questions around expansion:**

- **What actually triggers provisioning?** When the pool decides it needs a new member, what creates the provisioning deployment? A controller watching pool state? Something in the orchestration workflow? This is a workflow concern we haven't worked through yet.
- **How does contraction work?** Removing a member means moving its workloads to other members first. That needs coordination with placement and rollout. This is complex enough to be its own spike.
- **What happens if provisioning fails?** The pool is stuck in "expanding" but the new cluster didn't come up. Retry? Alert? Fall back to advisory?

### How Pool connects to placement

Today the orchestration loads all targets and hands them to placement:

```
Deployment starts
  → load ALL targets
  → placement strategy picks from everything
```

With pools, a deployment could reference a pool. Then placement only sees that pool's members:

```
Deployment starts
  → load the pool's members
  → placement strategy picks from the pool
```

The rest of the pipeline (rollout, manifest generation, delivery) doesn't care — it already works with whatever targets it receives. The change is only in what targets get loaded.

If a deployment doesn't reference a pool, it works like today. Backward compatible.

**Open question:** Is pool-scoped placement enough, or do we also need a pool-aware placement strategy that knows about capacity (e.g., "pick the member with the most room")? Scoping the target set is the simple first step. Capacity-aware binpacking is the smarter follow-up.

### Pool type sketch

```go
type Pool struct {
    ID             PoolID
    Name           string
    NeighborhoodID *NeighborhoodID
    MemberSelector TargetSelector
    TargetTemplate *TargetTemplate
    Capacity       CapacityDimension
    Expansion      ExpansionPolicy
    State          PoolState          // computed, not stored
}
```

## Neighborhood

A Neighborhood groups pools that exist for the same reason. If a Pool is "these 5 clusters host control planes," a Neighborhood is "all our HCP pools across all regions."

### Why have Neighborhoods at all?

You could just label pools and query by label. But there are reasons to make it a real concept:

- **Operators think at this level.** "How's our HCP infrastructure?" is a question about all HCP pools together. A neighborhood gives you that view.
- **Shared defaults.** All HCP pools probably track the same capacity metric (hosted control planes) with similar thresholds. A neighborhood could provide these defaults so you don't set them up from scratch on every new pool.
- **Future policy attachment point.** The design doc mentions things like target profiles (security boundaries), upgrade schedules, and managed resource type bindings that could live at the neighborhood level. We're not building those now, but the neighborhood is where they'd go.

### What a Neighborhood looks like

At its simplest:

- **Name and description** — "hcp", "argo", "virt", "ai", or whatever makes sense for the deployment.
- **Default capacity and expansion settings** — so new pools in this neighborhood start with sensible values.

Pools reference their neighborhood. The neighborhood doesn't own pools — it's a shared identity that pools opt into. You can have pools without a neighborhood.

### Example

```
Neighborhood: "hcp"
  Description: "HCP management clusters hosting customer control planes"
  Default capacity: hosted_control_planes, max 80 per target
  Default expansion: advisory, expand at 80%, contract at 20%

  Pools:
    - hcp-us-east (5 clusters, healthy)
    - hcp-eu-west (3 clusters, nearing capacity)
    - hcp-ap-south (2 clusters, healthy)
```

### What else could Neighborhood carry?

The design doc describes several things that could attach to neighborhoods. We're not building these now, but listing them here to show the direction and get feedback:

- **Target profiles** — which fleetlet channels are available on clusters in this neighborhood (full access vs restricted vs buffered). This is about security boundaries for factory clusters.
- **Managed resource type binding** — which consumer-facing service this neighborhood supports. "The HCP neighborhood serves HostedCluster requests." This ties into the provider/consumer model.
- **Upgrade lifecycle** — when and how clusters in this neighborhood get upgraded. "HCP management clusters upgrade on Tuesdays, Argo clusters on Thursdays."

These are all described in the design doc (sections 5-8). The question is whether they belong on Neighborhood, on Pool, on some other concept, or split across several. Worth discussing.

### Neighborhood type sketch

```go
type Neighborhood struct {
    ID               NeighborhoodID
    Name             string
    Description      string
    DefaultCapacity  *CapacityDimension
    DefaultExpansion *ExpansionPolicy
    Labels           map[string]string
}
```

**Open question: defaults vs overrides.** When a pool sets its own capacity/expansion values, those should probably win over the neighborhood defaults. But should there be a way for a neighborhood to *enforce* settings, not just suggest them? e.g., "all HCP pools MUST use advisory expansion, no auto-scaling." That's a different thing — defaults vs policies.

## Note on Workspaces

Workspaces (org structure, access boundaries) are a separate concept from Pools and Neighborhoods. Pools answer "which clusters do what job." Workspaces answer "who can see what." They don't overlap — pools are global and can span workspaces.

Workspace design will get its own spike.

## Open questions summary

1. **Can a target be in multiple pools?** Useful for regional + workload-type grouping, but adds complexity to capacity tracking.
2. **Single vs multiple capacity dimensions?** One number is simple. Multiple might be needed for GPU or storage-heavy workloads.
3. **How does utilization data arrive?** Addon, Prometheus, fleetlet channel, push API?
4. **What triggers expansion?** A controller? An orchestration activity? Something else?
5. **How does contraction/draining work?** Moving workloads off a member before removing it. Complex enough for its own spike.
6. **What if provisioning fails?** Retry, alert, fallback?
7. **Pool-aware placement strategy?** Beyond just scoping the target set — should placement know about capacity and do binpacking?
8. **Smarter invalidation.** A target change should only re-trigger placement for deployments scoped to the affected pool, not all deployments.
9. **Neighborhood defaults vs policies.** Should neighborhoods suggest defaults or enforce rules?
10. **What else attaches to Neighborhood?** Target profiles, managed resource types, upgrade lifecycle — where do these live? Worth discussing with the team.
11. **HyperFleet** (Alec's comment). How do these concepts map to HyperFleet? Need to compare.
