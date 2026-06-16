# Report API

## What this doc covers
## When to read this
## What is intentionally elsewhere
## Related docs
## Overview
## Reporting into inventory

ReportInventory
- Identity
    - unique key value pairs (must not map to two different inventory items)
        - when does the inventory item assignment happen?
    - needs a canonical name – how?
        1. admin assigns
        2. target associates with managed resource that is already named
    - must be namespaced by the cluster identity, which itself is likely to be namespaced by addon
- Parent
    - assume none for now?
- Relationships
    - assume none for now?
- Target?
    - Addon knows this likely
    - Note that these aren't necessarily targets in the, "can accept manifests" sense; might need a better word for it
- Observations
- ConditionEvents
- Outputs


Extensions (aka addons) are authorized to report inventory. The shape of an inventory resource is described in [resource_indexing.md](./resource_indexing.md).

An extension reports inventory in batches specifying each item's state.

> TODO: Does state need to be potentially be additive from other addons? Or should they just report their own, related resources? Leaning towards the latter.

### Resource identity

Where resources are referenced by name, we _may_ want to support arbitrary key:value references, allowing different identifying keys to match, such as a cluster' or host's platform identifier (e.g. cloud instance ID). This is likely to be es

### Target linking, target identity, and target lifecycle

A target is often associated with a resource. For example, when you deploy a cluster (e.g. using a managed cluster)

## Transport and connectivity

An extension needs to be able to reach the server to be able to report inventory. The server needs to be addressable. How?

1. **Go through the hub as an aggregated API server.** This works-ish, and the security model is _okay_ for workload-to-workload communication like this. But having to ship an aggregated API server just for this, by default, is steep. And we're back to API server coupling, which is an availability and scaling concern.
2. **Go through a Route/Service (LoadBalancer type)/Gateway.** This is ideal, but requires more set up on the customer end for name resolution. However, OpenShift install already requires this. We can support other vendors, too; especially cloud-managed k8s, as the clouds tend to offer more integration with DNS / load balancing services.
3. **Direct hub->spoke w/ kubeconfig.** 

See also: [fleetlet](./fleetlet_and_transport.md).