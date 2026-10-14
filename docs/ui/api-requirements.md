# API Requirements

FleetShift's UI layer does not own API aggregation or authentication — those are handled elsewhere in the platform. However, the UI does require specific API capabilities from the platform to function correctly. This document describes what the UI needs.

## API Layers

The UI requires the following API capabilities to support its multi-cluster interface:

### 1. Aggregate Data Layer

Aggregate pages (e.g. "All Pods across all clusters", "All Deployments", dashboard widgets) need a single API that returns data spanning multiple clusters. Querying each cluster individually from the browser does not scale — with tens or hundreds of clusters, the UI cannot fan out requests to every cluster on every page load.

The platform must provide an aggregate data API. Candidates include:

- **ACM Search** — Red Hat Advanced Cluster Management provides a search API that indexes resources across managed clusters. This is a natural fit if clusters are managed by ACM.
- **An alternative aggregation service** — if ACM is not available, the platform must provide an equivalent: a service that indexes or proxies multi-cluster resource queries behind a single endpoint.

#### Data Normalization

Different clusters may run different versions of Kubernetes or OpenShift. API resource shapes can differ between versions — fields may be added, deprecated, or restructured. The aggregate data layer must normalize responses so that the UI receives a consistent data shape regardless of the source cluster's version.

This normalization should happen at the API layer, not in the UI. The UI should not need to know which Kubernetes version a cluster runs in order to render a list of pods.

### 2. Drill-Down Proxy Layer

When the user navigates to a specific resource on a specific cluster (e.g. viewing a pod's logs, scaling a deployment, inspecting a node), the UI needs direct access to that cluster's Kubernetes API.

The platform must provide a proxy that:

- Routes requests to the correct cluster based on a cluster identifier
- Handles authentication and authorization transparently — the UI sends requests to the proxy, and the proxy forwards them with the appropriate credentials for the target cluster
- Supports the full Kubernetes API surface needed by the UI (not just read — also watch, exec, log streaming, etc.)

The proxy path should follow a predictable pattern so that plugins can construct API URLs without hard-coding cluster-specific details. For example:

```
/api/proxy/{clusterId}/api/v1/namespaces/{ns}/pods/{name}/log
```

Plugins receive the cluster ID as input and use it to construct proxy URLs. They do not need to know the cluster's actual API server address or credentials.

### 3. Real-Time Updates (nice to have)

Some UI features need live, push-based updates rather than request/response — for example, streaming pod logs, watching resource status changes, or updating metrics dashboards in real time. Polling is inefficient and introduces latency.

The recommended approach is to leverage the Kubernetes informer interface on the backend. Informers use the K8s watch API to receive a stream of change events (ADDED, MODIFIED, DELETED) for a given resource type. The backend maintains informer connections to each cluster and forwards relevant events to the frontend over a WebSocket connection.

Informers work on both vanilla Kubernetes and OpenShift clusters — the watch API is part of core Kubernetes, which all conformant clusters support. On OpenShift clusters, informers can additionally watch OpenShift-specific resources (Routes, DeploymentConfigs, BuildConfigs, etc.) through the same mechanism, since those custom resources are registered with the K8s API server.

```
┌──────────┐     K8s watch API     ┌──────────────┐     WebSocket     ┌──────────┐
│ Cluster  │ ───────────────────▶  │   Backend    │ ────────────────▶ │    UI    │
│  API     │   (informer)          │   (informer  │   (push events)   │ (plugin) │
│          │                       │    + relay)  │                   │          │
└──────────┘                       └──────────────┘                   └──────────┘
```

This pattern has several advantages:

- **No polling** — the UI receives updates as they happen, with no delay and no wasted requests
- **Efficient** — a single informer connection per resource type per cluster serves all connected UI sessions
- **Scalable** — the backend can multiplex events from many clusters onto a single WS connection per UI session
- **Consistent** — the UI receives the same event structure regardless of which cluster the event originated from

The backend should normalize informer events across cluster versions (same concern as the aggregate data layer) so that the UI receives a consistent event shape.

This is not a hard requirement — the UI can function with request/response APIs and periodic refresh. But for features like log streaming, real-time metrics, and resource status monitoring, informer-backed WS transport provides a significantly better user experience.

## Requirements

### REQ-API-1: Aggregate queries must be available for multi-cluster pages

The UI must be able to fetch resources across multiple clusters through a single API call. Fan-out from the browser is not acceptable at scale.

### REQ-API-2: Aggregate responses must be normalized across cluster versions

Resources returned by the aggregate API must have a consistent shape regardless of the Kubernetes or OpenShift version running on the source cluster. The UI must not be responsible for version-specific data transformation.

### REQ-API-3: Drill-down proxy must support full K8s API surface

The per-cluster proxy must support read, write, watch, exec, and log streaming. It must not be limited to a subset of the Kubernetes API — plugins may need any part of it.

### REQ-API-4: Proxy must handle auth transparently

The UI must not manage per-cluster credentials. The proxy authenticates requests on behalf of the user using whatever mechanism the platform provides. The UI only needs to know the cluster ID.

### REQ-API-5: Proxy URL pattern must be predictable

Plugins construct proxy URLs using a cluster ID and a standard Kubernetes API path. The proxy path convention must be documented and stable so that plugins can be built without coupling to a specific proxy implementation.

### REQ-API-6: Real-time updates should use K8s informers with WS transport (nice to have)

For live data such as logs, metrics, and resource status changes, the platform should leverage the Kubernetes informer interface on the backend and deliver updates to the frontend via WebSocket transport. This avoids polling and provides a responsive, real-time experience. Not a hard requirement, but highly desirable for live update use cases.
