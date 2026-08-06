# Plugin Discovery

FleetShift dynamically determines which UI plugins to enable for each connected cluster. Discovery produces a `plugins: string[]` list stored on the cluster object. The GUI uses this list to decide which plugin UIs to load and which nav items to show.

## Overview

```
┌──────────────┐     ┌───────────────────┐     ┌──────────────┐
│  Add cluster │────▶│  connectCluster() │────▶│  Discovery   │
│  (UI / env)  │     │  verify API, get  │     │  pipeline    │
│              │     │  version, build   │     │              │
└──────────────┘     │  K8s clients      │     └──────┬───────┘
                     └───────────────────┘            │
                                                      ▼
                                             ┌─────────────────┐
                                             │ plugins: [...]  │
                                             │ platform: ...   │
                                             └─────────────────┘
```

## Discovery Pipeline (current implementation)

The current prototype runs three independent checks in sequence. Each check is wrapped in a try/catch so a failure in one does not block the others. See the [requirements](#requirements) section for how this should evolve.

### 1. Always-on Plugins

These plugins are enabled for every cluster unconditionally. They rely on core Kubernetes APIs (`/api/v1`, `/apis/apps/v1`) that are available on all clusters.

| Plugin Key | Description |
|-----------|-------------|
| `core` | Pods, Namespaces, dashboard widgets |
| `nodes` | Node listing and status |
| `storage` | PersistentVolumes and PersistentVolumeClaims |
| `events` | Kubernetes events |
| `alerts` | Alerts derived from pod/node conditions |
| `networking` | Services and Ingresses |
| `deployments` | Deployments listing and detail |
| `logs` | Container log streaming |
| `config` | ConfigMaps and Secrets |
| `upgrades` | Cluster upgrade management |

### 2. CRD-based Discovery

FleetShift lists all CustomResourceDefinitions on the cluster (`apiextensions.k8s.io/v1`) and matches their API group against a known map:

| CRD API Group | Plugin Key | What it enables |
|---------------|------------|-----------------|
| `monitoring.coreos.com` | `observability` | Prometheus-based monitoring |
| `kafka.strimzi.io` | `pipelines` | Kafka/Strimzi pipeline management |
| `keda.sh` | `autoscaling` | KEDA autoscaler management |
| `cert-manager.io` | `networking` | Certificate management (extends networking plugin) |
| `elasticsearch.k8s.elastic.co` | `logs` | Elasticsearch-based log aggregation (extends logs plugin) |
| `cloud.redhat.com` | `clowder` | Clowder ClowdApp/ClowdEnvironment management |
| `cyndi.cloud.redhat.com` | `pipelines` | Cyndi data pipeline management |

In the current prototype, if the CRD listing fails, CRD-based discovery is skipped and only always-on plugins are enabled. This is not acceptable for production (see [REQ-DISC-3](#req-disc-3-discoverability-of-cluster-capabilities-is-critical)).

### 3. API Group Discovery (prototype only)

As a prototyping convenience, FleetShift queries the cluster's aggregated API groups (`/apis`) to get a rough idea of cluster capabilities. API aggregation is **not** a problem this project is trying to solve — this check exists only to bootstrap the UI during development.

- **Metrics API** (`metrics.k8s.io`): If present, enables the `observability` plugin. This API group is registered by the Metrics Server and provides pod/node resource usage data.

In production, the plugin list should come from an authoritative source (CRDs, ConsolePlugins, or explicit configuration) rather than probing API groups.

### 4. OpenShift ConsolePlugin Discovery (OpenShift only)

On OpenShift clusters, FleetShift also queries `console.openshift.io/v1` ConsolePlugin resources. This allows plugins that are deployed as OpenShift console plugins to be discovered:

- If a ConsolePlugin named `clowder-plugin` exists → enables the `clowder` plugin

This check silently fails on vanilla Kubernetes clusters where the `console.openshift.io` API group does not exist.

## Platform Detection

During connection, FleetShift also determines whether the cluster is OpenShift or vanilla Kubernetes by checking API groups:

- If `route.openshift.io` or `apps.openshift.io` is present → `platform: "openshift"`
- Otherwise → `platform: "kubernetes"`

This is stored on the cluster object and available to the GUI for platform-specific UI decisions.

## Connection (current prototype)

The current prototype supports two connection methods (kubeconfig context and bearer token) for experimentation. Authentication and connection management are not goals of this project — they are handled elsewhere in the platform. The connection code exists only to make the UI functional during development.

## Error Handling (current implementation)

Discovery is currently designed to be non-blocking and fault-tolerant:

- Each discovery phase (CRDs, API groups, ConsolePlugins) runs independently in its own try/catch
- A 403 on CRD listing does not prevent API group or ConsolePlugin discovery
- Node count detection is optional — a 403 on `listNode` sets `nodeCount: 0`
- Metrics client initialization failure is silently ignored

The result is always a valid cluster with at least the always-on plugins enabled.

## Debug Endpoint

The discovery results can be inspected via the REST API:

```
GET /api/v1/clusters/:id/discovery
```

Returns the full `DiscoveryDetails` object:

```json
{
  "alwaysPlugins": ["core", "nodes", "storage", ...],
  "crdPluginMap": { "monitoring.coreos.com": "observability", ... },
  "crdGroups": ["apps.openshift.io", "cloud.redhat.com", ...],
  "apiGroups": ["metrics.k8s.io", "route.openshift.io", ...],
  "matchedCrdGroups": { "cloud.redhat.com": "clowder" },
  "hasMetricsApi": true,
  "resultPlugins": ["core", "nodes", "observability", "clowder", ...]
}
```

---

## Requirements

### Plugin discovery must be deterministic

The current graceful-degradation approach (silently falling back to always-on plugins when CRD listing fails) is not acceptable for production. If the UI adapts dynamically based on cluster capabilities, it **must** know what plugins are available for the current user — not necessarily the entire cluster, but at least the set relevant to that user's role and permissions. A silent fallback means the user sees a degraded UI with no indication of why.

### Re-use of existing OpenShift console plugins

Re-using existing OpenShift console plugins (`ConsolePlugin` resources from `console.openshift.io/v1`) should be considered a requirement. These plugins are the authoritative source of UI extensions on OpenShift clusters.

This will require some updates on the existing OCP plugin side as well — details will be covered in a separate OCP plugin re-use document.

### Discoverability of cluster capabilities is critical

Knowing what capabilities a cluster offers is critical for the UI to compose itself correctly — at least for the current user. Without this information, plugins like `clowder`, `pipelines`, and `observability` cannot be discovered and the user sees a degraded interface with no explanation.

Discovery must not silently fall back to a minimal plugin set. If capabilities cannot be determined, the user must be informed.

### Plugin list must react to changes

When a cluster is added, updated, or a plugin is installed or removed, FleetShift must be notified so it can adjust the environment accordingly. A full page refresh in the UI is acceptable if needed, but restarting the server from which the data is sourced must not be required.
