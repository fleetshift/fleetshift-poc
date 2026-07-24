# OpenShift Console Plugin Re-use

Re-using existing OpenShift console plugins in FleetShift is a priority. These plugins already contain mature, production-tested UI for cluster capabilities (monitoring, networking, storage, etc.) and rewriting them from scratch would be wasteful.

## The Problem

Existing OCP console plugins are built with the assumption that they run inside the OpenShift console. They rely on internally baked-in context that the OpenShift console provides — things like the active namespace, the current cluster's API proxy, user identity, feature flags, and console-specific extension points.

FleetShift is a multi-cluster shell, not the OpenShift console. It does not provide the same context. For an OCP plugin to work inside FleetShift, the plugin must not depend on context that only the OpenShift console can provide.

## Required Changes on the OCP Plugin Side

To make an existing OCP plugin reusable in FleetShift, the following changes are needed:

### 1. Remove hard-coded console context dependencies

Plugins must not import or rely on APIs that are specific to the OpenShift console runtime (e.g. console-internal hooks, console-provided Redux stores, or console-specific extension types that have no equivalent in FleetShift).

Instead of relying on global context provided through the shell, plugins can leverage Scalprum's shared stores and remote hooks to create self-contained state and context. This makes plugins portable across different host environments — they own their own data lifecycle rather than depending on a specific shell to provide it.

This will likely require API proxies so that plugins can reach the cluster APIs they need — the shell or platform must provide a way for plugins to make authenticated requests to the right cluster, even if the plugin itself doesn't know the details of how authentication works.

### 2. Accept cluster context as input

In the OpenShift console, there is always exactly one cluster. Plugins can assume a single-cluster context implicitly.

In FleetShift, a plugin may render for one specific cluster or across multiple clusters. Plugins must accept cluster identity as an explicit input rather than assuming a single implicit cluster.

### 3. Use a shared extension contract

OCP console plugins register extensions via `console.openshift.io/v1` ConsolePlugin manifests with console-specific extension types (`console.page/route`, `console.navigation/section`, etc.).

FleetShift uses its own extension types (`fleetshift.nav-item`, `fleetshift.deployment-detail-tab`, etc.) loaded via Module Federation and Scalprum.

For re-use, either:
- The plugin provides a FleetShift-compatible extension manifest alongside its OCP one, or
- FleetShift provides an adapter layer that maps OCP console extension types to FleetShift equivalents

The preferred approach is TBD and depends on how many plugins need to be re-used and how different the extension contracts are.

### 4. Decouple from console API proxy

OCP plugins typically make API calls through the console's built-in proxy (`/api/kubernetes/...`). In FleetShift, the API proxy path and mechanism will be different.

Plugins should accept the API base URL as configuration rather than hard-coding the console proxy path.

## Approach

The re-use effort should be incremental:

1. **Identify candidate plugins** — start with plugins that have the fewest console-specific dependencies and the highest value for multi-cluster use cases
2. **Assess coupling** — for each candidate, catalog what console-specific context it depends on
3. **Decouple** — make the minimal changes needed to remove hard-coded context, accepting inputs through a defined API instead
4. **Dual registration** — the plugin should continue to work in the OpenShift console while also being loadable by FleetShift

## Requirements

### REQ-OCP-1: Plugins must not depend on OpenShift console context

Plugins intended for re-use in FleetShift must not import or rely on APIs, hooks, Redux stores, or extension types that are specific to the OpenShift console runtime. Plugins must own their own state and data lifecycle using portable mechanisms (e.g. Scalprum shared stores and remote hooks).

### REQ-OCP-2: Plugins must accept cluster identity as explicit input

Plugins must not assume a single implicit cluster. They must accept cluster identity as an input parameter so they can render in both single-cluster and multi-cluster contexts.

### REQ-OCP-3: Plugins must support dual registration

Adapted plugins must continue to work in the OpenShift console while also being loadable by FleetShift. Re-use must not break the plugin's existing OCP deployment.

### REQ-OCP-4: Plugins must not hard-code the API proxy path

Plugins must accept the API base URL or proxy path as configuration rather than hard-coding the OpenShift console's built-in proxy path (`/api/kubernetes/...`). The platform must provide the proxy mechanism — see [API Requirements](./api-requirements.md).

### REQ-OCP-5: Extension contract compatibility must be defined

Either plugins must provide a FleetShift-compatible extension manifest alongside their OCP manifest, or FleetShift must provide an adapter layer that maps OCP console extension types to FleetShift equivalents. The chosen approach must be documented and consistent across all re-used plugins.

## Open Questions

- What is the minimal API contract FleetShift must provide to satisfy the most common OCP plugin needs?
- Can we provide a compatibility shim that emulates enough of the console context to run unmodified plugins, or is per-plugin adaptation required?
- How do we handle plugins that depend on console-specific backend proxies (e.g. Thanos querier proxy for monitoring)?
