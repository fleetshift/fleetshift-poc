# SPIKE: State Management and Cross-Plugin Data Sharing

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (Web UI)
**Status**: Done

## Problem

FleetShift is composed of independently built plugins. Each plugin manages its own domain — one handles pods, another handles nodes, another handles deployments, and so on. These plugins need to maintain state (the list of pods, their statuses, metrics, etc.) and some plugins need access to data that belongs to another plugin.

The traditional approach would be to introduce a global state management layer like Redux or Zustand where all plugins read from and write to a single shared store. This creates tight coupling — every plugin depends on the shape of the global state, changes to one plugin's data model can break others, and the global store becomes a bottleneck that every team has to coordinate around. It also means the shell application has to know about the data structures of every plugin upfront, which defeats the purpose of having independently deployable modules.

The question is how to let plugins own their state independently while still allowing controlled data sharing between them when needed.

## What was explored

### Plugin-owned stores

Each plugin defines its own store as a singleton. The store holds the plugin's data and exposes a React hook that components use to subscribe to it. The hook handles initialization (fetching data on first use) and keeps the state up to date as new data arrives.

Because the store is a singleton — only one instance exists regardless of how many components use it — all consumers within the same plugin get the same data without duplicating fetches or state. The singleton is shared through Module Federation's shared scope, which means even if multiple plugins reference it, they all get the same instance.

The plugin decides how to populate its store. In the POC, most stores fetch initial data via REST and then subscribe to a shared WebSocket channel for live updates. But this is an implementation choice — a store could just as easily poll, use Server-Sent Events, or fetch on demand. The pattern does not prescribe the data source.

Using a single shared WebSocket channel (rather than each plugin opening its own connection) keeps the number of connections low. The shell provides an event bus that plugins subscribe to by topic. When a message arrives over the WebSocket, the shell dispatches it to the right subscribers. This is convenient but not required by the state management pattern itself.

### Cross-plugin data access via remote hooks

When a plugin needs data from another plugin, it does not reach into a global store. Instead, it calls the other plugin's hook remotely. Scalprum provides a mechanism for this — a plugin can execute a hook that is exposed by another plugin across the Module Federation boundary. The calling plugin specifies which plugin it wants to call and which hook to run. It gets back the same data the other plugin's own components would see.

This works because plugins expose their store hooks alongside their UI components in the Module Federation config. A hook is just another module that can be loaded remotely.

Two examples from the POC:

- **Observability reading from Core**: The metrics dashboard needs pod counts to cross-reference against its own metrics data. Instead of duplicating the pod-fetching logic, it calls the pod store hook from the core plugin and uses the result directly.
- **Core reading from Nodes**: The cluster overview shows node statistics alongside pod and namespace counts. It calls the node store hook from the nodes plugin to get the node data.

In both cases, the consuming plugin does not need to know how the data is fetched, stored, or updated. It just calls the hook and gets the current state.

### Plugin dependencies

The Dynamic Plugin SDK already supports declaring dependencies between plugins. A plugin can say it depends on another plugin, which ensures the dependency is loaded first. This is important for cross-plugin data access — if the observability plugin depends on the core plugin, it can safely call the core plugin's pod store hook knowing the module is already available.

This is not something we had to build. The SDK handles dependency resolution as part of the plugin loading lifecycle.

### Current state (proof of concept)

The pattern works end to end in the POC:

- 12+ stores across plugins (pods, namespaces, nodes, deployments, storage, events, alerts, networking, logs, and others).
- Cross-plugin hook calls work reliably through Scalprum's remote hook mechanism.
- Stores are singletons — multiple consumers (local or remote) share the same instance and see the same data.
- Live updates flow through a shared event bus without each plugin needing its own connection.

Limitations of the POC:

- There is no formal contract or versioning for store hooks. If a plugin changes the shape of its hook's return value, consuming plugins could break silently. A production implementation would need some form of interface contract.
- Error handling for remote hook failures is minimal. If a dependency plugin fails to load, the consuming plugin currently shows a loading state indefinitely.
- Store cleanup and garbage collection is not implemented. Once a store is initialized, it stays in memory for the session.

### Open questions

- Should store hooks have typed contracts that are versioned separately from the plugin itself?
- How do we handle the case where a plugin dependency is not available (e.g., the cluster does not have that plugin enabled)?
- Is there a need for write access across plugins, or is read-only sharing sufficient?

## References

- Scalprum shared stores documentation: https://github.com/scalprum/scaffolding/blob/main/packages/react-core/docs/shared-stores.md

## Key files

- Pod store (singleton pattern): `packages/mock-ui-plugins/src/plugins/core-plugin/podStore.ts`
- Namespace store: `packages/mock-ui-plugins/src/plugins/core-plugin/namespaceStore.ts`
- Node store: `packages/mock-ui-plugins/src/plugins/nodes-plugin/nodeStore.ts`
- Cross-plugin usage (metrics → pods): `packages/mock-ui-plugins/src/plugins/observability-plugin/MetricsDashboard.tsx`
- Cross-plugin usage (overview → nodes): `packages/mock-ui-plugins/src/plugins/core-plugin/ClusterOverview.tsx`
- Plugin dependency declarations: `packages/mock-ui-plugins/webpack.config.ts`
- Event bus (shared WebSocket): `packages/gui/src/hooks/useInvalidationSocket.ts`
- Shell API object: `packages/gui/src/App.tsx`
