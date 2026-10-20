# Console State Ownership and Cross-Plugin Access

Explicit state ownership keeps plugin consumers independent and makes shared stores and subscriptions deliberate across Module Federation boundaries, avoiding accidental coupling or assumptions about shared caches. The current console combines component-local React state with shell-owned infrastructure; it does not have the former mock prototype's collection of resource singleton stores or a working shared resource-topic event bus.

## Resource data stays with its consumer

The core resource hook fetches platform-held Kubernetes observations through `createResourceApi`. It keeps resources, loading state, and errors in React state and exposes a refetch function.

Requests are keyed by cluster context and resource kind. A request counter prevents an older response from replacing newer state. Fetching occurs on mount or dependency change; this hook does not subscribe to WebSocket updates or deduplicate requests across component instances.

Sources:

- [Core resource hook](../../extensions/core/client/src/plugins/core-plugin/useClusterResources.ts)
- [Resource query client](../../sdk/common/src/resourceApi.ts)
- [Pod view consumer](../../extensions/core/client/src/plugins/core-plugin/ClusterPodsTab.tsx)

Other features choose their own refresh behavior. For example, cluster views poll transient states and the overview periodically refreshes fleet data. There is no requirement that every plugin use one global resource store.

Sources: [cluster list](../../extensions/core/client/src/plugins/core-plugin/ClustersPage.tsx), [overview data](../../extensions/core/client/src/plugins/overview-plugin/useFleetData.ts).

## Shell-owned shared infrastructure

Navigation and extension presentation state use a shared extension store with IndexedDB persistence. The shell exposes its store instance through `api.fleet.extensionStore`; consumers that accept an external store can use that exact instance.

This explicit ownership matters across Module Federation boundaries. Separate copies of a module can otherwise create separate in-memory subscriptions even when they read the same persistent database. Module Federation alone is not a blanket guarantee of shared application state.

`useNavLayout` subscribes to layout/order changes and removes those subscriptions on cleanup. It reads the new-format layout first and falls back to legacy ordering when appropriate.

Sources:

- [Shared API contract](../../sdk/common/src/scalprum.ts)
- [Shell store instance](../../client/web/src/components/Root/ScalprumShell.tsx)
- [Layout lifecycle and cleanup](../../sdk/common/src/useNavLayout.ts)
- [Persistent extension store](../../sdk/common/src/extensionInstall.ts)

## Cross-plugin interfaces

Plugins should use explicit hooks or API contracts rather than depend on another feature's internal state shape. The existing navigation helpers are examples: consumers ask the shell to resolve a page identity rather than reading router internals.

The source tree also retains navigation hooks in a routing plugin. Their existence does not establish a general, versioned remote resource-store contract. The old observability-to-pod and overview-to-node store examples are not current implementations.

Sources: [common navigation hook](../../sdk/common/src/usePluginNavigate.tsx), [routing-plugin hook](../../extensions/core/client/src/plugins/routing-plugin/usePluginNavigate.tsx).

## Current subscription boundary

The shell's general `on` and cluster-change subscription functions are no-ops. The Go delivery-event WebSocket is a separate broadcaster; it is not wired into the resource hook above as a shared topic bus. Its route also has an outstanding authentication TODO.

Sources: [shell API implementation](../../client/web/src/components/Root/ScalprumShell.tsx), [delivery broadcaster](../../server/internal/transport/http/event_ws.go), [route/authentication wiring](../../server/internal/bootstrap/ui.go).

See [console API requirements](api-requirements.md) for current snapshot access and the distinction between inventory, live-target access, and future subscription contracts.
