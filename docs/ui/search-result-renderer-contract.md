# Search Result Renderer Contract

Search-result renderers separate resource retrieval from domain-specific display and navigation, letting plugins explain results without putting each resource schema into shell rendering code. Resources returned by `QueryResources` carry a `resourceType`, a full `name`, and a resource body; plugins contribute `ocm-fleet.render-search` extensions to map those results to display information and module-reference links.

This contract is implemented. Renderer lookup is extensible, but the current simple inventory-search queries still contain type-specific policy. See [Feature Contracts](feature-contracts.md), [Advanced Search](cel-advanced-search.md), and the [search diagram](diagrams/search/search.c4).

## Extension declaration

The extension type is the static string `ocm-fleet.render-search`; the backend resource type is a property, not encoded into the extension type.

| Property | Contract |
|----------|----------|
| `id` | Required local ID matching `/^[a-z][a-z0-9-]*$/` |
| `label` | Required display string, used as a result badge |
| `resourceType` | Required nonempty backend type identity |
| `resolve` | Required CodeRef to a synchronous resolver |
| `icon` | Optional CodeRef to a result icon |
| `component` | Optional CodeRef accepted by the build contract, but not consumed by the current renderer path |

```typescript
createSearchResultRenderer({
  id: "kind-cluster-renderer",
  label: "Kind Cluster",
  resourceType: "kind.open-cluster-management.io/Cluster",
  resolve: { $codeRef: "KindSearchResult.resolveKindCluster" },
  icon: { $codeRef: "KindSearchResult.KindClusterIcon" },
});
```

CodeRefs use `{ $codeRef: "Module.export" }` and refer to exposed modules in the declaring plugin. Build checks validate the required metadata, reference syntax, and exposed-module keys; they do not execute the resolver or validate its return value. See [Extension Validation](extension-validation.md) for runtime resolution and guard limits.

Sources: [builder and validation](../../sdk/build-utils/src/extensions/searchResultRenderer.ts), [set validation](../../sdk/build-utils/src/extensions/validate.ts), [Kind declarations](../../extensions/kind/client/rspack.config.ts).

## Resolver input and output

`InventoryResource` aliases the shared resource-query result type. The resolver receives the full envelope, not just the nested resource body. `resourceType` selects the renderer; instance identity is available through `name` and `resource.name`.

```typescript
interface SearchResultRender {
  scope: string;
  module: string;
  to?: string;
  search?: string;
  title?: string;
  description: ReactNode;
  navigable?: boolean;
}

type SearchResultResolve = (
  resource: InventoryResource,
) => SearchResultRender;
```

The resolver is synchronous: its job is to map already-fetched data to display and navigation properties. The description is rendered as a React node, allowing text or plugin-supplied badges/layout. The optional title overrides the default final segment of `resource.name`.

The Kind renderer targets the core Clusters route through its exposed JavaScript module `ClustersModule`. The route contribution uses `ocm-fleet.route`; the resolver's `module` field still names an `exposedModules` key, not a route ID:

```typescript
export function resolveKindCluster(
  resource: InventoryResource,
): SearchResultRender {
  const clusterId =
    resource.resource.name.split("/").pop() ?? resource.resource.name;
  return {
    scope: "core-plugin",
    module: "ClustersModule",
    to: clusterId,
    description:
      ((resource.resource as Record<string, unknown>).state as string)
        ?.toLowerCase() ?? "unknown",
  };
}
```

Sources: [shared types](../../sdk/common/src/searchResultRenderer.ts), [resource result types](../../sdk/common/src/resourceApi.ts), [Kind resolver](../../extensions/kind/client/src/KindSearchResult.tsx).

## Discovery and loading

`useInventorySearch` obtains matching extensions with `useExtensions` and resolves them with `useResolvedExtensions`. `FleetSearch` mounts this hook regardless of whether a result of a particular type is present. All matching renderer CodeRefs are resolved eagerly by that hook; loading is not deferred per encountered resource type.

Resolved renderers are stored in a map keyed by the exact `resourceType` string. Later entries replace earlier entries for a duplicate key; there is no renderer-type uniqueness check or priority contract. The extension label, resolver, and icon enter the map. The optional `component` does not.

The hook waits for renderer resolution before executing inventory queries. SDK resolution failures can exclude extensions, but successful resolution is not a structural check of the resolver's return value or icon component.

Sources: [renderer guard](../../client/web/src/extensions/isSearchResultRendererExtension.ts), [inventory hook](../../client/web/src/hooks/useInventorySearch.tsx), [FleetSearch](../../client/web/src/components/Search/FleetSearch.tsx).

## Rendering and navigation

For a matching renderer, the shell calls `resolve(result)`, adds a compact badge from the extension label, and displays the returned description and optional icon. All inventory results appear under Resources in simple search; advanced results use the same item-rendering path.

Links use `PluginLink` with the returned scope/module, relative `to`, and optional query string. The shell resolves the first matching backend page entry; it does not derive routes from resource names. A renderer must return a target module that actually implements the suffix it supplies.

Page registration is the only availability condition in this resolver. It does not prove local enablement, authorization, plugin health, or a matching detail route. A missing target makes `PluginLink` render its default empty fallback; a locally disabled target can still resolve and open its enablement page.

The current `navigable: false` flag does **not** disable the link. It mutes the description and adds a "Cluster" badge while retaining the module link. The Kubernetes renderer uses this for objects that link to their containing cluster rather than an object detail page. Pods, Namespaces, and Nodes have their own module targets.

Sources: [result rendering](../../client/web/src/components/Search/FleetSearch.tsx), [PluginLink](../../sdk/common/src/PluginLink.tsx), [page resolver](../../client/web/src/components/Root/ScalprumShell.tsx), [Kubernetes resolver](../../extensions/core/client/src/plugins/core-plugin/K8sObjectSearchResult.tsx).

## Fallbacks and failure boundaries

When a renderer is missing, or calling its resolver throws, the hook produces a fallback with the resource-name leaf as title and `resourceType` as description. It has no path and is rendered as a disabled menu item with a generic search icon. Resolver exceptions are logged.

This fallback does not add resource state to the description. The catch covers the resolver call, not every later property access or React render. A malformed return value, throwing description component, or broken icon is not universally isolated by this contract.

Feature-search `searchResult` components have a separate custom-component branch. That branch does not make the renderer extension's optional `component` property functional.

Sources: [result mapping](../../client/web/src/hooks/useInventorySearch.tsx), [menu branches](../../client/web/src/components/Search/FleetSearch.tsx).

## Current query policy and completeness

Simple inventory search makes two parallel requests after a 300 ms input debounce:

| Query | Filter scope | Requested page size |
|-------|--------------|---------------------|
| Clusters | Kind Cluster type; relative name starts with the term or `clusters/` plus the term | 5 |
| Kubernetes objects | Kubernetes Object type; Pod, Namespace, or Node; observation metadata name starts with the term | 5 |

The term is escaped for backslashes and double quotes before insertion into CEL. These are prefix queries, not Orama full-text queries over all inventory fields. Registering another renderer does not automatically add its type to simple search.

Advanced search sends the raw CEL filter in one request with page size 20. Both modes query scope `-`, do not traverse next-page tokens, and return empty results after logged API failures. Simple search uses `Promise.all`, so failure of either request discards the combined inventory result set. Neither mode promises a complete inventory listing or distinct user-visible query-error state.

Sources: [query construction and mapping](../../client/web/src/hooks/useInventorySearch.tsx), [debouncing](../../client/web/src/components/Search/FleetSearch.tsx), [query API client](../../sdk/common/src/resourceApi.ts).
