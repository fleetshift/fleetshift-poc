# Search Result Renderer Contract — Extension-Driven Inventory Display

**Jira:** [OME-194](https://redhat.atlassian.net/browse/OME-194)
**Epic:** [OME-3 — Addon / Extension Model](https://redhat.atlassian.net/browse/OME-3)
**Status:** Implemented
**Depends on:** [Feature Contracts](feature-contracts.md), [Search Architecture](diagrams/search.c4)

## Problem

The inventory API (`QueryResources`) returns resources with a `resourceType` (e.g., `kind.fleetshift.io/Cluster`, `gcphcp.fleetshift.io/Cluster`) and a `name` in AIP-123 format (e.g., `//kind.fleetshift.io/clusters/martin-kind01471218`). The shell needs to render these in search results but doesn't know:

- Where each resource type links to (route)
- What icon to show
- What description or badges to display

That knowledge belongs to addon plugins. The shell must not embed it.

## Goal

Define a **build-time contract** so addon plugins can declare search result renderers keyed by resource type. The shell discovers these renderers via the existing extension system and resolves them lazily — only loading plugin code when a search result of that type actually appears.

This doc covers three layers in order:

1. **Build-utils** — encoding function, builder, validation
2. **Extension discovery** — how the shell finds and loads renderers
3. **Rendering** — deferred to a follow-up doc / iteration

## Layer 1: Build-Utils

### Extension type

All search result renderers share a single static extension type: `fleetshift.render-search`. The `resourceType` string (e.g. `kind.fleetshift.io/Cluster`) is stored as a property, not encoded into the type.

**Why static, not encoded?** The SDK's `DynamicRemotePlugin` validates extension types against `/^[a-zA-Z]+(?:-[a-zA-Z]+)*\.[a-zA-Z]+(?:-[a-zA-Z]+)*$/` — only letters and hyphens. Base64url encoding (an earlier approach) produces digits and underscores, which the regex rejects. A static type with `resourceType` as a property is simpler and SDK-compliant.

**Renderer lookup at runtime:** The shell collects all resolved `fleetshift.render-search` extensions into a `Map<resourceType, renderer>`. For each inventory result, it looks up `result.resourceType` directly — no encoding needed.

### Why use `resourceType` as the renderer key?

The inventory API returns each resource with a `resourceType` (type-level) and a `name` (instance-level). Sample response:

```json
{
  "name": "//kind.fleetshift.io/clusters/martin-kind01471218",
  "resourceType": "kind.fleetshift.io/Cluster",
  "resource": {
    "name": "clusters/martin-kind01471218",
    "uid": "2b1ec087-72f7-484e-817a-9ce0d02945b5",
    "spec": {},
    "conditions": { "Ready": { "status": "True" } }
  }
}
```

`resourceType` is the addon + kind combination. It's defined by the addon at registration time and returned on every resource. The `name` is instance-specific (carries the cluster ID) — useful inside `resolve()` to build the route, but not as the renderer key.

Using `resourceType` as the key in `properties.resourceType` means:

1. **Direct mapping** — no encoding, no intermediary. `result.resourceType` → map lookup → resolved renderer.
2. **Addon-defined** — the addon chooses its resource type when it registers with the backend. The same string becomes the UI renderer key.
3. **1:1 with backend types** — `ExtensionResourceTypeService.ListTypes()` returns the same `resourceType` values.
4. **All resources of the same type render the same way** — every `kind.fleetshift.io/Cluster` uses the same renderer. Instance-specific data (name, state, conditions) is passed to the `resolve` function.

### Builder function

Follows the existing `createClusterProvider` pattern — CodeRefs are passed already wrapped as `{ $codeRef: "Module.export" }`:

```typescript
// packages/build-utils/src/extensions/searchResultRenderer.ts

export const RENDER_SEARCH_TYPE = "fleetshift.render-search" as const;

export type SearchResultRendererExtras = {
  resourceType: string;
  resolve: EncodedCodeRef;      // required
  component?: EncodedCodeRef;
  icon?: EncodedCodeRef;
};

export type SearchResultRendererProperties = BaseExtensionProperties & SearchResultRendererExtras;

export function createSearchResultRenderer(
  properties: SearchResultRendererProperties,
): FleetshiftExtension<typeof RENDER_SEARCH_TYPE, SearchResultRendererExtras> {
  const ctx = `${RENDER_SEARCH_TYPE} "${properties.id || "(no id)"}"`;
  throwOnErrors(validateSearchResultRendererProperties(properties, ctx), RENDER_SEARCH_TYPE);
  return { type: RENDER_SEARCH_TYPE, properties };
}
```

### Validation

Added to `validate.ts` as a `case RENDER_SEARCH_TYPE:` in the switch:

- `id` — required, matches `/^[a-z][a-z0-9-]*$/`
- `label` — required string
- `resourceType` — required, non-empty string
- `resolve` — required, valid CodeRef
- `component` — optional CodeRef
- `icon` — optional CodeRef

### `resolve` return type

The `resolve` CodeRef points to a function that takes an inventory resource and returns a **module reference** — not a raw pathname. Routing is handled by `PluginLink`/`usePluginNavigate`, which resolve the actual URL from the plugin module registry.

```typescript
interface SearchResultRender {
  scope: string;          // plugin scope (e.g., "core-plugin")
  module: string;         // exposed module that handles this route (e.g., "ClustersModule")
  to?: string;            // relative path within the module route (e.g., "martin-kind01471218")
  search?: string;        // optional query string
  description: ReactNode; // plugin owns the rendering — string, PF Labels, custom layout
}
```

`description` is a `ReactNode`, not a string. The shell renders it as-is inside the search result item — no format enforcement. A plugin can return a plain string, PF `Label` components, status badges, or any custom layout.

The shell renders the link as `<PluginLink scope={r.scope} module={r.module} to={r.to}>` — the same mechanism the clusters table uses. The `resolve` function never hardcodes a pathname; it only references the module that owns the detail route.

### Plugin registration example

```typescript
// rspack.config.ts — kind-plugin extensions array
createSearchResultRenderer({
  id: "kind-cluster-renderer",
  label: "Kind Cluster",
  resourceType: "kind.fleetshift.io/Cluster",
  resolve: { $codeRef: "KindSearchResult.resolveKindCluster" },
  icon: { $codeRef: "KindSearchResult.KindClusterIcon" },
}),
```

```typescript
// src/plugins/kind-plugin/KindSearchResult.tsx
export function resolveKindCluster(resource: InventoryResource): SearchResultRender {
  const clusterId = resource.resource.name.split("/").pop() ?? resource.resource.name;
  return {
    scope: "core-plugin",
    module: "ClustersModule",
    to: clusterId,
    description:
      (resource.resource as Record<string, unknown>).state?.toLowerCase() ?? "unknown",
  };
}
```

```typescript
// rspack.config.ts — gcphcp-plugin extensions array
createSearchResultRenderer({
  id: "gcphcp-cluster-renderer",
  label: "GCP HCP Cluster",
  resourceType: "gcphcp.fleetshift.io/Cluster",
  resolve: { $codeRef: "GcpHcpSearchResult.resolveGcpHcpCluster" },
  icon: { $codeRef: "GcpHcpSearchResult.GcpHcpClusterIcon" },
}),
```

Both addons route to `ClustersModule` in `core-plugin` — the clusters detail page handles all cluster types. An addon with its own detail page would reference its own scope/module instead.

## Layer 2: Extension Discovery

### What the SDK gives us

1. **Plugin registry** — the shell knows which plugin scopes exist and can load their manifests on demand.
2. **CodeRef resolution** — once a manifest is loaded, the SDK resolves CodeRefs (`{ $codeRef: "Module.export" }`) by loading the plugin's MF remote and grabbing the export.
3. **Open type system** — any extension type string is accepted. The `default` case in `validate.ts` applies base validation to unrecognized types.

### What we build on top

The SDK's `useResolvedExtensions` hook already handles the discovery flow — it takes a type-guard predicate, scans all loaded plugin manifests, resolves CodeRefs, and returns live functions/components. We define a type guard `isSearchResultRendererExtension` that matches `fleetshift.render-search` exactly:

```typescript
// packages/gui/src/extensions/isSearchResultRendererExtension.ts

export function isSearchResultRendererExtension(
  e: Extension,
): e is SearchResultRendererExtension {
  return e.type === "fleetshift.render-search";
}
```

### Discovery flow

```
Shell startup
  └─ useResolvedExtensions(isSearchResultRendererExtension)
       └─ Scans all loaded manifests for fleetshift.render-search extensions
       └─ Resolves CodeRefs → live resolve/icon functions
       └─ Returns [extensions, loaded]

useInventorySearch hook
  └─ Builds Map<resourceType, ResolvedRenderer> from extensions
  └─ search(term) called by FleetSearch

User types search query
  └─ search(term) → inventoryAPI.searchAll(...)
  └─ For each resource:
       ├─ Look up result.resourceType in rendererMap
       ├─ Found → call resolve(resource) → { scope, module, to, description }
       └─ Not found → fallback (visible, disabled, no link)
```

### Key property: SDK-driven resolution

The SDK resolves all renderer extensions eagerly via `useResolvedExtensions`. This is the same pattern used by `isClusterProviderExtension` for cluster provider extensions — consistent, proven, minimal custom code. The renderer map is built once from the resolved extensions and updated when extensions change.

### Backend: `ListTypes` as discovery source

The backend's `ExtensionResourceTypeService.ListTypes()` returns all registered `ExtensionResourceType` definitions. This is not currently exposed via HTTP/gRPC but could be:

- **UI config endpoint** — the `/api/v1/ui/config` response (which already provides `pluginPages`) could include a `resourceTypes: string[]` field listing all registered types. The shell would know upfront what types exist, even before any search happens.
- **Validation** — at build time or runtime, the shell could cross-reference declared renderer extensions against known backend types and warn about missing renderers.
- **Future: auto-enable plugins** — if the backend knows a resource type exists and a plugin provides a renderer for it, the shell could auto-enable that plugin (similar to how `plugin-discovery.md` describes CRD-based plugin enablement).

Not needed for v1. The search results themselves carry the `resourceType`, which is sufficient for on-demand discovery.

## Layer 3: Rendering

### Current state

`FleetSearch.tsx` renders search results as PF `MenuItem` components inside a `Menu`. Each item gets:

- An icon (`ResultIcon`)
- A title with highlighted text (`HighlightedText`)
- A description (also highlighted)
- A link via `getLinkComponent(pathname)` which creates `<Link to={pathname}>`

Inventory results are returned by `inventorySearch()` as `SearchResultItem[]` and merged into `GroupedResults` under a `resources` category. Currently they have hardcoded TODOs for pathname, description, and icon.

### What changes

Inventory results get a **separate rendering path** from Orama results. The shell renders them using `PluginLink` (module reference) instead of `Link` (raw pathname).

### `SearchResultItem` extension

The existing `SearchResultItem` interface gains optional fields for plugin-resolved results:

```typescript
interface SearchResultItem {
  // existing fields
  id: string;
  title: string;
  description: string;  // HTML string for Orama results
  category: string;
  pathname: string;
  icon: string;
  status: string;
  feature?: string;
  IconComponent?: React.ComponentType;
  Component?: React.ComponentType<{ title: string; description: string }>;

  // new: plugin-resolved inventory results
  pluginLink?: {
    scope: string;
    module: string;
    to?: string;
    search?: string;
  };
  descriptionNode?: ReactNode;  // from resolve(), replaces HTML description
}
```

When `pluginLink` is present, `renderItem` uses `PluginLink` instead of `getLinkComponent`. When `descriptionNode` is present, it renders the node directly instead of `HighlightedText`.

### `renderItem` changes

```typescript
const renderItem = (item: SearchResultItem) => {
  if (item.Component) {
    return (
      <div key={item.id} role="none" onClick={clearSearch}>
        <item.Component title={item.title} description={item.description} />
      </div>
    );
  }

  // Inventory result with resolved renderer — uses MenuItem's component prop
  // with a cached forwardRef wrapper around PluginLink (same pattern as getLinkComponent)
  if (item.pluginLink) {
    return (
      <MenuItem
        key={item.id}
        icon={<ResultIcon name={item.icon} IconComponent={item.IconComponent} />}
        description={item.descriptionNode ?? item.description}
        component={getPluginLinkComponent(item.pluginLink)}
        onClick={clearSearch}
      >
        <HighlightedText html={item.title} />
      </MenuItem>
    );
  }

  // Fallback: no renderer found — visible but not clickable
  if (!item.pathname) {
    return (
      <MenuItem
        key={item.id}
        icon={<ResultIcon name={item.icon} />}
        description={item.description}
        isDisabled
      >
        <HighlightedText html={item.title} />
      </MenuItem>
    );
  }

  // Orama result — existing path
  return (
    <MenuItem
      key={item.id}
      icon={<ResultIcon name={item.icon} IconComponent={item.IconComponent} />}
      description={
        item.description ? <HighlightedText html={item.description} /> : undefined
      }
      component={getLinkComponent(item.pathname)}
      onClick={clearSearch}
    >
      <HighlightedText html={item.title} />
    </MenuItem>
  );
};
```

Three render paths:
1. **Custom component** — plugin fully owns rendering (`item.Component`)
2. **Plugin-resolved** — `PluginLink` + `descriptionNode` from `resolve()`
3. **Fallback** — disabled `MenuItem`, no link, raw resource name
4. **Orama** — existing `Link` + highlighted text (nav, settings, etc.)

### `inventorySearch` → `useInventorySearch` hook

`inventorySearch` is currently a plain async function. To consume resolved extensions, it becomes a hook that uses `useResolvedExtensions` from the SDK:

```typescript
// packages/gui/src/hooks/useInventorySearch.tsx

function useInventorySearch(): {
  search: (term: string) => Promise<SearchResultItem[]>;
  loaded: boolean;
} {
  const [extensions, loaded] = useResolvedExtensions<SearchResultRendererExtension>(
    isSearchResultRendererExtension,
  );

  const rendererMap = useMemo(() => {
    const map = new Map<string, ResolvedRenderer>();
    for (const ext of extensions) {
      map.set(ext.properties.resourceType, {
        label: ext.properties.label,
        resolve: ext.properties.resolve,
        icon: ext.properties.icon,
      });
    }
    return map;
  }, [extensions]);

  const search = useCallback(async (term: string): Promise<SearchResultItem[]> => {
    const escaped = term.replace(/"/g, '\\"');
    const parts = [
      `resource.name.startsWith("${escaped}")`,
      `resource.name.startsWith("clusters/${escaped}")`,
      `resource_type.startsWith("${escaped}")`,
    ];
    const results = await client.searchAll({ filter: parts.join(" || ") });

    return results.map((result) => {
      const renderer = rendererMap.get(result.resourceType);
      if (!renderer) {
        return { /* fallback: disabled, no link */ };
      }
      const rendered = renderer.resolve(result);
      return {
        /* pluginLink + badgedDescription(renderer.label, rendered.description) */
      };
    });
  }, [rendererMap]);

  return { search, loaded };
}
```

The renderer map is keyed by `resourceType` directly — no encoding step. The `label` from the extension is used to render a compact PF `Label` badge in the search result description.

### Category grouping

All inventory results go under a single **"Resources"** group. The `label` from the renderer extension (e.g., "Kind Cluster", "GCP HCP Cluster") is not used for category splitting — too many small groups creates clutter when results are sparse. The label is available for future use (e.g., sub-headers within the Resources group).

### `resolve` is synchronous

The `resolve` function is sync. It maps resource data to display props — no I/O needed. The resource data is already available from the inventory API response. Plugin code loading (the async part) happens in `useRendererIndex` before `resolve` is ever called.

### Fallback

When no plugin provides a renderer for a `resourceType`:

- **Visible** — the user should know the resource exists
- **Not clickable** — no `PluginLink`, `MenuItem` is `isDisabled`
- **Raw description** — shows `resourceType` and `state` as plain text
- **Generic icon** — `SearchIcon` (default from `ResultIcon`)
- **No descriptionNode** — falls back to string `description`

This handles: addon provides backend resources but hasn't shipped a UI plugin, or the UI plugin is not installed.

## Files Changed

### New

| File | Layer | What |
|------|-------|------|
| `packages/build-utils/src/extensions/searchResultRenderer.ts` | Build | `RENDER_SEARCH_TYPE`, `createSearchResultRenderer`, types |
| `packages/build-utils/src/extensions/__tests__/searchResultRenderer.test.ts` | Build | Builder output, validation, `validateExtensionSet` integration |
| `packages/common/src/searchResultRenderer.ts` | Common | `SearchResultRender`, `InventoryResource`, `SearchResultResolve` types |
| `packages/gui/src/extensions/isSearchResultRendererExtension.ts` | Discovery | Type guard for `fleetshift.render-search` extensions |
| `packages/gui/src/hooks/useInventorySearch.tsx` | Rendering | Hook: `useResolvedExtensions` → renderer map → search function |
| `packages/mock-ui-plugins/src/plugins/kind-plugin/KindSearchResult.tsx` | Plugin | `resolveKindCluster`, `KindClusterIcon` re-export |
| `packages/mock-ui-plugins/src/plugins/gcphcp-plugin/GcpHcpSearchResult.tsx` | Plugin | `resolveGcpHcpCluster`, `GcpHcpClusterIcon` re-export |

### Modified

| File | Layer | What |
|------|-------|------|
| `packages/build-utils/src/extensions/validate.ts` | Build | `case RENDER_SEARCH_TYPE` in validation switch |
| `packages/build-utils/src/extensions/index.ts` | Build | Re-export builder, type constant, types |
| `packages/common/src/index.ts` | Common | Re-export `SearchResultRender`, `InventoryResource`, `SearchResultResolve` |
| `packages/mock-ui-plugins/rspack.config.ts` | Build | `createSearchResultRenderer` + `exposedModules` for both plugins |
| `packages/gui/src/components/Search/FleetSearch.tsx` | Rendering | `useInventorySearch` hook, `PluginLink` branch, fallback branch |
| `packages/gui/src/components/Search/searchIndex.ts` | Rendering | `SearchResultItem` gains `pluginLink?` + `descriptionNode?` fields |

### Deleted

| File | Why |
|------|-----|
| `packages/gui/src/components/Search/inventorySearch.ts` | Replaced by `useInventorySearch` hook |

### Not changed

| File | Why |
|------|-----|
| `SearchProvider.tsx` | Mock clusters removed; inventory results come from live API via `useInventorySearch` |

## Open Questions

- [x] **~~Expose `ListTypes` via HTTP?~~** — Not needed for v1. The search results carry the `resourceType` — sufficient for on-demand discovery.

- [x] **~~Extension type encoding~~** — Resolved. Static type `fleetshift.render-search` with `resourceType` as a property. No encoding needed — SDK regex rejects digits/underscores from base64url.

- [x] **~~Extension ID from resource name?~~** — Resolved. `resourceType` is the type-level key. `name` is instance-level, passed to `resolve()`.

- [x] **~~`inventorySearch` → hook refactor~~** — Resolved. Becomes `useInventorySearch` hook with `useResolvedExtensions`. See Layer 3.

- [x] **~~MenuItem click handling~~** — Resolved. PF `MenuItem` renders a `<button>` by default; nested `<a>` tags don't navigate. Use MenuItem's `component` prop with a cached `forwardRef` wrapper around `PluginLink`. `PluginLink` itself also gained `forwardRef` support.
