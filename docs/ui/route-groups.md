# Routes, Route Groups, and Default Navigation

Route contracts make plugin pages addressable and discoverable by default, so users need no navigation customization to find new route contributions. Routes declare pages; route groups organize them into expandable sidebar containers and provide canonical URL prefixes, while user customization can rearrange presentation without changing those URLs; this document describes current behavior and its limits.

See [Feature Contracts](feature-contracts.md) for route metadata and the [route-groups diagram](diagrams/route-groups/route-groups.c4) for the flow from declarations to navigation. A route contribution is not a JavaScript exposed module: its component CodeRef selects an `exposedModules` entry, and registry/Scalprum/`PluginLink` fields named `module` still identify that exposed module.

## New standalone route to default link

For example, a packaged plugin with registry key `reports` and scope `reports-plugin` can declare this extension in its `FleetPlugin` extension array:

```typescript
import { createRoute } from "@ocm-fleet/build-utils";

createRoute({
  id: "reports",
  label: "Reports",
  component: { $codeRef: "ReportsPage.default" },
  icon: { $codeRef: "ReportsIcon.default" },
});
```

`ReportsPage` and `ReportsIcon` must also be keys in that plugin's `exposedModules`. Once its manifest is included in the packaged `plugin-registry.json`, the default path is:

1. `generatePluginPages` projects the `ocm-fleet.route` declaration into `/api/ui/config`:

   ```json
   { "id": "reports.reports", "title": "Reports", "path": "reports/reports", "scope": "reports-plugin", "module": "ReportsPage", "pluginKey": "reports" }
   ```

2. `generateNavLayout` adds `{ "type": "page", "pageId": "reports.reports" }` to `/api/ui/user-config`'s `navLayout`. No separate nav-item declaration is needed.
3. The shell fetches both configurations. `ConsoleRoutes` registers `/reports/reports/*`; `AppNav` resolves the layout's page ID against `pluginPages` and renders **Reports** in Main. The browser link is `/app/reports/reports`.

This happens when the browser fetches configuration, normally when the app configuration provider mounts after a reload. The server reads the packaged registry on requests; an already-open shell does not watch that file, poll for new plugins, or subscribe to registry-change events. Adding assets alone, or registering an unrelated extension type, does not create a route/link. Missing backend navigation or a layout page ID absent from `pluginPages` produces no sidebar item.

Sources: [registry projection](../../server/internal/transport/http/uiconfig.go), [config fetch](../../client/web/src/contexts/AppConfigContext.tsx), [route registration](../../client/web/src/routes/ConsoleRoutes.tsx), [default sidebar](../../client/web/src/components/AppNav/AppNav.tsx).

## Declaring a group

`ocm-fleet.route-group` uses the shared `id`, `label`, optional `description`, `keywords`, and search customization properties. It has no component or required icon. A route references a group by its local ID:

```typescript
import { createRoute, createRouteGroup } from "@ocm-fleet/build-utils";

createRouteGroup({
  id: "settings",
  label: "Settings",
  description: "Workspace configuration and preferences",
  keywords: ["settings", "preferences", "configuration"],
});

createRoute({
  id: "navigation",
  label: "Navigation",
  group: "settings",
  component: { $codeRef: "SettingsPage.default" },
  icon: { $codeRef: "SettingsIcon.default" },
  description: "Manage nav layout and workspace preferences",
  keywords: ["nav", "order", "navigation"],
});

createRoute({
  id: "extensions",
  label: "Extensions",
  group: "settings",
  component: { $codeRef: "ExtensionsPage.default" },
  icon: { $codeRef: "ExtensionsIcon.default" },
});
```

For registry key `settings` and scope `settings-plugin`, these declarations produce `pluginPages` IDs `settings.navigation` and `settings.extensions`, with paths `settings/navigation` and `settings/extensions`. The backend emits a `settings` group containing those page IDs, as illustrated below. The default sidebar renders an expandable **Settings** group in Bottom with **Navigation**, then **Extensions**. Its header expands/collapses; it does not navigate. Visiting `/app/settings` separately redirects to `/app/settings/navigation`, the first declared child, regardless of alphabetical top-level ordering.

The current Settings plugin uses this group pattern. Route icons remain required even when the sidebar places the route inside a group.

Sources: [types](../../sdk/build-utils/src/extensions/types.ts), [builders](../../sdk/build-utils/src/extensions/builders.ts), [Settings declarations](../../extensions/core/client/rspack.config.ts).

## Authoring constraints and identity limits

`FleetPlugin` checks group IDs against the shared ID pattern and requires a route's string-valued group reference to exist in the same plugin's extension set. Duplicate route IDs within a group fail validation. Cross-plugin group references are not an authoring mechanism.

These checks do not provide runtime namespace isolation:

- The server collects groups and their children by bare group ID across the registry. Different plugins using the same group ID can collide or combine.
- Grouped route prefixes are global, not plugin-qualified.
- Build validation permits the same route ID in different groups, but the server still gives both pages the ID `{pluginKey}.{routeId}`. Page maps and route keys can then collide.

Use distinct group IDs across plugins and distinct route IDs throughout a plugin. This avoids current projection limitations; it is not a claim that the platform enforces global uniqueness.

Sources: [set validation](../../sdk/build-utils/src/extensions/validate.ts), [group collection and page generation](../../server/internal/transport/http/uiconfig.go).

## Routes and redirects

| Declaration | Router-relative route |
|-------------|-----------------------|
| Ungrouped route | `/{pluginKey}/{routeId}` |
| Grouped route | `/{groupId}/{routeId}` |
| Nonempty backend group | `/{groupId}` redirects to its first child page |

For example, Navigation is `/settings/navigation`, Extensions is `/settings/extensions`, and the ungrouped Clusters route is `/core/clusters`. Browser URLs include the `/app` basename.

The server emits a flat `pluginPages` list. The shell sorts pages by descending path length and registers `/{path}/*` routes. It separately derives group redirects from the backend `navLayout`. Groups do not render their own page components; a landing page must be a route contribution.

Redirects use the backend group's first child, not the user's customized order. Custom groups do not receive routes or redirects. Moving a page into another sidebar group does not move its route.

Sources: [server projection](../../server/internal/transport/http/uiconfig.go), [console routes](../../client/web/src/routes/ConsoleRoutes.tsx), [navigation customization](nav-customization.md).

## Backend navigation layout

`GET /api/ui/config` supplies global plugin bootstrap data and `pluginPages`. `GET /api/ui/user-config` supplies `navLayout`; the current layout is registry-derived rather than identity-personalized.

```json
{
  "navLayout": [
    { "type": "page", "pageId": "core.clusters" },
    {
      "type": "group",
      "groupId": "settings",
      "pluginKey": "settings",
      "label": "Settings",
      "children": [
        { "type": "page", "pageId": "settings.navigation" },
        { "type": "page", "pageId": "settings.extensions" }
      ]
    }
  ]
}
```

Group children follow route declaration order within a plugin. The server iterates a registry map, so it does not establish a stable alphabetical order for every top-level entry. It emits each declared group at its first encountered child; a group with no pages is not emitted. A grouped page referencing an unknown group falls back to a standalone layout item (while retaining its grouped canonical path). The explicit built-in `orchestration-detail` page is excluded from default navigation.

Source: [UI configuration handlers](../../server/internal/transport/http/uiconfig.go).

## Sidebar rendering

`AppNav` merges the backend layout with a saved override, or uses legacy flat ordering when no override exists. Groups render as PatternFly `NavExpandable`; their headers expand or collapse rather than navigate. Empty groups with no known child pages are hidden.

`CORE_EXTENSION_META` remains keyed by plugin scope. A plugin-defined group's main/bottom section is inferred from `${pluginKey}-plugin`, and its children stay in that section. Custom groups default to the main section. Plugin-defined group headers are label-only; custom groups have an icon-rendering path with the limitations described in [Navigation Customization](nav-customization.md).

With no full override or legacy saved order, top-level pages and groups sort alphabetically by display label **within each section**. Group children retain backend order, not alphabetical order. `signing-plugin` and `settings-plugin` are classified Bottom; all other scopes, including new scopes absent from `CORE_EXTENSION_META`, default Main. A saved legacy order puts listed IDs first and sorts remaining entries by label. With a full override, `mergeLayout` preserves the user's arrangement and incorporates new pages; see [Navigation Customization](nav-customization.md#reconciliation-with-backend-navigation).

### Visibility, enablement, and load states

Registration, the browser-local install preference, sidebar hiding, and authorization are separate:

| Condition | Sidebar and destination behavior |
|-----------|----------------------------------|
| Registered page referenced by `navLayout`, no customization | Link appears by default, without an extension-resolution or backend-readiness gate |
| Local install preference is false | Link remains, with puzzle icon and enable tooltip; clicking opens the enablement page if the scope is configured |
| No saved install preference | `CORE_EXTENSION_DEFAULTS` applies; Configuration and Virtualization default disabled, other listed plugins enabled; an unknown scope defaults enabled |
| Scope absent from Scalprum config, page still present | Link remains; destination shows **Plugin unavailable** |
| Route extension missing from resolved SDK results, page still present | Link remains from backend metadata; its plugin-provided icon may be missing |
| Page reference absent from `pluginPages` | Item is skipped; a group with no known child pages is hidden |
| Hidden through navigation customization | Entry moves into **More** rather than losing its route; direct links and feature search remain available |
| Backend operation forbidden | Link visibility and local enablement grant no permission; API authorization applies separately |

The install preference is scoped to the plugin, not an individual route, and stored in browser-origin IndexedDB. Initializing defaults fills missing known keys without overwriting saved choices. “Install” here means local presentation enablement, not packaging, server registration, addon lifecycle enablement, or downloading a plugin on demand: `PluginLoader` attempts all configured plugin loads independently of that preference.

Initial config loading and the loader's initial load attempt defer shell content; `AppNav` also waits for local navigation preferences to load. Those are loading boundaries, not proof that every plugin loaded successfully. `AppNavItem` does not remove links while install state loads or errors, or while CodeRefs resolve. At the destination, `PluginPage` checks scope presence first, then shows a spinner while install state loads, **Extension state unavailable** on a state error, or the enablement screen when disabled. An enabled page uses the Scalprum loading spinner and **Failed to load** fallback for component load errors. Neither that fallback nor an SDK icon-resolution failure retracts the backend link.

Sources: [nav item](../../client/web/src/components/AppNav/AppNavItem.tsx), [group child filtering](../../client/web/src/components/AppNav/AppNavGroup.tsx), [install hook](../../sdk/common/src/useExtensionInstall.ts), [defaults and ExtensionStore](../../sdk/common/src/extensionInstall.ts), [page states](../../client/web/src/pages/PluginPage.tsx), [initial loader](../../client/web/src/components/Root/PluginLoader.tsx), [ordering](../../sdk/common/src/orderByIds.ts).

Page active state compares the current path with the page's canonical path. Group active/expanded state instead tests the `/{groupId}/` prefix. It therefore does not reliably follow moved children or custom groups whose IDs differ from their children's route prefixes.

Sources: [AppNav](../../client/web/src/components/AppNav/AppNav.tsx), [group rendering](../../client/web/src/components/AppNav/AppNavGroup.tsx), [page rendering](../../client/web/src/components/AppNav/AppNavItem.tsx), [section metadata](../../sdk/common/src/extensionInstall.ts).

## Search hierarchy

Search reads groups from the merged navigation layout, including groups inside More. Group entries and route contributions use category `nav`; routes link to their parent through `feature: "group.{groupId}"`. Cluster-provider entries can nest below routes. Other extension types do not automatically enter this hierarchy.

The result UI renders an indented tree and injects missing ancestors. It does not provide collapsible search headers. Orama relevance determines result order; sidebar ordering is not a search-order guarantee. Legacy flat ordering is not applied by the search provider.

Group search links currently use the first child's first route segment. For an unchanged plugin group, that usually reaches its canonical redirect. For a custom or rearranged group, it can target another group or an unregistered path such as `/core`. It is not a reliable "open first customized child" operation.

There is no generic shell breadcrumb chain derived from `navLayout`. Individual pages may render their own breadcrumbs independently.

Sources: [SearchProvider](../../client/web/src/components/Search/SearchProvider.tsx), [search result rendering](../../client/web/src/components/Search/FleetSearch.tsx), [search diagram](diagrams/search/search.c4).

## User customization

The Navigation settings page supports nested ordering, custom-group creation/editing/deletion, moving pages between groups, icon selection, and hiding/restoring entries. These are implemented browser-local preferences, not pending route-group features.

The canonical shapes and reconciliation functions live in [navLayout.ts](../../sdk/common/src/navLayout.ts). See [Navigation Customization](nav-customization.md) for persistence, reset behavior, and current edge cases.
