# UI Feature Contracts

Feature contracts give independently built plugins a predictable way to contribute pages and extension slots, with metadata the console can use for routing and feature discovery. This document describes current metadata, routing, and feature-search behavior; see the [feature-contract diagram](diagrams/feature-contract/feature-contract.c4) for the overall flow.

## Plugin declarations

Plugins declare extensions through builders from `@ocm-fleet/build-utils` in their rspack configuration. `FleetPlugin` validates the extension set before delegating Module Federation output to the Dynamic Plugin SDK.

Every extension has a type and properties. The shared properties are:

| Property | Contract |
|----------|----------|
| `id` | Required local identifier matching `/^[a-z][a-z0-9-]*$/` |
| `label` | Required nonempty display string |
| `description` | Optional summary |
| `keywords` | Optional search terms |
| `searchResult` | Optional custom feature-search component CodeRef |
| `searchIcon` | Optional feature-search icon CodeRef |

A CodeRef has the form `{ $codeRef: "ModuleName.exportName" }`. The module name identifies an entry in the declaring plugin's `exposedModules`. A type declaration is not proof that the runtime export has the expected shape; see [Extension Validation](extension-validation.md).

An `ocm-fleet.route` additionally requires `component` and `icon` CodeRefs. It declares a page and its default navigation link, and may declare a `group` and named `extensionPoints`. Each extension point contains `type` and `description`; the slot itself does not declare another route or a runtime validation function. A route contribution is distinct from the JavaScript exposed module it renders: `RouteProperties`/`RouteExtras` and `RouteGroupProperties`/`RouteGroupExtras` name contribution contracts, while `exposedModules`, CodeRefs, and Scalprum/`PluginLink` `module` props retain Module Federation terminology.

```typescript
createRoute({
  id: "clusters",
  label: "Clusters",
  component: { $codeRef: "ClustersModule.default" },
  icon: { $codeRef: "ClustersIcon.default" },
  description: "View and manage your fleet of clusters",
  keywords: ["cluster", "fleet", "create"],
  extensionPoints: {
    providers: {
      description: "Cluster providers available in the creation wizard",
      type: "ocm-fleet.cluster-provider",
    },
  },
});
```

Other contracts include route groups (`ocm-fleet.route-group`, built with `createRouteGroup`), setup steps, cluster providers, onboarding actions, cluster-detail tabs, and inventory search renderers. An extension point advertises a contribution type; the consuming component implements how those contributions are selected and rendered. Matching metadata does not automatically create routes, navigation entries, or search results for every extension type.

Sources: [extension types](../../sdk/build-utils/src/extensions/types.ts), [builders](../../sdk/build-utils/src/extensions/builders.ts), [FleetPlugin](../../sdk/build-utils/src/extensions/FleetPlugin.ts), [core plugin declarations](../../extensions/core/client/rspack.config.ts).

## Page identity and route ownership

The resource manager server projects route metadata into flat `pluginPages` entries containing `id`, `title`, `path`, `scope`, `module`, and `pluginKey`. The `module` field remains the exposed JavaScript module key, not the route ID.

| Route metadata | Router-relative path |
|-----------------|----------------------|
| Usable `id` and `group` | `/{group}/{id}` |
| Usable `id`, no group | `/{pluginKey}/{id}` |
| Missing or unusable `id` | Slug derived from label |

For a usable ID, the page ID is `{pluginKey}.{id}`. The server extracts the exposed module name from the component CodeRef. Duplicate paths are skipped. These are projections of metadata, not runtime checks of plugin exports or route contents.

The shell registers each page at `/{path}/*`. The plugin module owns the suffix and query parameters. It can use internal `<Routes>`, an outlet, or query-driven UI; an `<Outlet>` is not a universal requirement. Browser URLs include the `/app` basename, so `/core/clusters` corresponds to `/app/core/clusters`.

Global `/api/ui/config` supplies plugin pages, plugin entries, and Scalprum configuration. `/api/ui/user-config` supplies `navLayout`. The current user-config layout is generated from the registry, not personalized by identity. Each projected route becomes a default page link or a child of its declared route group, without user customization. [Routes, Route Groups, and Default Navigation](route-groups.md) traces standalone and grouped declarations through both responses to visible links, including ordering, enablement, loading, and identity limits. Newly packaged declarations enter an open browser only after configuration is fetched again, not through live registry discovery.

Sources: [server UI configuration](../../server/internal/transport/http/uiconfig.go), [console routes](../../client/web/src/routes/ConsoleRoutes.tsx), [browser router](../../client/web/src/App.tsx).

## Cross-plugin navigation

`PluginLink` and `usePluginNavigate` from `@ocm-fleet/common` address a page by plugin scope and exposed module name:

```tsx
<PluginLink scope="core-plugin" module="ClustersModule" to="my-cluster">
  View cluster
</PluginLink>

// Opens the Kind creation wizard on the Clusters page.
navigate({ search: "?create=kind" });
```

Here `navigate` is obtained from `usePluginNavigate("core-plugin", "ClustersModule")`. A string or object `pathname` is appended to the resolved base path; `search` supplies the query string. Callers must provide a suffix appropriate to the target module. The helpers do not validate that its internal route exists.

The resolver returns the first `pluginPages` match for `(scope, module)`. Hook `available` means that such a page exists, not that the plugin is enabled, authorized, healthy, or loaded. Missing targets make navigation a no-op and make `PluginLink` render its fallback, which defaults to nothing. Local enablement is checked separately when the page renders.

`PluginLink` accepts React Router's `To` object and preserves its `hash`. The navigation hook's `PluginNavigateTo` supports only `pathname` and `search`. Feature-search composition also uses only pathname and search. There is no automatic scroll-to-heading contract.

Sources: [shell resolver](../../client/web/src/components/Root/ScalprumShell.tsx), [PluginLink](../../sdk/common/src/PluginLink.tsx), [navigation hook](../../sdk/common/src/usePluginNavigate.tsx), [page enablement](../../client/web/src/pages/PluginPage.tsx).

## Cluster-provider example

The Clusters route lives at `/core/clusters`, rendering the exposed `ClustersModule`. `?create` opens provider selection; `?create=kind` opens the Kind wizard. A provider requires a description and icon, card, and wizard CodeRefs:

```typescript
createClusterProvider({
  id: "kind",
  label: "Kind",
  description: "Create a local Kind cluster for development and testing.",
  keywords: ["kind", "local", "development", "testing"],
  to: { search: "?create=kind" },
  icon: { $codeRef: "KindProviderCard.KindIcon" },
  card: { $codeRef: "KindProviderCard.default" },
  wizard: { $codeRef: "CreateClusterWizard.default" },
  searchIcon: { $codeRef: "KindIcon.default" },
});
```

The page component implements provider selection and renders the selected wizard. The provider metadata does not generate a `/kind` child route.

Sources: [Kind declarations](../../extensions/kind/client/rspack.config.ts), [Clusters page](../../extensions/core/client/src/plugins/core-plugin/ClustersPage.tsx), [provider modal](../../extensions/core/client/src/plugins/core-plugin/CreateClusterModal.tsx).

## Feature search

`SearchProvider` eagerly resolves route and cluster-provider extensions through the SDK. It indexes route labels, descriptions, and keywords, fallback pages from `pluginPages`, a debug entry, and groups from the merged navigation layout. It does not build a metadata-only index over arbitrary extension types.

Route contributions are matched to pages by label, which makes duplicate labels ambiguous. Declared provider extension points establish parent links, and the provider scan composes the parent's path with `to.pathname` and `to.search`. A synthetic "Create cluster" entry is added for a cluster-provider extension point. Other extension-point types are not automatically indexed.

Groups, routes, and provider entries use category `nav`, displayed as "Pages". Parent relationships use the `feature` field. Results form an indented tree, not collapsible search groups. Orama ranks matches; the index keeps at most five raw hits per category before missing ancestors are injected. Sidebar customization changes grouping, not relevance ordering. Hidden items remain searchable, and disabled route pages are marked `not-enabled` rather than universally excluded.

Custom `searchResult` components receive `{ title, description }`; `searchIcon` is used by provider entries, while route entries use their route icon. These customization properties are not uniformly consumed across every extension type. Inventory results use the separate [search-result renderer contract](search-result-renderer-contract.md); [advanced search](cel-advanced-search.md) executes CEL against the resource API.

Sources: [SearchProvider](../../client/web/src/components/Search/SearchProvider.tsx), [index query](../../client/web/src/components/Search/searchIndex.ts), [search rendering](../../client/web/src/components/Search/FleetSearch.tsx), [search diagram](diagrams/search/search.c4).

## Asset delivery: current implementation and target design

Currently, the server reads a local `plugin-registry.json`, advertises plugin bootstrap metadata, and serves configured frontend assets under `/app`. Origin-rooted asset paths are rewritten to that prefix.

The established direction is independently packaged frontend assets distributed outside the resource manager, without fleetlet asset uploads. The older [addon UI distribution design](../design/addon-ui-architecture.md) describes OCI artifacts, catalog metadata and asset URLs, with direct browser fetches from an external host. The newer [Client Asset Separation draft](../design/client-asset-separation.md) instead proposes deployment-owned asset policy and a same-origin Gateway/Operator arrangement. Their delivery and authority differences remain unresolved; neither is implemented by the current local registry-serving path. See [Plugin Discovery](plugin-discovery.md#draft-asset-trust-refinement).
