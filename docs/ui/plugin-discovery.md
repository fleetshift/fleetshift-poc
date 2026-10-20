# Plugin Discovery and Asset Distribution

Plugin discovery gives the shell an inventory of available UI contributions while keeping registration, executable asset sources, and target capability signals distinct. The current inventory is a build-generated registry served by the resource manager server; runtime addon registration and decoupled frontend asset distribution are established design direction, not the current registry implementation.

## Current bootstrap

1. Plugin builds produce manifests and frontend assets. The packaged web directory includes `plugin-registry.json`.
2. The resource manager reads that file from `WebDir` and assembles Scalprum configuration, plugin pages, and plugin entries.
3. The browser loads global metadata from `/api/ui/config` and default navigation from `/api/ui/user-config`.
4. The shell initializes Scalprum and loads each configured plugin from an embedded manifest or manifest location.

The current routes are:

| Route | Current role |
| --- | --- |
| `GET /api/ui/config` | Public pre-login plugin and OIDC bootstrap metadata |
| `GET /api/ui/plugin-registry` | Public build-generated registry |
| `GET /api/ui/user-config` | Default generated navigation; authentication required when OIDC is configured |
| `/app/` and asset paths below it | Shell and plugin assets served from the configured web directory |

The user-config handler does not currently persist a separate layout per identity. Browser navigation overrides and presentation enablement are held locally in IndexedDB.

`ocm-fleet.route` declarations in the packaged registry generate both `pluginPages` and default navigation links, optionally inside an `ocm-fleet.route-group`; no user customization is needed. An open shell does not auto-discover newly packaged plugins: its app configuration provider fetches configuration on mount, normally after reload. Local install preferences do not remove generated links. See [Routes and Route Groups](route-groups.md) for examples, ordering, and destination load/enablement states.

Sources:

- [Server registry/config assembly](../../server/internal/transport/http/uiconfig.go)
- [Static asset mounting and authentication wiring](../../server/internal/bootstrap/ui.go)
- [Browser bootstrap](../../client/web/src/contexts/AppConfigContext.tsx)
- [Plugin loading](../../client/web/src/components/Root/PluginLoader.tsx)
- [Local navigation overrides](../../sdk/common/src/useNavLayout.ts)

## Availability is not one plugin list

Keep these concerns distinct:

- **Registration:** which plugin identities, manifests, and extension metadata the platform offers.
- **Enablement and dependencies:** which addon capabilities are available under the addon lifecycle.
- **Target capability signals:** whether a feature has a relevant data source on a particular target.
- **User presentation:** which available pages the user chooses to show and how they are arranged.
- **Authorization:** which requests the platform and target permit.

The old mock server's CRD/API-group mapping, always-on plugin table, and cluster discovery endpoint are not current implementation. The shell's `getClusterIdsForPlugin` returns an empty list, and its cluster-change subscription is a stub; those APIs do not implement per-cluster plugin filtering today. See [Scalprum shell](../../client/web/src/components/Root/ScalprumShell.tsx).

Target capability signals do not establish compatibility with Fleet extension contracts or authorize introducing executable plugin code.

## Established addon and distribution direction

The [addon UI architecture](../design/addon-ui-architecture.md#addon-catalog) defines a runtime, per-instance addon catalog. The resource manager owns addon registration and lifecycle metadata; remote clusters report state rather than introducing new addon definitions. UI capability availability follows enablement and declared dependencies, rather than inherently requiring a backend in the same addon.

That design separates metadata from executable assets:

- UI assets are independently packaged as versioned OCI artifacts and deployed to an asset host.
- Registration supplies manifest and asset-location metadata.
- The resource manager is not the frontend asset store or distribution path.
- Fleetlet instances do not upload JavaScript bundles to the resource manager.
- Version retention and rollback belong to the artifact/distribution lifecycle.

The current local registry and merged web directory do not implement that runtime catalog or decoupled distribution model. The older design's illustrative `/v1alpha/addon-plugins` route is not the current bootstrap endpoint.

## Draft asset trust refinement

[Client Asset Separation](../design/client-asset-separation.md) is a draft refinement of ownership, routing, and integrity. The older addon UI design describes the browser fetching assets directly from an external host. The draft instead proposes an independently trusted shell, a Console Gateway for same-origin plugin delivery, and a Client Asset Operator for discovery and policy.

These are different delivery and authority arrangements. Their reconciliation remains unresolved here; the draft components and `/client/*` routes are not implemented or asserted as settled by this reference. Both documents retain the direction of moving executable frontend distribution out of the resource manager. Current `/api/ui/*` and `/app` behavior must not be confused with either future arrangement.
