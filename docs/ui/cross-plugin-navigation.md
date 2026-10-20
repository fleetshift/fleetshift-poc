# Cross-Plugin Navigation

Cross-plugin navigation decouples links from another plugin's base URL and provides a fallback when an optional destination has no registered page. Plugins address pages by plugin scope and exposed module; the shell resolves that identity against the backend-provided page registry.

Pages are declared with `ocm-fleet.route`, optionally inside an `ocm-fleet.route-group`. These contribution contracts use route terminology; reference-based navigation retains the JavaScript `module` key and exposed names such as `ClustersModule` and `PodsModule`. See [routes and route groups](route-groups.md) for default links, visibility, and canonical paths.

## Current resolution

`ScalprumShell` implements `api.fleet.getPluginPagePath(scope, module)`. It finds the first matching entry in `pluginPages` and returns its route, or `undefined` when no entry matches. The resolver is wired into the shell today.

Page paths come from backend configuration. Navigation layout and local customization affect presentation; they are not inputs to this resolver. A page can remain resolvable when absent from the visible sidebar.

Sources:

- [Shell resolver](../../client/web/src/components/Root/ScalprumShell.tsx)
- [Page metadata and routes](../../server/internal/transport/http/uiconfig.go)
- [Browser configuration](../../client/web/src/contexts/AppConfigContext.tsx)

## Shared helpers

`@ocm-fleet/common` exports:

- `usePluginNavigate(scope, module)`: returns `navigate` and `available`; re-resolves the base route when navigation is invoked.
- `PluginLink`: renders a router link for a resolved page, or the supplied fallback when resolution fails.

Both support a relative sub-path. The navigation hook also accepts an object containing `pathname` and `search`. `PluginLink` accepts a router `To` value and preserves object fields such as `hash`; the navigation hook does not provide the same hash option. Neither helper promises scrolling to a fragment.

```tsx
import { PluginLink } from "@ocm-fleet/common";

<PluginLink
  scope="core-plugin"
  module="PodsModule"
  to={`${clusterId}/${podUid}`}
>
  View pod
</PluginLink>;
```

The suffix is relative to the resolved module path. Callers remain responsible for the destination's parameter and context contract.

Sources: [navigation hook](../../sdk/common/src/usePluginNavigate.tsx), [link component](../../sdk/common/src/PluginLink.tsx), [pod-list usage](../../extensions/core/client/src/plugins/core-plugin/ClusterPodsTab.tsx).

## Availability has a narrow meaning

`available` means a matching page entry exists. It does not check local presentation enablement, user permissions, cluster capability, backend readiness, or whether the remote component loaded successfully.

The resolver takes the first match, so callers need an unambiguous scope/module identity. It does not select between multiple versions or multiple routes for the same pair. It also does not automatically switch cluster scope.

The source tree retains routing-plugin versions of the navigation hook, link component, and batch-link hook. Shared common helpers and those remote exports should not be confused with a guarantee that every plugin must load navigation through a remote hook.

Sources: [routing-plugin helpers](../../extensions/core/client/src/plugins/routing-plugin/), [shared shell API contract](../../sdk/common/src/scalprum.ts).

## Design relationship

The [addon UI architecture](../design/addon-ui-architecture.md#cross-plugin-navigation) establishes reference-based navigation as the decoupling mechanism between plugins. Current resolver behavior supplies that basic mechanism; version negotiation, readiness checks, and automatic context changes are not implied by it.
