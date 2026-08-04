# SPIKE: Cross-Plugin Navigation

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (Web UI)
**Status**: Done

## Problem

Plugins need to link to each other. A pod list might offer a "View Deployment" action that takes the user to the deployment details page — which belongs to a different plugin. A cluster overview might link to the observability dashboard. These cross-plugin navigations happen all the time in a management UI.

The problem is that plugins do not know each other's URLs. The set of installed plugins varies by cluster. The pathnames are assigned by the shell based on what pages are registered, not hardcoded by the plugin authors. A plugin cannot just do `navigate("/deployments")` because it does not know if that path exists, or if the deployment plugin is even installed.

Hardcoding paths between plugins creates tight coupling — if a path changes or a plugin is not available, the link breaks silently. We need a way for plugins to reference each other by identity (which plugin, which module) rather than by URL.

## What was explored

### Referencing plugins by scope and module

Instead of navigating to a pathname, plugins reference a target by its plugin name (scope) and the module it exposes. The shell knows which modules are registered and what pathnames they are mounted at. A resolution function takes scope + module and returns the current pathname for that page.

This means a plugin can say "navigate to the DeploymentDetails module in the deployments-plugin" without knowing that it lives at `/deployment-details` or any other path. The shell resolves it at runtime based on what is currently registered.

Three utilities were built for this:

- **A navigation hook** — takes a scope and module, returns a `navigate()` function and an `available` flag. The navigate function accepts an optional sub-path or search params. It re-resolves the base path at call time so it always reflects the current state.
- **A link component** — wraps the standard router link but resolves the `to` path from scope + module. If the target plugin is not available, it renders a fallback (or nothing).
- **A batch hook** — same as the navigation hook but for multiple targets at once. Useful when a component needs to check availability of several plugin pages (e.g., to enable or disable multiple kebab actions).

All three rely on a resolution function provided by the shell through the shared API. The shell has access to the full page registry and can map any scope + module pair to its current pathname.

### Availability checking

A key part of this is knowing whether the target is available before rendering a link or enabling an action. If the deployment plugin is not installed on the current cluster, the "View Deployment" kebab action should be disabled rather than navigating to a broken route.

The resolution function returns `undefined` when the target is not found. The utilities expose this as an `available` boolean that components can use to conditionally render or disable navigation elements.

This also handles the case where a page exists in the plugin registry but has been removed from the navigation layout. The resolution falls back to the page registry — the page is still routable even if the user has hidden it from their nav.

### Constraint: one module, one path

This approach relies on each top-level page module being mounted at exactly one pathname. If the same module were registered at multiple paths, the resolver would not know which one to return. In practice this is a reasonable assumption — a page like "Deployment Details" exists once in the route tree, not at multiple URLs. As long as top-level page modules are unique in the routing, the resolution is reliable.

### Current state (proof of concept)

The routing utilities are implemented as a dedicated plugin that other plugins consume via remote hooks. The e2e tests verify the full flow — navigating from a pod list kebab menu to the deployment details page, including sub-paths and search parameters.

The shell-side resolution function (`getPluginPagePath`) is defined as part of the API contract but is not fully wired up yet. A production implementation would need the shell to build a lookup table from the page registry (mapping scope + module to pathname) and expose it through the shared API.

### Open questions

- Should the resolution function handle versioning? If a plugin exposes a new module name and old plugins still reference the old one, what happens?
- How do we handle navigation to a page that requires a specific cluster scope? Should the navigation utility also switch scope, or is that the caller's responsibility?
- Could this be extended to deep-link into specific tabs or sections within a plugin page?

## Key files

- Navigation hook: `packages/mock-ui-plugins/src/plugins/routing-plugin/usePluginNavigate.tsx`
- Link component: `packages/mock-ui-plugins/src/plugins/routing-plugin/PluginLink.tsx`
- Batch navigation hook: `packages/mock-ui-plugins/src/plugins/routing-plugin/usePluginLinks.tsx`
- Plugin registration: `packages/mock-ui-plugins/webpack.config.ts`
- E2E tests: `packages/e2e/tests/routing-plugin.spec.ts`
