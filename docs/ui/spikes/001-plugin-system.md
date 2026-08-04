# SPIKE: Plugin System

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (Web UI)
**Status**: Done

## Problem

FleetShift needs a frontend that can change what it shows based on the environment it runs in and who is using it. Different clusters have different capabilities installed. Different users care about different things. The UI cannot be a single hardcoded application — it has to figure out at runtime what pieces to load and how to put them together.

Specifically:

- The set of UI features depends on which plugins are enabled on each cluster. A cluster with observability installed should show metrics. One without it should not.
- Users have different roles (ops vs dev) and preferences. The nav and available pages should reflect that.
- New features need to be added without rebuilding or redeploying the shell application.
- Multiple teams should be able to ship UI modules independently.

## Approach

Use the OpenShift Dynamic Plugin SDK and Scalprum, both built and maintained at Red Hat. These tools exist specifically for this problem — they have been used in the OpenShift Console, Hybrid Cloud Console, and other products. They build on top of webpack Module Federation, which is the standard way to compose applications from independently built and deployed modules at runtime.

### How it works

**Module Federation** lets you build separate webpack bundles that can share dependencies (React, PatternFly, etc.) and load each other's modules at runtime. Instead of one giant bundle, you have a shell app and N remote modules that get pulled in on demand.

**Scalprum** wraps Module Federation with a runtime API. It initializes the shell with a config map of available remotes and their manifest locations. It provides React bindings that let you work with remote modules as regular React components and hooks — no special loading or wiring needed from the consumer side. It also lets the shell pass a shared API object to all plugins so they can access common data and callbacks.

**Dynamic Plugin SDK** adds an extension system on top. Plugins declare what they contribute (pages, widgets, tabs, etc.) as typed extensions. The shell discovers and resolves these at runtime — no hardcoded knowledge of what plugins exist. The SDK also provides build tooling that generates the plugin manifest and wires up Module Federation automatically from the extension declarations.

### What the shell does

1. On startup, the shell asks the server for the current user's config. The server figures out which plugins are relevant based on what clusters are installed and what plugins they have enabled. It returns a Scalprum config (what to load and from where), a list of pages contributed by plugins, and the user's nav layout.

2. The shell loads all plugin manifests and registers them with Scalprum.

3. Routes are generated dynamically from the plugin pages. Each route renders the corresponding remote component.

4. The nav sidebar is built from the user's nav layout, which references the plugin pages.

5. Dashboard widgets and detail page sub components are discovered from plugin extensions at runtime and rendered where they belong.

### How plugins are structured

Each plugin is a self-contained module with:
- A build config that declares what it exposes and what extensions it contributes.
- Access to the shell's shared API for things like the API base URL, the current cluster IDs, and an event bus.
- Components that fetch data and render UI using PatternFly.
- Extension declarations that tell the shell what kind of thing the plugin provides (a page, a widget, a tab, etc.).

### Shared dependencies

React, ReactDOM, react-router-dom, Scalprum, the dynamic-plugin-sdk, shared utilities, are shared as Module Federation singletons so they are not duplicated across plugins. PatternFly is shared at the individual component level using a build-time TypeScript transform that rewrites barrel imports to per-component paths.

### Scope model

The shell tracks a "scope" — either "all clusters" or a specific cluster. Plugins receive a list of cluster IDs which is the intersection of the current scope and the clusters that have the plugin enabled. This way a plugin always gets the right set of clusters without needing to know about the scope system.

## Key files

- Shell setup: `packages/gui/src/App.tsx`
- Plugin build config: `packages/mock-ui-plugins/webpack.config.ts`
- Extension type contracts: `packages/gui/src/utils/extensions.ts`
- Server config assembly: `packages/mock-servers/src/routes/users.ts`
- Plugin registry loader: `packages/mock-servers/src/pluginRegistry.ts`
- Build utils (PF transforms, shared module scanning): `packages/build-utils/src/`
