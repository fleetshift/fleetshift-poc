# Plugin Versioning

FleetShift loads UI plugins from multiple connected clusters. Different clusters may have the same plugin installed but at different versions — for example, cluster A might run `monitoring-plugin@1.2.0` while cluster B runs `monitoring-plugin@1.4.0`.

## The Problem

The current plugin loading stack (Dynamic Plugin SDK + Scalprum + Module Federation) does not support loading multiple versions of the same plugin simultaneously. When two clusters provide the same plugin at different versions, only one version can be loaded — the other is either ignored or overwrites it.

This is because Module Federation uses `output.uniqueName` to identify a remote container in the global scope. If two builds of the same plugin share the same `uniqueName`, they collide — the runtime reuses the already-loaded container even though the code is different.

## Required Changes

### 1. Version-aware `output.uniqueName`

Webpack's [`output.uniqueName`](https://webpack.js.org/configuration/output/#outputuniquename) determines the global identifier for a Module Federation container. By default, it is derived from the name specified in the Module Federation configuration, which means all versions of the same plugin produce containers with the same identifier.

To support multiple versions, the plugin build must incorporate the version into the `uniqueName`. For example:

- Default: `monitoring-plugin`
- Versioned: `monitoring-plugin@1.4.0`

This ensures that two versions of the same plugin produce distinct containers that can coexist in the global scope without collision.

### 2. Extend the Dynamic Plugin SDK

The `@openshift/dynamic-plugin-sdk-webpack` `DynamicRemotePlugin` does not currently expose control over `output.uniqueName`. The SDK must be extended to allow the plugin version to influence the `uniqueName` used during compilation.

The version is already known at build time — the Dynamic Plugin SDK's `DynamicRemotePlugin` configuration includes a version field. The SDK just needs to use it to influence the `uniqueName`.

### 3. Update Scalprum to accept a version parameter

Scalprum's plugin loading mechanism needs to be aware of plugin versions so it can:

- Distinguish between two instances of the same plugin at different versions
- Load the correct version for each cluster context
- Re-use an already-loaded version when another cluster needs the same one (avoiding redundant downloads)

The Scalprum config and `ScalprumComponent` must accept a version parameter that maps to the versioned `uniqueName` used during the plugin's build.

### 4. Version plugins during build

Plugins must be properly versioned at build time. The version should be:

- Consistent with the deployed artifact (so the shell knows which version it is loading)
- Available in the plugin manifest (so the shell can determine the version before loading the entry script)
- Used to set `output.uniqueName` before compilation begins

## Requirements

### REQ-VER-1: Multiple versions of the same plugin must coexist

The runtime must support loading two or more versions of the same plugin simultaneously without collision. This is required because different clusters may provide the same plugin at different versions.

### REQ-VER-2: Plugin builds must produce version-unique containers

The plugin build pipeline must incorporate the version into the Module Federation `output.uniqueName` so that each version produces a distinct container in the global scope.

### REQ-VER-3: The Dynamic Plugin SDK must support versioned `uniqueName`

The `DynamicRemotePlugin` webpack plugin must be extended to use the plugin version (already present in its configuration) to set `output.uniqueName` automatically.

### REQ-VER-4: Scalprum must load plugins by name and version

Scalprum's loading mechanism must accept a version parameter, use it to resolve the correct versioned container, and re-use already-loaded versions when multiple clusters need the same one.

### REQ-VER-5: Plugin manifests must include the version

The plugin manifest must contain the version so the shell can determine which version it is loading before fetching the entry script. This version must match the `uniqueName` used during the build.

## Open Questions

- How do we handle breaking changes between versions of the same plugin? If a plugin's extension contract changes between v1 and v2, the shell needs to know which contract to use.
- Should the shell prefer the newest version when multiple clusters provide the same plugin, or should it load the version specific to each cluster independently?
- How does plugin versioning interact with shared module versions? Two versions of the same plugin may depend on different versions of shared libraries (e.g. PatternFly).
