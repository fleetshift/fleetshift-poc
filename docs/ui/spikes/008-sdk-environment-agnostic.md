# SPIKE: Environment-Agnostic Plugin SDK and Scalprum

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - Shared Build Tooling
**Status**: Done

## Problem

The Dynamic Plugin SDK and Scalprum were built for browser-based micro-frontends. They assume they are running in a browser — using `window` for global state, relying on DOM APIs, and generating webpack configurations that target web environments. This works for the GUI but breaks when we try to use the same plugin system for the CLI.

FleetShift has both a web UI and a CLI. Ideally, both share the same plugin architecture — plugins declare extensions, the host discovers and loads them at runtime via Module Federation. The extension system, shared stores, and remote hooks should work the same way regardless of whether the host is a browser application or a Node.js process.

Right now, the CLI plugins have to bypass the SDK entirely and use `@module-federation/enhanced` directly because the SDK's webpack plugin and runtime cannot run outside the browser. This means two separate build setups, no shared tooling, and no shared runtime abstractions between GUI and CLI plugins.

## What was explored

### What needs to change

The core issue is browser-specific globals and APIs scattered through the SDK and Scalprum internals. The main changes needed are:

- **Replace `window` with `globalThis`**. JavaScript has a standard global object reference (`globalThis`) that works in browsers, Node.js, and other environments. The SDK and Scalprum use `window` to store shared state (plugin registries, shared scope, etc.). Switching to `globalThis` makes the same code work everywhere.

- **Avoid DOM-specific APIs**. Some parts of the SDK interact with the DOM (e.g., creating script elements to load remote modules). In a Node.js environment, there is no DOM. The module loading path needs to be abstracted so it can use different strategies depending on the environment.

- **Use the Module Federation runtime package**. Module Federation has a runtime package (`@module-federation/runtime` or similar) that handles the low-level operations — shared scope initialization, remote module loading, container management. The SDK currently does some of this manually with browser-specific code. Delegating to the MF runtime would make the SDK environment-agnostic for free, since the MF runtime already supports both browser and Node.js.

- **Update the SDK's webpack plugin**. The `DynamicRemotePlugin` webpack plugin generates configurations that assume a web target. It needs to be flexible enough to generate configs for Node.js targets as well, or at least not force browser-specific settings.

- **Rspack compatibility**. Rspack is a fast Rust-based webpack alternative that is increasingly adopted. If the SDK fully adopts `@module-federation/enhanced` for its Module Federation setup, Rspack compatibility should come largely for free since `@module-federation/enhanced` supports both webpack and Rspack. However, any internal code in the SDK's webpack library that uses webpack-specific APIs directly would need to be reviewed and updated to work with Rspack's plugin system as well.

### Current state

The GUI uses the Dynamic Plugin SDK and Scalprum as intended — the `DynamicRemotePlugin` webpack plugin generates the plugin manifests and Module Federation config, and Scalprum loads and manages the plugins at runtime.

The CLI plugins use `@module-federation/enhanced` directly, bypassing the SDK. They work, but they lose the extension system, the manifest format, and the shared runtime that the GUI plugins benefit from.

The gap is clear: the SDK and Scalprum need targeted updates to remove browser assumptions so that one plugin system can serve both environments.

### What stays the same

The extension model (plugins declaring what they contribute), the shared store pattern, remote hooks, and the manifest format do not need to change conceptually. These are environment-agnostic ideas — they just need their implementations to stop reaching for browser globals.

### Open questions

- Should the updates be contributed upstream to the Dynamic Plugin SDK and Scalprum repositories, or should we maintain a fork?
- Are there other environments beyond browser and Node.js that we should consider (e.g., Deno, edge runtimes)?
- How do we handle the webpack plugin — one plugin that detects the target, or separate plugins for web and node?

## Key files

- CLI plugin config (uses MF directly): `packages/cli/webpack.config.ts`
- GUI plugin config (uses SDK): `packages/mock-ui-plugins/webpack.config.ts`
- SDK webpack plugin: `@openshift/dynamic-plugin-sdk-webpack` (external)
- Scalprum core: `@scalprum/core` (external, source at `/Users/martin/scalprum/scaffolding/`)
