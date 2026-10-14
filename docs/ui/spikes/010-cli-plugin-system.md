# SPIKE: CLI Plugin System

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (CLI)
**Status**: Done
**Related**: [008 - Environment-Agnostic Plugin SDK and Scalprum](008-sdk-environment-agnostic.md)

## Problem

The CLI needs to be extensible in the same way as the GUI. When a cluster has an observability plugin installed, the GUI gets a metrics page. The CLI should get a corresponding command. When a new plugin is added to a cluster, the CLI should pick up new commands without being rebuilt or redeployed.

The plugin system explored in the GUI spike (001) uses the Dynamic Plugin SDK and Scalprum on top of Module Federation. These tools assume a browser environment and cannot be used directly in a Node.js CLI (see spike 008). The question is how to bring the same extensibility model to the CLI using Module Federation directly, while sharing the same backend infrastructure and plugin discovery as the GUI.

## What was explored

### Module Federation in Node.js

The CLI uses `@module-federation/enhanced` with its runtime API to load remote modules. This is the same underlying technology as the GUI but without the SDK wrapper. The CLI creates a Module Federation instance at startup, registers remote plugins, and loads their exposed modules on demand.

The key difference from the GUI is that there is no script tag injection or DOM-based loading. The Node.js runtime plugin from `@module-federation/node` handles the loading in a way that works outside the browser.

### How plugins expose CLI commands

Each CLI plugin exposes one or more modules through Module Federation. In the POC, each exposed module exports a function that takes the API base URL and a cluster ID, fetches data, and returns a React element rendered by Ink. The CLI discovers what a plugin exposes from its manifest and registers each exposed module as a command. The exact contract for plugin commands will need to be defined — the POC uses a simple function signature but the final design may use a more structured extension model similar to the GUI.

### Shared backend and plugin discovery

The CLI and GUI share the same backend. The server already tracks which clusters have which plugins enabled. For the GUI, it serves a Scalprum config with manifest locations. For the CLI, it serves a separate registry endpoint with Module Federation manifests.

Plugin discovery works the same way regardless of the consumer — when a cluster is added and its plugins are detected (via CRDs, API groups, etc.), both the GUI and CLI benefit. The only difference is the registry format: the GUI registry contains Scalprum manifests, the CLI registry contains raw Module Federation manifests.

### Distinguishing GUI and CLI extensions

A plugin can contribute to both the GUI and the CLI. In the GUI, a plugin might expose pages, widgets, and dashboard components. In the CLI, it might expose commands. These are different extension types with different contracts.

The distinction happens at the plugin build level — the webpack config declares what the plugin exposes for each environment. The server serves separate registries for GUI and CLI, each containing only the relevant extensions. A plugin that only has GUI extensions will not appear in the CLI registry, and vice versa.

### Cluster-aware command filtering

Just like in the GUI, CLI commands from plugins are only available for clusters that have the plugin enabled. If the observability plugin is installed on the production cluster but not on staging, the metrics command will only accept "production" as a cluster argument. Tab completion is aware of this — it only suggests clusters where the command is valid.

### SDK and Scalprum updates are required

The GUI plugin system is built on the Dynamic Plugin SDK and Scalprum. These provide the extension type system, plugin manifests, dependency resolution, and runtime loading abstractions. The CLI cannot use them today because they assume a browser environment (spike 008).

For the CLI and GUI to share a single plugin architecture — where a plugin can contribute both UI pages and CLI commands from the same build — the SDK and Scalprum need to be updated to work in both environments. Without these updates, the CLI has to reimplement plugin loading, manifest handling, and discovery separately, which defeats the purpose of having a shared plugin system.

### Current state (proof of concept)

The CLI plugin system works end to end in the POC:

- Plugins are loaded at startup from the CLI plugin registry.
- Exposed modules become CLI commands automatically.
- Commands are filtered by cluster plugin enablement.
- Tab completion knows which commands are available for which clusters.

Limitations of the POC:

- The CLI bypasses the Dynamic Plugin SDK and uses Module Federation directly. Once the SDK is updated to be environment-agnostic (spike 008), the CLI should use the SDK for consistency with the GUI.
- There is no extension type system for the CLI. The GUI has typed extension contracts (nav-item, dashboard-widget). The CLI just expects a default function export. A more structured extension model would help as the CLI grows.
- Plugin loading happens once at startup. If plugins change during a session (e.g., a new cluster is installed), the CLI does not pick them up until restart.

### Open questions

- Should the CLI support the same extension type system as the GUI, or is a simpler contract sufficient?
- How do we handle plugin dependencies in the CLI? In the GUI, the SDK manages load order. Without the SDK, the CLI would need its own dependency resolution.
- Should plugins be able to contribute sub-commands to existing command groups, or only top-level commands?

## Key files

- Plugin loading and registration: `packages/cli/src/plugins.ts`
- CLI entry point (plugin init): `packages/cli/src/cli.tsx`
- Command registration: `packages/cli/src/commands/index.tsx`
- CLI webpack config (MF setup): `packages/cli/webpack.config.ts`
- Server CLI plugin registry: `packages/mock-servers/src/cliPluginRegistry.ts`
