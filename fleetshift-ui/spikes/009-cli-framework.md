# SPIKE: CLI Framework

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (CLI)
**Status**: Done

## Problem

FleetShift needs a CLI tool alongside the web UI. Some users prefer working in the terminal — scripting, automation, SSH sessions where there is no browser, or simply personal preference. The CLI needs to cover the same core operations as the GUI: listing resources, managing clusters, installing and enabling plugins.

Building a CLI from scratch with raw stdout writes and manual input handling is tedious and error-prone. Layout, colors, spinners, tables — all of this has to be reinvented. And since the team already knows React from building the GUI, it would be ideal to use the same mental model for the terminal.

## What was explored

### Ink.js as the rendering framework

Ink is a React renderer for the terminal. Instead of rendering to the DOM, it renders to stdout. Components use the same patterns as a React web app — JSX, hooks, state, effects — but the output is terminal text with layout, colors, and interactivity.

The main reason for choosing Ink is familiarity. The team builds the GUI with React. Using the same component model for the CLI means no context switch. A developer who knows how to build a React component can build a CLI component without learning a new framework.

Ink provides layout primitives (`Box` for flexbox containers, `Text` for styled text), input handling, and a component library (`@inkjs/ui`) with ready-made elements like text inputs, spinners, status messages, and badges. It also supports theming for consistent styling across the tool.

### Three rendering modes

The CLI supports three modes of operation, each suited to a different use case:

**Single command mode.** The user runs a command like `fleetshift pods prod-cluster`, gets the output, and the process exits. This is classic CLI behavior — good for scripting, piping to other tools, and automation. The tool renders the result once and terminates.

**Interactive scrolling mode (default).** A REPL-like experience where the user types commands and sees results. Past output scrolls up naturally with the terminal's own scrollback. An input prompt sits at the bottom with tab completion. This mode behaves like a normal terminal session — output flows naturally and can be scrolled through.

**Interactive fullscreen mode.** The tool takes over the entire terminal using the alternate screen buffer. It draws its own borders, has a scrollable content area, and manages layout explicitly. This is closer to a terminal application than a traditional CLI. It is better for focused interactive work where the user wants a contained experience without mixing with other terminal output.

All three modes render the same command output — the difference is how the frame around it behaves. Commands return React elements regardless of the mode, so adding a new command does not require thinking about which mode it runs in.

### Input handling and navigation

The interactive modes include features that make the CLI usable for repeated work:

- **Tab completion** with context-aware suggestions. The suggestions change based on where the user is — at the root level, inside a sub-menu, or partway through typing a command that takes a cluster argument. Commands that require a cluster only suggest clusters that have the relevant plugin enabled.
- **Command history** via up and down arrows, with the current unfinished input stashed while navigating.
- **Sub-menu navigation.** Some command groups (like user management and cluster configuration) can be entered as a sub-menu with its own prompt and suggestion set. The `back` command returns to the root.

### Current state (proof of concept)

The CLI works end to end in the POC:

- Single command, scrolling, and fullscreen modes all function.
- Commands cover listing pods, nodes, alerts, deployments, clusters, and managing plugin enablement.
- Tab completion is cluster-aware and plugin-aware.
- The fullscreen mode handles terminal resize and scrolling.

Limitations of the POC:

- There are no live updates. All data is fetched once per command via REST. There is no streaming or real-time refresh.
- Output formatting is basic. Tables are rendered but there is no pagination, filtering, or sorting within the CLI itself.
- The fullscreen mode's scrolling is somewhat limited — large outputs can be slow to navigate.

### Open questions

- Should the fullscreen mode support split panes or tabbed views for monitoring multiple resources simultaneously?
- Is there a need for a watch mode that re-runs a command on an interval and refreshes the output in place?
- How should the CLI handle very large outputs (thousands of pods)? Pagination, streaming, or piping to a pager?

## References

- Ink.js: https://github.com/vadimdemedes/ink

## Key files

- CLI entry point: `packages/cli/src/cli.tsx`
- Main interactive app: `packages/cli/src/App.tsx`
- Scrolling frame: `packages/cli/src/components/ScrollingFrame.tsx`
- Fullscreen frame: `packages/cli/src/components/FullScreenFrame.tsx`
- Single command executor: `packages/cli/src/components/SingleCommand.tsx`
- Input handling and completion: `packages/cli/src/hooks/useCommandInput.ts`
- Tab completion menu: `packages/cli/src/components/CompletionMenu.tsx`
- Theme: `packages/cli/src/theme.ts`
