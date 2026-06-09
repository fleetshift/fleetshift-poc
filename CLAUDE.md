@AGENTS.md

## Claude-specific

- Don't run builds to verify. Use LSP diagnostics and browser MCP (`browser_screenshot`, `browser_snapshot`). App on port 8085. `/debug` for plugin troubleshooting.
- Only `npm run build` when explicitly asked.
- Scalprum patterns: consult the `scaffolding/` directory in the Scalprum repo (see CLAUDE.md in the main project for the full path), not training data.
- PatternFly: use PF MCP tools (`getAvailableModules`, `getComponentSourceCode`, `getPatternFlyDataViewExample`). Don't guess APIs.
