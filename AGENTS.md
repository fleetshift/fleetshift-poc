# Rules for Agents

FleetShift UI monorepo — React 18 shell + Scalprum micro-frontend plugins, webpack + Module Federation.

## Source of truth

**Code > docs.** If they disagree, code wins — update the doc. Design docs live in `docs/`. Update/create when making design changes.

## Packages

- `packages/gui` — Shell SPA (routing, auth, search, layout). No business logic.
- `packages/mock-ui-plugins` — All plugins under `src/plugins/<name>-plugin/`.
- `packages/common` — Shared types, utils, cross-plugin hooks. Dual CJS/ESM.
- `packages/build-utils` — Webpack helpers (PF transforms, ts-loader). No build step.
- `packages/e2e` — Playwright tests.

## Components

- Small files. One component, one job. Split at ~250 lines.
- Repeated JSX → extract component, iterate over data array.
- `useMemo`/`useCallback` only for expensive computation or stable-ref requirements. Not everywhere.
- Functional programming. Pure functions, hooks, composition. No classes for UI logic.
- Compose but don't over-abstract. If a shared component needs >2-3 boolean flags for variants, it's two components. Some duplication beats cognitive complexity.
- Collocate. Plugin components, hooks, API helpers, types live together in its directory.

## State

- Cross-plugin → Scalprum shared stores as hooks, re-exported via `@fleetshift/common`. Pattern: `usePluginNavigate`.
- Intra-plugin → React hooks. Keep state local.
- API data → fetched where needed. Each plugin has `api.ts` with typed fetch helpers against `/v1/*`.

## Testing

- Unit tests for edge cases and bug candidates, not happy-path snapshots.
- Tests next to code (`__tests__/` or `.test.ts`). Vitest + `@testing-library/react`.
- E2E in `packages/e2e`, Playwright.

## Style

- TypeScript strict. Avoid `any` — use `unknown` + narrowing or define a type.
- ESLint flat config + Prettier (double quotes, trailing commas). `npm run lint` / `lint:fix`.
- No default exports except MF-exposed page components.
- Prefer named function declarations for components (better DevTools + stack traces).
- Never generate `.js`/`.d.ts` in `src/` — build artifacts go in `dist/`.

## Plugins

- Registered as `DynamicRemotePlugin` in `webpack.config.ts`.
- Directory: `src/plugins/<name>-plugin/` — components, `api.ts`, hooks.
- Extensions declare UI capabilities; Go backend reads manifests for navigation.
- Shared deps (react, PF, scalprum, oidc) are MF singletons. New shared dep → update `sharedModules`.
- `ScalprumComponent` `module` must match `exposedModules` key exactly — no `./` prefix.

## Build & MF

- Webpack only (not rspack). Rspack MF breaks Scalprum cold start.
- `ts-loader` via `createTsLoaderRule` from `@fleetshift/build-utils`.
- PF imports: barrel → dynamic paths via AST transformer. `getDynamicModules` + `createTransformer`.
- `@fleetshift/common` in plugins → must be in MF `sharedModules`.
- Entry point: async boundary (`index.ts` → `import("./bootstrap")`).

## Commands

```bash
npm run build:all          # common → plugins → GUI → merge
npm run lint               # check
npm run lint:fix           # auto-fix
npm test                   # vitest
```

## Verification

- Don't run builds to verify. Use LSP diagnostics + browser MCP.
- App served on port 8085. `/debug` route for plugin/nav troubleshooting.
- `npm run lint` + `npm test` before done.
