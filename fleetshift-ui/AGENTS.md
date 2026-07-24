# Rules for Agents

FleetShift UI monorepo — React 18 shell + Scalprum micro-frontend plugins, rspack + Module Federation.

## Source of truth

**Code > docs > diagrams.** Disagree → code wins, update doc/diagram. Design docs: `docs/`. Architecture diagrams: `docs/diagrams/` (LikeC4 `.c4`). Update on design changes.

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

- TypeScript strict. `no-explicit-any` is enforced — use `unknown` + narrowing or define a type. In tests, `as unknown as X` is acceptable for stubs; in production code, prefer type guards over type assertions.
- Import order: side-effect imports first, then node_modules, then local (relative). Enforced by `simple-import-sort`. Run `npm run lint:fix` to auto-sort.
- ESLint flat config + Prettier (double quotes, trailing commas). `npm run lint` / `lint:fix`.
- Never generate `.js`/`.d.ts` in `src/` — build artifacts go in `dist/`.

## SCSS class naming

All CSS classes are scoped with a prefix to prevent collisions across MF boundaries. BEM convention.

| Scope | Prefix | Example |
|-------|--------|---------|
| Shell (gui) | `ome-` | `ome-search`, `ome-search__menu`, `ome-search__menu--open` |
| Core plugin | `ome-core-` | `ome-core-clusters`, `ome-core-clusters__toolbar` |
| Overview plugin | `ome-overview-` | `ome-overview-dashboard`, `ome-overview-capacity__bar` |
| GCP HCP plugin | `ome-gcphcp-` | `ome-gcphcp-wizard`, `ome-gcphcp-wizard__step` |
| Day One plugin | `ome-day-one-` | `ome-day-one-welcome`, `ome-day-one-welcome__card` |
| Signing plugin | `ome-signing-` | `ome-signing-keys`, `ome-signing-keys__form` |
| Management plugin | `ome-mgmt-` | `ome-mgmt-targets`, `ome-mgmt-targets__row` |
| Kind plugin | `ome-kind-` | `ome-kind-wizard`, `ome-kind-wizard__step` |
| Settings plugin | `ome-settings-` | `ome-settings-nav-order`, `ome-settings-nav-order__item` |

Enforced by stylelint (`stylelint.config.mjs`) with per-plugin overrides. Run `npm run lint:css` to check.

**PF utility classes first.** For simple spacing, font, color, display, flex — use PF utility classes (`pf-v6-u-mb-md`, `pf-v6-u-font-size-sm`, `pf-v6-u-text-color-subtle`, `pf-v6-u-display-flex`, `pf-v6-u-flex-1`, etc.) directly in `className`. Don't create a custom SCSS class just to set `margin-bottom: var(--pf-t--global--spacer--md)`. Custom `ome-*` classes are for multi-property styles, component-specific layouts (gap, grid), or things PF utilities don't cover.

**Conditional classes → `clsx`.** Use `clsx` (already in mock-ui-plugins) for combining className strings conditionally. No manual template literals or ternaries for class composition.

```tsx
// GOOD
import clsx from "clsx";
<div className={clsx("pf-v6-u-mb-md", isActive && "ome-core-active")} />

// BAD
<div className={`pf-v6-u-mb-md ${isActive ? "ome-core-active" : ""}`} />
```

**Vendor class overrides** (`pf-*`, `react-*`): allowed ONLY nested inside your own `ome-*` class — never as top-level selectors. Apply a custom `ome-*` className to the element, then nest the vendor override inside it.

```scss
// GOOD — scoped override
.ome-core-clusters__toolbar {
  .pf-v6-c-toolbar__item { flex-basis: auto; }
}

// BAD — unscoped top-level vendor selector
.pf-v6-c-toolbar__item { flex-basis: auto; }
```

## Plugins

- Registered as `DynamicRemotePlugin` in `rspack.config.ts`.
- Directory: `src/plugins/<name>-plugin/` — components, `api.ts`, hooks.
- Extensions declare UI capabilities; Go backend reads manifests for navigation.
- Shared deps (react, PF, scalprum, oidc) are MF singletons. New shared dep → update `sharedModules`.
- `ScalprumComponent` `module` must match `exposedModules` key exactly — no `./` prefix.

## Build & MF

- Rspack 2.x with `builtin:swc-loader` (not webpack, not ts-loader).
- PF imports: barrel → granular dynamic paths via SWC `transformImport` templates + `PfModuleReplacementPlugin` (corrects naive paths using `dynamic-modules.json`). `getDynamicModules` for MF shared entries, `createPfTransformImport` + `createPfModuleReplacementPlugin` from `@fleetshift/build-utils`.
- `@fleetshift/common` in plugins → must be in MF `sharedModules`.
- Entry point: async boundary (`index.ts` → `import("./bootstrap")`).

## Commands

```bash
npm run build:all          # common → plugins → GUI → merge
npm run lint               # eslint + stylelint
npm run lint:fix           # auto-fix both
npm run lint:css           # stylelint only
npm test                   # vitest
```

## Diagrams

- `docs/diagrams/` — LikeC4 `.c4` files. Config: `docs/diagrams/likec4.config.json`.
- **Code > diagrams.** Changed search/extensions/build/plugin code → validate `.c4` still matches. Diverged → update diagram.
- **Generic, not specific.** Model pattern (modules → extensionPoints → extensions), not instances. Specific types = examples in descriptions only.
- Current:
  - `feature-contract.c4` — build validation, extension model, Go backend manifests, shell rendering. Trigger: `packages/build-utils/src/extensions/`, `packages/mock-ui-plugins/rspack.config.ts`, `packages/gui/src/extensions/`.
  - `search.c4` — indexing, extensionPoint linking, query/grouping. Trigger: `packages/gui/src/components/Search/`.

## Verification

- Don't run builds to verify. Use LSP diagnostics + browser MCP.
- App served on port 8085. `/debug` route for plugin/nav troubleshooting.
- `npm run lint` + `npm test` before done.
