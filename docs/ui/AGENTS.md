# Rules for Agents

OCM Fleet console — React 19 shell + Scalprum micro-frontend plugins, Rspack + Module Federation.

## Source of truth

**Code > docs > diagrams.** Disagree → code wins, update doc/diagram. Design docs: `docs/ui/`. Architecture diagrams: `docs/ui/diagrams/` (LikeC4 `.c4`). Update on design changes.

`index.md` is the reading guide. Document implemented behavior accurately, including important limitations. Preserve established future direction with explicit status; do not present draft proposals or removed prototypes as implemented contracts. Asset distribution remains planned: `docs/design/addon-ui-architecture.md` and the draft `docs/design/client-asset-separation.md` describe different delivery arrangements that still need reconciliation.

Implemented UI references belong directly in `docs/ui/`. Use `spikes/` only for unimplemented experiments; retired link stubs may remain for existing references. Move implemented topics into the reference set and update inbound links. Mark future sections explicitly within reference pages.

## Packages

- `client/web` (`@ocm-fleet/web`) — Shell SPA (routing, auth, search, layout). No business logic.
- `extensions/core/client` (`@ocm-fleet/mock-ui-plugins`) — Core plugins under `src/plugins/<name>-plugin/`.
- `extensions/kind/client` (`@ocm-fleet/kind-plugin`) — Kind local cluster provider plugin (separated from core).
- `sdk/common` (`@ocm-fleet/common`) — Shared types, utils, cross-plugin hooks. Dual CJS/ESM.
- `sdk/build-utils` (`@ocm-fleet/build-utils`) — Rspack helpers (PF transforms, FleetPlugin). No build step.
- `e2e` — Playwright tests.

## Components

- Small files. One component, one job. Split at ~250 lines.
- Repeated JSX → extract component, iterate over data array.
- `useMemo`/`useCallback` only for expensive computation or stable-ref requirements. Not everywhere.
- Functional programming. Pure functions, hooks, composition. No classes for UI logic.
- Compose but don't over-abstract. If a shared component needs >2-3 boolean flags for variants, it's two components. Some duplication beats cognitive complexity.
- Collocate. Plugin components, hooks, API helpers, types live together in its directory.

## State

- Cross-plugin → Scalprum shared stores as hooks, re-exported via `@ocm-fleet/common`. Pattern: `usePluginNavigate`.
- Intra-plugin → React hooks. Keep state local.
- API data → fetched where needed. Each plugin has `api.ts` with typed fetch helpers against `/v1alpha/*`.

## Testing

- Unit tests for edge cases and bug candidates, not happy-path snapshots.
- Tests next to code (`__tests__/` or `.test.ts`). Vitest + `@testing-library/react`.
- **Component tests**: Playwright CT (`@playwright/experimental-ct-react`). Config per package (`playwright.config.ts`). Test files: `*.ct.tsx` in `__tests__/`. Mount components directly or via harness wrappers for components needing context providers (DDF, routers). See `client/web/src/components/Search/advanced/__tests__/advancedSearch.ct.tsx` for patterns.
- E2E in `e2e`, Playwright.

### Playwright CT notes

- Components mounted via `mount()` must be **imported**, not defined in the test file. For components requiring wrappers (FormRenderer, providers), create harness components in a separate `harnesses.tsx` file.
- Bootstrap files: `playwright/index.html` + `playwright/index.tsx` (imports PF CSS).
- For plugins using DDF, if CJS/ESM dual imports cause `FormRenderer` duplicate declaration errors, alias deep imports (e.g. `@data-driven-forms/pf4-component-mapper/form-template`) to ESM paths in `ctViteConfig.resolve.alias` and dedupe `@data-driven-forms/react-form-renderer` + `react-final-form`.

## Data Driven Forms (DDF)

The core plugin package retains `@data-driven-forms/react-form-renderer` + `@data-driven-forms/pf4-component-mapper` as placeholders for future schema-driven forms; neither is currently used.

- **Custom components**: DDF's pf4-component-mapper components are PF4-era. For PF6 controls (FormSelect, etc.), create custom DDF components using `useFieldApi` from `react-form-renderer`. Register in `componentMapper` with a custom string key.
- **Wizard steps**: Use `nextStep: "step-name"` to chain steps (without it, DDF shows "Submit" instead of "Next"). The DDF wizard step template uses PF4 class `pf-c-form`. Override with `StepTemplate` using `<div className="pf-v6-c-form">` — not `<Form>` to avoid form-in-form since `FormTemplate` already wraps in `<form>`.
- **Async validators**: Must **throw** error strings. `composeValidators` catches via `.catch(error => error)`. Sync validators return error strings.
- **Reactive updates in custom components**: Each field component must register via `useFieldApi(props)`. `useFormApi().getState().values` is a snapshot that won't re-render. For read-only access to all values, use `FormSpy` with `subscription={{ values: true }}`.
- **Wrapping existing components**: To progressively migrate, wrap existing step components as DDF fields. Use `useFieldApi` to bridge DDF form state to component props.

## Style

- TypeScript strict. `no-explicit-any` is enforced — use `unknown` + narrowing or define a type. In tests, `as unknown as X` is acceptable for stubs; in production code, prefer type guards over type assertions.
- Import order: side-effect imports first, then node_modules, then local (relative). Enforced by `simple-import-sort`. Run `npx nx run web:lint:fix` to auto-sort.
- ESLint flat config + Prettier (double quotes, trailing commas). `npx nx run web:lint`.
- Never generate `.js`/`.d.ts` in `src/` — build artifacts go in `dist/`.

## SCSS class naming

All CSS classes are scoped with a prefix to prevent collisions across MF boundaries. BEM convention.

| Scope | Prefix | Example |
|-------|--------|---------|
| Shell (web) | `ocm-fleet-` | `ocm-fleet-search`, `ocm-fleet-search__menu`, `ocm-fleet-search__menu--open` |
| Core plugin | `ocm-fleet-core-` | `ocm-fleet-core-clusters`, `ocm-fleet-core-clusters__toolbar` |
| Overview plugin | `ocm-fleet-overview-` | `ocm-fleet-overview-dashboard`, `ocm-fleet-overview-capacity__bar` |
| Day One plugin | `ocm-fleet-day-one-` | `ocm-fleet-day-one-welcome`, `ocm-fleet-day-one-welcome__card` |
| Signing plugin | `ocm-fleet-signing-` | `ocm-fleet-signing-keys`, `ocm-fleet-signing-keys__form` |
| Management plugin | `ocm-fleet-mgmt-` | `ocm-fleet-mgmt-targets`, `ocm-fleet-mgmt-targets__row` |
| Kind plugin | `ocm-fleet-kind-` | `ocm-fleet-kind-wizard`, `ocm-fleet-kind-wizard__step` |
| Settings plugin | `ocm-fleet-settings-` | `ocm-fleet-settings-nav-order`, `ocm-fleet-settings-nav-order__item` |

Enforced by stylelint (`stylelint.config.mjs`) with per-plugin overrides. Run `npx nx run web:lint:css` / `npx nx run plugins:lint:css` to check.

**PF utility classes first.** For simple spacing, font, color, display, flex — use PF utility classes (`pf-v6-u-mb-md`, `pf-v6-u-font-size-sm`, `pf-v6-u-text-color-subtle`, `pf-v6-u-display-flex`, `pf-v6-u-flex-1`, etc.) directly in `className`. Don't create a custom SCSS class just to set `margin-bottom: var(--pf-t--global--spacer--md)`. Custom `ocm-fleet-*` classes are for multi-property styles, component-specific layouts (gap, grid), or things PF utilities don't cover.

**Conditional classes → `clsx`.** Use `clsx` (already in extensions/core/client) for combining className strings conditionally. No manual template literals or ternaries for class composition.

```tsx
// GOOD
import clsx from "clsx";
<div className={clsx("pf-v6-u-mb-md", isActive && "ocm-fleet-core-active")} />

// BAD
<div className={`pf-v6-u-mb-md ${isActive ? "ocm-fleet-core-active" : ""}`} />
```

**Vendor class overrides** (`pf-*`, `react-*`): allowed ONLY nested inside your own `ocm-fleet-*` class — never as top-level selectors. Apply a custom `ocm-fleet-*` className to the element, then nest the vendor override inside it.

```scss
// GOOD — scoped override
.ocm-fleet-core-clusters__toolbar {
  .pf-v6-c-toolbar__item { flex-basis: auto; }
}

// BAD — unscoped top-level vendor selector
.pf-v6-c-toolbar__item { flex-basis: auto; }
```

## Plugins

- Declared through `FleetPlugin` in `rspack.config.ts`; it validates metadata before delegating to `DynamicRemotePlugin`.
- Core plugins: `extensions/core/client/src/plugins/<name>-plugin/`. Separated plugins (Kind): `extensions/<name>/client/src/` with own `rspack.config.ts` and `package.json`.
- Extensions declare UI capabilities; Go backend reads manifests for navigation.
- Page contributions use `ocm-fleet.route`/`createRoute` and `ocm-fleet.route-group`/`createRouteGroup`; contribution identity is `routeId`. Use route/route-group terminology for contracts, types, guards, and docs. Preserve JavaScript Module Federation names (`exposedModules`, CodeRefs, `module` props, and exposed component names).
- New routes generate default links through `pluginPages` and `navLayout` without customization. See [routes and route groups](route-groups.md) before describing visibility, ordering, or enablement. Registry/config fetching is not live plugin discovery.
- Shared deps (react, PF, scalprum, oidc) are MF singletons. New shared dep → update `sharedModules`.
- `ScalprumComponent` `module` must match `exposedModules` key exactly — no `./` prefix.

## Build & MF

- Rspack 2.x with `builtin:swc-loader` (not webpack, not ts-loader).
- PF imports: barrel → granular dynamic paths via SWC `transformImport` templates + `PfModuleReplacementPlugin` (corrects naive paths using `dynamic-modules.json`). `getDynamicModules` for MF shared entries, `createPfTransformImport` + `createPfModuleReplacementPlugin` from `@ocm-fleet/build-utils`.
- `@ocm-fleet/common` in plugins → must be in MF `sharedModules`.
- Entry point: async boundary (`index.ts` → `import("./bootstrap")`).

## Commands

npm workspaces are declared at the repo root. Run `npm install` from the repo root.

```bash
# Via Nx (preferred — cached, dependency-aware):
npx nx run common:build    # shared types/helpers
npx nx run plugins:build   # MF remote plugins
npx nx run web:build       # SPA shell
npx nx run web:dev         # rebuild SPA into repo-root web/ (serve via AIO + LOCAL_WEB)
npx nx run web:dev:watch   # watch-rebuild only (skip initial build)
npx nx run web:lint                 # eslint
npx nx run web:lint:css             # stylelint
npx nx run common:test              # vitest
```

## Diagrams

- `docs/ui/diagrams/<name>/` — each diagram is its own LikeC4 project (`likec4.config.json` + `.c4`). Projects must stay separate: LikeC4 merges all `.c4` files under one config into a single model.
- **Code > diagrams.** Changed search/extensions/build/plugin code → validate `.c4` still matches. Diverged → update diagram.
- **Generic, not specific.** Model pattern (routes → extensionPoints → extensions), not instances. Specific types = examples in descriptions only.
- Current:
  - `feature-contract/` — build validation, extension model, Go backend manifests, shell rendering. Trigger: `sdk/build-utils/src/extensions/`, `extensions/core/client/rspack.config.ts`, `client/web/src/extensions/`.
  - `route-groups/` — route grouping and default navigation model.
  - `search/` — indexing, extensionPoint linking, query/grouping. Trigger: `client/web/src/components/Search/`.

## Verification

- Don't run builds to verify. Use LSP diagnostics + browser MCP.
- App served on port 8085. `/app/debug` route for plugin/nav troubleshooting.
- `npx nx run web:lint` + `npx nx run common:test` before done.
- Documentation-only audits: verify relative links, examples, source claims, and `git diff --check`. No application build is required. Validate LikeC4 syntax when tooling is available; report when it is not.
