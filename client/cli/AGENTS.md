# TypeScript CLI Rules

## Scope

- This package owns TypeScript `fleetctl`.
- Keep command groups under `src/commands/<group>/`.
- Keep every command in its own file.
- Keep parser, rendering, config, and transport code outside command groups.
- Do not add shared SDK or plugin loading yet.

## Structure

- `src/main.tsx`: process entry point and dispatch.
- `src/commands/registry.ts`: command registration only.
- `src/commands/help.ts`: help rendering.
- `src/commands/<group>/index.ts`: group metadata and child registration.
- `src/commands/<group>/<command>.tsx`: one command per file when JSX is needed.
- `src/__tests__/`: Vitest and Ink component tests.

## Verification

Run from repository root through Nx:

```text
npx nx run cli:build
npx nx run cli:test
npx nx run cli:lint
```

Use `ink-testing-library` for Ink components. Use `execa` for subprocess tests.

Executable name is `fleetctl`. Build through root target `npx nx run fleetctl`; it updates top-level `bin/fleetctl`.
