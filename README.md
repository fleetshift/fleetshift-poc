# FleetShift User Interface

Multi-cluster Kubernetes management dashboard. Composable pages built from dynamically loaded plugins via Module Federation and Scalprum.

## Architecture

npm workspaces monorepo with packages under `packages/`:

| Package | Description |
|---------|-------------|
| `@fleetshift/gui` | React SPA shell — Module Federation host with Scalprum plugin loading |
| `@fleetshift/mock-ui-plugins` | Webpack Module Federation remote plugins (management, routing) |
| `@fleetshift/common` | Shared types and signing helpers (dual CJS/ESM) |
| `@fleetshift/build-utils` | Shared webpack helpers — PatternFly dynamic module sharing and ts-loader import transforms |

## Serving Model

The Go backend (`fleetshift-poc/fleetshift-server`) serves everything on a single port:

| Prefix | Handler | Purpose |
|--------|---------|---------|
| `/v1/` | gRPC-gateway | Deployment, AuthMethod, SignerEnrollment APIs |
| `/api/ui/` | UI config handlers | OIDC config, plugin registry, user config |
| `/` | Static file server | index.html, JS/CSS bundles, plugin assets (SPA fallback) |

There is no webpack dev server or standalone frontend process. All static assets are built in a container and served by the Go backend on **port 8085**.

## Development

No host Node.js or npm installation is required. The full stack (frontend build + Go backend + Keycloak) runs in containers via the POC repo:

```bash
cd ../fleetshift-poc
task deploy:dev       # builds frontend in a container, starts Go backend serving on :8085
```

Open [http://localhost:8085](http://localhost:8085) once the stack is up.

### Local Watch Mode

For iterating on the frontend without rebuilding containers on every change:

```bash
# Terminal 1 — start the stack with local web assets
cd ../fleetshift-poc
task podman:dev LOCAL_WEB=true

# Terminal 2 — watch & rebuild on file changes
npm install
npm run dev              # full build + watch mode
```

Changes to plugin or GUI source are automatically rebuilt by webpack and merged into `web/`. The Go backend serves them from the host bind mount — just refresh the browser.

Use `npm run dev:watch` to skip the initial full build if `web/` already has current assets.

### Building Locally (optional)

If you do have Node.js 22+ and npm installed:

```bash
npm install
npm run build:all     # common -> plugins -> GUI -> merge into web/
```

Output goes to `web/` — pass `--web-dir=./web/` to the Go backend to serve it.

### Individual Package Builds

```bash
npm run build --workspace=packages/common
npm run build --workspace=packages/mock-ui-plugins
npm run build --workspace=packages/gui
```

## Containerized Build

`Dockerfile.web` builds all frontend assets inside a UBI9 container. It is used as an init container in the compose stack — the Go backend mounts the output volume and serves the assets.

```
Dockerfile.web  ->  web-builder init container  ->  /srv/web volume  ->  Go backend serves /
```

## Lint

```bash
npm run lint          # check
npm run lint:fix      # auto-fix
```

## Auth

OIDC config (authority, client ID) is fetched dynamically from `GET /api/ui/config` at app startup. No Keycloak-specific settings are hardcoded in this repo. User info (username, roles) is extracted from Keycloak token claims client-side.

## Plugin Architecture

Plugins are Module Federation remotes loaded via Scalprum. Each plugin is a `DynamicRemotePlugin` in `packages/mock-ui-plugins/webpack.config.ts`:

| Plugin | Key | Purpose |
|--------|-----|---------|
| management-plugin | management | Targets, Orchestration, Signing Keys |
| routing-plugin | routing | Shared routing utilities (PluginLink, usePluginNavigate) |
| day-one-plugin | day-one | Day One setup wizard, initial OIDC auth configuration |
| core-plugin | core | Cluster list and detail pages |

Plugin pages and nav layout are computed by the Go backend from `plugin-registry.json`. The GUI fetches this from `GET /api/ui/user-config`.

## Day One Setup

The `/setup` route renders the Day One plugin's `InitialSetupForm` module via Scalprum, outside the auth gate. This allows first-time users to configure OIDC authentication before any identity provider is available.

A single `ScalprumProvider` serves both the setup route and authenticated routes — plugin config is fetched unauthenticated from `/api/ui/user-config`, so Scalprum can load the Day One plugin without a valid token.

The setup form configures OIDC auth via the backend API and receives real-time provisioning status via WebSocket. Once setup completes, the user is redirected to the login page to authenticate with the newly configured provider.

