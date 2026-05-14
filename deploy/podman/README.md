# Podman (Local Development)

Local container-based deployment using podman and docker-compose. Runs the full FleetShift stack on your workstation with optional local Keycloak.

## Prerequisites

- **podman** — container runtime
- **[docker-compose](https://docs.docker.com/compose/install/)** — `podman-compose` is not compatible
- **jq** — JSON processing
- **kind** — for local cluster provisioning
- `.env` file — copy from `.env.template`

> **macOS:** Podman only forwards IPv6 loopback. Add this one-time `/etc/hosts` entry or Keycloak will be unreachable:
> ```bash
> echo "::1 keycloak" | sudo tee -a /etc/hosts
> ```

## Quick Start

```bash
cp .env.template .env         # configure (edit as needed)
task build:all                # build all Go binaries
task podman:up                # start the stack (demo mode)
task podman:cli-setup         # configure fleetctl CLI
bin/fleetctl auth login       # log in (opens browser)
```

For development with hot-reload:

```bash
task podman:dev               # builds from source, mounts source dirs
```

## Deploy Modes

| Mode | DB | Auth | Use Case |
|------|-----|------|----------|
| `demo` (default) | SQLite | Local Keycloak | Local dev, demos |
| `prod` | PostgreSQL | External OIDC | Production-like |

Override axes independently with `DB=sqlite|postgres` and `AUTH=local|external`.

```bash
task podman:up DEPLOY_MODE=prod
task podman:up DB=postgres AUTH=local
```

## Dev Mode

`task podman:dev` builds frontend assets in a container (using `Dockerfile.web` from the UI repo) and starts the Go backend serving everything on `:8085`. No host Node.js or npm required. Requires `UI_DIR` in `.env` pointing to the `fleetshift-user-interface` repo.

After changing Go code, run `task podman:rebuild` to rebuild and restart. After changing frontend code, run `task podman:clean` then `task podman:dev` to rebuild the web assets.

### Local Web Watch Mode

For faster frontend iteration, serve assets directly from your host filesystem instead of rebuilding the Docker web-builder on every change:

```bash
# Terminal 1 — start the stack with local web assets
task podman:dev LOCAL_WEB=true

# Terminal 2 — watch & rebuild in the UI repo
cd /path/to/fleetshift-user-interface
npm run dev
```

This skips the Docker web-builder and bind-mounts the UI repo's `web/` directory into the container. Webpack watches for source changes, rebuilds, and the Go backend picks up the new assets — just refresh the browser.

Set `UI_DIR` in `.env` if the UI repo is not at the default `../../../fleetshift-user-interface` relative path.

## Tasks

All tasks use the `podman:` namespace (alias `pd:`).

| Task | Description |
|------|-------------|
| `podman:up` | Start the stack (demo mode by default) |
| `podman:dev` | Dev mode — source mounts + hot-reload |
| `podman:down` | Stop containers, preserve data |
| `podman:clean` | Stop + delete all data/volumes/network |
| `podman:rebuild` | Stop, rebuild images, restart |
| `podman:build` | Build container images without restarting |
| `podman:pull` | Pull latest images |
| `podman:logs` | Follow logs from all containers |
| `podman:logs:<service>` | Tail specific service (e.g. `podman:logs:fleetshift-server`) |
| `podman:status` | Show running containers |
| `podman:restart:<service>` | Restart a specific container |
| `podman:rebuild-web` | Rebuild frontend without restarting server |
| `podman:cli-setup` | Configure fleetctl for local auth |
| `podman:test-attestation` | Run end-to-end attestation flow |
| `podman:reset-keycloak` | Wipe Keycloak state (AUTH=local only) |

## Configuration

Copy `.env.template` to `.env` and edit. All available settings are documented in the template. Command-line variables always override `.env`.
