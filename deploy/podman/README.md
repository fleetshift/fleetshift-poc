# Podman (Local Development)

Local container-based deployment using podman and docker-compose. Runs the full FleetShift stack on your workstation with optional local Keycloak.

## Prerequisites

- **podman** — container runtime
- **docker-compose** — `podman-compose` is not compatible
- **[jq](https://github.com/jqlang/jq)** — JSON processing
- **kind** — for local cluster provisioning
- **[mkcert](https://github.com/filosottile/mkcert)** - trusted dev cert for local keycloak
- `.env` file — copy from `.env.template`

**macOS:** Podman only forwards IPv6 loopback. Add this one-time `/etc/hosts` entry or Keycloak will be unreachable:
```bash
echo "::1 keycloak" | sudo tee -a /etc/hosts
echo "127.0.0.1 keycloak" | sudo tee -a /etc/hosts
```

## Quick Start

```bash
cp .env.template .env         # configure (edit as needed)
task build:cli                # build fleetctl Go binaries
task podman:up                # start the stack (demo mode)
task podman:cli-setup         # configure fleetctl CLI
bin/fleetctl auth login       # log in (opens browser)
```

`gcphcp` is opt-in in the Podman harness. A plain `task podman:up` starts the
default local addon set without `gcphcp`. To enable `gcphcp`, set
`GCPHCP_ENABLED=true` and `GCPHCP_GATEWAY_URL` (the only required value). The
seven optional `GCPHCP_*` overrides default in
`deploy/scripts/render-gcphcp-config.sh` when left empty:

```bash
GCPHCP_ENABLED=true
GCPHCP_GATEWAY_URL=https://<your-cls-gateway>
# Optional overrides — leave empty to use renderer defaults:
# GCPHCP_GATEWAY_AUDIENCE, GCPHCP_TARGET_ID, GCPHCP_GCP_PROJECT,
# GCPHCP_GCP_REGION, GCPHCP_WORKFORCE_POOL, GCPHCP_WORKFORCE_PROVIDER,
# GCPHCP_BROKER_SA_EMAIL

task podman:up AUTH=external
```

`GCPHCP_ENABLED=true` requires `AUTH=external`. The task fails early if local
Keycloak auth is selected. For `AUTH=external`, also set `OIDC_ISSUER_URL` in
`.env`.

At startup, the harness renders `deploy/podman/.gcphcp.yaml` from `.env`
(before Compose starts), mounts that file into `fleetshift-server`, and adds
`gcphcp` to the explicit addon list for the deployment.

## All-in-one image

Published CI image `quay.io/stolostron/fleetshift:latest` bundles the server (from `fleetshift-server-local`, including a container runtime CLI for kind), baked-in UI assets, and defaults to serving the UI. It is assembled on a schedule from `fleetshift-server-local:latest` and `fleetshift-web:latest`, so it can lag component merges by a few hours.

API + UI (runs as non-root by default):

Assumes an OIDC issuer is already running (not bundled). Use
`host.docker.internal` rather than `localhost` so the same issuer URL works
from the browser, the server process inside the container, and kind nodes
(`localhost` inside a container is that container, not the host). Map
`host.docker.internal` to loopback in the host’s `/etc/hosts` so the browser
can resolve it; add `--add-host=host.docker.internal:host-gateway` if the
runtime does not inject it. Compose Keycloak path:

`https://host.docker.internal:8443/auth/realms/fleetshift`

```bash
podman run --rm -p 8085:8085 -p 50051:50051 \
  --add-host=host.docker.internal:host-gateway \
  -e OIDC_ISSUER_URL=https://host.docker.internal:8443/auth/realms/fleetshift \
  quay.io/stolostron/fleetshift:latest
```

With kind provisioning (same privileges/socket pattern as compose). This path is a trusted local/dev tool: privileged + host container socket means full control of the host engine. With no GCP HCP variables, the image starts with `kind,kubernetes`. Supply only `-e GCPHCP_GATEWAY_URL=...` to activate `gcphcp` with the shared renderer defaults.

```bash
podman run --rm \
  --privileged --user 0:0 \
  -p 8085:8085 -p 50051:50051 \
  -v /tmp:/tmp \
  -v ${PODMAN_SOCKET:-/var/run/docker.sock}:/var/run/docker.sock \
  --add-host=host.docker.internal:host-gateway \
  -e CONTAINER_HOST=unix:///var/run/docker.sock \
  -e OIDC_ISSUER_URL=https://host.docker.internal:8443/auth/realms/fleetshift \
  -e KIND_EXPERIMENTAL_DOCKER_NETWORK=kind \
  --network kind \
  quay.io/stolostron/fleetshift:latest
```

In the future, we can hopefully extend this with separate process addons by leveraging a minimal supervisor like s6.

This Podman compose stack remains the local multi-service setup (server + web-builder init, optional Keycloak/Postgres). Use the all-in-one image when you want a single container with API + UI (and optionally kind).

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

## Full Stack Dev Mode

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

## Configuration

Copy `.env.template` to `.env` and edit. All available settings are documented in the template. Command-line variables always override `.env`.

### `gcphcp` Addon Toggle

- Default: `kind,kubernetes`
- Add `gcphcp`: set `GCPHCP_ENABLED=true` and `GCPHCP_GATEWAY_URL` in `.env`
  (`AUTH=external` required). Optional `GCPHCP_*` overrides use renderer
  defaults when empty.
- Runtime artifact: Podman renders `deploy/podman/.gcphcp.yaml` from `.env` and
  mounts it as `/config/gcphcp.yaml`
- Follow-on tasks such as `task podman:logs`, `task podman:status`, and
  `task podman:rebuild` recalculate the rendered config from the current `.env`
