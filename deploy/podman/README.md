# FleetShift Podman Deployment

Deploy the full FleetShift stack using podman containers with compose.

---

> **macOS REQUIRED SETUP**
>
> Podman on macOS only forwards IPv6 loopback connections. You **must** add this
> entry to `/etc/hosts` or Keycloak will be unreachable from the browser and CLI:
>
> ```bash
> echo "::1 keycloak" | sudo tee -a /etc/hosts
> ```
>
>> This is a one-time setup. Without it, OIDC login, GUI access to Keycloak, and
> all authenticated CLI operations will hang.
>
> **Not needed on Linux** — port forwarding works on IPv4 there.

---

## Prerequisites

- **podman** installed and running (`podman --version`)
  - macOS: `podman machine init && podman machine start`
- **jq** installed (`jq --version`) — used for realm password generation
- **kind** installed (`kind --version`) — used for local cluster provisioning
- **`/etc/hosts` entry** on macOS (see above)
- Container images built or available (see [Building Dev Images](#building-dev-images))

## Quick Start

```bash
cd deploy/podman

# Start in demo mode (sqlite + local keycloak, the default)
make up

# Credentials are printed at the end of startup output.
# GUI: http://localhost:3000
# Keycloak: http://keycloak:8180 (requires /etc/hosts on macOS)
```

## Profiles & Overrides

FleetShift uses named profiles with per-axis overrides for flexible deployment configuration.

### Named Profiles

| Profile | DB Backend | Auth Provider | Use Case |
|---|---|---|---|
| `demo` (default) | SQLite | Local Keycloak | Quick demos, local dev without external dependencies |
| `prod` | PostgreSQL | External OIDC | Production-like deployments |

```bash
make up                    # demo (default)
make up PROFILE=prod       # prod
```

### Per-Axis Overrides

Override the DB backend or auth provider independently:

| Variable | Options | Description |
|---|---|---|
| `DB` | `sqlite`, `postgres` | Database backend |
| `AUTH` | `local`, `external` | Auth provider (`local` = bundled Keycloak) |

```bash
make up DB=postgres AUTH=local       # postgres + local keycloak
make up PROFILE=prod AUTH=local      # same as above (prod defaults to postgres)
make up DB=sqlite AUTH=external      # sqlite + external oidc
```

### Sticky Configuration

Set `PROFILE` in `deploy/.env` to avoid passing it every time:

```bash
# deploy/.env
PROFILE=prod
```

Command-line always takes precedence over `.env`.

## Available Commands

| Command | Description |
|---|---|
| `make up` | Start the FleetShift stack. Accepts `PROFILE=`, `DB=`, `AUTH=` overrides. |
| `make down` | Stop all containers, preserve data volumes. |
| `make clean` | Stop all containers and remove ALL data (volumes, network, kind clusters). |
| `make status` | Show running containers and health status. |
| `make logs` | Tail logs from all containers. |
| `make logs-<service>` | Tail logs from one container (e.g., `make logs-fleetshift-server`). |
| `make restart-<service>` | Restart one container (e.g., `make restart-fleetshift-mock-servers`). |
| `make cli-setup` | Configure `fleetctl` CLI for local OIDC auth (requires running stack). |
| `make reset-keycloak` | Wipe Keycloak state, re-import realm with new passwords. |
| `make help` | Show all available targets. |

## Configuration

On first `make up`, the script copies `deploy/.env.template` to `deploy/.env`. Edit `.env` to customize. The `.env` file is gitignored.

Key settings:

| Variable | Default | Description |
|---|---|---|
| `PROFILE` | `demo` | Named profile: `demo` or `prod` |
| `KC_HOSTNAME` | `keycloak` | Hostname Keycloak uses in all URLs (issuer, endpoints) |
| `KC_HTTP_PORT` | `8180` | Keycloak HTTP port |
| `POSTGRES_PASSWORD` | `changeme` | PostgreSQL password (used when `DB=postgres`) |
| `FLEETSHIFT_LOG_LEVEL` | `debug` | Server log level |

### Dev User (optional)

Uncomment these in `.env` to auto-create a personal Keycloak user during startup (only applies when `AUTH=local`):

```bash
DEV_USER_USERNAME=mshort@redhat.com
DEV_USER_PASSWORD=mypassword
DEV_USER_GITHUB=mshort55
DEV_USER_ROLES=ops,dev
```

The user is created idempotently — re-running `make up` updates the existing user.

For ad-hoc user creation, use the script directly:

```bash
scripts/add-user.sh \
  --admin-password <admin password from make up output> \
  --username someone@redhat.com \
  --password theirpass \
  --github their-github \
  --roles ops,dev
```

### Image Overrides

By default, images come from `quay.io/stolostron/fleetshift-*:latest`. To use custom images, uncomment and set in `.env`:

```bash
FLEETSHIFT_SERVER_IMAGE=quay.io/mshort/fleetshift-server:latest
FLEETSHIFT_MOCK_SERVERS_IMAGE=quay.io/mshort/fleetshift-mock-servers:latest
FLEETSHIFT_MOCK_UI_PLUGINS_IMAGE=quay.io/mshort/fleetshift-mock-ui-plugins:latest
FLEETSHIFT_GUI_IMAGE=quay.io/mshort/fleetshift-gui:latest
```

## Building Dev Images

Images are built and pushed to your personal quay.io namespace. `DEV_REGISTRY` defaults to `quay.io/<your OS username>`.

```bash
# Server image (from fleetshift-poc repo root)
make image-build                              # tags as quay.io/$USER/fleetshift-server:latest
make image-build DEV_REGISTRY=quay.io/mshort  # override registry
make image-build IMAGE_TAG=0.2.0              # override tag
make image-push                               # push to registry

# UI images (from fleetshift-user-interface repo root)
make image-build-all    # builds all three UI images
make image-push         # pushes all three
```

After building, set the image overrides in `.env` and `make up`.

## CLI Setup

```bash
# Configure CLI auth (requires stack to be running)
make cli-setup

# Log in (opens browser for OIDC flow at http://keycloak:8180)
bin/fleetctl auth login

# Use it
bin/fleetctl --server localhost:50051 deployments list
```

## Attestation Test

Run the full end-to-end attestation flow (signing key enrollment, signed cluster deployment, signed ConfigMap delivery):

```bash
scripts/test-attestation.sh
```

This creates a dev user, logs in, enrolls a signing key, creates a signed kind cluster deployment, and deploys a signed ConfigMap to the managed cluster.

## Services

| Service | URL | Description |
|---|---|---|
| GUI | http://localhost:3000 | FleetShift web interface |
| Mock API | http://localhost:4000 | Express middleware (auth, proxy, WebSocket) |
| FleetShift API | http://localhost:8085 | Go backend (HTTP gateway) |
| FleetShift gRPC | localhost:50051 | Go backend (gRPC, used by fleetctl) |
| Plugin assets | http://localhost:8001 | Module Federation plugin bundles |
| Keycloak (HTTP) | http://keycloak:8180 | OIDC provider (demo mode, requires /etc/hosts on macOS) |
| Keycloak (HTTPS) | https://keycloak:8443 | OIDC provider with TLS (demo mode) |

## Authentication

Passwords are generated on each `make up` (or `make reset-keycloak`) and printed to the console.

**Keycloak admin console** (`http://keycloak:8180/auth/admin`): `admin` / `<generated>`

**FleetShift realm users:**

| Username | Role | Description |
|---|---|---|
| `ops` | ops | Operations persona — manages clusters |
| `dev` | dev | Developer persona — manages applications |

If `DEV_USER_*` is configured in `.env`, your personal user is also created.

## How Networking Works

All containers share a podman network called `fleetshift`. Containers resolve each other by service name via podman's built-in DNS (aardvark-dns).

`KC_HOSTNAME=keycloak` means Keycloak produces all URLs with the `keycloak` hostname. This works because:

- **Inside containers**: `keycloak` resolves to Keycloak's container IP via aardvark-dns
- **On the host (macOS)**: `keycloak` resolves to `::1` via `/etc/hosts`, and podman's gvproxy forwards IPv6 loopback connections to the VM
- **On the host (Linux)**: `keycloak` resolves to `127.0.0.1` via `/etc/hosts`, and port forwarding works directly

The `/etc/hosts` entry is only needed because the browser and CLI run on the host and need to reach Keycloak at the same hostname the server uses.

## Troubleshooting

### Keycloak unreachable from browser or CLI (macOS)

```bash
# Symptom: curl http://keycloak:8180/... hangs, but curl http://localhost:8180/... works
# Cause: missing /etc/hosts entry for IPv6 loopback
echo "::1 keycloak" | sudo tee -a /etc/hosts

# Verify:
curl -s http://keycloak:8180/auth/realms/master | head -c 50
```

### Container won't start

```bash
make logs-<service>

# Common issues:
# - Port already in use: stop whatever is using the port, or change in .env
# - Image not found: build it first (see "Building Dev Images")
```

### Keycloak crash loop

```bash
make logs-keycloak

# Common causes:
# - Invalid realm JSON (check deploy/keycloak/fleetshift-realm.json)
# - TLS cert permissions (make clean fixes this)
```

### GUI shows blank white screen

Check the browser DevTools Console and Network tab. Common causes:

- OIDC discovery request pending/failed — Keycloak may still be starting. Wait 30-60 seconds.
- OIDC authority URL wrong — the GUI image must be built with `authority: "http://keycloak:8180/..."` in oidcConfig.ts. Rebuild the GUI image if needed.
- API requests failing — check `make logs-fleetshift-mock-servers`

### FleetShift server keeps restarting

```bash
make logs-fleetshift-server

# Common causes:
# - "OIDC CA file not found" — keycloak-certs init container hasn't finished yet.
#   Usually resolves after a few restarts. If persistent: make clean && make up
```

### Podman socket issues

The podman socket path is auto-detected by `start.sh`. If cluster provisioning fails:

```bash
# Check your socket path
podman info --format '{{.Host.RemoteSocket.Path}}'

# If auto-detection fails, set it manually in .env:
# PODMAN_SOCKET=/path/to/podman.sock
```

### Stale Keycloak state

If realm changes aren't taking effect:

```bash
make reset-keycloak
make up
```

This wipes the Keycloak database and TLS certs, re-imports the realm, and generates new passwords.
