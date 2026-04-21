# FleetShift Podman Deployment

Deploy the full FleetShift stack using podman containers with docker-compose.

## Prerequisites

- **podman** installed and running (`podman --version`)
  - macOS: `podman machine init && podman machine start`
- **jq** installed (`jq --version`) — used for realm password generation
- Both repos cloned (only needed for building images, not for deployment)

## Quick Start

```bash
cd deploy/podman

# Start the stack (auto-creates .env from template on first run)
make up

# Credentials are printed at the end of startup output.
# GUI: http://localhost:3000
```

## Available Commands

```
make up                 Start the full FleetShift stack
make down               Stop all containers, preserve data
make clean              Stop all containers and remove ALL data
make status             Show running containers
make logs               Tail logs from all containers
make logs-<service>     Tail logs from one container (e.g., make logs-fleetshift-server)
make restart-<service>  Restart one container (e.g., make restart-fleetshift-mock-servers)
make cli-setup          Configure fleetctl CLI for local deployment
make reset-keycloak     Wipe Keycloak state and re-import realm with new passwords
```

## Configuration

On first `make up`, the script copies `deploy/.env.template` to `deploy/.env`. Edit `.env` to customize. The `.env` file is gitignored.

### Dev User (optional)

Uncomment these in `.env` to auto-create a personal Keycloak user during startup:

```bash
DEV_USER_USERNAME=mshort@redhat.com
DEV_USER_PASSWORD=mypassword
DEV_USER_GITHUB=mshort55
DEV_USER_ROLES=ops,dev
```

The user is created idempotently — re-running `make up` updates the existing user.

For ad-hoc user creation (e.g., adding extra users), use the script directly:

```bash
scripts/add-user.sh \
  --admin-password <admin password from make up output> \
  --username someone@redhat.com \
  --password theirpass \
  --github their-github \
  --roles ops,dev
```

### Image Overrides

By default, images come from `quay.io/stolostron/fleetshift-*:latest`. To use custom images (e.g., locally built dev images), uncomment and set in `.env`:

```bash
FLEETSHIFT_SERVER_IMAGE=quay.io/mshort/fleetshift-server:latest
FLEETSHIFT_MOCK_SERVERS_IMAGE=quay.io/mshort/fleetshift-mock-servers:latest
FLEETSHIFT_MOCK_UI_PLUGINS_IMAGE=quay.io/mshort/fleetshift-mock-ui-plugins:latest
FLEETSHIFT_GUI_IMAGE=quay.io/mshort/fleetshift-gui:latest
```

## Building Dev Images

Images are built and pushed to your personal quay.io namespace. `DEV_REGISTRY` defaults to `quay.io/<your OS username>`.

```bash
# Server image (from fleetshift-poc repo)
make image-build                              # tags as quay.io/$USER/fleetshift-server:latest
make image-build DEV_REGISTRY=quay.io/mshort  # override registry
make image-build IMAGE_TAG=0.2.0              # override tag
make image-push                               # push to registry

# UI images (from fleetshift-user-interface repo)
cd /path/to/fleetshift-user-interface
make image-build-all    # builds all three UI images
make image-push         # pushes all three
```

After building, set the image overrides in `.env` and `make up`.

## CLI Setup

To use `fleetctl` with the local deployment:

```bash
# Configure CLI auth (requires stack to be running)
make cli-setup

# Log in (opens browser for OIDC flow)
bin/fleetctl auth login

# Use it
bin/fleetctl --server localhost:50051 deployment list
```

## Services

| Service | URL | Description |
|---|---|---|
| GUI | http://localhost:3000 | FleetShift web interface |
| Mock API | http://localhost:4000 | Express middleware (auth, proxy, WebSocket) |
| FleetShift API | http://localhost:8085 | Go backend (HTTP gateway) |
| FleetShift gRPC | localhost:50051 | Go backend (gRPC, used by fleetctl) |
| Plugin assets | http://localhost:8001 | Module Federation plugin bundles |
| Keycloak (HTTP) | http://localhost:8180 | OIDC provider (demo mode) |
| Keycloak (HTTPS) | https://localhost:8443 | OIDC provider with TLS (demo mode) |

## Authentication

Passwords are generated on each `make up` (or `make reset-keycloak`) and printed to the console.

**Keycloak admin console** (`http://localhost:8180/auth/admin`): `admin` / `<generated>`

**FleetShift realm users:**

| Username | Role | Description |
|---|---|---|
| `ops` | ops | Operations persona — manages clusters |
| `dev` | dev | Developer persona — manages applications |

If `DEV_USER_*` is configured in `.env`, your personal user is also created.

## Troubleshooting

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

# The path should NOT include the unix:// prefix.
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
