# OpenShift Deployment Example

Deploy FleetShift to an OpenShift cluster using Kustomize manifests. Everything runs in the `fleetshift` namespace. For local Kubernetes provisioning, use the [Podman/Kind quick start](../podman/README.md).

## What gets deployed

- **PostgreSQL** — StatefulSet with PVC (5Gi), headless Service, file-based secret mounting (`_FILE` convention)
- **FleetShift server** — Deployment with web UI init container, `--database-url-file` for credentials, the generic Kubernetes addon, and serve OIDC bootstrap flags that install the first AuthMethod when the store is empty
- **Networking** — OpenShift Routes (edge TLS) for HTTP/UI and gRPC, Service with ports 8085 (http) and 50051 (grpc)
- **ImageStreams** — Pull from quay.io with scheduled import and deployment triggers
- **ConfigMap + Secret** — Generated from `.env` at deploy time via Kustomize generators

## Prerequisites

- `oc` CLI installed and logged into an OpenShift cluster
- External Keycloak deployed and accessible (see [deploy/keycloak/README.md](../keycloak/README.md))
- Images pushed to quay.io (`quay.io/stolostron/fleetshift-server:latest`, `quay.io/stolostron/fleetshift-web:latest`)
- `.env` configured at the repo root (copy from `.env.template`)

## Quick Start

```bash
npx nx run k8s:deploy            # deploy everything
npx nx run k8s:status            # check pods, services, routes
npx nx run k8s:teardown          # remove everything
```

The deploy script generates `config.env` and `secrets.env`
from the root `.env`, applies Kustomize manifests, waits for PostgreSQL and the
server, and imports images. On completion it prints the frontend and gRPC URLs.
First-trust AuthMethod install is performed by `fleetshift serve` from the
Deployment's OIDC bootstrap flags (there is no auth-setup Job; public
`CreateAuthMethod` is not a Day One path). `fleetctl auth setup` only writes
local Fleetctl OIDC client config via issuer discovery.

## Tasks

All tasks use the `k8s:*` Nx targets.

| Task | Description |
|------|-------------|
| `k8s:deploy` | Deploy FleetShift (manifests and images) |
| `k8s:teardown` | Remove all resources and namespace |
| `k8s:status` | Show pods, services, routes; warn if image override is active |
| `k8s:logs` | Tail logs from fleetshift-server (all containers) |
| `k8s:set-image TAG=<tag>` | Override the server image via ImageStream (e.g. PR testing) |
| `k8s:reset-image` | Restore default `:latest` tag with scheduled import |
| `k8s:import-images` | Force reimport of images from quay.io |
| `k8s:register-redirect USER=<u> PASSWORD=<p>` | Register UI redirect URI in Keycloak |

## Configuration

The deploy script reads the root `.env` and generates two files consumed by
Kustomize generators. Values already exported in the process environment take
precedence over `.env` (so `OIDC_ISSUER_URL=... npx nx run k8s:deploy` wins).

**`config.env`** (ConfigMap) — OIDC issuer URL, client IDs, audience, key
enrollment settings, log level, and the `kubernetes` addon selection.

**`secrets.env`** (Secret) — PostgreSQL user, password, database name, and `DATABASE_URL`.

Use the root `.env.template` for the authoritative input keys. The exact
generated `config.env` and `secrets.env` shapes are defined by
`deploy/kubernetes/scripts/deploy.mjs`.

## Image Management

**Override for PR testing:**

```bash
npx nx run k8s:set-image -- TAG=PR48-abc123   # point ImageStream to a PR image
npx nx run k8s:reset-image                    # restore :latest with scheduled import
```

**Force reimport** (e.g. after pushing a new `:latest`):

```bash
npx nx run k8s:import-images
```

ImageStreams use `importPolicy.scheduled: true` for automatic periodic pulls. The `set-image` command replaces the tag spec (disabling scheduled import); `reset-image` restores it.

## gRPC Route Certificate

External gRPC access requires HTTP/2, which needs a trusted certificate on the Route. After deploying FleetShift, run the certificate workflow as a post-deploy step:

```bash
npx nx run k8s:grpc-cert:deploy -- ACME_EMAIL=you@example.com
```

See [grpc-route-cert/README.md](grpc-route-cert/README.md) for details.

## CLI Access

After deployment, the server has already installed `authMethods/default` from
its OIDC bootstrap flags when the AuthMethod store was empty. Configure local
Fleetctl client settings separately (fleetctl auth setup), then login:

```bash
GRPC_ROUTE=$(oc get route grpc -n fleetshift -o jsonpath='{.spec.host}')

# Login and use (after local Fleetctl OIDC client config exists)
bin/fleetctl auth login
bin/fleetctl deployment list --server "$GRPC_ROUTE:443" --server-tls
```
