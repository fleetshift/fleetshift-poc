# OpenShift Deployment Example

This example deploys OCM Resource Manager to an OpenShift cluster using Kustomize manifests. Everything runs in the `ocm-resource-manager` namespace. For a local Podman/Kind setup, see the [Podman stack guide](../podman/README.md).

## What gets deployed

- **PostgreSQL** — StatefulSet with PVC (5Gi), headless Service, file-based secret mounting (`_FILE` convention)
- **Resource manager server** — Deployment with web UI init container, `--database-url-file` for credentials, the generic Kubernetes addon, and serve OIDC bootstrap flags that install the first AuthMethod when the store is empty
- **Networking** — OpenShift Routes (edge TLS) for HTTP/UI and gRPC, Service with ports 8085 (http) and 50051 (grpc)
- **ImageStreams** — Pull from quay.io with scheduled import and deployment triggers
- **ConfigMap + Secret** — Generated from `.env` at deploy time via Kustomize generators

## Prerequisites

- `oc` CLI installed and logged into an OpenShift cluster
- External Keycloak deployed and accessible (see [deploy/openshift/keycloak/README.md](keycloak/README.md))
- Images pushed to quay.io (`quay.io/stolostron/ocm-resource-manager-server:latest`, `quay.io/stolostron/ocm-resource-manager-web:latest`)
- `.env` configured at the repo root (copy from `.env.template`)

## Quick Start

```bash
npx nx run deploy:openshift:deploy            # deploy everything
npx nx run deploy:openshift:status            # check pods, services, routes
npx nx run deploy:openshift:teardown          # remove everything
```

The deploy script generates `config.env` and `secrets.env`
from the root `.env`, applies Kustomize manifests, waits for PostgreSQL and the
server, and imports images. On completion it prints the frontend and gRPC URLs.
First-trust AuthMethod install is performed by `resource-manager serve` from the
Deployment's OIDC bootstrap flags (there is no auth-setup Job; public
`CreateAuthMethod` is not a Day One path). `fleetctl auth setup` only writes
local Fleetctl OIDC client config via issuer discovery.

## Tasks

Tasks for this deployment use the `deploy:openshift:*` Nx targets.

| Task | Description |
|------|-------------|
| `deploy:openshift:deploy` | Deploy OCM Resource Manager (manifests and images) |
| `deploy:openshift:teardown` | Remove all resources and namespace |
| `deploy:openshift:status` | Show pods, services, routes; warn if image override is active |
| `deploy:openshift:logs` | Tail logs from ocm-resource-manager-server (all containers) |
| `deploy:openshift:set-image TAG=<tag>` | Override the server image via ImageStream (e.g. PR testing) |
| `deploy:openshift:reset-image` | Restore default `:latest` tag with scheduled import |
| `deploy:openshift:import-images` | Force reimport of images from quay.io |
| `deploy:openshift:register-redirect USER=<u> PASSWORD=<p>` | Register UI redirect URI in Keycloak |

## Configuration

The deploy script reads the root `.env` and generates two files consumed by
Kustomize generators. Values already exported in the process environment take
precedence over `.env` (so `OIDC_ISSUER_URL=... npx nx run deploy:openshift:deploy` wins).

**`config.env`** (ConfigMap) — OIDC issuer URL, client IDs, audience, key
enrollment settings, log level, and the `kubernetes` addon selection.

**`secrets.env`** (Secret) — PostgreSQL user, password, database name, and `DATABASE_URL`.

Use the root `.env.template` for the authoritative input keys. The exact
generated `config.env` and `secrets.env` shapes are defined by
`deploy/openshift/scripts/deploy.mjs`.

## Image Management

**Override for PR testing:**

```bash
npx nx run deploy:openshift:set-image -- TAG=PR48-abc123   # point ImageStream to a PR image
npx nx run deploy:openshift:reset-image                    # restore :latest with scheduled import
```

**Force reimport** (e.g. after pushing a new `:latest`):

```bash
npx nx run deploy:openshift:import-images
```

ImageStreams use `importPolicy.scheduled: true` for automatic periodic pulls. The `set-image` command replaces the tag spec (disabling scheduled import); `reset-image` restores it.

## gRPC Route Certificate

External gRPC access requires HTTP/2, which needs a trusted certificate on the Route. After deploying OCM Resource Manager, run the certificate workflow as a post-deploy step:

```bash
npx nx run deploy:openshift:grpc-cert:deploy -- ACME_EMAIL=you@example.com
```

See [grpc-route-cert/README.md](grpc-route-cert/README.md) for details.

## CLI Access

After deployment, the server has already installed `authMethods/default` from
its OIDC bootstrap flags when the AuthMethod store was empty. Configure local
Fleetctl client settings separately (fleetctl auth setup), then login:

```bash
GRPC_ROUTE=$(oc get route grpc -n ocm-resource-manager -o jsonpath='{.spec.host}')

# Login and use (after local Fleetctl OIDC client config exists)
bin/fleetctl auth login
bin/fleetctl deployment list --server "$GRPC_ROUTE:443" --server-tls
```
