# Kubernetes / OpenShift Deployment

Deploy FleetShift to an OpenShift cluster using Kustomize manifests. Everything runs in the `fleetshift` namespace.

## What gets deployed

- **PostgreSQL** — StatefulSet with PVC (5Gi), headless Service, file-based secret mounting (`_FILE` convention)
- **FleetShift server** — Deployment with web UI init container, `--database-url-file` for credentials, env-driven addon selection, and optional mounted `gcphcp.yaml`
- **Networking** — OpenShift Routes (edge TLS) for HTTP/UI and gRPC, Service with ports 8085 (http) and 50051 (grpc)
- **ImageStreams** — Pull from quay.io with scheduled import and deployment triggers
- **Auth setup** — Job that registers the OIDC auth method via `fleetctl auth setup`
- **ConfigMap + Secret** — Generated from `.env` at deploy time via Kustomize generators

## Prerequisites

- `oc` CLI installed and logged into an OpenShift cluster
- External Keycloak deployed and accessible (see [deploy/keycloak/README.md](../keycloak/README.md))
- Images pushed to quay.io (`quay.io/stolostron/fleetshift-server:latest`, `quay.io/stolostron/fleetshift-web:latest`)
- `.env` configured at the repo root (copy from `.env.template`)

## Quick Start

```bash
task kubernetes:deploy          # deploy everything
task kubernetes:status          # check pods, services, routes
task kubernetes:teardown        # remove everything
```

The deploy script generates `config.env`, `secrets.env`, and `gcphcp.yaml`
from the root `.env`, applies Kustomize manifests, waits for PostgreSQL and the
server, imports images, and runs the auth-setup job. On completion it prints
the frontend and gRPC URLs.

## Tasks

All tasks use the `kubernetes:` namespace (alias `k8:`).

| Task | Description |
|------|-------------|
| `kubernetes:deploy` | Deploy FleetShift (manifests, images, auth-setup) |
| `kubernetes:teardown` | Remove all resources and namespace |
| `kubernetes:status` | Show pods, services, routes; warn if image override is active |
| `kubernetes:logs` | Tail logs from fleetshift-server (all containers) |
| `kubernetes:logs:<pod>` | Tail logs from a specific pod (e.g. `kubernetes:logs:postgres-0`) |
| `kubernetes:set-image TAG=<tag>` | Override the server image via ImageStream (e.g. PR testing) |
| `kubernetes:reset-image` | Restore default `:latest` tag with scheduled import |
| `kubernetes:import-images` | Force reimport of images from quay.io |
| `kubernetes:register-redirect USER=<u> PASSWORD=<p>` | Register UI redirect URI in Keycloak |
| `kubernetes:auth-setup` | Re-run the auth-setup job (deletes previous first) |

## Configuration

The deploy script reads the root `.env` and generates three files consumed by
Kustomize generators:

**`config.env`** (ConfigMap) — OIDC issuer URL, client IDs, audience, key
enrollment settings, log level, resolved addon list, and optional
`GCPHCP_CONFIG_PATH`.

**`secrets.env`** (Secret) — PostgreSQL user, password, database name, and `DATABASE_URL`.

**`gcphcp.yaml`** (ConfigMap file entry) — rendered from the `GCPHCP_*` values
in `.env`. When `GCPHCP_ENABLED=true`, the server mounts this file and starts
with `gcphcp` enabled. When `false`, the generated file is a disabled
placeholder and the addon list stays `ocp,kubernetes`.

Use the root `.env.template` for the authoritative input keys. The exact
generated `config.env`, `secrets.env`, and `gcphcp.yaml` shapes are defined by
`deploy/kubernetes/scripts/deploy.sh`.

## Image Management

**Override for PR testing:**

```bash
task kubernetes:set-image TAG=PR48-abc123    # point ImageStream to a PR image
task kubernetes:reset-image                  # restore :latest with scheduled import
```

**Force reimport** (e.g. after pushing a new `:latest`):

```bash
task kubernetes:import-images
```

ImageStreams use `importPolicy.scheduled: true` for automatic periodic pulls. The `set-image` command replaces the tag spec (disabling scheduled import); `reset-image` restores it.

## gRPC Route Certificate

External gRPC access requires HTTP/2, which needs a trusted certificate on the Route. After deploying FleetShift, run the certificate workflow as a post-deploy step:

```bash
task kubernetes:grpc-route-cert:deploy ACME_EMAIL=you@example.com
```

See [grpc-route-cert/README.md](grpc-route-cert/README.md) for details.

## CLI Access

After deployment and auth setup:

```bash
GRPC_ROUTE=$(oc get route grpc -n fleetshift -o jsonpath='{.spec.host}')

# Setup local auth config
bin/fleetctl auth setup \
  --server "$GRPC_ROUTE:443" --server-tls \
  --issuer-url <OIDC_ISSUER_URL> \
  --client-id fleetshift-cli \
  --audience fleetshift-cli \
  --key-enrollment-client-id fleetshift-signing \
  --registry-id github.com \
  --registry-subject-expression "claims.github_username"

# Login and use
bin/fleetctl auth login
bin/fleetctl deployment list --server "$GRPC_ROUTE:443" --server-tls
```
