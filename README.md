# fleetshift-poc

This repository represents both a **prototype** for a next generation k8s/OpenShift cluster management vision, alongside **individual POCs** for exploration of isolated concepts.

## Prerequisites

- **Go 1.22+**
- **[Task](https://taskfile.dev/)** — `go install github.com/go-task/task/v3/cmd/task@latest`
- **buf** — for protobuf generation (`brew install bufbuild/buf/buf`)
- `.env` file — copy from `.env.template`

Deployment-specific prerequisites (podman, oc, kind, etc.) are listed in each deployment guide below.

## Build

```bash
task build:all              # build all Go binaries → bin/
task build:server           # fleetshift-server
task build:cli              # fleetctl CLI
```

Builds are incremental — only recompiles when source files change.

## Test

```bash
task test:all               # unit tests for all modules
```

## Generate & Images

```bash
task protogen               # regenerate protobuf and gRPC stubs
task image:build            # build server + web container images
task image:aio              # build all-in-one image from local server-local + web
task image:push             # push server, server-local, and web to DEV_REGISTRY (not the AIO image)
```

## Configuration

Copy `.env.template` to `.env` and edit. All available settings are documented in the template.

## Run

The simplest way to run FleetShift is a single container (API + UI). That image is a sandbox for playing around, bootstrapping a real cluster, or testing — not a production deployment. Pass `OIDC_ISSUER_URL` so the UI can log users in (`OIDC_UI_CLIENT_ID` defaults to `fleetshift-ui`). Day One `/setup` can create a server-side auth method, but the UI’s OIDC client still reads the issuer from this startup config — it does not discover it from configured auth methods yet.

```bash
podman run --rm -it \
  -p 8085:8085 -p 50051:50051 \
  -e OIDC_ISSUER_URL=https://your-oidc-issuer/realms/fleetshift \
  quay.io/stolostron/fleetshift:latest
```

Open http://localhost:8085. Build a local image with `task image:aio` instead of pulling from Quay when iterating on this repo.

### With kind

Privileged + host container socket (full control of the host engine — local/dev only). Create the `kind` network once if needed: `podman network create kind`.

```bash
podman run --rm -it \
  --privileged --user 0:0 \
  -p 8085:8085 -p 50051:50051 \
  -v /tmp:/tmp \
  -v ${PODMAN_SOCKET:-/var/run/docker.sock}:/var/run/docker.sock \
  -e CONTAINER_HOST=unix:///var/run/docker.sock \
  -e KIND_EXPERIMENTAL_DOCKER_NETWORK=kind \
  -e OIDC_ISSUER_URL=https://your-oidc-issuer/realms/fleetshift \
  --network kind \
  quay.io/stolostron/fleetshift:latest
```

### With GCP HCP

Until the service is GA, enable the addon by passing the CLS gateway URL (optional overrides use shared renderer defaults):

```bash
podman run --rm -it \
  -p 8085:8085 -p 50051:50051 \
  -e OIDC_ISSUER_URL=https://your-oidc-issuer/realms/fleetshift \
  -e GCPHCP_GATEWAY_URL=https://your-cls-gateway \
  quay.io/stolostron/fleetshift:latest
```

Combine with the kind flags above when you also need local cluster provisioning.

### Other ways to run

| Method | Use case | Guide |
|--------|----------|-------|
| Podman compose | Multi-service local stack (Keycloak, Postgres, hot-reload) | [deploy/podman/](deploy/podman/README.md) |
| Kubernetes / OpenShift | Cluster deployment | [deploy/kubernetes/](deploy/kubernetes/README.md) |
| Keycloak (OpenShift) | External OIDC provider for those deployments | [deploy/keycloak/](deploy/keycloak/README.md) |

## Day One Setup

The Day One setup flow is an unauthenticated UI page at `/setup` that guides initial OIDC configuration before any identity provider has been registered. A WebSocket endpoint at `/api/ui/setup/ws` broadcasts auth method lifecycle events to the UI in real time so the setup page can react as provisioning progresses.

The equivalent CLI command:

```bash
fleetctl auth setup \
  --issuer-url=<URL> \
  --client-id=<CLIENT_ID> \
  --audience=<AUDIENCE> \
  --key-enrollment-client-id=<AUDIENCE>
```

Optional flags for key registry configuration:

```bash
--registry-id=github.com \
--registry-subject-expression=claims.github_username
```
