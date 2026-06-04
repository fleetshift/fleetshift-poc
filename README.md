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
task image:build            # build server container images
task image:push             # push to DEV_REGISTRY
```

## Configuration

Copy `.env.template` to `.env` and edit. All available settings are documented in the template.

## Deployment

| Method | Use case | Guide |
|--------|----------|-------|
| Podman (local) | Local dev, demos | [deploy/podman/](deploy/podman/README.md) |
| Kubernetes / OpenShift | Cluster deployment | [deploy/kubernetes/](deploy/kubernetes/README.md) |
| Keycloak (OpenShift) | External OIDC provider | [deploy/keycloak/](deploy/keycloak/README.md) |

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
