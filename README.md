# fleetshift-poc

This repository represents both a **prototype** for a next generation k8s/OpenShift cluster management vision, alongside **individual POCs** for exploration of isolated concepts.

## Prerequisites

- **Go 1.22+**
- **Node.js 20+** — for Nx and UI packages
- **[Task](https://taskfile.dev/)** — `go install github.com/go-task/task/v3/cmd/task@latest`
- **buf** — for protobuf generation (`brew install bufbuild/buf/buf`)
- `.env` file — copy from `.env.template`

Deployment-specific prerequisites (podman, oc, kind, etc.) are listed in each deployment guide below.

## Setup

```bash
npm install                 # install all workspace dependencies (from repo root)
```

All UI packages are npm workspaces declared at the root. A single `npm install` at the repo root handles everything — no separate install needed per package.

## Monorepo

This workspace uses [Nx](https://nx.dev) for build orchestration — caching, dependency graph, affected detection, and parallel execution. Nx wraps the existing Taskfile commands, so both `task` and `nx` work.

```bash
npx nx show projects        # list all projects
npx nx graph                # visualize dependency graph
npx nx run server:test      # run a single target (cached)
npx nx run-many -t test     # run target across all projects (parallel)
npx nx affected -t test     # only test what changed
```

Projects: `server`, `cli`, `proto`, `gui`, `common`, `build-utils`, `plugins`.

## Build

```bash
npx nx run server:build     # server
npx nx run cli:build        # fleetctl CLI
npx nx run common:build     # shared UI types/helpers
npx nx run plugins:build    # all MF remote plugins
npx nx run gui:build        # SPA shell
npx nx run-many -t build    # build all (parallel, cached)

# Or via Taskfile directly:
task build:server
task build:cli
task build:all
```

Builds are cached — unchanged sources skip recompilation entirely.

## UI Development

```bash
npx nx run gui:dev          # dev server (http://localhost:8085)
npx nx run gui:dev:watch    # dev server with hot reload
npx nx run gui:test:ct      # component tests (playwright)
npx nx run plugins:test:ct  # plugin component tests
```

UI packages: `gui` (SPA shell), `common` (shared types), `build-utils` (rspack helpers), `plugins` (12 MF remotes)

## Test

```bash
npx nx run-many -t test     # unit tests for all modules
npx nx affected -t test     # only test what changed
npx nx run server:test      # Go server tests (cached)
npx nx run common:test      # shared UI lib tests

# Or via Taskfile:
task test:all
```

## Generate & Images

```bash
npx nx run proto:generate   # regenerate protobuf and gRPC stubs

# Or via Taskfile:
task protogen
task image:build            # build server + web container images
task image:aio              # build all-in-one image from local server-local + web
task image:push             # push server, server-local, and web to DEV_REGISTRY (not the AIO image)
```

## Local Development (Podman)

All podman deploy commands are available through Nx. Env vars (AUTH, LOCAL_WEB, UI_SETUP, DB, etc.) pass through to Taskfile.

```bash
npx nx run pd:dev                                    # start local dev stack
AUTH=external LOCAL_WEB=true UI_SETUP=true npx nx run pd:dev  # with external auth + local web
npx nx run pd:up                                     # start stack (non-dev)
npx nx run pd:down                                   # stop stack
npx nx run pd:clean                                  # stop + remove volumes
npx nx run pd:status                                 # show container status
npx nx run pd:logs                                   # tail all logs
npx nx run pd:rebuild                                # rebuild and restart
npx nx run pd:rebuild-web                            # rebuild web container only
npx nx run pd:cli-setup                              # configure CLI against local stack
npx nx run pd:cert-init                              # generate local mkcert certs
npx nx run pd:reset-keycloak                         # reset keycloak realm
npx nx run pd:clock-drift                            # fix podman clock drift
npx nx run pd:test-attestation                       # test attestation flow

# Or via Taskfile directly:
task pd:dev AUTH=external LOCAL_WEB=true UI_SETUP=true
```

Keycloak OCP (`task kc:*`) and Kubernetes OCP (`task k8s:*`) commands remain Taskfile-only — they target remote clusters, not local dev.

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
