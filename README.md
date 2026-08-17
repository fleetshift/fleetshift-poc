# fleetshift-poc

This repository represents both a **prototype** for a next generation k8s/OpenShift cluster management vision, alongside **individual POCs** for exploration of isolated concepts.

## Prerequisites

- **Go 1.22+**
- **Node.js 20+** — for Nx and UI packages
- **[Task](https://taskfile.dev/)** — `go install github.com/go-task/task/v3/cmd/task@latest`
- **buf** — for protobuf generation (`brew install bufbuild/buf/buf`)
- `.env` file — copy from `.env.template` (compose stack and Kubernetes only; not required for the all-in-one image)

Deployment-specific prerequisites (podman, oc, kind, etc.) are listed in each deployment guide.

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

Projects: `server`, `cli`, `proto`, `web`, `common`, `build-utils`, `plugins`.

## Build

```bash
npx nx run server:build     # server
npx nx run cli:build        # fleetctl CLI
npx nx run common:build     # shared UI types/helpers
npx nx run plugins:build    # all MF remote plugins
npx nx run web:build        # SPA shell
npx nx run-many -t build    # build all (parallel, cached)

# Or via Taskfile directly:
task build:server
task build:cli
task build:all
```

Builds are cached — unchanged sources skip recompilation entirely.

## UI Development

```bash
npx nx run web:dev          # dev server (http://localhost:8085)
npx nx run web:dev:watch    # dev server with hot reload
npx nx run web:test:ct      # component tests (playwright)
npx nx run plugins:test:ct  # plugin component tests
```

UI packages: `web` (SPA shell), `common` (shared types), `build-utils` (rspack helpers), `plugins` (12 MF remotes)

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

## Local compose stack

The multi-service local harness (`task pd:*` / `task podman:*`) is documented in
[deploy/podman/README.md](deploy/podman/README.md). Commands are also available
through Nx; env vars (`AUTH`, `LOCAL_WEB`, `DB`, etc.) pass through to Taskfile.

```bash
npx nx run pd:dev                                    # start local dev stack
AUTH=external LOCAL_WEB=true npx nx run pd:dev       # with external auth + local web
npx nx run pd:up                                     # start stack (non-dev)
npx nx run pd:down                                   # stop stack
npx nx run pd:clean                                  # stop + remove volumes
npx nx run pd:status                                 # show container status
npx nx run pd:logs                                   # tail all logs
npx nx run pd:rebuild                                # rebuild and restart
npx nx run pd:rebuild-web                            # rebuild web container only
npx nx run pd:cert-init                              # generate local mkcert certs
npx nx run pd:reset-keycloak                         # reset keycloak realm
npx nx run pd:clock-drift                            # fix podman clock drift
npx nx run pd:test-attestation                       # test attestation flow

# Or via Taskfile directly:
task pd:dev AUTH=external LOCAL_WEB=true
```

Keycloak OCP (`task kc:*`) and Kubernetes OCP (`task k8s:*`) commands remain
Taskfile-only — they target remote clusters, not local compose.

## Configuration

For the **compose stack** and **Kubernetes** deployments, copy `.env.template`
to `.env` and edit. Settings are documented in the template.

The **all-in-one image** does not use `.env`; configure it with container env
vars (`-e`). See [deploy/aio/README.md](deploy/aio/README.md).

## Run

Both local options below typically use podman as the container engine. Pick by
**what you launch**, not by the engine:

| Path | What you launch | Guide |
|------|-----------------|-------|
| All-in-one image | One container (`quay.io/stolostron/fleetshift`) with API + UI + peer Dex | This section + [deploy/aio/](deploy/aio/README.md) |
| Local compose stack | Multi-service harness (server, web builder, Keycloak, optional Postgres) | [deploy/podman/](deploy/podman/README.md) |
| Kubernetes / OpenShift | Cluster deployment | [deploy/kubernetes/](deploy/kubernetes/README.md) |
| Keycloak (OpenShift) | External OIDC for cluster/compose `AUTH=external` | [deploy/keycloak/](deploy/keycloak/README.md) |
| Nx remote cache | Shared build cache backed by MinIO | [docs/nx-remote-cache.md](docs/nx-remote-cache.md) |

### All-in-one image (simplest)

Sandbox for trying FleetShift — not a production deployment. Bare run needs
**no** OIDC flags: packaging starts peer Dex and wires AuthMethod/UI defaults
into `fleetshift serve`. Demo users: `ops@fleetshift.local` /
`fleetshift-ops` and `dev@fleetshift.local` / `fleetshift-dev`.

```bash
podman run -d --rm -it \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  quay.io/stolostron/fleetshift:latest
```

Open https://fleetshift-sandbox.localhost:8085 and accept the browser's
certificate warning (unknown sandbox CA — Advanced/Proceed or Accept Risk).
Build locally with `task image:aio` when iterating on this repo.

External issuer, kind, GCP HCP, env defaults, and fleetctl:
[deploy/aio/README.md](deploy/aio/README.md).

## Day One Setup

`fleetshift serve` installs the initial AuthMethod when the store is empty and
complete OIDC bootstrap config is present. Who supplies those flags depends on
the path:

- **AIO Dex-on:** packaging fills them automatically (including registry mapping
  to `claims.preferred_username` for peer Dex users).
- **AIO Dex-off / compose / Kubernetes:** packaging or deploy manifests pass
  explicit serve flags or `.env` values. Compose/Keycloak typically uses
  `claims.github_username` via `KEY_REGISTRY_*` in `.env.template`.
