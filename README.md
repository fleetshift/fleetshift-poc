# fleetshift-poc

This repository represents both a **prototype** for a next generation k8s/OpenShift cluster management vision, alongside **individual POCs** for exploration of isolated concepts.

## Start here

The fastest way to try FleetShift is the all-in-one image (API, UI, and peer
Dex in one container). Needs **podman**. Sandbox only — not a production
deployment.

Open https://fleetshift-sandbox.localhost:8085 (redirects to `/app`) and accept
the browser certificate warning (unknown sandbox CA — Advanced/Proceed or
Accept Risk). Publish ports on `127.0.0.1` only.

Demo users (login is the email): `ops@fleetshift.local` / `fleetshift-ops` and
`dev@fleetshift.local` / `fleetshift-dev`.

### Bare run (peer Dex)

No OIDC flags. Packaging starts Dex and fills AuthMethod/UI defaults.

```bash
podman run -d --rm -it \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  quay.io/stolostron/fleetshift:latest
```

### With kind

Privileged + host engine socket (local/dev only). Create the network once:
`podman network create kind`. The `fleetshift` network alias is required.
Linux rootless: `systemctl --user enable --now podman.socket` and
`export PODMAN_SOCKET=$XDG_RUNTIME_DIR/podman/podman.sock` (not
`/var/run/docker.sock`).

```bash
podman run -d --rm -it \
  --privileged \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -v /tmp:/tmp \
  -v ${PODMAN_SOCKET:-/var/run/docker.sock}:/var/run/docker.sock \
  --network kind:alias=fleetshift \
  quay.io/stolostron/fleetshift:latest
```

### With GCP HCP

Peer Dex cannot back this addon. Set an external issuer and the CLS gateway;
packaging turns `gcphcp` on. Register
`https://fleetshift-sandbox.localhost:8085`, `/app/auth/callback`, and
`/app/silent-renew.html` on that IdP.

```bash
podman run -d --rm -it \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -e OIDC_ISSUER_URL=https://your-oidc-issuer/realms/fleetshift \
  -e GCPHCP_GATEWAY_URL=https://your-cls-gateway \
  quay.io/stolostron/fleetshift:latest
```

Add the kind flags from above when you also want local clusters (keep
`OIDC_ISSUER_URL`).

Build the image from this repo with `task image:aio`. Env defaults, Dex-off,
fleetctl, and packaging internals:
[deploy/aio/README.md](deploy/aio/README.md).

### Other ways to run

| Path | What you launch | Guide |
|------|-----------------|-------|
| Local compose stack | All-in-one image via compose/Taskfile (HTTPS origin, source builds, local-web watch) | [deploy/podman/](deploy/podman/README.md) |
| Kubernetes / OpenShift | Cluster deployment | [deploy/kubernetes/](deploy/kubernetes/README.md) |
| Keycloak (OpenShift) | External OIDC for cluster deploy or AIO compose (`OIDC_ISSUER_URL` in `.env`) | [deploy/keycloak/](deploy/keycloak/README.md) |
| Nx remote cache | Shared build cache backed by MinIO | [docs/nx-remote-cache.md](docs/nx-remote-cache.md) |

## Develop in this repo

### Prerequisites

- **Go 1.22+**
- **Node.js 20+** — for Nx and UI packages
- **[Task](https://taskfile.dev/)** — `go install github.com/go-task/task/v3/cmd/task@latest`
- **buf** — for protobuf generation (`brew install bufbuild/buf/buf`)
- `.env` file — copy from `.env.template` (compose stack and Kubernetes only)

Deployment-specific tools (podman, oc, kind, etc.) are listed in each
deployment guide.

### Setup

```bash
npm install                 # install all workspace dependencies (from repo root)
```

All UI packages are npm workspaces declared at the root. A single `npm install`
at the repo root handles everything — no separate install needed per package.

### Monorepo

This workspace uses [Nx](https://nx.dev) for build orchestration — caching,
dependency graph, affected detection, and parallel execution. Nx wraps the
existing Taskfile commands, so both `task` and `nx` work.

```bash
npx nx show projects        # list all projects
npx nx graph                # visualize dependency graph
npx nx run server:test      # run a single target (cached)
npx nx run-many -t test     # run target across all projects (parallel)
npx nx affected -t test     # only test what changed
```

Projects: `server`, `cli`, `deploy-aio`, `proto`, `web`, `common`, `build-utils`, `plugins`.

### Build

```bash
npx nx run server:build     # server
npx nx run cli:build        # fleetctl CLI
npx nx run deploy-aio:build # AIO packaging binaries
npx nx run common:build     # shared UI types/helpers
npx nx run plugins:build    # all MF remote plugins
npx nx run web:build        # SPA shell
npx nx run-many -t build    # build all (parallel, cached)

# Or via Taskfile directly:
task build:server
task build:cli
task build:aio
task build:all
```

Builds are cached — unchanged sources skip recompilation entirely.

### UI development

`web:dev` is **not** a standalone TLS origin — it rebuilds the SPA shell and
MF plugins on change into the repo-root `web/` dir, which the running AIO
stack serves. Run it in a second terminal alongside the stack:

```bash
npx nx run pd:dev LOCAL_WEB=true   # terminal 1: stack serves UI from host web/
npx nx run web:dev                 # terminal 2: full build, then watch-rebuild
npx nx run web:dev:watch           # terminal 2 alt: watch only (skip initial build)
```

Then open **https://fleetshift-sandbox.localhost:8085** (redirects to `/app`)
and accept the browser certificate warning. Dex is same-origin under `/idp`.

```bash
npx nx run web:test:ct      # component tests (playwright)
npx nx run plugins:test:ct  # plugin component tests
```

UI packages: `web` (SPA shell), `common` (shared types), `build-utils` (rspack
helpers), `plugins` (12 MF remotes)

### Test

```bash
npx nx run-many -t test     # unit tests for all modules
npx nx affected -t test     # only test what changed
npx nx run server:test      # Go server tests (cached)
npx nx run deploy-aio:test  # AIO packaging unit tests
npx nx run common:test      # shared UI lib tests

# Or via Taskfile:
task test:all
```

### Generate and images

```bash
npx nx run proto:generate   # regenerate protobuf and gRPC stubs

# Or via Taskfile:
task protogen
task image:build            # build server + web container images
task image:aio              # build all-in-one image from local server-local + web
task image:push             # push server, server-local, and web to DEV_REGISTRY (not the AIO image)
```

## Local compose stack

The local harness (`task pd:*` / `task podman:*`) runs the all-in-one image via
compose and is documented in [deploy/podman/README.md](deploy/podman/README.md).
Copy `.env.template` to `.env` first. Commands are also available through Nx;
env vars (`LOCAL_WEB`, `DEV`, `BUILD`, `NX_CACHE`) pass through to Taskfile.

Open https://fleetshift-sandbox.localhost:8085 after `pd:up` / `pd:dev`. If a
persisted volume still has the old `https://127.0.0.1:5556/dex` issuer, run
`npx nx run pd:clean` once.

```bash
npx nx run pd:dev                                    # build AIO from source, start stack
npx nx run pd:dev LOCAL_WEB=true                     # + serve UI from host web/ (watch mode)
npx nx run pd:up                                     # start stack (prebuilt image)
npx nx run pd:down                                   # stop stack
npx nx run pd:clean                                  # stop + remove volumes
npx nx run pd:status                                 # show container status
npx nx run pd:logs                                   # tail all logs
npx nx run pd:rebuild                                # rebuild and restart
npx nx run pd:rebuild-web                            # rebuild AIO image (baked UI) and restart
npx nx run pd:clock-drift                            # fix podman clock drift

# Or via Taskfile directly:
task pd:dev LOCAL_WEB=true
```

Point at an external issuer by setting `OIDC_ISSUER_URL` in `.env` (peer Dex
then parks) — see the OIDC scope caveat in `.env.template`.

Keycloak OCP (`task kc:*`) and Kubernetes OCP (`task k8s:*`) commands remain
Taskfile-only — they target remote clusters, not local compose.
