# OCM Fleet

OCM Fleet is an experimental variant of Open Cluster Management (OCM). The OCM Fleet architecture is a resource manager architecture, built around a dedicated resource manager server. The project includes the server, console, `fleetctl` CLI, fleetlet, and addon/extension interfaces. Addons participate in platform deployments through API, UI, orchestration, and delivery capabilities. Kubernetes is the primary built-in target; addon interfaces support additional target types. Standalone proofs of concept explore individual parts of the design.

The resource manager server owns domain APIs and persistence directly in SQLite or PostgreSQL, providing the foundation for consolidated management and inventory. Native multitenancy and role-based access control are architectural goals. This differs from OCM's Kubernetes API-based management plane; standalone operation itself is already supported by OCM. This experiment is preparation for OCM upstream discussions, without a claim of formal acceptance or complete component compatibility.

See the [architecture](docs/design/architecture.md), [architecture glossary](docs/design/glossary.md), and [contributor acknowledgment](docs/contributors.md).

## Start here

### Podman

One way to try OCM Fleet is the all-in-one image (API, UI, and peer
Dex in one container). Needs **podman**. Sandbox only — not a production
deployment.

Open https://ocm-fleet.localhost:8085 (redirects to `/app`) and accept
the browser certificate warning (unknown sandbox CA — Advanced/Proceed or
Accept Risk). Publish ports on `127.0.0.1` only.

Demo users (login is the email): `ops@ocm-fleet.local` / `ocm-fleet-ops` and
`dev@ocm-fleet.local` / `ocm-fleet-dev`.

#### Bare run (peer Dex)

No OIDC flags. Packaging starts Dex and fills AuthMethod/UI defaults.

```bash
podman run \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  quay.io/stolostron/ocm-fleet:latest
```

#### With kind

Privileged + host engine socket (local/dev only). Create the network once:
`podman network create kind`. The `ocm-fleet` network alias is required.
Linux rootless: `systemctl --user enable --now podman.socket` and
`export PODMAN_SOCKET=$XDG_RUNTIME_DIR/podman/podman.sock` (not
`/var/run/docker.sock`).

```bash
podman run \
  --privileged \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -v /tmp:/tmp \
  -v ${PODMAN_SOCKET:-/var/run/docker.sock}:/var/run/docker.sock \
  --network kind:alias=ocm-fleet \
  quay.io/stolostron/ocm-fleet:latest
```

Build the image from this repo with `npx nx run image:aio`. Env defaults, Dex-off,
fleetctl, and packaging internals:
[deploy/aio/README.md](deploy/aio/README.md).

Use fresh data volumes and client state for this prototype, then set up
authentication and re-enroll signing keys using the [CLI guide](client/cli/README.md)
or console. Remove any previous development clusters and volumes explicitly.

### Other ways to run

| Path | What you launch | Guide |
|------|-----------------|-------|
| Podman compose stack | All-in-one image via compose/Nx (HTTPS origin, source builds, local-web watch) | [deploy/podman/](deploy/podman/README.md) |
| OpenShift example | Cluster deployment using Routes and ImageStreams | [deploy/openshift/](deploy/openshift/README.md) |
| Keycloak (OpenShift) | External OIDC for cluster deploy or AIO compose (`OIDC_ISSUER_URL` in `.env`) | [deploy/openshift/keycloak/](deploy/openshift/keycloak/README.md) |
| Nx remote cache | Shared build cache backed by MinIO | [docs/nx-remote-cache.md](docs/nx-remote-cache.md) |

## Develop in this repo

### Prerequisites

- **Go 1.22+**
- **Node.js 20+** — for Nx and UI packages
- **buf** — for protobuf generation (`brew install bufbuild/buf/buf`)
- `.env` file — copy from `.env.template` for the Podman and OpenShift examples

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
dependency graph, affected detection, and parallel execution.

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
npx nx run cli:build        # package build; also updates bin/fleetctl
npx nx run fleetctl         # shortcut for the package build
npx nx run deploy-aio:build # AIO packaging binaries
npx nx run common:build     # shared UI types/helpers
npx nx run plugins:build    # all MF remote plugins
npx nx run web:build        # SPA shell
npx nx run-many -t build    # build all (parallel, cached)

```

Builds are cached — unchanged sources skip recompilation entirely.

CLI guide: [client/cli/README.md](client/cli/README.md).

### Server debugging in VS Code

Select `resource-manager serve` (debug logging) or `resource-manager serve (info)`
in Run and Debug. Both launch the API server with a fresh temporary SQLite
database, HTTP on `127.0.0.1:8090`, and gRPC on `127.0.0.1:50052`.

The fresh database requires a running OIDC issuer. The launch prompts for its URL
and CA file; defaults use the [local Podman stack](deploy/podman/README.md), which
must be started first. CA paths are relative to `server/` or absolute; leave the
CA input empty for an issuer trusted by the system. These profiles serve the API
only; the Podman stack continues to serve its own API and console.

### UI development

`web:dev` is **not** a standalone TLS origin — it rebuilds the SPA shell and
MF plugins on change into the repo-root `web/` dir.

#### Podman

With the Podman AIO stack, run it in a second terminal alongside the stack:

```bash
npx nx run deploy:podman:dev LOCAL_WEB=true   # terminal 1: stack serves UI from host web/
npx nx run web:dev                 # terminal 2: full build, then watch-rebuild
npx nx run web:dev:watch           # terminal 2 alt: watch only (skip initial build)
```

Then open **https://ocm-fleet.localhost:8085** (redirects to `/app`)
and accept the browser certificate warning. Dex is same-origin under `/idp`.

#### Component tests

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

npx nx test:e2e e2e-web      # Playwright UI journeys (needs AIO at :8085)
npx nx test:e2e e2e-cli      # Playwright CLI journeys (fleetctl, Kind, delivery)
```

Frontend e2e expects the sandbox origin
(`https://ocm-fleet.localhost:8085`). Playwright flags after `--`,
for example `npx nx test:e2e e2e-web -- --ui` — see
[e2e/web/README.md](e2e/web/README.md). CLI e2e builds fleetctl and starts
the same shared sandbox runner. How to add tests, Kind pool usage, and CI:
[docs/testing/end-to-end.md](docs/testing/end-to-end.md).

### Generate and images

```bash
npx nx run proto:generate   # regenerate protobuf and gRPC stubs

```

## Deployment stacks

### Podman

The Podman harness (`deploy:podman:*` Nx targets) runs the all-in-one image via
compose and is documented in [deploy/podman/README.md](deploy/podman/README.md).
Copy `.env.template` to `.env` first. Commands are also available through Nx;
env vars (`LOCAL_WEB`, `DEV`, `BUILD`, `NX_CACHE`) pass through to deployment scripts.

Open https://ocm-fleet.localhost:8085 after `deploy:podman:up` / `deploy:podman:dev`. If a
persisted volume still has the old `https://127.0.0.1:5556/dex` issuer, run
`npx nx run deploy:podman:clean` once.

```bash
npx nx run deploy:podman:dev                                    # build AIO from source, start stack
npx nx run deploy:podman:dev LOCAL_WEB=true                     # + serve UI from host web/ (watch mode)
npx nx run deploy:podman:up                                     # start stack (prebuilt image)
npx nx run deploy:podman:down                                   # stop stack
npx nx run deploy:podman:clean                                  # stop + remove volumes
npx nx run deploy:podman:status                                 # show container status
npx nx run deploy:podman:logs                                   # tail all logs
npx nx run deploy:podman:rebuild                                # rebuild and restart
npx nx run deploy:podman:clock-drift                            # check podman clock drift

```

Point at an external issuer by setting `OIDC_ISSUER_URL` in `.env` (peer Dex
then parks) — see the OIDC scope caveat in `.env.template`.

### OpenShift

The `deploy:openshift:*` targets use Nx and target remote clusters,
including the Keycloak and MinIO deployment workflows.
See [deploy/openshift/](deploy/openshift/README.md).
