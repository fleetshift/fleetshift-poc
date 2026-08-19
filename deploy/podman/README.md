# Local compose stack

Compose launcher for the all-in-one image (`quay.io/stolostron/fleetshift`)
under `deploy/podman`: podman + docker-compose via Taskfile
(`task podman:*` / `task pd:*`). One container runs the TLS edge, API, baked-in
UI, and peer Dex.

This is **not** a multi-service Keycloak/Postgres harness. Packaging internals
and a raw `podman run` are documented in
[deploy/aio/README.md](../aio/README.md). Both paths use the same AIO image and
the same public origin:

```text
https://fleetshift-sandbox.localhost:8085
```

That URL redirects to `/app`. Accept the browser certificate warning (unknown
sandbox CA). Dex is same-origin under `/idp`. Port `5556` is not published.
gRPC remains on `127.0.0.1:50051` (plaintext; known gap).

## Prerequisites

- **podman** — container runtime
- **docker-compose** — `podman-compose` is not supported
- **kind** — for local cluster provisioning
- `.env` file — copy from `.env.template`

Create the kind network once: `podman network create kind`. Linux rootless:
`systemctl --user enable --now podman.socket` and
`export PODMAN_SOCKET=$XDG_RUNTIME_DIR/podman/podman.sock`.

## Quick Start

```bash
cp .env.template .env         # leave OIDC_ISSUER_URL unset for peer Dex
task build:cli                # build fleetctl Go binaries
task podman:dev               # build AIO from source and start
```

Open https://fleetshift-sandbox.localhost:8085 and accept the certificate
warning. Demo users (login is the email): `ops@fleetshift.local` /
`fleetshift-ops` and `dev@fleetshift.local` / `fleetshift-dev`.

If `/data` was persisted from a pre-HTTPS AIO run, the AuthMethod issuer is
still `https://127.0.0.1:5556/dex`. Reset once:

```bash
task pd:clean
task podman:dev
```

`start.sh` copies the sandbox CA to `deploy/podman/.certs/ca.crt` for fleetctl.
Do not install that CA into the host or browser trust store.

```bash
bin/fleetctl auth setup \
  --issuer-url https://fleetshift-sandbox.localhost:8085/idp \
  --client-id fleetshift-cli \
  --key-enrollment-client-id fleetshift-signing \
  --oidc-ca-file deploy/podman/.certs/ca.crt \
  --scopes 'openid,profile,email,audience:server:client_id:fleetshift'
bin/fleetctl auth login
```

Point at an external issuer by setting `OIDC_ISSUER_URL` in `.env` (peer Dex
then parks). Register `https://fleetshift-sandbox.localhost:8085`,
`/app/auth/callback`, and `/app/silent-renew.html` on that IdP.

AIO enables `gcphcp` from `GCPHCP_GATEWAY_URL` alone. Set it in `.env` when
needed. Do not commit a concrete CLS gateway URL.

## Tasks

All tasks use the `podman:` namespace (alias `pd:`).

| Task | Description |
|------|-------------|
| `podman:up` | Start the AIO stack (prebuilt image) |
| `podman:dev` | Build the AIO image from source, then up |
| `podman:down` | Stop containers, preserve data |
| `podman:clean` | Stop + delete volumes and `.certs` |
| `podman:rebuild` | Stop, rebuild the AIO image, restart |
| `podman:build` | Build the AIO image without restarting |
| `podman:pull` | Pull the latest all-in-one image |
| `podman:logs` | Follow logs from all containers |
| `podman:logs:<service>` | Tail specific service (e.g. `podman:logs:fleetshift-server`) |
| `podman:status` | Show running containers |
| `podman:restart:<service>` | Restart a specific container |
| `podman:rebuild-web` | Rebuild the AIO image (baked UI) and restart |

## Full Stack Dev Mode

`task podman:dev` builds the AIO image from this repo (`task image:aio`) and
starts it. After changing Go or UI sources that are baked into the image, run
`task podman:rebuild`.

### Local Web Watch Mode

For faster frontend iteration, bind-mount host `web/` over the baked UI:

```bash
# Terminal 1 — start the stack with local web assets
task podman:dev LOCAL_WEB=true

# Terminal 2 — watch & rebuild merged UI assets into monorepo-root web/
npx nx run web:dev
```

Open https://fleetshift-sandbox.localhost:8085 and refresh after rebuilds.

## Configuration

Copy `.env.template` to `.env` and edit. Command-line variables always override
`.env`. This stack uses AIO `OIDC_*` names; Kubernetes uses different ones —
see [deploy/aio/README.md](../aio/README.md) and
[deploy/kubernetes/README.md](../kubernetes/README.md).

Leave `OIDC_UI_SCOPE` unset. Packaging picks Dex-on vs Dex-off from whether
`OIDC_ISSUER_URL` is set. Setting the portable scope on Dex-on drops
`aud=fleetshift` from access tokens.
