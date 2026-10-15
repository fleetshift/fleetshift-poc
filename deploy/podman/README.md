# Local Podman compose stack

Compose launcher for the all-in-one image (`quay.io/stolostron/ocm-resource-manager`)
under `deploy/podman`: podman + docker-compose via Nx (`deploy:podman:*` targets). One container runs the TLS edge, API, baked-in
UI, and peer Dex.

This is **not** a multi-service Keycloak/Postgres harness. Packaging internals
and a raw `podman run` are documented in
[deploy/aio/README.md](../aio/README.md). Both paths use the same AIO image and
the same public origin:

```text
https://ocm-resource-manager.localhost:8085
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
  npx nx run cli:build          # build TypeScript fleetctl
  npx nx run deploy:podman:dev             # build AIO from source and start
```

Open https://ocm-resource-manager.localhost:8085 and accept the certificate
warning. Demo users (login is the email): `ops@ocm-resource-manager.local` /
`ocm-resource-manager-ops` and `dev@ocm-resource-manager.local` / `ocm-resource-manager-dev`.

If `/data` was persisted from a pre-HTTPS AIO run, the AuthMethod issuer is
still `https://127.0.0.1:5556/dex`. Reset once:

```bash
 npx nx run deploy:podman:clean
 npx nx run deploy:podman:dev
```

`start.mjs` copies the sandbox CA to `deploy/podman/.certs/ca.crt` for fleetctl.
Do not install that CA into the host or browser trust store.

```bash
bin/fleetctl auth setup \
  --issuer-url https://ocm-resource-manager.localhost:8085/idp \
  --client-id ocm-resource-manager-cli \
  --key-enrollment-client-id ocm-resource-manager-signing \
  --oidc-ca-file deploy/podman/.certs/ca.crt \
  --scopes 'openid,profile,email,audience:server:client_id:ocm-resource-manager'
bin/fleetctl auth login
```

Point at an external issuer by setting `OIDC_ISSUER_URL` in `.env` (peer Dex
then parks). Register `https://ocm-resource-manager.localhost:8085`,
`/app/auth/callback`, and `/app/silent-renew.html` on that IdP.

## Tasks

Commands for this Podman stack use Nx `deploy:podman:*` targets.

| Task | Description |
|------|-------------|
| `deploy:podman:up` | Start the AIO stack (prebuilt image) |
| `deploy:podman:dev` | Build the AIO image from source, then up |
| `deploy:podman:down` | Stop containers, preserve data |
| `deploy:podman:clean` | Stop + delete volumes and `.certs` |
| `deploy:podman:rebuild` | Stop, rebuild the AIO image, restart |
| `image:aio` | Build the AIO image without restarting |
| `deploy:podman:pull` | Pull the latest all-in-one image |
| `deploy:podman:logs` | Follow logs from all containers |
| `deploy:podman:status` | Show running containers |
| `deploy:podman:clock-drift` | Check Podman machine clock drift |

## Full Stack Dev Mode

`npx nx run deploy:podman:dev` builds the AIO image from this repo (`npx nx run image:aio`) and
starts it. After changing Go or UI sources that are baked into the image, run
`npx nx run deploy:podman:rebuild`.

### Local Web Watch Mode

For faster frontend iteration, bind-mount host `web/` over the baked UI:

```bash
# Terminal 1 — start the stack with local web assets
npx nx run deploy:podman:dev LOCAL_WEB=true

# Terminal 2 — watch & rebuild merged UI assets into monorepo-root web/
npx nx run web:dev
```

Open https://ocm-resource-manager.localhost:8085 and refresh after rebuilds.

## Configuration

Copy `.env.template` to `.env` and edit. Command-line variables always override
`.env`. This stack uses AIO `OIDC_*` names; the OpenShift example uses different ones —
see [deploy/aio/README.md](../aio/README.md) and
[deploy/openshift/README.md](../openshift/README.md).

Leave `OIDC_UI_SCOPE` unset. Packaging picks Dex-on vs Dex-off from whether
`OIDC_ISSUER_URL` is set. Setting the portable scope on Dex-on drops
`aud=ocm-resource-manager` from access tokens.
