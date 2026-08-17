# Local compose stack

Multi-service local deployment under `deploy/podman`: podman + docker-compose
via Taskfile (`task podman:*` / `task pd:*`). Runs server, web-builder, and
optional Keycloak/Postgres on your workstation.

This is **not** the all-in-one image. For the single-container AIO
(`quay.io/stolostron/fleetshift`, peer Dex, packaging defaults), see
[deploy/aio/README.md](../aio/README.md). Both paths often use podman as the
container engine; they are different products.

## Prerequisites

- **podman** — container runtime
- **docker-compose** — `podman-compose` is not compatible
- **[jq](https://github.com/jqlang/jq)** — JSON processing
- **kind** — for local cluster provisioning
- **[mkcert](https://github.com/filosottile/mkcert)** - trusted dev cert for local keycloak
- `.env` file — copy from `.env.template`

**Host mapping:** `fleetctl` (and `task podman:up` Keycloak checks) run on the
host and must resolve `keycloak`. That name is compose DNS only — add a
one-time `/etc/hosts` entry on Linux and macOS:

```bash
echo "127.0.0.1 keycloak" | sudo tee -a /etc/hosts
```

**macOS:** Podman may only forward IPv6 loopback; also add:

```bash
echo "::1 keycloak" | sudo tee -a /etc/hosts
```

## Quick Start

```bash
cp .env.template .env         # configure (edit as needed)
task build:cli                # build fleetctl Go binaries
task podman:up                # start the stack (demo mode)
bin/fleetctl auth setup \
  --issuer-url https://keycloak:8443/auth/realms/fleetshift \
  --client-id fleetshift-cli \
  --key-enrollment-client-id fleetshift-signing \
  --oidc-ca-file deploy/podman/.certs/ca.crt
bin/fleetctl auth login       # log in (opens browser)
```

`gcphcp` is opt-in in this harness. A plain `task podman:up` starts the default
local addon set without `gcphcp`. To enable `gcphcp`, set
`GCPHCP_ENABLED=true` and `GCPHCP_GATEWAY_URL` (the only required value). The
seven optional `GCPHCP_*` overrides default in
`deploy/scripts/render-gcphcp-config.sh` when left empty:

```bash
GCPHCP_ENABLED=true
GCPHCP_GATEWAY_URL=https://<your-cls-gateway>
# Optional overrides — leave empty to use renderer defaults:
# GCPHCP_GATEWAY_AUDIENCE, GCPHCP_TARGET_ID, GCPHCP_GCP_PROJECT,
# GCPHCP_GCP_REGION, GCPHCP_WORKFORCE_POOL, GCPHCP_WORKFORCE_PROVIDER,
# GCPHCP_BROKER_SA_EMAIL

task podman:up AUTH=external
```

`GCPHCP_ENABLED=true` requires `AUTH=external`. The task fails early if local
Keycloak auth is selected. For `AUTH=external`, also set `OIDC_ISSUER_URL` in
`.env`. For `AUTH=local`, the stack always uses bundled Keycloak
(`https://$KC_HOSTNAME:$KC_HTTPS_PORT/auth/realms/fleetshift`) and ignores
`.env`'s `OIDC_ISSUER_URL`.

At startup, the harness renders `deploy/podman/.gcphcp.yaml` from `.env`
(before Compose starts), mounts that file into `fleetshift-server`, and adds
`gcphcp` to the explicit addon list for the deployment.

## Deploy Modes

| Mode | DB | Auth | Use Case |
|------|-----|------|----------|
| `demo` (default) | SQLite | Local Keycloak | Local dev, demos |
| `prod` | PostgreSQL | External OIDC | Production-like |

Override axes independently with `DB=sqlite|postgres` and `AUTH=local|external`.

```bash
task podman:up DEPLOY_MODE=prod
task podman:up DB=postgres AUTH=local
```

## Tasks

All tasks use the `podman:` namespace (alias `pd:`).

| Task | Description |
|------|-------------|
| `podman:up` | Start the stack (demo mode by default) |
| `podman:dev` | Dev mode — source mounts + hot-reload |
| `podman:down` | Stop containers, preserve data |
| `podman:clean` | Stop + delete all data/volumes/network |
| `podman:rebuild` | Stop, rebuild images, restart |
| `podman:build` | Build container images without restarting |
| `podman:pull` | Pull latest images |
| `podman:logs` | Follow logs from all containers |
| `podman:logs:<service>` | Tail specific service (e.g. `podman:logs:fleetshift-server`) |
| `podman:status` | Show running containers |
| `podman:restart:<service>` | Restart a specific container |
| `podman:rebuild-web` | Rebuild frontend without restarting server |
| `podman:test-attestation` | Run end-to-end attestation flow |
| `podman:reset-keycloak` | Wipe Keycloak state (AUTH=local only) |

## Full Stack Dev Mode

`task podman:dev` builds frontend assets in a container (using `Dockerfile.web`) and starts the Go backend serving everything on `:8085`. No host Node.js or npm required.

After changing Go code, run `task podman:rebuild` to rebuild and restart. After changing frontend code, run `task podman:clean` then `task podman:dev` to rebuild the web assets.

### Local Web Watch Mode

For faster frontend iteration, serve assets directly from your host filesystem instead of rebuilding the Docker web-builder on every change:

```bash
# Terminal 1 — start the stack with local web assets
task podman:dev LOCAL_WEB=true

# Terminal 2 — watch & rebuild merged UI assets into monorepo-root web/
npx nx run web:dev
```

This skips the Docker web-builder and bind-mounts the monorepo-root `web/` directory (from `tools/merge-web.mjs`) into the container. The watch build rebuilds on source changes, and the Go backend picks up the new assets — just refresh the browser.

**Open the UI at `http://127.0.0.1:8085` — not `localhost`.** With the built-in
Dex sandbox IdP, peer Dex issues at `https://127.0.0.1:5556/dex` and only
registers the `127.0.0.1` redirect URI and CORS origin. A `localhost:8085` tab
loads the SPA but fails the OIDC redirect (`redirect_uri did not match`).

**Trust the Dex CA once** so the browser stops warning on the login redirect:

```bash
task pd:trust-cert     # macOS: adds the sandbox CA to the login keychain
                       # task pd:trust-cert -- --remove   to undo
```

Clicking through Chrome's "proceed anyway" only covers `127.0.0.1:5556` for that
session and does not fix the app login (the SPA calls Dex in the background,
which Chrome still blocks on an untrusted cert). `pd:trust-cert` is the real fix.

## Configuration

Copy `.env.template` to `.env` and edit. All available settings are documented in the template. Command-line variables always override `.env`.

This stack runs the all-in-one image, so `fleetshift-server` is configured with
the AIO OIDC env names (`OIDC_ISSUER_URL`, `OIDC_UI_SCOPE`,
`OIDC_REGISTRY_ID` / `OIDC_REGISTRY_SUBJECT_EXPRESSION` /
`OIDC_PUBLIC_KEY_CLAIM_EXPRESSION`, …). The Kubernetes deploy reads the same
`.env` but uses different names (`KEY_REGISTRY_*`, `PUBLIC_KEY_CLAIM_EXPR`,
`OIDC_AUDIENCE`) — see [deploy/aio/README.md](../aio/README.md) and
[deploy/kubernetes/README.md](../kubernetes/README.md).

### External OIDC scopes

Leaving `OIDC_ISSUER_URL` unset uses the built-in Dex sandbox IdP, whose default
UI scope requests a Dex-specific resource audience:

```
OIDC_UI_SCOPE="openid profile email groups audience:server:client_id:fleetshift"
```

A general external OIDC provider (Keycloak, etc.) does **not** understand the
`audience:server:client_id:...` syntax and rejects the login with an invalid
scope error. When you set `OIDC_ISSUER_URL` to an external issuer, override the
scope with plain values and configure the token audience in the IdP instead:

```
OIDC_UI_SCOPE="openid profile email"
```

### `gcphcp` Addon Toggle

- Default: `kind,kubernetes`
- Add `gcphcp`: set `GCPHCP_ENABLED=true` and `GCPHCP_GATEWAY_URL` in `.env`
  (`AUTH=external` required). Optional `GCPHCP_*` overrides use renderer
  defaults when empty. (AIO enables gcphcp from `GCPHCP_GATEWAY_URL` alone.)
- Runtime artifact: this harness renders `deploy/podman/.gcphcp.yaml` from `.env`
  and mounts it as `/config/gcphcp.yaml`
- Follow-on tasks such as `task podman:logs`, `task podman:status`, and
  `task podman:rebuild` recalculate the rendered config from the current `.env`
