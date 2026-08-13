# FleetShift all-in-one image

Single container (`quay.io/stolostron/fleetshift`) with API, baked-in UI, and
peer Dex under s6-overlay. Documented and tested with **podman**. This is
**not** the multi-service compose harness in
[deploy/podman/](../podman/README.md).

Sandbox for demos, bootstrapping, and testing — not a production deployment.
Packaging owns PID 1, peer Dex (default), sandbox PKI, and the ordinary
`fleetshift serve` argv. The server stays IdP-agnostic.

## Build

```bash
task image:aio
```

That builds `Dockerfile` / `Dockerfile.local` / `Dockerfile.web`, then assembles
via `Dockerfile.fleetshift`. To reassemble from already-built component tags:

```bash
podman build -f Dockerfile.fleetshift \
  --build-arg SERVER_IMAGE=fleetshift-server-local:latest \
  --build-arg WEB_IMAGE=quay.io/stolostron/fleetshift-web:latest \
  -t quay.io/stolostron/fleetshift:latest .
```

## Quick start (Dex-on)

Bare run needs **no** OIDC flags. Packaging starts peer Dex at
`https://127.0.0.1:5556/dex` and fills AuthMethod/UI defaults for `serve`.

```bash
podman run -d --rm -it \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -p 127.0.0.1:5556:5556 \
  quay.io/stolostron/fleetshift:latest
```

Open http://127.0.0.1:8085.

Browsers in the same host network namespace will see Dex's sandbox CA as an
unknown authority; continue only the interstitial for `https://127.0.0.1:5556`
(do not import the CA into the browser). Non-browser clients should copy the
sandbox CA and use it as a scoped trust root:

```bash
podman cp <ctr>:/data/sandbox/pki/ca.crt ./ca.crt
```

### Demo users (Dex-on)

Public sandbox fixtures (not production credentials). Login identifier is the
email:

| Persona | Email | Password | `preferred_username` | Group |
|---|---|---|---|---|
| Operator | `ops@fleetshift.local` | `fleetshift-ops` | `ops-user` | `ops` |
| Developer | `dev@fleetshift.local` | `fleetshift-dev` | `dev-user` | `dev` |

## Packaging defaults

AIO fills omitted AuthMethod/UI fields. These are **packaging** defaults, not
`fleetshift serve` binary defaults. Compose/Kubernetes use a different config
surface (`.env` / `KEY_REGISTRY_*`); do not mix the two.

| Setting | Env (AIO) | Packaging default |
|---|---|---|
| Issuer | `OIDC_ISSUER_URL` | Peer Dex `https://127.0.0.1:5556/dex` when unset |
| UI client | `OIDC_UI_CLIENT_ID` | `fleetshift-ui` |
| UI scope | `OIDC_UI_SCOPE` | `openid profile email groups audience:server:client_id:fleetshift` |
| Resource audience | `OIDC_RESOURCE_AUDIENCE` | `fleetshift` |
| Enrollment audience | `OIDC_KEY_ENROLLMENT_AUDIENCE` | `fleetshift-signing` |
| Registry ID | `OIDC_REGISTRY_ID` | `github.com` (when not using public-key claim) |
| Registry subject | `OIDC_REGISTRY_SUBJECT_EXPRESSION` | `claims.preferred_username` |
| Public-key claim | `OIDC_PUBLIC_KEY_CLAIM_EXPRESSION` | none (caller-chosen; mutually exclusive with registry) |
| OIDC CA | `OIDC_CA_FILE` | sandbox CA on Dex-on; optional on Dex-off |
| Log level | `FLEETSHIFT_LOG_LEVEL` | `debug` |
| Addons | `FLEETSHIFT_SERVER_ADDONS` | `kind,kubernetes` (adds `gcphcp` when gateway/config set) |
| Container socket | `CONTAINER_HOST` | `unix:///var/run/docker.sock` |

Registry id and subject expression must be set together when overriding either.
Registry mapping and `OIDC_PUBLIC_KEY_CLAIM_EXPRESSION` are mutually exclusive.

## External issuer (Dex-off)

Presence of `OIDC_ISSUER_URL` skips peer Dex and forwards that issuer into the
same serve bootstrap path. Packaging still fills omitted fields above. Pass
`OIDC_CA_FILE` only when discovery/TLS needs non-system trust. Issuer URL
shape, CA readability/PEM, and registry/claim pairing are validated by
`fleetshift serve`. Packaging still fails closed on registry half-pairs /
claim+registry mutual exclusion when applying serve defaults.

Minimal:

```bash
podman run -d --rm -it \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -e OIDC_ISSUER_URL=https://your-oidc-issuer/realms/fleetshift \
  quay.io/stolostron/fleetshift:latest
```

All OIDC overrides (values shown are packaging defaults except issuer/CA and the
public-key claim alternative):

```bash
podman run -d --rm -it \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -e OIDC_ISSUER_URL=https://your-oidc-issuer/realms/fleetshift \
  -e OIDC_CA_FILE=/path/to/ca.crt \
  -e OIDC_UI_CLIENT_ID=fleetshift-ui \
  -e OIDC_UI_SCOPE='openid profile email groups audience:server:client_id:fleetshift' \
  -e OIDC_RESOURCE_AUDIENCE=fleetshift \
  -e OIDC_KEY_ENROLLMENT_AUDIENCE=fleetshift-signing \
  -e OIDC_REGISTRY_ID=github.com \
  -e OIDC_REGISTRY_SUBJECT_EXPRESSION=claims.preferred_username \
  quay.io/stolostron/fleetshift:latest
```

To use a public-key claim instead of registry mapping, omit
`OIDC_REGISTRY_ID` / `OIDC_REGISTRY_SUBJECT_EXPRESSION` and set
`-e OIDC_PUBLIC_KEY_CLAIM_EXPRESSION=claims.spk`.

## With kind

Privileged + host container socket (full control of the host engine —
local/dev only). Create the `kind` network once if needed:
`podman network create kind`.

When a live unix socket is present at `CONTAINER_HOST`, packaging writes
`/run/fleetshift/kind.env`:

- `KIND_EXPERIMENTAL_DOCKER_NETWORK=kind` unless already set (including
  intentionally empty to disable)
- On Dex-on only: `KIND_NODE_ROUTE_BACKEND=<aio-ip>:5556` so kind control-plane
  nodes DNAT `127.0.0.1:5556` to this AIO for the loopback issuer URL. If no
  suitable IPv4 is available (e.g. not on `--network kind`), init fails.

```bash
podman run -d --rm -it \
  --privileged --user 0:0 \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -p 127.0.0.1:5556:5556 \
  -v /tmp:/tmp \
  -v ${PODMAN_SOCKET:-/var/run/docker.sock}:/var/run/docker.sock \
  --network kind \
  quay.io/stolostron/fleetshift:latest
```

## With GCP HCP

GCP HCP requires an external OIDC issuer (peer Dex is not supported for this
addon). Set `OIDC_ISSUER_URL` so packaging skips Dex. Supply
`GCPHCP_GATEWAY_URL` alone to enable the addon (no `GCPHCP_ENABLED` needed —
unlike the compose harness). Optional `GCPHCP_*` overrides use shared renderer
defaults:

```bash
podman run -d --rm -it \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -e OIDC_ISSUER_URL=https://your-oidc-issuer/realms/fleetshift \
  -e GCPHCP_GATEWAY_URL=https://your-cls-gateway \
  quay.io/stolostron/fleetshift:latest
```

Combine with the kind flags above when you also need local cluster provisioning
(still keep `OIDC_ISSUER_URL`; omit the Dex `5556` publish).

## Fleetctl against Dex-on

```bash
podman cp <container>:/data/sandbox/pki/ca.crt .

fleetctl auth setup \
  --issuer-url https://127.0.0.1:5556/dex \
  --client-id fleetshift-cli \
  --key-enrollment-client-id fleetshift-signing \
  --oidc-ca-file ca.crt \
  --scopes 'openid,profile,email,audience:server:client_id:fleetshift'

fleetctl auth login
fleetctl auth inspect-token   # aud should include fleetshift (and fleetshift-cli)
fleetctl deployments list
```

`auth login` opens a browser to Dex; sign in with a demo user above. Omit the
`audience:server:client_id:fleetshift` scope and API calls fail with `aud` not
satisfied (`fleetshift-cli` only).

---

## Packaging internals

### Pins

Dex image digest and s6-overlay version/checksums are `ARG` defaults in
[`Dockerfile.fleetshift`](../../Dockerfile.fleetshift). The build pulls Dex by
digest and verifies s6 tarball SHA-256 after download. Image inspection should
show `/init` as PID 1.

### Layout

| Path | Role |
|---|---|
| `cmd/aio-init` | Init helper: Dex-on/off branch, PKI, Dex config, serve argv |
| `internal/aioinit` | Packaging helpers (same package, separate files: `endpoints`, `pki`, `dexconfig`, `serveargv`, `gcphcp`, `kind`) |
| `s6/` | s6-overlay v3 service defs (copied to `/etc/s6-overlay/`) |

s6 services follow the [s6-overlay README](https://github.com/just-containers/s6-overlay/blob/v3.2.3.2/README.md)
source format:

- definitions under `s6-rc.d/{aio-init,dex,fleetshift}/`
- membership via `user-bundles.d/user/contents.d/`
- oneshot `aio-init` script at `scripts/aio-init` (referenced from `up`)
- longruns depend on `base` (and on each other: `dex` → `aio-init`, `fleetshift` → `aio-init`+`dex`)

### Branches

- **Dex-on (default):** no `OIDC_ISSUER_URL`. `aio-init` writes
  `/run/fleetshift/dex.enabled`; the `dex` longrun execs peer Dex; packaging
  wires AuthMethod/UI defaults into serve.
- **Dex-off:** `OIDC_ISSUER_URL` set. No `dex.enabled` flag; the `dex` longrun
  parks on `s6-pause`. Packaging forwards the issuer and fills the same defaults
  for omitted fields.

### Identities

- FleetShift: `1000:1000`
- Dex: `1001:1001`
- s6 starts as container root only to initialize and drop privileges
