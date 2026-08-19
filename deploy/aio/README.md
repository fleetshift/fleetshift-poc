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

Bare run needs **no** OIDC flags. Packaging starts peer Dex behind the AIO TLS
edge at `https://fleetshift-sandbox.localhost:8085/idp` and fills AuthMethod/UI
defaults for `serve`.

```bash
podman run -d --rm -it \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  quay.io/stolostron/fleetshift:latest
```

Open https://fleetshift-sandbox.localhost:8085 — the exact path `/` redirects
to `/app/`.

Routing on this origin is prefix-based, not a catch-all into the SPA:

1. **Edge** — `/idp`… goes to peer Dex (Dex-on only). Every other path is
   forwarded to FleetShift.
2. **FleetShift** — serves the paths below. Only exact `/` and `/app` redirect
   to `/app/`. Unknown paths return 404; they are not rewritten into `/app`.

| Path | Serves |
|------|--------|
| `/idp`… | Peer Dex (Dex-on only) |
| `/app`… | SPA and static assets |
| `/api`, `/v1`, `/apis`… | FleetShift HTTP APIs |
| `/livez`, `/readyz` | Health probes |
| `/` or `/app` (exact) | 302 to `/app/` |

The sandbox certificate is intentionally browser-untrusted (private CA). In an
unmanaged desktop Chrome, Firefox, or Safari profile that allows overrides,
accept the top-level warning (Advanced → Proceed / Accept the Risk). That is
the only certificate interstitial for the SPA, API, WebSockets, and peer Dex:
they share this origin.

Host CLIs (`curl`, `fleetctl`, Go clients) still need a scoped `--cacert` /
`--oidc-ca-file`; they do not pick up the browser exception.

`.localhost` names can resolve to both `127.0.0.1` and `::1`. The documented
command publishes IPv4 loopback only. If a supported platform does not fall
back to IPv4, add a second `[::1]:8085:8085` publish for that platform — never
replace the scoped bind with `0.0.0.0` or `::`.

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

**Required from you** — everything else may be left unset.

| Mode | You must set |
|---|---|
| Dex-on (default) | nothing |
| Dex-off | `OIDC_ISSUER_URL` |
| Dex-off + GCP HCP | `OIDC_ISSUER_URL` and `GCPHCP_GATEWAY_URL` (or `GCPHCP_CONFIG`) |
| Kind | a live engine socket mounted at `CONTAINER_HOST` |

**Optional — packaging fills if omitted.** Set an env var only to override.

| Setting | Env | If omitted |
|---|---|---|
| Issuer | `OIDC_ISSUER_URL` | Peer Dex `https://fleetshift-sandbox.localhost:8085/idp` (Dex-on). Setting this switches to Dex-off; there is no packaging issuer in that mode. |
| UI client | `OIDC_UI_CLIENT_ID` | `fleetshift-ui` |
| UI scope | `OIDC_UI_SCOPE` | Dex-on: `openid profile email groups audience:server:client_id:fleetshift`. Dex-off: `openid profile email` |
| Resource audience | `OIDC_RESOURCE_AUDIENCE` | `fleetshift` |
| Enrollment audience | `OIDC_KEY_ENROLLMENT_AUDIENCE` | `fleetshift-signing` |
| Registry ID | `OIDC_REGISTRY_ID` | `github.com`, unless a public-key claim is set |
| Registry subject | `OIDC_REGISTRY_SUBJECT_EXPRESSION` | `claims.preferred_username`, unless a public-key claim is set |
| Public-key claim | `OIDC_PUBLIC_KEY_CLAIM_EXPRESSION` | unset (registry mapping is used instead) |
| OIDC CA | `OIDC_CA_FILE` | sandbox CA on Dex-on. Dex-off: unset unless discovery/TLS needs non-system trust |
| Log level | `FLEETSHIFT_LOG_LEVEL` | `debug` |
| Addons | `FLEETSHIFT_SERVER_ADDONS` | `kind,kubernetes` (`gcphcp` is appended when gateway/config is set) |
| Container socket | `CONTAINER_HOST` | image default `unix:///var/run/docker.sock`. Override only if the mount path differs |
| Kind loopback forward | `KIND_LOOPBACK_FORWARD_TO` | Dex-on + live socket: `fleetshift:8085`. Set empty to disable. Not written on Dex-off |

If you override registry ID or subject expression, set both. Registry mapping and
`OIDC_PUBLIC_KEY_CLAIM_EXPRESSION` are mutually exclusive (`fleetshift serve`
enforces this).

## External issuer (Dex-off)

Presence of `OIDC_ISSUER_URL` skips peer Dex and forwards that issuer into the
same serve bootstrap path. The AIO UI/API edge stays
`https://fleetshift-sandbox.localhost:8085`; do not proxy the external IdP
under `/idp`. Register that origin, `/app/auth/callback`, and
`/app/silent-renew.html` on the external client. Packaging still fills omitted
fields above, but UI scope becomes portable OIDC (`openid profile email`)
instead of Dex cross-client audience scopes that Keycloak rejects as
`invalid_scope`. Pass `OIDC_CA_FILE` only when discovery/TLS needs non-system
trust. Issuer URL shape, CA readability/PEM, and registry/claim pairing are
validated by `fleetshift serve`. Packaging fills omitted AuthMethod/UI defaults
and forwards the resolved serve argv.

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
  -e OIDC_UI_SCOPE='openid profile email' \
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
- On Dex-on only: `KIND_LOOPBACK_FORWARD_TO=fleetshift:8085` so kind
  control-plane nodes run a loopback TCP proxy (`127.0.0.1:8085` →
  `fleetshift:8085`, the AIO TLS edge). The kind addon also installs a
  kube-apiserver `hostAliases` mapping for the OIDC issuer hostname
  (via a kubeadm patch directory extra-mounted into control-plane nodes)
  so that name resolves to `127.0.0.1` before first start. A TCP proxy
  binary is run as a systemd unit on the node (not iptables DNAT) so
  Fedora and macOS behave the same. Podman DNS resolves the `fleetshift`
  alias, so AIO restarts keep working. Override with a different
  `host:port`, or set the variable empty to disable both the proxy and
  the hostAliases overlay.

Join `--network kind:alias=fleetshift`. Without that alias the default
destination host does not resolve from kind nodes.

On Linux, rootless Podman does not listen on `/var/run/docker.sock` (that
path is Docker, or a macOS `podman machine` helper symlink). The API socket
is `$XDG_RUNTIME_DIR/podman/podman.sock`, and only after the user systemd
unit is running:

```bash
export PODMAN_SOCKET=$XDG_RUNTIME_DIR/podman/podman.sock
systemctl --user enable podman.socket
systemctl --user restart podman.socket
```

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
(still keep `OIDC_ISSUER_URL`). The AIO UI remains
`https://fleetshift-sandbox.localhost:8085`; register that origin,
`/app/auth/callback`, and `/app/silent-renew.html` on the external IdP. `/idp`
is not used in this mode.

## Fleetctl against Dex-on

Host CLIs do not inherit the browser certificate exception. Copy the sandbox CA
for a scoped `--oidc-ca-file` only — do not install it into the system trust
store:

```bash
podman cp <container>:/data/sandbox/pki/ca.crt .

fleetctl auth setup \
  --issuer-url https://fleetshift-sandbox.localhost:8085/idp \
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
| `cmd/aio-init` | Init helper: Dex-on/off branch, PKI, Dex config, serve argv, hosts alias, public.env |
| `cmd/aio-proxy` | TLS edge: terminates the gateway cert and reverse-proxies Dex and FleetShift |
| `cmd/kind-loopback-forward` | TCP proxy copied onto kind control-planes for the public origin |
| `internal/aioinit` | Packaging helpers (same package, separate files: `endpoints`, `pki`, `dexconfig`, `serveargv`, `gcphcp`, `kind`, `hosts`) |
| `internal/aioproxy` | Reverse-proxy implementation used by `aio-proxy` |
| `internal/loopbackforward` | Proxy implementation used by `kind-loopback-forward` |
| `s6/` | s6-overlay v3 service defs (copied to `/etc/s6-overlay/`) |

s6 services follow the [s6-overlay README](https://github.com/just-containers/s6-overlay/blob/v3.2.3.2/README.md)
source format:

- definitions under `s6-rc.d/{aio-init,aio-proxy,dex,fleetshift}/`
- membership via `user-bundles.d/user/contents.d/`
- oneshot `aio-init` script at `scripts/aio-init` (referenced from `up`)
- longruns depend on `base` (`dex` and `aio-proxy` → `aio-init`, `fleetshift` → `aio-init`+`dex`+`aio-proxy`)

### Branches

- **Dex-on (default):** no `OIDC_ISSUER_URL`. `aio-init` writes
  `/run/fleetshift/dex.enabled`; the `dex` longrun execs peer Dex on
  loopback HTTP; `aio-proxy` serves `https://fleetshift-sandbox.localhost:8085`;
  packaging wires AuthMethod/UI defaults into serve.
- **Dex-off:** `OIDC_ISSUER_URL` set. No `dex.enabled` flag; the `dex` longrun
  parks on `s6-pause`. The AIO TLS edge and public callback stay the same.
  Packaging forwards the issuer and fills the same defaults for omitted fields.

### Identities

- FleetShift: `1000:1000`
- Dex: `1001:1001`
- aio-proxy: `1002:1002` (owns the gateway leaf key; cannot read `/data` or Dex state)
- s6 starts as container root only to initialize and drop privileges
