# Embedded Dex IDP

Embedded Dex instance providing OIDC authentication for FleetShift without requiring an external identity provider. Designed for trial deployments, local development, and quick-start scenarios where standing up a full IdP is unnecessary.

Dex runs as a sidecar process inside the FleetShift container, started automatically by `entrypoint.sh` when `EMBEDDED_IDP=true`. The server, CLI, and UI all discover the issuer URL through `OIDC_ISSUER_URL` — no manual configuration needed.

## How It Works

```
┌─────────────────────────────────────────────┐
│  FleetShift Container                       │
│                                             │
│  entrypoint.sh                              │
│    ├─ starts dex-idp on :5556               │
│    ├─ health-checks /dex/.well-known/...    │
│    ├─ exports OIDC_ISSUER_URL               │
│    └─ exec fleetshift serve                 │
│         └─ reads OIDC_ISSUER_URL via env    │
│            └─ serves /api/ui/config         │
│               └─ UI reads authority from it │
└─────────────────────────────────────────────┘
```

The issuer URL is `http://localhost:5556/dex`. Dex mounts all OIDC routes under the `/dex` prefix (discovery at `/dex/.well-known/openid-configuration`, token endpoint at `/dex/token`, etc.).

## Quick Start

### Via Podman (recommended)

```bash
# From repo root
npx nx run pd:up AUTH=embedded
```

This is the default — `AUTH=embedded` is used when no mode is specified in demo deployments.

### Standalone (for dex development)

```bash
cd poc/dex-idp
CGO_ENABLED=1 go build -o dex-idp ./cmd/dex-idp/
./dex-idp dex
# Dex listening on :5556, issuer at http://localhost:5556/dex
```

Test credentials: `admin@email.com` / `password`

## AUTH Modes

FleetShift supports three authentication modes, controlled by the `AUTH` variable:

| Mode | IdP | OIDC_ISSUER_URL | Use Case |
|------|-----|-----------------|----------|
| `embedded` (default) | Dex sidecar in container | Set by entrypoint.sh | Trial, demos, local dev |
| `external` | External IdP (GitHub, Okta, etc.) | Set in `.env` | Production |
| `local` | Local Keycloak container | Set by compose override | Development with full IdP features |

### How OIDC_ISSUER_URL propagates

The issuer URL flows through the stack identically in all modes:

1. **Source** — entrypoint.sh (embedded), `.env` (external), compose override (local)
2. **Server** — `serve.go` reads `OIDC_ISSUER_URL` env var, falls back to `http://localhost:5556/dex`
3. **UI** — server exposes it via `GET /api/ui/config` → `{"oidc":{"authority":"..."}}`
4. **CLI** — `fleetctl auth setup --issuer-url=<url>` registers the auth method server-side

No compose command flags are used for OIDC config — everything flows through environment variables at container runtime.

### Switching modes

```bash
# Embedded (default)
npx nx run pd:up

# External IdP — requires OIDC_ISSUER_URL in .env
npx nx run pd:up AUTH=external

# Local Keycloak — requires mkcert
npx nx run pd:up AUTH=local
```

## Auth Method Setup

After the server starts, register the OIDC auth method. With embedded dex, this can be done from the UI or CLI:

```bash
fleetctl auth setup \
  --server localhost:50051 \
  --issuer-url http://localhost:5556/dex \
  --client-id fleetshift-ui
```

For `AUTH=external`, the `auth-setup` init container in `external-oidc.yaml` handles this automatically.

## Next Steps

These are improvements needed before embedded dex is production-ready. Ordered roughly by priority.

### 1. Make dex configurable via CLI flags / env vars

Currently all values are hardcoded in `dex-idp.go`. The following should be configurable:

- **Issuer URL** — hardcoded to `http://localhost:5556/dex`. Should read from env/flag for HTTPS or custom domain deployments.
- **Listen address** — hardcoded to `:5556`. Should be a CLI flag.
- **SQLite path** — hardcoded to `dex.db`. Should support config for persistent volume mounts.
- **OAuth client ID** — hardcoded to `fleetshift-ui`. Should be configurable for multi-client setups.
- **Static user credentials** — hardcoded `admin@email.com` / `password`. Should read from env vars or config file.
- **Allowed CORS origins** — hardcoded to `http://localhost:8085`. Should be configurable for different UI ports/domains.
- **Connector type** — only `local` (email+password). Should support adding LDAP or upstream OIDC connectors via config.

### 2. Replace mock signer with persistent key storage

`signer.NewMockSigner(nil)` generates a random RSA key at startup. Every restart invalidates all tokens. Replace with dex's built-in rotating key signer backed by the database, or an external KMS.

### 3. Add PostgreSQL storage option

SQLite works for single-instance but doesn't support HA or multi-replica. Add a config option to switch between SQLite (quick setup / trial) and PostgreSQL (multi-replica / durable).

### 4. Register explicit redirect URIs

`RedirectURIs` is nil so dex accepts any localhost redirect. This is convenient for the CLI (uses random ephemeral ports) but insecure. Register explicit URIs for the UI and use device code grant or a fixed port for the CLI.

### 5. Audience claim compatibility

Dex does not support custom audiences on OAuth clients. Tokens have `aud` set to the client ID (`fleetshift-ui`), not a configurable audience like `fleetshift` that Keycloak provides.

The server's token verifier skips audience enforcement when the auth method's audience field is empty (`identity_token.go`). So embedded dex works today if `fleetctl auth setup` is called without `--audience`. But this means:

- **Embedded mode** — no audience verification. Any valid token from the issuer is accepted regardless of intended audience.
- **External mode** — audience is set (e.g. `fleetshift`), tokens are properly scoped.

Open question: should we enforce audience uniformly? Options:
- Accept client ID as audience in embedded mode (dex sets `aud: "fleetshift-ui"`, register that as the audience)
- Keep audience empty for embedded and accept the weaker validation — acceptable for trial/dev
- Investigate dex's `claimMapping` or custom connectors for audience injection

### 6. Roles scope not available in dex

External IdPs (Keycloak, GitHub) can return a `roles` claim via a custom `roles` scope. Dex does not support custom scopes — only `openid`, `profile`, `email`, `groups`, `federated:id`, `offline_access`, and `audience:server:client_id` cross-client scopes (see [dex docs: custom scopes, claims, and client features](https://dexidp.io/docs/configuration/custom-scopes-claims-clients/)).

The UI already hardcodes `scope: "openid profile email"` noting this limitation. The server does not currently use roles from tokens for authorization decisions — RBAC is not yet implemented. If/when RBAC lands, this becomes a real gap for embedded mode.

Options when that happens:
- Map dex `groups` claim to roles (dex supports groups via LDAP/OIDC upstream connectors, not the local connector)
- Implement authorization server-side based on persisted user identity, not token claims
- Limit embedded mode to single-user / admin-only (no RBAC needed)

### 7. Ensure external/Keycloak OIDC still works

The `envOrDefault` pattern in `serve.go` means the server falls back to `http://localhost:5556/dex` when no `OIDC_ISSUER_URL` is set. This fallback only applies when running without a container (direct `go run`). In container deployments:

- `AUTH=external` — `external-oidc.yaml` passes `OIDC_ISSUER_URL` from `.env` into the container env, overriding the default.
- `AUTH=local` — `local-keycloak.yaml` uses a full command override with `--oidc-ui-authority` and `--oidc-ca-file` flags, which take precedence over env vars.
- `AUTH=embedded` — `entrypoint.sh` exports `OIDC_ISSUER_URL=http://localhost:5556/dex` at container runtime before exec'ing the server.

Token verification is independent of the UI authority default — it uses the persisted auth method issuer from `fleetctl auth setup`. So even if the UI config defaults to dex, token verification uses whatever issuer was registered.

### 8. HTTPS support

Issuer is HTTP-only. Fine when dex and the server share a container/pod, but should support TLS for split deployments.

## Known Issues

### Go Module Workaround

Dex tags releases as `v2.x` but its `go.mod` declares `module github.com/dexidp/dex` (no `/v2`). The Go proxy refuses to serve past `v2.13.0`. Workaround: `replace` directive pointing at a pseudo-version for the v2.45.1 commit.

- **Upstream issue:** https://github.com/dexidp/dex/issues/4222
- **Impact:** Pinned to a specific commit hash — won't receive updates automatically. If dex fixes their module path, remove the replace directive.

### CGO Required

`mattn/go-sqlite3` (dex dependency) requires `CGO_ENABLED=1`. The Dockerfile uses a separate `dex-builder` stage with CGO enabled to keep the main server build CGO-free.

### SkipApprovalScreen

OAuth consent screen is disabled. Fine for first-party clients, inappropriate for third-party integrations.
