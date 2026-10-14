# TypeScript Fleetctl CLI

TypeScript `fleetctl` CLI for the FleetShift AIO runtime.

## Prerequisites

- Podman with Docker Compose provider
- Node.js and workspace dependencies installed
- Kind network available for the AIO stack

Install dependencies from repository root:

```bash
npm install
```

## Start AIO

Use Nx deployment targets. Do not invoke `docker compose` directly.

```bash
cp .env.template .env
npx nx run pd:dev
```

For built-in Dex, leave `OIDC_ISSUER_URL` unset in `.env`. If it is set, AIO runs Dex-off and `/idp` is unavailable. To reset a local stack that was previously started in Dex-off mode:

```bash
# Remove OIDC_ISSUER_URL from .env first.
npx nx run pd:clean
npx nx run pd:dev
```

Check runtime status:

```bash
npx nx run pd:status
```

Expected endpoints:

- UI/API: `https://fleetshift-sandbox.localhost:8085`
- gRPC: `127.0.0.1:50051`
- Dex: `https://fleetshift-sandbox.localhost:8085/idp`

Demo credentials:

- Operator: `ops@fleetshift.local` / `fleetshift-ops`
- Developer: `dev@fleetshift.local` / `fleetshift-dev`

## Prepare CLI

Run following commands from repository root, `fleetshift-poc/`.

Build executable and update top-level `bin/fleetctl` symlink:

```bash
npx nx run fleetctl
```

Equivalent package target:

```bash
npx nx run cli:build
```

For current shell, add top-level `bin` to `PATH`:

```bash
export PATH="$(pwd)/bin:$PATH"
fleetctl --help
```

`package.json#bin` points directly to `bin/fleetctl`; no Node prefix or wrapper is required.

For a persistent npm-style global link instead:

```bash
npm link --workspace=@fleetshift/cli
fleetctl --help
```

To test installation flow closer to published npm usage:

```bash
npm pack --workspace=@fleetshift/cli --pack-destination /tmp
npm install --global /tmp/fleetshift-cli-0.0.0.tgz
fleetctl --help
```

Before publishing for real, remove package `private` flag and use release versioning. Local tarball install works while package remains private.

## Recover AIO CA

`pd:dev` normally copies sandbox CA to `deploy/podman/.certs/ca.crt`. If that file is missing, copy it manually. Direct Compose commands need `PODMAN_SOCKET` set because Compose interpolates it before executing `cp`:

```bash
mkdir -p deploy/podman/.certs
PODMAN_SOCKET="${PODMAN_SOCKET:-/var/run/docker.sock}" \
  podman compose \
  -f deploy/podman/compose.yaml \
  --env-file .env \
  cp fleetshift-server:/data/sandbox/pki/ca.crt \
  deploy/podman/.certs/ca.crt
```

Verify discovery before configuring CLI:

```bash
curl --cacert deploy/podman/.certs/ca.crt \
  https://fleetshift-sandbox.localhost:8085/idp/.well-known/openid-configuration
```

`502` from `/idp` means AIO is Dex-off or Dex is not ready. Check `npx nx run pd:logs` and verify `OIDC_ISSUER_URL` is unset for built-in Dex.

## Configure And Login

Use separate config directory while validating TS CLI:

```bash
export FLEETCTL_CONFIG_DIR=/tmp/fleetshift-ts

fleetctl auth setup \
  --issuer-url https://fleetshift-sandbox.localhost:8085/idp \
  --client-id fleetshift-cli \
  --key-enrollment-client-id fleetshift-signing \
  --oidc-ca-file deploy/podman/.certs/ca.crt \
  --scopes 'openid,profile,email,audience:server:client_id:fleetshift'

fleetctl auth login
```

The login flow opens a browser and starts a loopback callback listener. Use the demo operator credentials above.

TS CLI writes Go-compatible files:

- `$FLEETCTL_CONFIG_DIR/auth.json`
- `$FLEETCTL_CONFIG_DIR/credentials.json`

## Run Live Commands

Default values are designed for local AIO development:

- Server: `https://fleetshift-sandbox.localhost:8085`
- Config directory: `FLEETCTL_CONFIG_DIR`, or `~/.config/fleetshift`

Set `FLEETCTL_SERVER` only when targeting another runtime. Set `FLEETCTL_CONFIG_DIR` when isolating credentials. Both command flags are optional and override these defaults.

```bash
export FLEETCTL_SERVER=https://fleetshift-sandbox.localhost:8085

fleetctl deployment list
fleetctl resource query
```

With `FLEETCTL_SERVER` and `FLEETCTL_CONFIG_DIR` exported, flags are optional:

```bash
fleetctl deployment list
fleetctl resource query
```

Explicit `--server` and `--config-dir` values override environment defaults. Use `--output json` for machine-readable responses.

Measure total process time from CLI startup through command/render completion. Debug output goes to stderr so command stdout remains script-safe:

```bash
DEBUG=true fleetctl deployment list \
  --server "$FLEETCTL_SERVER" \
  --config-dir "$FLEETCTL_CONFIG_DIR"

fleetctl --debug deployment list \
  --server "$FLEETCTL_SERVER" \
  --config-dir "$FLEETCTL_CONFIG_DIR"
```

## Tests

Run package checks through Nx:

```bash
npx nx run cli:build
npx nx run cli:test
npx nx run cli:lint
```

CLI E2E harness runs TypeScript `fleetctl` by default and uses same AIO login/browser/scenario flow:

```bash
npx nx test:e2e e2e-cli -- --grep "login"
npx nx test:e2e e2e-cli
```

E2E requires Podman keyring quotas and a clean AIO sandbox.

## Current Migration Surface

Implemented command handlers cover auth setup/login/logout/inspect/enroll, deployment CRUD/resume/signing, resource CRUD/query, and reflection-backed resource types/schema discovery.

## Cleanup

Stop AIO while preserving local data:

```bash
npx nx run pd:down
```

Remove AIO containers, volumes, and generated sandbox certificates:

```bash
npx nx run pd:clean
```

`pd:clean` is destructive for local AIO data. Use it when switching between Dex-on and Dex-off modes or when persisted auth configuration is stale.

Remove TS CLI credentials and generated executable:

```bash
fleetctl auth logout
rm -rf "$FLEETCTL_CONFIG_DIR"
rm -rf client/cli/bin
```

If `FLEETCTL_CONFIG_DIR` is unset, remove the default `~/.config/fleetshift` only when no other FleetShift CLI setup uses it. Do not print or commit `credentials.json`.

Remove global npm link, if created:

```bash
npm unlink --global @fleetshift/cli
```

Remove current-shell PATH override by opening a new shell or exporting the previous `PATH` value.
