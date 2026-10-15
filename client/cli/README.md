# TypeScript Fleetctl CLI

TypeScript `fleetctl` CLI for Resource Manager. The local setup examples below use
the [Podman AIO stack](../../deploy/podman/README.md).

## Prerequisites

- Node.js and workspace dependencies installed

Install dependencies from repository root:

```bash
npm install
```

### Podman

- Podman with Docker Compose provider
- Kind network available for the AIO stack

## Start AIO

### Podman

To run the Podman AIO stack, use its Nx deployment targets:

```bash
cp .env.template .env
npx nx run deploy:podman:dev
```

For built-in Dex, leave `OIDC_ISSUER_URL` unset in `.env`. If it is set, AIO runs Dex-off and `/idp` is unavailable. To reset a local stack that was previously started in Dex-off mode:

```bash
# Remove OIDC_ISSUER_URL from .env first.
npx nx run deploy:podman:clean
npx nx run deploy:podman:dev
```

Check runtime status:

```bash
npx nx run deploy:podman:status
```

Expected endpoints:

- UI/API: `https://ocm-resource-manager.localhost:8085`
- gRPC: `127.0.0.1:50051`
- Dex: `https://ocm-resource-manager.localhost:8085/idp`

Demo credentials:

- Operator: `ops@ocm-resource-manager.local` / `ocm-resource-manager-ops`
- Developer: `dev@ocm-resource-manager.local` / `ocm-resource-manager-dev`

## Prepare CLI

Run following commands from repository root, `ocm-resource-manager/`.

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
npm link --workspace=@ocm-resource-manager/cli
fleetctl --help
```

To test installation flow closer to published npm usage:

```bash
npm pack --workspace=@ocm-resource-manager/cli --pack-destination /tmp
npm install --global /tmp/ocm-resource-manager-cli-0.0.0.tgz
fleetctl --help
```

Before publishing for real, remove package `private` flag and use release versioning. Local tarball install works while package remains private.

## Recover AIO CA

### Podman

`deploy:podman:dev` normally copies sandbox CA to `deploy/podman/.certs/ca.crt`. If that file is missing, copy it manually. Direct Compose commands need `PODMAN_SOCKET` set because Compose interpolates it before executing `cp`:

```bash
mkdir -p deploy/podman/.certs
PODMAN_SOCKET="${PODMAN_SOCKET:-/var/run/docker.sock}" \
  podman compose \
  -f deploy/podman/compose.yaml \
  --env-file .env \
  cp ocm-resource-manager-server:/data/sandbox/pki/ca.crt \
  deploy/podman/.certs/ca.crt
```

Verify discovery before configuring CLI:

```bash
curl --cacert deploy/podman/.certs/ca.crt \
  https://ocm-resource-manager.localhost:8085/idp/.well-known/openid-configuration
```

`502` from `/idp` means AIO is Dex-off or Dex is not ready. Check `npx nx run deploy:podman:logs` and verify `OIDC_ISSUER_URL` is unset for built-in Dex.

## Configure And Login

TS CLI writes Go-compatible files:

- `$FLEETCTL_CONFIG_DIR/auth.json`
- `$FLEETCTL_CONFIG_DIR/credentials.json`

### Podman

To connect to the Podman AIO stack, use a separate config directory while validating TS CLI:

```bash
export FLEETCTL_CONFIG_DIR=/tmp/ocm-resource-manager-ts

fleetctl auth setup \
  --issuer-url https://ocm-resource-manager.localhost:8085/idp \
  --client-id ocm-resource-manager-cli \
  --key-enrollment-client-id ocm-resource-manager-signing \
  --oidc-ca-file deploy/podman/.certs/ca.crt \
  --scopes 'openid,profile,email,audience:server:client_id:ocm-resource-manager'

fleetctl auth login
```

The login flow opens a browser and starts a loopback callback listener. Use the demo operator credentials above.

## Run Live Commands

CLI defaults (the server URL matches the Podman AIO stack):

- Server: `https://ocm-resource-manager.localhost:8085`
- Config directory: `FLEETCTL_CONFIG_DIR`, or `~/.config/ocm-resource-manager`

Set `FLEETCTL_SERVER` only when targeting another runtime. Set `FLEETCTL_CONFIG_DIR` when isolating credentials. Both command flags are optional and override these defaults.

Explicit `--server` and `--config-dir` values override environment defaults. Use `--output json` for machine-readable responses.

### Podman

```bash
export FLEETCTL_SERVER=https://ocm-resource-manager.localhost:8085

fleetctl deployment list
fleetctl resource query
```

With `FLEETCTL_SERVER` and `FLEETCTL_CONFIG_DIR` exported, flags are optional:

```bash
fleetctl deployment list
fleetctl resource query
```

### Timing and debug output

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

### Podman

Stop the Podman AIO stack while preserving local data:

```bash
npx nx run deploy:podman:down
```

Remove the Podman stack's containers, volumes, and generated sandbox certificates:

```bash
npx nx run deploy:podman:clean
```

`deploy:podman:clean` is destructive for local AIO data. Use it when switching between Dex-on and Dex-off modes or when persisted auth configuration is stale.

### CLI

Remove TS CLI credentials and generated executable:

```bash
fleetctl auth logout
rm -rf "$FLEETCTL_CONFIG_DIR"
rm -rf client/cli/bin
```

If `FLEETCTL_CONFIG_DIR` is unset, remove the default `~/.config/ocm-resource-manager` only when no other Resource Manager CLI setup uses it. Do not print or commit `credentials.json`.

Remove global npm link, if created:

```bash
npm unlink --global @ocm-resource-manager/cli
```

Remove current-shell PATH override by opening a new shell or exporting the previous `PATH` value.
