# FleetShift CLI end-to-end tests

This Playwright Test suite exercises Fleetctl, the HTTPS gateway, Dex login,
Kind lifecycle and delivery, and resource queries. Playwright is the test
runner and supplies the browser used to complete Fleetctl's authorization-code
login; these are CLI tests rather than UI tests.

## Run locally

Install workspace dependencies and Playwright Chromium, and ensure the user
Podman socket is running. The command builds Fleetctl and the AIO image before
starting the shared runner-owned sandbox:

```bash
npx playwright install chromium
npx nx test:e2e e2e-cli
```

CI restores the image rather than rebuilding it and sets the prebuilt flag. It
still verifies that the required image is loaded:

```bash
FLEETSHIFT_E2E_AIO_PREBUILT=1 npx nx test:e2e e2e-cli
```

Arguments after `--` are forwarded to Playwright. For example:

```bash
npx nx test:e2e e2e-cli -- tests/scenarios/gateway.spec.ts
npx nx test:e2e e2e-cli -- --grep "ops login"
```

The suite uses five workers and no retries. The AIO owns fixed ports and is
shared. Each worker gets its own Fleetctl config directory so credentials do
not collide; Kind cluster identity is run-wide. Tests declare Kind needs up
front with `kindClusters` on the exported `test` helper: `read-only` or
`modifiable` access, `clean` or `any` starting state, and how many clusters.
The fixture leases compatible ready clusters from a shared pool (or creates
capacity when none match) and returns them as `kindClusters`. Tests that
create a private cluster, or that do not use Kind, declare `kindClusters: []`.
Do not acquire pool clusters from the test body. The sandbox fixture owns
connection facts plus worker-specific paths. `e2e/sandbox/run.mjs` owns
startup, failure diagnostics, and exact-run teardown; do not start another AIO
on ports 8085 or 50051 while it runs. Set `FLEETSHIFT_E2E_KEEP=1` to retain its
container, Kind nodes, pool state, and temporary directory for local
debugging.

## Fast checks

```bash
npx nx lint e2e-cli
npx nx test e2e-cli
```

Fleetctl state, copied CAs, manifests, and login state live in a private OS
temporary directory that is removed during teardown. Login traces,
screenshots, and videos are disabled. Never print or upload authorization
URLs, tokens, credentials files, cookies, authorization codes, or PKCE data.
