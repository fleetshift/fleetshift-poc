# FleetShift CLI end-to-end tests

This Playwright Test suite exercises Fleetctl, the HTTPS gateway, Dex login,
Kind lifecycle and delivery, and resource queries. Playwright is the test
runner and supplies the browser used to complete Fleetctl's authorization-code
login; these are CLI tests rather than UI tests.

Shared prerequisites, kernel keyring quotas, the sandbox runner, Kind
declarations, and CI:
[end-to-end.md](../../docs/testing/end-to-end.md).

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

Five workers, no retries, one shared AIO. Each worker gets its own Fleetctl
config directory. Declare Kind needs with `kindClusters` on the exported
`test` helper; do not acquire pool clusters from the test body. Table and
pool rules:
[Kind cluster declarations](../../docs/testing/end-to-end.md#kind-cluster-declarations).
Do not start another AIO on ports 8085 or 50051 while the runner is up. Set
`FLEETSHIFT_E2E_KEEP=1` to retain the sandbox for local debugging.

Rootless Kind needs raised kernel keyring quotas. The runner checks before
the image build and does not change your machine. Remediation and the local
override:
[Kernel keyring quotas](../../docs/testing/end-to-end.md#kernel-keyring-quotas-rootless-podman).

## Fast checks

```bash
npx nx lint e2e-cli
npx nx test e2e-cli
```

Fleetctl state, copied CAs, manifests, and login state live in a private OS
temporary directory that is removed during teardown. Login traces,
screenshots, and videos are disabled. Never print or upload authorization
URLs, tokens, credentials files, cookies, authorization codes, or PKCE data.
