# Web e2e tests

Playwright journeys against the packaged AIO image (HTTPS origin
`https://fleetshift-sandbox.localhost:8085`, SPA under `/app`). How to add
UI or backend e2e tests, and how CI runs them:
[docs/testing/end-to-end.md](../../docs/testing/end-to-end.md).

The Nx target uses the shared sandbox runner, which builds the AIO image before
startup and owns diagnostics and cleanup. CI sets
`FLEETSHIFT_E2E_AIO_PREBUILT=1` after restoring the image. Set
`FLEETSHIFT_E2E_KEEP=1` to retain the sandbox after a local run.

```bash
npx nx test:e2e e2e-web
npx nx test:e2e e2e-web -- --ui
npx nx test:e2e e2e-web -- --project=chromium
```

The runner sets `BASE_URL` to the branded HTTPS origin. Playwright uses
`ignoreHTTPSErrors` for the sandbox private CA; do not `update-ca-trust` on
the host. CI asserts Dex port 5556 is not published.

The legacy backend harness still shells out to a second Playwright config that
is **not** part of `nx test:e2e e2e-web`. The spec opens the printed `AUTH_URL`
(Dex) and fills the form. `PERSONA` is `ops` or `dev`. To run the helper
yourself, pass a live URL from a login still waiting on the callback:

```bash
cd e2e/web
AUTH_URL='https://fleetshift-sandbox.localhost:8085/idp/auth?...' PERSONA=ops \
  npx playwright test --config=playwright.cli-login.mts
```
