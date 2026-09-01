# Web e2e tests

Playwright journeys against the packaged AIO image (HTTPS origin
`https://fleetshift-sandbox.localhost:8085`, SPA under `/app`). How to add
UI or CLI e2e tests, and how CI runs them:
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
