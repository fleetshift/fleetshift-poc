# Web e2e tests

Playwright journeys against the packaged AIO image (HTTPS origin
`https://fleetshift-sandbox.localhost:8085`, SPA under `/app`). How to add
UI or CLI e2e tests, and how CI runs them:
[docs/testing/end-to-end.md](../../docs/testing/end-to-end.md).

The Nx target uses the shared sandbox runner, which builds the AIO image before
startup and owns diagnostics and cleanup. CI (`e2e.yml`) sets
`FLEETSHIFT_E2E_AIO_PREBUILT=1` after restoring a **this-checkout** image; a
failure there is a problem with the current branch.

`e2e-published.yml` sets `FLEETSHIFT_E2E_AIO_PULL=1` and pulls
`quay.io/stolostron/fleetshift:latest`. That job is a **sanity** of the
**already published** Quay AIO (start, Dex login, masthead/Clusters), not a
re-run of the full UI suite and not the image from current changes. Playwright
project `chromium-sanity` only; no Kind journey. A failure does not indicate a
bug in the current branch. It indicates the live published image failed basic
function and end users are affected; investigate immediately (confirm
`podman run` of `:latest`, OpenShift CI mirror/republish, restore a working
tag). Set `FLEETSHIFT_E2E_KEEP=1` to retain the sandbox after a local run.

```bash
npx nx test:e2e e2e-web
npx nx test:e2e e2e-web -- --ui
npx nx test:e2e e2e-web -- --project=chromium
FLEETSHIFT_E2E_AIO_PULL=1 npx nx test:e2e e2e-web -- --project=chromium-sanity
```

The runner sets `BASE_URL` to the branded HTTPS origin. Playwright uses
`ignoreHTTPSErrors` for the sandbox private CA; do not `update-ca-trust` on
the host. CI asserts Dex port 5556 is not published.

