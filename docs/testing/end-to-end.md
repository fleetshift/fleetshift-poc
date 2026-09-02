# End-to-end tests

Product coverage against the packaged all-in-one (AIO) image: HTTPS UI,
bundled Dex, gRPC, and the Kind addon on the host container engine. Suites
live under `e2e/`.

Do not use this for unit tests, Playwright component tests (`*.ct.tsx`), or
server `//go:build integration` tests. Those stay next to the code they
cover. Write an e2e test when the claim is about the assembled product.

| Suite | Nx project | Proves                                      | Who starts the AIO |
| ----- | ---------- | ------------------------------------------- | ------------------ |
| CLI   | `e2e-cli`  | fleetctl, gateway, Kind, delivery, indexing | shared Node runner |
| UI    | `e2e-web`  | console journeys                            | shared Node runner |

Do not run these suites at once. They need the sandbox HTTPS and gRPC ports
(8085 and 50051) and will refuse to start if either port is taken.

Orchestration stress tests are separate; see
[orchestration_stress_test.md](./orchestration_stress_test.md).

## Quick start

```bash
# UI: builds the packaged AIO image, then starts the sandbox
npx nx test:e2e e2e-web
npx nx test:e2e e2e-web -- --ui
npx nx test:e2e e2e-web -- --project=chromium
npx nx test:e2e e2e-web -- tests/login.spec.ts

# CLI: builds fleetctl and the packaged AIO image
npx nx test:e2e e2e-cli
npx nx test:e2e e2e-cli -- tests/scenarios/gateway.spec.ts
```

Flags after `--` go to Playwright.

Short CLI cheat sheet: [e2e/cli/README.md](../../e2e/cli/README.md).

Short UI cheat sheet: [e2e/web/README.md](../../e2e/web/README.md).

## Prerequisites

- `podman`, `npx`, `go` on `PATH`, and `npm ci` at the repo root
- `fleetshift-sandbox.localhost` resolving to loopback (a `.localhost` name;
  do not add `/etc/hosts`)
- Linux: `systemctl --user enable --now podman.socket` so Kind can use the
  host engine. Override with `PODMAN_SOCKET` if needed. On macOS, a live
  Docker-compatible socket or `podman machine` is enough.

The sandbox CA is private. Playwright uses `ignoreHTTPSErrors`; do not
`update-ca-trust` on the host.

Dex personas are `ops` and `dev`. Emails, passwords, and masthead labels live
in [`e2e/shared/personas.ts`](../../e2e/shared/personas.ts)
(public sandbox fixtures). The console does not yet distinguish their roles;
CLI tests still use them for token isolation and Kind OIDC (ops can write;
dev is forbidden on the cluster API).

### Shared sandbox runner

The UI and CLI Nx targets invoke `e2e/sandbox/run.mjs`. By default it builds
the AIO image, starts one fixed-port sandbox, waits for its private CA and
readiness, runs Playwright, prints sanitized diagnostics on failure, and always
removes the AIO plus Kind nodes carrying that run's unique cluster prefix. CI
sets `FLEETSHIFT_E2E_AIO_PREBUILT=1` after restoring the image; the runner then
requires that image without rebuilding it.

The published-image workflow sets `FLEETSHIFT_E2E_AIO_PULL=1` so the runner
pulls `quay.io/stolostron/fleetshift:latest` instead of building, then runs a
**sanity** (start, Dex login, console masthead/Clusters) — not the full UI
suite. That path tests the **already published** Quay AIO, not
the image that would be built from the current branch. After pull or restore,
`podman run` uses `--pull=never`.
Set `FLEETSHIFT_E2E_KEEP=1` only for local debugging when the sandbox should
remain after the command exits.

Tests receive connection facts through `BASE_URL`, `FLEETSHIFT_GRPC_TARGET`,
`FLEETSHIFT_CA_FILE`, `FLEETSHIFT_E2E_WORK_DIR`, and
`FLEETSHIFT_KIND_PREFIX`; they do not manage the AIO lifecycle.

---

## CLI

```
e2e/cli/
  playwright.config.ts   # five workers, no retries
  tests/fixtures.ts      # worker login + test-scoped Kind pool leases
  tests/scenarios/       # product stories
  tests/steps/           # fleetctl/Kind/query actions and asserts
  tests/support/         # pool, sandbox, fleetctl, cleanup
```

`npx nx test:e2e e2e-cli` starts the shared sandbox runner, then Playwright.
Workers share one AIO and one Kind pool under `FLEETSHIFT_E2E_WORK_DIR`. Each
worker still gets a private Fleetctl config directory.

### Kind cluster declarations

Every test must pass `kindClusters` to the `test` helper exported from
`tests/fixtures.ts`. The automatic fixture leases that many clusters before
the body runs:

| Declaration | Use when |
| --- | --- |
| `[{ access: "read-only", state: "any" }]` | query-only work |
| `[{ access: "modifiable", state: "any" }]` | delivery, persona, or Kind API writes |
| two modifiable requests | fan-out |
| `[]` | gateway, login, bootstrap, or a private Kind lifecycle |

`state: "clean"` is only for assertions that need a baseline cluster. Pool
identity is `kind-e2e-<run-id>-pool-<id>`; private lifecycle IDs stay outside
that prefix. Continue using unique namespace and deployment IDs — the pool
does not isolate test data.

Failed tests discard their leased records for the rest of the run. Final
sandbox teardown still removes every Kind node with the run prefix.
`FLEETSHIFT_E2E_KEEP=1` retains the sandbox, clusters, and pool state.

### Add a CLI scenario

```ts
import { test } from "../fixtures";

test(
  "resource query returns Kubernetes objects",
  { kindClusters: [{ access: "read-only", state: "any" }] },
  async ({ cli, kindClusters: [cluster] }) => {
    await cli.query.indexedKubernetesObjectsExist(cluster.id);
  },
);
```

Import `test` and `expect` from `./fixtures`. Put product waits in
`tests/steps/`. Copy the closest file under `tests/scenarios/`.

---

## Frontend

Playwright against `https://fleetshift-sandbox.localhost:8085` (SPA under
`/app`). Dex is same-origin under `/idp` on that port; do not talk to Dex
on 5556.

```
e2e/web/
  playwright.config.mts          # nx test:e2e e2e-web
  tests/auth-setup.ts            # Dex login once per persona
  tests/fixtures.ts              # restores oidc sessionStorage
  tests/clusters.ts              # Kind create/delete list helpers
  tests/*.spec.ts
```

### Auth for specs

A `setup` project logs each persona in through Dex and writes `.auth/`.
Playwright `storageState` does not persist `sessionStorage`, where the OIDC
client keeps the user, so `fixtures.ts` re-injects a sibling `*-session.json`.

Import `test` and `expect` from `./fixtures`, not `@playwright/test`.
Projects default to ops. Override per describe:

```ts
import { expect, test } from "./fixtures";
import { DEVELOPER } from "../../shared/personas";

test.describe("developer persona", () => {
  test.use({ storageState: ".auth/dev.json" });

  test("lands on the console identified as the developer", async ({ page }) => {
    await page.goto("/app/");
    await expect(
      page.getByRole("button", { name: DEVELOPER.usernameLabel }),
    ).toBeVisible();
  });
});
```

Logged-out screens: `test.use({ storageState: { cookies: [], origins: [] } })`.

### Add a UI spec

1. New file under `e2e/web/tests/`. Import from `./fixtures`.
2. Prefer role locators. Put repeated wizard or table steps in
   purpose-named files next to the specs (see `tests/clusters.ts`).
3. Kind create/delete is slow. Give that journey a wide `test.setTimeout`.
   Do not add another Kind lifecycle unless the UI path is different —
   `e2e-cli` already covers delivery and indexing.
4. Poll live data with `expect.poll` when the page already refreshes in
   place; a reload just replays the skeleton.
5. CI this-checkout e2e (`e2e.yml`) runs Chromium only, including Kind.
   Published Quay sanity (`e2e-published.yml`) uses project `chromium-sanity`
   (`tests/login.spec.ts` plus Dex `auth-setup`). Firefox and WebKit are
   local extras.

Traces, screenshots, and video on failure go under `e2e/web/test-results/`
and `playwright-report/`. `.auth/` is gitignored.

---

## GitHub Actions

Two workflows run against an AIO. They are not interchangeable: one runs the
full UI/CLI e2e on **this checkout's** image; the other runs a **sanity** on
the **already published** Quay AIO that end users `podman run`.

[`.github/workflows/e2e.yml`](../../.github/workflows/e2e.yml) plus
[`.github/actions/setup-e2e`](../../.github/actions/setup-e2e/) test **this
checkout's** AIO image (Nx `image:aio`). A failure there is a problem with
the current branch.

[`.github/workflows/e2e-published.yml`](../../.github/workflows/e2e-published.yml)
pulls **`quay.io/stolostron/fleetshift:latest`** from Quay and runs a UI
**sanity** only (`chromium-sanity`: container starts, Dex login, masthead and
Clusters). It is not a re-run of the full UI suite (no Kind journey, a11y, or
other specs); `e2e.yml` covers those on this checkout. It does **not**
build or use the AIO from the current branch, PR, or commit. A failure does
**not** mean the current branch is broken. It means the **active, already
published** AIO on Quay failed basic function (will not start, or
login/console fails). That is what users get from the README `podman run`
command. Treat it as a live product incident and investigate immediately:
confirm `podman run quay.io/stolostron/fleetshift:latest`, check OpenShift CI
image mirror/republish jobs, and restore a working `:latest`.

Unit/component CI (`.github/workflows/test.yml`) runs
`npx nx test e2e-cli` and Playwright **component** tests. It does not
run these suites.

```
aio-image          # role: cache — build or restore this-checkout AIO tar
 └── e2e-playwright # role: e2e — UI and CLI matrix, one runner each

e2e-published      # role: published — pull quay:latest, UI sanity only
```

| Workflow | Image under test | What it runs | A failure means |
| --- | --- | --- | --- |
| `e2e.yml` | This checkout, built by Nx | Full UI + CLI e2e (including Kind) | The current branch's AIO is broken |
| `e2e-published.yml` | `quay.io/stolostron/fleetshift:latest` already on Quay | Sanity only: start, Dex login, console | The live published AIO failed basic function; users are affected now |

`e2e-published.yml` runs on pull requests, pushes to `main`, a cron 30 minutes
after OpenShift CI republishes `:latest` (03:00/09:00/15:00/21:00 UTC), and
`workflow_dispatch`. On a PR the red check is still about Quay `:latest`, not
the PR's Dockerfiles. Do not "fix" it by changing unrelated PR code; fix or
republish the published image.

The Playwright matrix entries restore the same tree-keyed tar and run in
parallel. They do not share a running container.

`setup-e2e` is the only place that should install Go, Node, or Playwright,
load or pull the image, start user `podman.socket`, and ensure the `kind`
network. The `cache` role never starts FleetShift. The `e2e` role never
rebuilds the image because every test job sets `FLEETSHIFT_E2E_AIO_PREBUILT=1`.
The `published` role never restores the checkout tar; it pulls the published
tag. The test job sets `FLEETSHIFT_E2E_AIO_PULL=1` and runs Playwright
`chromium-sanity` only (not the full UI project). Go is installed on
`e2e` so the CLI suite can build fleetctl, not on `published`.

The shared runner starts unique UI and CLI sandboxes and uploads Playwright
artifacts through their jobs. Do not add `podman run`, log dump, or
`podman rm` workflow steps: a second AIO fights over ports and the `kind`
network alias, and prefix-wide Kind cleanup can delete the other job's nodes.

The image cache is a **multi-image** archive. Saving without that makes the
AIO tag an alias of the server image; `setup-e2e` refuses to load that tar.
Any tracked-file change busts the key (git tree hash). Bump the key prefix
in the action only if the tar format itself changes.
