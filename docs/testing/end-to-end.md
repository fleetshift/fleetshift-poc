# End-to-end tests

Product coverage against the packaged all-in-one (AIO) image: HTTPS UI,
bundled Dex, gRPC, and the Kind addon on the host container engine. Suites
live under `e2e/`.

Do not use this for unit tests, Playwright component tests (`*.ct.tsx`), or
server `//go:build integration` tests. Those stay next to the code they
cover. Write an e2e test when the claim is about the assembled product.

| Suite | Nx project | Proves | Who starts the AIO |
| --- | --- | --- | --- |
| Backend | `e2e-backend` | fleetctl, gateway, Kind, delivery, indexing | `TestMain` |
| UI | `e2e-web` | console journeys | you (local) or the `e2e-ui` job |

Do not run both suites at once. They both need the sandbox HTTPS port
(8085). The backend also binds gRPC (50051) and will refuse to start if
either port is taken.

Orchestration stress tests are separate; see
[orchestration_stress_test.md](./orchestration_stress_test.md).

## Quick start

```bash
# UI: AIO already running at https://fleetshift-sandbox.localhost:8085
npx nx test:e2e e2e-web
npx nx test:e2e e2e-web -- --ui
npx nx test:e2e e2e-web -- --project=chromium
npx nx test:e2e e2e-web -- tests/login.spec.ts

# Backend: starts and stops its own AIO (podman + a live engine socket)
npx nx test:e2e e2e-backend
npx nx test:e2e e2e-backend -- -run TestFanOut
```

Flags after `--` go to Playwright or `go test`.

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
in [`e2e/web/tests/helpers/personas.ts`](../../e2e/web/tests/helpers/personas.ts)
(public sandbox fixtures). The console does not yet distinguish their roles;
backend tests still use them for token isolation and Kind OIDC (ops can
write; dev is forbidden on the cluster API).

---

## Backend

```
e2e/backend/
  scenarios/           # //go:build e2e — product stories
  internal/harness/    # AIO + host fleetctl
  internal/steps/      # actions/asserts used by scenarios
```

`npx nx test:e2e e2e-backend` is the slow suite. It is not Nx-cached.
Scenarios share one AIO and a Kind pool: do not call `t.Parallel()`.

### What TestMain already did

`scenarios.TestMain` starts the AIO, logs in as **ops**, runs tests, then
stops the fixture. On success it deletes the shared Kind pool through
fleetctl **after** `PASS` (that wait can look like a hang; the harness logs
it). On failure it dumps AIO logs and Kind/podman evidence, then removes
leftover suite Kind nodes.

Do not log in as ops again into the suite `--config-dir`. Use
`steps.LoginAsDev` for a second persona (separate config dir). Never log
`credentials.json` or `AUTH_URL` query strings.

### Shared Kind vs a private cluster

Kind create is expensive. Workload tests borrow a process-wide pool; only a
lifecycle test should create and delete its own cluster.

| Helper | Use when |
| --- | --- |
| `steps.SharedKind(t, suite)` | one target |
| `steps.SharedKindPair(t, suite)` | fan-out or cross-cluster filters |
| `steps.UniqueKindClusterID(t)` | create → ready → delete |

`SharedKindPair`'s first id is the same cluster as `SharedKind`. The first
caller creates the cluster(s) and waits until they are ready and OIDC works;
later tests reuse them. `DeleteKindCluster` / `CleanupKindCluster` refuse
the pool ids.

Namespace and deployment ids must not be derived from the cluster name. Use
`steps.UniqueID(t, prefix)` so tests can share the pool without colliding.

### Scenarios vs steps vs harness

- **scenarios** — readable stories. `//go:build e2e`, `steps.RunStep`, no
  fleetctl/HTTP/kubectl in the test body. Copy the closest file under
  `scenarios/`.
- **steps** — one product action or assertion. Put new fleetctl verbs, waits,
  and parsers here, with unit tests (no e2e build tag).
- **harness** — fixture process (start/stop AIO, `Run`, `LoginAs`). Keep
  product waits out of here.

`RunStep` is a subtest. After a failure, later steps skip so testdox still
lists the whole story. Names are phrases with spaces (no slashes).

Create/submit helpers do **not** wait. Pair them with `WaitFor*` / `Assert*`.
`t.Cleanup` deployments you create. `WaitForDeploymentActive` fails
immediately on `FAILED` or a pause reason — use `WaitForDeploymentPaused`
when you expect delivery-auth pause.

Resource **query** `resourceType` values are API identities
(`kind.fleetshift.io/Cluster`), not the fleetctl collection spelling used
on create/get (`kind.fleetshift.v1/clusters`). See `internal/steps/query.go`.

Prefer asserting through fleetctl JSON or the Kind API, not the UI.

### Add a backend scenario

```go
//go:build e2e

package scenarios

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/steps"
)

func TestSomethingVisibleInTestdox(t *testing.T) {
	ns := steps.UniqueID(t, "e2e")
	nsID := steps.UniqueID(t, "ns")
	t.Cleanup(func() {
		steps.CleanupDeployment(t, suite, nsID)
	})

	var cluster string
	steps.RunStep(t, "ensure a shared kind cluster", func(t *testing.T) {
		cluster = steps.SharedKind(t, suite)
	})
	steps.RunStep(t, "deploy a namespace", func(t *testing.T) {
		steps.CreateNamespaceDeploymentOn(t, suite, nsID, ns, cluster)
	})
	steps.RunStep(t, "wait until the namespace is active", func(t *testing.T) {
		steps.WaitForDeploymentActive(t, suite, nsID)
	})
}
```

Local runs print live testdox steps. CI uses the GitHub Actions gotestsum
format. JSON/JUnit land in `e2e/backend/tmp/` (gitignored).

The Kind addon inside the AIO talks to the **host** engine through a mounted
socket. If that path is wrong, cluster create fails with opaque podman
errors after a long poll. The harness (and the UI CI job) smoke this before
those tests. On failure, the dump compares host Kind nodes with the same
list through the AIO mount.

---

## Frontend

Playwright against `https://fleetshift-sandbox.localhost:8085` (SPA under
`/app`). Dex is same-origin under `/idp` on that port; do not talk to Dex
on 5556.

```
e2e/web/
  playwright.config.mts          # nx test:e2e e2e-web
  playwright.cli-login.mts       # fleetctl login helper — not this suite
  tests/auth-setup.ts            # Dex login once per persona
  tests/fixtures.ts              # restores oidc sessionStorage
  tests/helpers/
  tests/*.spec.ts
```

`complete-cli-login.spec.ts` is ignored by the main config. The backend
harness is the only supported caller. Do not put console assertions there.

### Auth for specs

A `setup` project logs each persona in through Dex and writes `.auth/`.
Playwright `storageState` does not persist `sessionStorage`, where the OIDC
client keeps the user, so `fixtures.ts` re-injects a sibling `*-session.json`.

Import `test` and `expect` from `./fixtures`, not `@playwright/test`.
Projects default to ops. Override per describe:

```ts
import { expect, test } from "./fixtures";
import { DEVELOPER } from "./helpers/personas";

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
   `tests/helpers/` (see `clusters.ts`).
3. Kind create/delete is slow. Give that journey a wide `test.setTimeout`.
   Do not add another Kind lifecycle unless the UI path is different —
   backend already covers delivery and indexing.
4. Poll live data with `expect.poll` when the page already refreshes in
   place; a reload just replays the skeleton.
5. CI runs Chromium only. Firefox and WebKit are local extras.

Traces, screenshots, and video on failure go under `e2e/web/test-results/`
and `playwright-report/`. `.auth/` is gitignored.

---

## GitHub Actions

[`.github/workflows/e2e.yml`](../../.github/workflows/e2e.yml) plus
[`.github/actions/setup-e2e`](../../.github/actions/setup-e2e/).

Unit/component CI (`.github/workflows/test.yml`) runs
`npx nx test e2e-backend` and Playwright **component** tests. It does not
run these suites.

```
aio-image          # role: cache — build or restore the AIO tar
 ├── e2e-ui        # role: e2e — start sandbox, Playwright
 └── e2e-backend   # role: e2e — TestMain owns AIO (image already loaded)
```

The test jobs restore the same tree-keyed tar and run in parallel. They do
not share a running container.

`setup-e2e` is the only place that should install Go, Node, or Playwright,
load the image, start user `podman.socket`, and ensure the `kind` network.
The `cache` role never starts FleetShift. The `e2e` role never rebuilds the
image (`FLEETSHIFT_E2E_AIO_PREBUILT=1` for backend).

**Do not** add `podman run`, log dump, or `podman rm` steps to `e2e-backend`.
A second AIO fights over ports and the `kind` network alias; a dump after
`Stop` sees an empty host; prefix-wide Kind cleanup can delete the other
job's nodes. The UI job is the one that starts a named sandbox, publishes
8085 only, and uploads Playwright artifacts. Backend uploads
`e2e/backend/tmp/`.

The image cache is a **multi-image** archive. Saving without that makes the
AIO tag an alias of the server image; `setup-e2e` refuses to load that tar.
Any tracked-file change busts the key (git tree hash). Bump the key prefix
in the action only if the tar format itself changes.
