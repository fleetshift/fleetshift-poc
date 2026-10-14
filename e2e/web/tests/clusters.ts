import { expect, type Locator, type Page } from "@playwright/test";

import { uniqueKindClusterIdFromEnv } from "../../shared/kind-cluster-id";

export const CLUSTERS_PATH = "/app/core/clusters";

// Kind clusters render state ACTIVE ("Active") or RUNNING ("Running") once
// ready (observed: Kind reports "Active"); CREATING/PROVISIONING while still
// coming up. Status is a bare <span> (no role), so it is matched by text.
const READY_STATE = /Running|Active/;

const DEFAULT_POLL_TIMEOUT = 5 * 60 * 1000;

/** Generates an RFC1123-safe cluster name unique across parallel Playwright projects. */
export function uniqueClusterName(): string {
  return uniqueKindClusterIdFromEnv();
}

/** DataView table row for a cluster, matched by its id link. */
export function clusterRow(page: Page, name: string): Locator {
  return page
    .getByRole("row")
    .filter({ has: page.getByRole("link", { name, exact: true }) });
}

/**
 * Opens the Kind create wizard directly and provisions a cluster with default
 * networking/nodes, then waits for the wizard to close back to the list.
 */
export async function createKindCluster(
  page: Page,
  name: string,
): Promise<void> {
  await page.goto(`${CLUSTERS_PATH}?create=kind`);

  const wizard = page.getByRole("dialog", { name: "Create cluster" });
  await expect(wizard.getByLabel("Cluster name")).toBeVisible();
  await wizard.getByLabel("Cluster name").fill(name);

  // Step through Networking + Nodes (defaults) to Review, then submit.
  await wizard.getByRole("button", { name: "Next" }).click(); // -> Networking
  await wizard.getByRole("button", { name: "Next" }).click(); // -> Nodes
  await wizard.getByRole("button", { name: "Next" }).click(); // -> Review
  await wizard.getByRole("button", { name: "Create cluster" }).click();

  // Modal closes once creation is accepted and the new row is listed.
  await expect(wizard).toBeHidden();
  await expect(clusterRow(page, name)).toBeVisible({ timeout: 30_000 });
}

/**
 * Polls the live list until the cluster reports ready. No reload: ClustersPage
 * self-refreshes in place every 5s while a row is transient, so the status
 * label flips without navigating (reloading would just replay the skeleton).
 */
export async function waitForClusterReady(
  page: Page,
  name: string,
  timeout = DEFAULT_POLL_TIMEOUT,
): Promise<void> {
  await expect
    .poll(
      async () => {
        const row = clusterRow(page, name);
        if ((await row.count()) === 0) return "(absent)";
        return (await row.first().innerText()).replace(/\s+/g, " ");
      },
      {
        message: `cluster "${name}" never reached a ready state`,
        timeout,
        intervals: [4000],
      },
    )
    .toMatch(READY_STATE);
}

/** Deletes a cluster via the list row kebab and confirms the dialog. */
export async function deleteClusterFromList(
  page: Page,
  name: string,
): Promise<void> {
  await page.goto(CLUSTERS_PATH);
  const row = clusterRow(page, name);
  await expect(row).toBeVisible();

  await row.getByRole("button", { name: "Kebab toggle" }).click();
  await page.getByRole("menuitem", { name: "Delete", exact: true }).click();

  const dialog = page.getByRole("dialog", { name: "Delete cluster" });
  await expect(dialog).toContainText(`delete "${name}"`);
  await dialog.getByRole("button", { name: "Delete", exact: true }).click();
  await expect(dialog).toBeHidden();
}

/** Polls the live list until the cluster row is gone. No reload. */
export async function waitForClusterGone(
  page: Page,
  name: string,
  timeout = DEFAULT_POLL_TIMEOUT,
): Promise<void> {
  await expect
    .poll(
      async () => {
        return clusterRow(page, name).count();
      },
      {
        message: `cluster "${name}" was still listed after deletion`,
        timeout,
        intervals: [4000],
      },
    )
    .toBe(0);
}
