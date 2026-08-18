import { expect, test } from "./fixtures";
import {
  clusterRow,
  createKindCluster,
  deleteClusterFromList,
  uniqueClusterName,
  waitForClusterGone,
  waitForClusterReady,
} from "./helpers/clusters";

// OME-257: the create-cluster-to-ready / delete-cluster-to-gone UI journey,
// run as the operator against the bundled Kind addon. Kind provisioning is
// slow, so the whole journey gets a wide timeout.
test.describe("kind cluster lifecycle", () => {
  test.use({ storageState: ".auth/ops.json" });
  test.setTimeout(15 * 60 * 1000);

  test("create, observe ready, delete, and confirm gone", async ({ page }) => {
    const name = uniqueClusterName();

    await createKindCluster(page, name);
    await waitForClusterReady(page, name);
    await deleteClusterFromList(page, name);
    await waitForClusterGone(page, name);

    await expect(clusterRow(page, name)).toHaveCount(0);
  });
});
