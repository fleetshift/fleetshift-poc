/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { test } from "../fixtures";
import { uniqueId } from "../steps/deployments";

test(
  "rejected Kubernetes apply stays creating and the deployment can be deleted",
  { kindClusters: [{ access: "modifiable", state: "any" }] },
  async ({ cli, kindClusters: [cluster] }) => {
    const { deployments, kind } = cli;
    const id = uniqueId("fail");
    const name = uniqueId("e2e");

    await test.step("create a valid JSON ConfigMap that Kubernetes will reject", () =>
      deployments.createManifest({
        id,
        manifest: {
          apiVersion: "v1",
          immutable: "not-a-boolean",
          kind: "ConfigMap",
          metadata: { name, namespace: "default" },
        },
        targets: [cluster.id],
      }));
    await test.step("wait until the deployment stays creating instead of succeeding or failing", () =>
      deployments.waitUntilRemainsCreating(id));
    await test.step("verify the named ConfigMap was never created", () =>
      kind.expectConfigMapAbsent(cluster.id, "default", name));
    await test.step("delete the creating deployment and confirm it disappears", async () => {
      await deployments.delete(id);
      await deployments.waitUntilGone(id);
    });
  },
);
