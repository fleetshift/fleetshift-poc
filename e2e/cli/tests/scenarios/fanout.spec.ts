/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { test } from "../fixtures";
import { uniqueId } from "../steps/deployments";

test(
  "delivery fans out to two clusters",
  {
    kindClusters: [
      { access: "modifiable", state: "any" },
      { access: "modifiable", state: "any" },
    ],
  },
  async ({ cli, kindClusters: [first, second] }) => {
    const { deployments, kind } = cli;
    const namespace = uniqueId("e2e");
    const namespaceDeployment = uniqueId("ns");
    const configMapDeployment = uniqueId("cm");

    await test.step("deploy and verify namespace fan-out", async () => {
      await deployments.createNamespace({
        id: namespaceDeployment,
        namespace,
        targets: [first.id, second.id],
      });
      await deployments.waitUntilActive(namespaceDeployment);
      await Promise.all([
        kind.waitUntilNamespaceExists(first.id, namespace),
        kind.waitUntilNamespaceExists(second.id, namespace),
      ]);
    });
    await test.step("deploy and verify ConfigMap fan-out", async () => {
      await deployments.createConfigMap({
        id: configMapDeployment,
        namespace,
        targets: [first.id, second.id],
      });
      await deployments.waitUntilActive(configMapDeployment);
      await Promise.all([
        kind.waitUntilConfigMapExists(first.id, namespace),
        kind.waitUntilConfigMapExists(second.id, namespace),
      ]);
    });
    await test.step("remove ConfigMap deployment and verify cleanup", async () => {
      await deployments.delete(configMapDeployment);
      await deployments.waitUntilGone(configMapDeployment);
      await Promise.all([
        kind.waitUntilConfigMapGone(first.id, namespace),
        kind.waitUntilConfigMapGone(second.id, namespace),
      ]);
    });
    await test.step("remove namespace deployment and verify cleanup", async () => {
      await deployments.delete(namespaceDeployment);
      await deployments.waitUntilGone(namespaceDeployment);
      await Promise.all([
        kind.waitUntilNamespaceGone(first.id, namespace),
        kind.waitUntilNamespaceGone(second.id, namespace),
      ]);
    });
  },
);
