/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { test } from "../fixtures";
import { uniqueId } from "../steps/deployments";
import { uniqueKindClusterId } from "../steps/kind";

test(
  "Kind cluster lifecycle removes product and host state",
  { kindClusters: [] },
  async ({ cli }) => {
    const { kind } = cli;
    const cluster = uniqueKindClusterId();

    await test.step("create a private Kind cluster and wait until it is ready", async () => {
      await kind.create(cluster);
      await kind.waitUntilReady(cluster);
    });
    await test.step("wait until OIDC authentication works", () =>
      kind.waitUntilAPIAcceptsToken(cluster));
    await test.step("remove the cluster and verify product and host cleanup", async () => {
      await kind.delete(cluster);
      await kind.waitUntilGone(cluster);
      await kind.waitUntilHostClusterGone(cluster);
    });
  },
);

test(
  "ops token can write directly to the Kind API",
  { kindClusters: [{ access: "modifiable", state: "any" }] },
  async ({ cli, kindClusters: [cluster] }) => {
    const { kind } = cli;
    const namespace = uniqueId("oidc");

    await test.step("create a namespace with the ops token", () =>
      kind.createNamespaceViaOIDC(cluster.id, namespace));
    await test.step("read the namespace with the ops token", () =>
      kind.waitUntilNamespaceExistsViaOIDC(cluster.id, namespace));
  },
);

test(
  "Kind delivery completes a round trip",
  { kindClusters: [{ access: "modifiable", state: "any" }] },
  async ({ cli, kindClusters: [cluster] }) => {
    const { deployments, kind, query } = cli;
    const namespace = uniqueId("e2e");
    const namespaceDeployment = uniqueId("ns");
    const configMapDeployment = uniqueId("cm");

    await test.step("deploy and verify namespace", async () => {
      await deployments.createNamespace({
        id: namespaceDeployment,
        namespace,
        targets: [cluster.id],
      });
      await deployments.waitUntilActive(namespaceDeployment);
      await kind.waitUntilNamespaceExists(cluster.id, namespace);
    });
    await test.step("deploy and verify ConfigMap", async () => {
      await deployments.createConfigMap({
        id: configMapDeployment,
        namespace,
        targets: [cluster.id],
      });
      await deployments.waitUntilActive(configMapDeployment);
      await kind.waitUntilConfigMapExists(cluster.id, namespace);
    });
    await test.step("find the ConfigMap in the resource index", () =>
      query.indexedConfigMapExists(cluster.id, namespace));
    await test.step("remove ConfigMap deployment and verify cleanup", async () => {
      await deployments.delete(configMapDeployment);
      await deployments.waitUntilGone(configMapDeployment);
      await kind.waitUntilConfigMapGone(cluster.id, namespace);
    });
    await test.step("remove namespace deployment and verify cleanup", async () => {
      await deployments.delete(namespaceDeployment);
      await deployments.waitUntilGone(namespaceDeployment);
      await kind.waitUntilNamespaceGone(cluster.id, namespace);
    });
  },
);
