/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { test } from "../fixtures";
import { RECOVERY_WAIT_TIMEOUT_MS, uniqueId } from "../steps/deployments";
import { uniqueKindClusterId } from "../steps/kind";

test(
  "Kind cluster lifecycle removes product and host state",
  { kindClusters: [] },
  async ({ cli }) => {
    const { kind, query } = cli;
    const cluster = uniqueKindClusterId();

    await test.step("create a private Kind cluster and wait until it is ready", async () => {
      await kind.create(cluster);
      await kind.waitUntilReady(cluster);
    });
    await test.step("wait until OIDC authentication works", () =>
      kind.waitUntilAPIAcceptsToken(cluster));
    await test.step("wait until the Kind Cluster is indexed", () =>
      query.indexedKindClusterExists(cluster));
    await test.step("remove the cluster and verify product and host cleanup", async () => {
      await kind.delete(cluster);
      await kind.waitUntilGone(cluster);
      await kind.waitUntilHostClusterGone(cluster);
    });
    await test.step("wait until the indexed Kind Cluster is gone", () =>
      query.indexedKindClusterGone(cluster));
  },
);

test(
  "ops token can write directly to the Kind API",
  { kindClusters: [{ access: "modifiable", state: "any" }] },
  async ({ cli, kindClusters: [cluster] }) => {
    const { kind } = cli;
    const namespace = uniqueId("oidc");

    await test.step("create a namespace with the ops token", () =>
      kind.createNamespaceViaKindAPI(cluster.id, namespace));
    await test.step("read the namespace with the ops token", () =>
      kind.waitUntilNamespaceExistsViaKindAPI(cluster.id, namespace));
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
    await test.step("wait until the indexed ConfigMap entry is gone", () =>
      query.indexedConfigMapGone(cluster.id, namespace));
    await test.step("remove namespace deployment and verify cleanup", async () => {
      await deployments.delete(namespaceDeployment);
      await deployments.waitUntilGone(namespaceDeployment);
      await kind.waitUntilNamespaceGone(cluster.id, namespace);
    });
  },
);

const MULTI_NODE_SPEC = {
  nodes: [{ role: "control-plane" }, { role: "worker" }],
} as const;

test(
  "Kind Cluster with a worker node matches the requested spec",
  {
    kindClusters: [
      { access: "read-only", spec: MULTI_NODE_SPEC, state: "any" },
    ],
  },
  async ({ cli, kindClusters: [cluster] }) => {
    const { kind, query } = cli;

    await test.step("verify typed get and list preserve the nested node spec", () =>
      kind.expectSpec(cluster.id, MULTI_NODE_SPEC));
    const nodes =
      await test.step("verify the Kubernetes API has one control-plane and one worker", () =>
        kind.waitUntilNodeRoles(cluster.id, { controlPlane: 1, worker: 1 }));
    await test.step("verify Kind Node and Kubernetes Node indexes match both names", () =>
      query.nodeIdentitiesMatch(
        cluster.id,
        nodes.map(({ name }) => name),
      ));
  },
);

test(
  "deletion converges after a temporary target outage",
  { kindClusters: [{ access: "modifiable", state: "any" }] },
  async ({ cli, kindClusters: [cluster] }) => {
    const { deployments, kind } = cli;
    const namespace = uniqueId("e2e");
    const namespaceDeployment = uniqueId("ns");
    const configMapDeployment = uniqueId("cm");

    await test.step("deploy a namespace and ConfigMap and wait until both are active", async () => {
      await deployments.createNamespace({
        id: namespaceDeployment,
        namespace,
        targets: [cluster.id],
      });
      await deployments.waitUntilActive(namespaceDeployment);
      await kind.waitUntilNamespaceExists(cluster.id, namespace);
      await deployments.createConfigMap({
        id: configMapDeployment,
        namespace,
        targets: [cluster.id],
      });
      await deployments.waitUntilActive(configMapDeployment);
      await kind.waitUntilConfigMapExists(cluster.id, namespace);
    });
    try {
      await test.step("pause Kind nodes and delete the ConfigMap deployment", async () => {
        await kind.pauseHostCluster(cluster.id);
        await deployments.delete(configMapDeployment);
      });
      await test.step("wait until deletion stays blocked while the target is down", () =>
        deployments.waitUntilRemainsListed(configMapDeployment));
      await test.step("restore Kind nodes", () =>
        kind.unpauseHostCluster(cluster.id));
      await test.step("wait until the deployment and ConfigMap converge away", async () => {
        await deployments.waitUntilGone(
          configMapDeployment,
          RECOVERY_WAIT_TIMEOUT_MS,
        );
        await kind.waitUntilConfigMapGone(
          cluster.id,
          namespace,
          RECOVERY_WAIT_TIMEOUT_MS,
        );
      });
    } finally {
      await kind.unpauseHostCluster(cluster.id);
    }
    await test.step("remove namespace deployment and verify cleanup", async () => {
      await deployments.delete(namespaceDeployment);
      await deployments.waitUntilGone(namespaceDeployment);
      await kind.waitUntilNamespaceGone(cluster.id, namespace);
    });
  },
);
