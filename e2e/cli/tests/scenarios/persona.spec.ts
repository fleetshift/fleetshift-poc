/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { DEVELOPER } from "../../../shared/personas";
import { test } from "../fixtures";
import { uniqueId } from "../steps/deployments";

test(
  "developer delivery pauses until ops resumes it",
  { kindClusters: [{ access: "modifiable", state: "any" }] },
  async ({ cli, kindClusters: [cluster] }) => {
    const { deployments, kind, login, loginAs } = cli;
    const namespace = uniqueId("e2e");
    const namespaceDeployment = uniqueId("ns");
    const configMapDeployment = uniqueId("cm");
    const developerConfig = await test.step("log in as the developer", () =>
      loginAs(DEVELOPER));

    await test.step("confirm the token belongs to the developer", () =>
      login.tokenBelongsTo(DEVELOPER.email, developerConfig));
    await test.step("confirm the developer can list deployments", () =>
      login.listsDeployments(developerConfig));
    await test.step("confirm ops credentials remain isolated", () =>
      login.credentialsAreIsolated());
    await test.step("reject the developer token on the cluster", () =>
      kind.waitUntilForbidden(cluster.id, developerConfig));
    await test.step("deploy a namespace as ops", async () => {
      await deployments.createNamespace({
        id: namespaceDeployment,
        namespace,
        targets: [cluster.id],
      });
      await deployments.waitUntilActive(namespaceDeployment);
    });
    await test.step("deploy a ConfigMap as the developer", () =>
      deployments.createConfigMap({
        configDir: developerConfig,
        id: configMapDeployment,
        namespace,
        targets: [cluster.id],
      }));
    await test.step("wait until delivery pauses for fresh credentials", () =>
      deployments.waitUntilPausedForDeliveryAuth(configMapDeployment));
    await test.step("resume and verify ConfigMap delivery", async () => {
      await deployments.resume(configMapDeployment);
      await deployments.waitUntilResumed(configMapDeployment);
      await kind.waitUntilConfigMapExists(cluster.id, namespace);
    });
    await test.step("remove ConfigMap deployment and verify cleanup", async () => {
      await deployments.delete(configMapDeployment);
      await deployments.waitUntilGone(configMapDeployment);
      await kind.waitUntilConfigMapGone(cluster.id, namespace);
    });
  },
);
