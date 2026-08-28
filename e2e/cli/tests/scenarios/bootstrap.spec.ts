/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { test } from "../fixtures";

const BOOTSTRAP_DEPLOYMENT = "idp-trust-default";

test(
  "bootstrap deployment is listed and active",
  { kindClusters: [] },
  async ({ cli }) => {
    const { deployments } = cli;
    await test.step("find the bundled IdP trust deployment and wait until it is active", async () => {
      await deployments.waitUntilListed(BOOTSTRAP_DEPLOYMENT);
      await deployments.waitUntilActive(BOOTSTRAP_DEPLOYMENT);
    });
  },
);
