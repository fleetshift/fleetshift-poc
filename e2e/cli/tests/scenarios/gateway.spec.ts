/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { test } from "../fixtures";

const BOOTSTRAP_DEPLOYMENT = "idp-trust-default";

test(
  "HTTP gateway enforces authentication",
  { kindClusters: [] },
  async ({ cli }) => {
    const { gateway } = cli;
    await test.step("serve live and ready probes anonymously", async () => {
      await gateway.servesHealth("/livez");
      await gateway.servesHealth("/readyz");
    });
    await test.step("reject deployment listing without a token", () =>
      gateway.rejectsDeploymentList());
    await test.step("reject deployment listing with a bad token", () =>
      gateway.rejectsDeploymentList("not-a-jwt"));
    await test.step("list the bootstrap deployment with the ops token", () =>
      gateway.listsDeployment(BOOTSTRAP_DEPLOYMENT));
  },
);
