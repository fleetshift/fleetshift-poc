/* eslint-disable playwright/expect-expect -- Assertions live in domain step methods. */
import { OPERATOR } from "../../../shared/personas";
import { test } from "../fixtures";

test(
  "ops login is isolated to its config directory",
  { kindClusters: [] },
  async ({ cli }) => {
    const { login } = cli;
    await test.step("confirm the token belongs to the operator", () =>
      login.tokenBelongsTo(OPERATOR.email));
    await test.step("confirm an empty config cannot use suite credentials", () =>
      login.credentialsAreIsolated());
  },
);

test(
  "logout rejects then login recovers on a private config directory",
  { kindClusters: [] },
  async ({ cli }) => {
    const { login, loginAs } = cli;
    const configDir = await test.step("create a separate operator login", () =>
      loginAs(OPERATOR));
    await test.step("confirm the private config can list deployments", () =>
      login.listsDeployments(configDir));
    await test.step("log out of the private config", () =>
      login.logout(configDir));
    await test.step("confirm credentials are gone and unauthenticated calls fail", async () => {
      await login.credentialsCleared(configDir);
      await login.inspectTokenRejected(configDir);
      await login.listDeploymentsUnauthenticated(configDir);
    });
    await test.step("log in again using the same config directory", () =>
      login.relogin(OPERATOR, configDir));
    await test.step("confirm token identity and successful deployment listing", async () => {
      await login.tokenBelongsTo(OPERATOR.email, configDir);
      await login.listsDeployments(configDir);
    });
  },
);
