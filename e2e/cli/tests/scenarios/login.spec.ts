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
