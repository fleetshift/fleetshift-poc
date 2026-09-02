import { DEVELOPER, OPERATOR } from "../../shared/personas";
import { expect, test } from "./fixtures";

// OME-257: prove the packaged image reaches a working, logged-in console for
// both sandbox personas. The real authorization-code + PKCE login through the
// rendered Dex screen is exercised once per persona in auth-setup; each test
// here reuses that persona's saved session (Playwright's multi-role pattern)
// and asserts the console identifies the right persona.

test.describe("operator persona", () => {
  test.use({ storageState: ".auth/ops.json" });

  test("lands on the console identified as the operator", async ({ page }) => {
    await page.goto("/app/");

    await expect(
      page.getByRole("button", { name: OPERATOR.usernameLabel }),
    ).toBeVisible();
    // Console navigation is present (i.e. not bounced to the login screen).
    await expect(page.getByRole("link", { name: "Clusters" })).toBeVisible();
  });
});

test.describe("developer persona", () => {
  test.use({ storageState: ".auth/dev.json" });

  test("lands on the console identified as the developer", async ({ page }) => {
    await page.goto("/app/");

    await expect(
      page.getByRole("button", { name: DEVELOPER.usernameLabel }),
    ).toBeVisible();
    // The developer session is not mistaken for the operator.
    await expect(
      page.getByRole("button", { name: OPERATOR.usernameLabel }),
    ).toHaveCount(0);
  });
});
