import { expect, test } from "./fixtures";

test("should show console when already authenticated", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("link", { name: "Clusters" }), {
    message: "Expected 'Clusters' to be visible without hitting the login page",
  }).toBeVisible();
});
