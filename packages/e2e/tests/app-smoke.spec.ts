import { test, expect } from "@playwright/test";

test.describe("App smoke tests", () => {
  test("app loads after OIDC login", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("#root")).not.toBeEmpty();
  });

  test("nav sidebar shows plugin pages", async ({ page }) => {
    await page.goto("/");
    const nav = page.getByRole("navigation");
    await expect(nav.getByRole("link", { name: "Targets" })).toBeVisible();
    await expect(
      nav.getByRole("link", { name: "Orchestration" }),
    ).toBeVisible();
    await expect(nav.getByRole("link", { name: "Signing Keys" })).toBeVisible();
  });

  test("navigating to Targets loads plugin module", async ({ page }) => {
    await page.goto("/targets");
    await expect(page.locator("#root")).not.toBeEmpty();
  });
});
