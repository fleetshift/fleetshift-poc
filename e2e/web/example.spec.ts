import { expect, test } from "@playwright/test";

test("should redirect to /login", async ({ page }) => {
  await page.goto("/");

  // Expect h1 to contain a substring.
  expect(await page.locator("h1").innerText()).toContain("Login");

  await page.getByLabel("username").fill("John");
  await page.getByLabel("password").fill("password");

  await page.getByRole("button").click();

  await page.waitForURL("/");

  expect(await page.locator("h1").innerText()).toContain("Home");

  expect(await page.locator("p").innerText()).toContain("Welcome, John!");
});
