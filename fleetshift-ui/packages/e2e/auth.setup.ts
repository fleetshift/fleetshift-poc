import { test as setup } from "@playwright/test";

const KEYCLOAK_URL = process.env.KEYCLOAK_URL ?? "http://localhost:8180";

setup("authenticate via Keycloak", async ({ page }) => {
  await page.goto("/");

  await page.waitForURL(`${KEYCLOAK_URL}/**`, { timeout: 15_000 });

  await page.getByLabel("Username or email").fill("ops");
  await page.getByLabel("Password").fill("test");
  await page.getByRole("button", { name: "Sign In" }).click();

  await page.waitForURL("http://localhost:8085/**", { timeout: 15_000 });

  await page.context().storageState({ path: "playwright/.auth/user.json" });
});
