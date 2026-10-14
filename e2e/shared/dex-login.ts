import { type Page } from "@playwright/test";

import { type Persona } from "./personas";

/** Fill Dex's password login form and submit. Does not wait for post-login UI. */
export async function fillDexLogin(
  page: Page,
  persona: Persona,
): Promise<void> {
  await page
    .locator("h2")
    .filter({ hasText: "Log in to Your Account" })
    .waitFor();

  await page
    .getByRole("textbox", { name: "Email Address" })
    .fill(persona.email);
  await page.getByRole("textbox", { name: "Password" }).fill(persona.password);

  // Dex renders a single submit button on the password screen.
  await page.getByRole("button").click();
}

/** Complete Dex login for a fleetctl --no-browser loopback callback. */
export async function completeDexCliLogin(
  page: Page,
  persona: Persona,
): Promise<void> {
  await fillDexLogin(page, persona);
  await page.waitForURL(
    (callback) =>
      callback.hostname === "127.0.0.1" && callback.pathname === "/callback",
    { timeout: 10_000 },
  );
}
