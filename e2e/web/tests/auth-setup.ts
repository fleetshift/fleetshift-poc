/* eslint-disable playwright/no-standalone-expect */
import { expect, type Page, test as setup } from "@playwright/test";
import fs from "fs";
import path from "path";

import { fillDexLogin } from "./helpers/dex-login";
import { type Persona, PERSONAS } from "./helpers/personas";

const authDir = path.join(__dirname, "..", ".auth");

// Logs a persona in through the real Dex screen and saves its authenticated
// state for reuse. storageState covers cookies + localStorage; the oidc token
// lives in sessionStorage, so it is dumped to a sibling file that fixtures.ts
// re-injects (keyed off the storageState path).
async function authenticate(page: Page, persona: Persona) {
  await page.goto("/app/");
  await fillDexLogin(page, persona);

  // Console is ready once the masthead identifies the signed-in persona.
  await expect(
    page.getByRole("button", { name: persona.usernameLabel }),
    `expected masthead to show "${persona.usernameLabel}" after login`,
  ).toBeVisible({ timeout: 30_000 });

  await page
    .context()
    .storageState({ path: path.join(authDir, `${persona.id}.json`) });
  const storage = await page.evaluate(() => JSON.stringify(sessionStorage));
  fs.writeFileSync(
    path.join(authDir, `${persona.id}-session.json`),
    storage,
    "utf-8",
  );
}

for (const persona of PERSONAS) {
  setup(`authenticate as ${persona.id}`, async ({ page }) => {
    await authenticate(page, persona);
  });
}
