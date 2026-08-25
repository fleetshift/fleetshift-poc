import { expect, test } from "@playwright/test";

import { fillDexLogin } from "./helpers/dex-login";
import {
  DEVELOPER,
  OPERATOR,
  type Persona,
  type PersonaId,
} from "./helpers/personas";

// Invoked only via playwright.cli-login.mts (AUTH_URL + PERSONA). Never part of
// `nx test:e2e e2e-web`.

const PERSONA_BY_ID: Record<PersonaId, Persona> = {
  ops: OPERATOR,
  dev: DEVELOPER,
};

function personaFromEnv(): Persona {
  const raw = process.env["PERSONA"] ?? "ops";
  if (raw !== "ops" && raw !== "dev") {
    throw new Error(`PERSONA must be ops or dev, got ${JSON.stringify(raw)}`);
  }
  return PERSONA_BY_ID[raw];
}

test("completes Fleetctl Dex login", async ({ page }) => {
  const authURL = process.env["AUTH_URL"];
  expect(
    authURL,
    "AUTH_URL is required (this spec is for --config=playwright.cli-login.mts)",
  ).toBeTruthy();

  await page.goto(authURL!);
  await fillDexLogin(page, personaFromEnv());

  await page.waitForURL(
    (url) => url.hostname === "127.0.0.1" && url.pathname === "/callback",
    { timeout: 10_000 },
  );
});
