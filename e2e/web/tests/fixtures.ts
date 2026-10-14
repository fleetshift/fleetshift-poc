import { test as base } from "@playwright/test";
import fs from "fs";
import path from "path";

// storageState (per-project or per-test via test.use) restores cookies +
// localStorage, but react-oidc-context keeps the signed-in user in
// sessionStorage, which Playwright does not persist. For each storageState file
// (e.g. ".auth/ops.json") auth-setup also writes a sibling session file
// (".auth/ops-session.json"); this fixture re-injects it so the selected
// persona stays authenticated without any per-test wiring.
function sessionFileFor(storageState: unknown): string | null {
  if (typeof storageState !== "string") return null;
  const rel = storageState.replace(/\.json$/, "-session.json");
  // storageState paths are resolved relative to the config dir (e2e/web);
  // __dirname is e2e/web/tests, so step up one level.
  return path.isAbsolute(rel) ? rel : path.join(__dirname, "..", rel);
}

export const test = base.extend({
  context: async ({ context, storageState }, use) => {
    const sessionFile = sessionFileFor(storageState);
    if (sessionFile && fs.existsSync(sessionFile)) {
      const session = JSON.parse(fs.readFileSync(sessionFile, "utf-8"));
      await context.addInitScript((storage: Record<string, string>) => {
        for (const [key, value] of Object.entries(storage)) {
          window.sessionStorage.setItem(key, value);
        }
      }, session);
    }
    // this is not a react use, its a PW use
    // eslint-disable-next-line react-hooks/rules-of-hooks
    await use(context);
  },
});

export { expect } from "@playwright/test";
