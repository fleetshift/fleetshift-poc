import { defineConfig, devices } from "@playwright/test";

// Completes Fleetctl's authorization URL against Dex. Not part of nx test:e2e.
export default defineConfig({
  testDir: "./tests",
  testMatch: /complete-cli-login\.spec\.ts/,
  outputDir: "./test-results-cli-login",
  reporter: [["list"]],
  use: {
    ignoreHTTPSErrors: true,
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
