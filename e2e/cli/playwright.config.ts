import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/scenarios",
  outputDir: "./test-results",
  reporter: [["list"]],
  retries: 0,
  workers: 5,
  timeout: 25 * 60 * 1000,
  expect: { timeout: 2 * 60_000 },
  use: {
    ignoreHTTPSErrors: false,
    screenshot: "off",
    trace: "off",
    video: "off",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
