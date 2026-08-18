import { defineConfig, devices } from '@playwright/test';

const baseURL = process.env['BASE_URL'] || 'http://127.0.0.1:8085';

export default defineConfig({
  testDir: './tests',
  outputDir: './test-results',
  // 'list' streams a line per test so CI shows live progress. The CI default
  // ('dot') emits newline-less dots that GitHub Actions buffers until the step
  // ends, which looks like a hang while slow specs (cluster lifecycle) run.
  // HTML report must live outside outputDir (test-results); the html reporter
  // wipes its folder before writing, which would nuke per-test traces/videos.
  reporter: [
    ['list'],
    ['html', { open: 'never', outputFolder: 'playwright-report' }],
  ],
  use: {
    baseURL,
    trace: 'on-first-retry',
    // Capture visual evidence for failed tests (kept out of the repo via
    // test-results/, useful for debugging slow cluster-lifecycle journeys).
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },

  projects: [
    {
      name: "setup",
      testMatch: /.*auth-setup\.ts/,
    },
    {
      name: "chromium",
      // Default persona; specs select another via test.use({ storageState }).
      use: { ...devices["Desktop Chrome"], storageState: '.auth/ops.json', },
      dependencies: ['setup'],
    },
    {
      name: "firefox",
      use: { ...devices["Desktop Firefox"], storageState: '.auth/ops.json', },
      dependencies: ['setup'],
    },
    {
      name: "webkit",
      use: { ...devices["Desktop Safari"], storageState: '.auth/ops.json', },
      dependencies: ['setup'],
    },
  ],
});
