import { findWorkspaceRoot } from "@fleetshift/build-utils/findWorkspaceRoot";
import { defineConfig, devices } from "@playwright/experimental-ct-react";
import path from "path";

const root = findWorkspaceRoot(import.meta.dirname);

export default defineConfig({
  testDir: "./src",
  testMatch: "**/*.ct.tsx",
  timeout: 15_000,
  expect: { timeout: 5_000 },
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: process.env.CI ? "github" : "list",
  use: {
    ctPort: 3102,
    ctViteConfig: {
      resolve: {
        alias: {
          "@fleetshift/common": path.resolve(root, "sdk/common/src"),
          "@data-driven-forms/pf4-component-mapper/form-template": path.resolve(
            root,
            "node_modules/@data-driven-forms/pf4-component-mapper/esm/form-template/index.js",
          ),
          "@data-driven-forms/pf4-component-mapper/text-field": path.resolve(
            root,
            "node_modules/@data-driven-forms/pf4-component-mapper/esm/text-field/index.js",
          ),
          "@data-driven-forms/pf4-component-mapper/wizard": path.resolve(
            root,
            "node_modules/@data-driven-forms/pf4-component-mapper/esm/wizard/index.js",
          ),
          "@data-driven-forms/pf4-component-mapper/select": path.resolve(
            root,
            "node_modules/@data-driven-forms/pf4-component-mapper/esm/select/index.js",
          ),
        },
        dedupe: [
          "@data-driven-forms/react-form-renderer",
          "react",
          "react-dom",
          "react-final-form",
        ],
      },
    },
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
