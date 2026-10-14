import path from "path";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "@fleetshift/common": path.resolve(__dirname, "sdk/common/src"),
      "ink-table": path.resolve(
        __dirname,
        "client/cli/src/test-helpers/ink-table.tsx",
      ),
    },
  },
  test: {
    include: [
      "./sdk/*/src/**/__tests__/**/*.test.ts",
      "./client/*/src/**/__tests__/**/*.test.ts",
      "./extensions/**/src/**/__tests__/**/*.test.ts",
    ],
    globals: true,
  },
});
