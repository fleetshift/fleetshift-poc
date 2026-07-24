import path from "path";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "@fleetshift/common": path.resolve(__dirname, "packages/common/src"),
    },
  },
  test: {
    include: ["packages/*/src/**/__tests__/**/*.test.ts"],
    globals: true,
  },
});
