import path from "path";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "@fleetshift/common": path.resolve(__dirname, "fleetshift-ui/common/src"),
    },
  },
  test: {
    include: ["./fleetshift-ui/*/src/**/__tests__/**/*.test.ts"],
    globals: true,
  },
});
