import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: [
      "tests/**/*.test.ts",
      "../shared/**/*.test.ts",
      "../sandbox/**/*.test.mjs",
    ],
  },
});
