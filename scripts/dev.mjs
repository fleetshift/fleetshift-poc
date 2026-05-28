import { spawn } from "child_process";
import { watch, mkdirSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { execSync } from "child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const guiDist = resolve(root, "packages/gui/dist");
const pluginsDist = resolve(root, "packages/mock-ui-plugins/dist");
const watchOnly = process.argv.includes("--watch");

// Always rebuild common — it's fast (tsc only) and webpack depends on its dist
console.log("Building @fleetshift/common...");
execSync("npm run build -w packages/common", { cwd: root, stdio: "inherit" });

let mergeTimer = null;

function merge() {
  clearTimeout(mergeTimer);
  mergeTimer = setTimeout(() => {
    try {
      execSync("node scripts/merge-web.mjs --incremental", {
        cwd: root,
        stdio: "inherit",
      });
    } catch {
      console.error("merge-web.mjs failed");
    }
  }, 1500);
}

if (!watchOnly) {
  console.log("Running initial build...");
  execSync("npm run build -w packages/mock-ui-plugins", {
    cwd: root,
    stdio: "inherit",
  });
  execSync("node scripts/generate-plugin-registry.mjs", {
    cwd: root,
    stdio: "inherit",
  });
  execSync("npm run build -w packages/gui", { cwd: root, stdio: "inherit" });
  execSync("node scripts/merge-web.mjs", { cwd: root, stdio: "inherit" });
}

console.log("\nStarting watch mode...\n");

const pluginsWatch = spawn(
  "npx",
  ["webpack", "--watch", "--config", "webpack.config.ts"],
  {
    cwd: resolve(root, "packages/mock-ui-plugins"),
    stdio: "inherit",
    env: { ...process.env, NODE_OPTIONS: "--loader ts-node/esm" },
  },
);

const guiWatch = spawn(
  "npx",
  ["webpack", "--watch", "--config", "webpack.config.ts"],
  {
    cwd: resolve(root, "packages/gui"),
    stdio: "inherit",
    env: { ...process.env, NODE_OPTIONS: "--loader ts-node/esm" },
  },
);

mkdirSync(guiDist, { recursive: true });
mkdirSync(pluginsDist, { recursive: true });
watch(guiDist, { recursive: true }, merge);
watch(pluginsDist, { recursive: true }, merge);

process.on("SIGINT", () => {
  pluginsWatch.kill();
  guiWatch.kill();
  process.exit(0);
});
