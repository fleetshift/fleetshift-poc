import { spawn } from "child_process";
import { mkdirSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { execSync } from "child_process";
import Watchpack from "watchpack";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const guiDist = resolve(root, "packages/gui/dist");
const pluginsDist = resolve(root, "packages/mock-ui-plugins/dist");
const watchOnly = process.argv.includes("--watch");

// Always rebuild common — it's fast (tsc only) and rspack depends on its dist
console.log("Building @fleetshift/common...");
execSync("npm run build -w packages/common", { cwd: root, stdio: "inherit" });

function merge() {
  try {
    execSync("node scripts/merge-web.mjs --incremental", {
      cwd: root,
      stdio: "inherit",
    });
  } catch {
    console.error("merge-web.mjs failed");
  }
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

function spawnRspack(cwd) {
  return spawn(
    "npx",
    ["rspack", "build", "--watch"],
    {
      cwd,
      stdio: "inherit",
      env: { ...process.env, NODE_OPTIONS: "--max-old-space-size=8192" },
    },
  );
}

const pluginsCwd = resolve(root, "packages/mock-ui-plugins");
const guiCwd = resolve(root, "packages/gui");

let pluginsWatch = spawnRspack(pluginsCwd);
let guiWatch = spawnRspack(guiCwd);

const pluginsConfig = resolve(pluginsCwd, "rspack.config.ts");
const guiConfig = resolve(guiCwd, "rspack.config.ts");
const configWatcher = new Watchpack({ aggregateTimeout: 300 });
configWatcher.watch({ files: [pluginsConfig, guiConfig] });
configWatcher.on("change", (changedFile) => {
  if (changedFile === pluginsConfig) {
    console.log("\nplugins rspack.config.ts changed — restarting plugins build...\n");
    const prev = pluginsWatch;
    prev.kill();
    prev.on("close", () => {
      pluginsWatch = spawnRspack(pluginsCwd);
    });
  } else if (changedFile === guiConfig) {
    console.log("\ngui rspack.config.ts changed — restarting gui build...\n");
    const prev = guiWatch;
    prev.kill();
    prev.on("close", () => {
      guiWatch = spawnRspack(guiCwd);
    });
  }
});

mkdirSync(guiDist, { recursive: true });
mkdirSync(pluginsDist, { recursive: true });
const distWatcher = new Watchpack({ aggregateTimeout: 1500 });
distWatcher.watch({ directories: [guiDist, pluginsDist] });
distWatcher.on("aggregated", merge);

process.on("SIGINT", () => {
  pluginsWatch.kill();
  guiWatch.kill();
  configWatcher.close();
  distWatcher.close();
  process.exit(0);
});
