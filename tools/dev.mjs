import { spawn } from "child_process";
import { mkdirSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { execSync } from "child_process";
import Watchpack from "watchpack";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const wsRoot = root;
const guiDist = resolve(root, "client/web/dist");
const pluginsDist = resolve(root, "extensions/core/client/dist");
const watchOnly = process.argv.includes("--watch");

function nx(target) {
  execSync(`npx nx run ${target}`, { cwd: wsRoot, stdio: "inherit" });
}

// Always rebuild common — it's fast (tsc only) and rspack depends on its dist
console.log("Building @fleetshift/common...");
nx("common:build");

function merge() {
  try {
    execSync("node tools/merge-web.mjs --incremental", {
      cwd: root,
      stdio: "inherit",
    });
  } catch {
    console.error("merge-web.mjs failed");
  }
}

if (!watchOnly) {
  console.log("Running initial build...");
  nx("plugins:build");
  nx("ui:generate-registry");
  nx("gui:build");
  nx("ui:merge-web");
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

const pluginsCwd = resolve(root, "extensions/core/client");
const guiCwd = resolve(root, "client/web");

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
