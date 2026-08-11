import { spawn } from "child_process";
import { mkdirSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { execSync } from "child_process";
import Watchpack from "watchpack";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const wsRoot = root;
const guiDist = resolve(root, "client/web/dist");
const corePluginsDist = resolve(root, "extensions/core/client/dist");
const gcphcpPluginsDist = resolve(root, "extensions/gcphcp/client/dist");
const kindPluginsDist = resolve(root, "extensions/kind/client/dist");
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
  nx("@fleetshift/gcphcp-plugin:build");
  nx("@fleetshift/kind-plugin:build");
  nx("ui:generate-registry");
  nx("web:build");
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
      env: { ...process.env, NODE_ENV: "development", NODE_OPTIONS: "--max-old-space-size=8192" },
    },
  );
}

const corePluginsCwd = resolve(root, "extensions/core/client");
const gcphcpPluginsCwd = resolve(root, "extensions/gcphcp/client");
const kindPluginsCwd = resolve(root, "extensions/kind/client");
const guiCwd = resolve(root, "client/web");

let corePluginsWatch = spawnRspack(corePluginsCwd);
let gcphcpPluginsWatch = spawnRspack(gcphcpPluginsCwd);
let kindPluginsWatch = spawnRspack(kindPluginsCwd);
let guiWatch = spawnRspack(guiCwd);

const corePluginsConfig = resolve(corePluginsCwd, "rspack.config.ts");
const gcphcpPluginsConfig = resolve(gcphcpPluginsCwd, "rspack.config.ts");
const kindPluginsConfig = resolve(kindPluginsCwd, "rspack.config.ts");
const guiConfig = resolve(guiCwd, "rspack.config.ts");
const configWatcher = new Watchpack({ aggregateTimeout: 300 });
configWatcher.watch({ files: [corePluginsConfig, gcphcpPluginsConfig, kindPluginsConfig, guiConfig] });
configWatcher.on("change", (changedFile) => {
  if (changedFile === corePluginsConfig) {
    console.log("\ncore plugins rspack.config.ts changed — restarting build...\n");
    const prev = corePluginsWatch;
    prev.kill();
    prev.on("close", () => {
      corePluginsWatch = spawnRspack(corePluginsCwd);
    });
  } else if (changedFile === gcphcpPluginsConfig) {
    console.log("\ngcphcp plugins rspack.config.ts changed — restarting build...\n");
    const prev = gcphcpPluginsWatch;
    prev.kill();
    prev.on("close", () => {
      gcphcpPluginsWatch = spawnRspack(gcphcpPluginsCwd);
    });
  } else if (changedFile === kindPluginsConfig) {
    console.log("\nkind plugins rspack.config.ts changed — restarting build...\n");
    const prev = kindPluginsWatch;
    prev.kill();
    prev.on("close", () => {
      kindPluginsWatch = spawnRspack(kindPluginsCwd);
    });
  } else if (changedFile === guiConfig) {
    console.log("\ngui rspack.config.ts changed — restarting web build...\n");
    const prev = guiWatch;
    prev.kill();
    prev.on("close", () => {
      guiWatch = spawnRspack(guiCwd);
    });
  }
});

mkdirSync(guiDist, { recursive: true });
mkdirSync(corePluginsDist, { recursive: true });
mkdirSync(gcphcpPluginsDist, { recursive: true });
mkdirSync(kindPluginsDist, { recursive: true });
const distWatcher = new Watchpack({ aggregateTimeout: 1500 });
distWatcher.watch({ directories: [guiDist, corePluginsDist, gcphcpPluginsDist, kindPluginsDist] });
distWatcher.on("aggregated", merge);

process.on("SIGINT", () => {
  corePluginsWatch.kill();
  gcphcpPluginsWatch.kill();
  kindPluginsWatch.kill();
  guiWatch.kill();
  configWatcher.close();
  distWatcher.close();
  process.exit(0);
});
