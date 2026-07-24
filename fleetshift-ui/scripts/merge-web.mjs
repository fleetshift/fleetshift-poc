import { cpSync, rmSync, mkdirSync, readdirSync, existsSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { execFileSync } from "child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const webDir = resolve(root, "web");
const pluginsDist = resolve(root, "packages/mock-ui-plugins/dist");
const guiDist = resolve(root, "packages/gui/dist");
const incremental = process.argv.includes("--incremental");

mkdirSync(webDir, { recursive: true });
if (!incremental) {
  // Clear contents without removing the directory itself.
  // Removing the dir breaks Podman virtiofs bind mounts (stale inode).
  for (const entry of readdirSync(webDir)) {
    rmSync(resolve(webDir, entry), { recursive: true, force: true });
  }
}

const hasPluginManifests =
  existsSync(pluginsDist) &&
  readdirSync(pluginsDist, { recursive: true }).some((p) =>
    String(p).endsWith("-manifest.json"),
  );

if (hasPluginManifests) {
  cpSync(pluginsDist, webDir, { recursive: true, force: true });
  // Generate registry into web/ directly (not dist/) to avoid re-triggering the watcher
  execFileSync("node", [resolve(root, "scripts/generate-plugin-registry.mjs"), webDir], {
    cwd: root,
    stdio: "inherit",
  });
}

// Copy GUI shell last so index.html is always present
if (existsSync(guiDist)) {
  cpSync(guiDist, webDir, { recursive: true, force: true });
}

console.log("Merged GUI + plugin assets into web/");
