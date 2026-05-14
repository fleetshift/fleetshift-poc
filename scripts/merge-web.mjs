import { cpSync, rmSync, mkdirSync, readdirSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const webDir = resolve(root, "web");

// Clear contents without removing the directory itself.
// Removing the dir breaks Podman virtiofs bind mounts (stale inode).
mkdirSync(webDir, { recursive: true });
for (const entry of readdirSync(webDir)) {
  rmSync(resolve(webDir, entry), { recursive: true, force: true });
}

// Copy GUI shell (index.html, main.js, CSS, fonts, chunks)
cpSync(resolve(root, "packages/gui/dist"), webDir, { recursive: true });

// Merge plugin assets (manifests, entry scripts, plugin-registry.json)
cpSync(resolve(root, "packages/mock-ui-plugins/dist"), webDir, {
  recursive: true,
  force: true,
});

console.log("Merged GUI + plugin assets into web/");
