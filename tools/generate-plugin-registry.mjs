import { readdirSync, readFileSync, writeFileSync } from "fs";
import { resolve, dirname, relative, sep } from "path";
import { fileURLToPath } from "url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const distDirs = [
  resolve(root, "extensions/core/client/dist"),
  resolve(root, "extensions/gcphcp/client/dist"),
  resolve(root, "extensions/kind/client/dist"),
];
const outputDir = process.argv[2] ? resolve(process.argv[2]) : distDirs[0];

const pluginMeta = [
  { name: "management-plugin", key: "management", label: "Management", persona: "ops" },
  { name: "core-plugin", key: "core", label: "Core Plugin", persona: "ops" },
  { name: "kind-plugin", key: "kind", label: "Kind", persona: "ops" },
  { name: "signing-plugin", key: "signing", label: "Signing Keys", persona: "ops" },
  { name: "routing-plugin", key: "routing", label: "Routing", persona: "ops" },
  { name: "gcphcp-plugin", key: "gcphcp", label: "GCP HCP", persona: "ops" },
  { name: "setup-plugin", key: "setup", label: "Setup", persona: "ops" },
  { name: "overview-plugin", key: "overview", label: "Overview", persona: "ops" },
  { name: "configuration-plugin", key: "configuration", label: "Configuration", persona: "obs" },
  { name: "virtualization-plugin", key: "virtualization", label: "Virtualization", persona: "obs" },
  { name: "addon-demo-plugin", key: "addon-demo", label: "Addon Demo", persona: "obs" },
  { name: "settings-plugin", key: "settings", label: "Settings", persona: "obs" }
];

const metaByName = new Map(pluginMeta.map((p) => [p.name, p]));

const APP_BASENAME = "/app";

function prefixPluginManifest(manifest) {
  const out = { ...manifest };
  const base = out.baseURL;
  if (!base || base === "/" || base === "auto") {
    out.baseURL = APP_BASENAME + "/";
  } else if (
    typeof base === "string" &&
    base.startsWith("/") &&
    base !== APP_BASENAME &&
    !base.startsWith(APP_BASENAME + "/")
  ) {
    out.baseURL = APP_BASENAME + base;
  }
  if (Array.isArray(out.loadScripts)) {
    out.loadScripts = out.loadScripts.map((s) => {
      if (
        typeof s === "string" &&
        s.startsWith("/") &&
        s !== APP_BASENAME &&
        !s.startsWith(APP_BASENAME + "/")
      ) {
        return APP_BASENAME + s;
      }
      return s;
    });
  }
  return out;
}

function findManifests(dir, base) {
  const results = [];
  let entries;
  try {
    entries = readdirSync(dir, { withFileTypes: true });
  } catch {
    return results;
  }
  for (const entry of entries) {
    const full = resolve(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...findManifests(full, base));
    } else if (entry.name.endsWith("-manifest.json")) {
      results.push({ path: full, rel: relative(base, full) });
    }
  }
  return results;
}

const registry = { assetsHost: "/app", plugins: {} };
const manifests = distDirs.flatMap((dir) => findManifests(dir, dir));
if (manifests.length === 0) {
  throw new Error(`No plugin manifests found under ${distDirs.join(", ")}`);
}

for (const { path: filePath, rel } of manifests) {
  let manifest;
  try {
    manifest = JSON.parse(readFileSync(filePath, "utf-8"));
  } catch (err) {
    throw new Error(`Invalid manifest JSON at ${filePath}: ${err.message}`);
  }

  const name = manifest.name;
  if (!name || !Array.isArray(manifest.loadScripts)) continue;

  const meta = metaByName.get(name);
  if (!meta) continue;

  registry.plugins[name] = {
    name: meta.name,
    key: meta.key,
    label: meta.label,
    persona: meta.persona,
    manifestPath: APP_BASENAME + "/" + rel.split(sep).join("/"),
    pluginManifest: prefixPluginManifest(manifest),
  };
}

const registryPath = resolve(outputDir, "plugin-registry.json");
writeFileSync(registryPath, JSON.stringify(registry, null, 2));
console.log(`Plugin registry written (${Object.keys(registry.plugins).length} plugins) → ${registryPath}`);
