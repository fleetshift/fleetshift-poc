import { readdirSync, readFileSync, writeFileSync } from "fs";
import { resolve, dirname, relative, sep } from "path";
import { fileURLToPath } from "url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const distDir = resolve(root, "packages/mock-ui-plugins/dist");
const outputDir = process.argv[2] ? resolve(process.argv[2]) : distDir;

const pluginMeta = [
  { name: "management-plugin", key: "management", label: "Management", persona: "ops" },
  { name: "day-one-plugin", key: "day-one", label: "Day One", persona: "ops" },
  { name: "core-plugin", key: "core", label: "Core Plugin", persona: "ops" },
  { name: "signing-plugin", key: "signing", label: "Signing Keys", persona: "ops" },
  { name: "routing-plugin", key: "routing", label: "Routing", persona: "ops" },
  { name: "gcphcp-plugin", key: "gcphcp", label: "GCP HCP", persona: "ops" },
];

const metaByName = new Map(pluginMeta.map((p) => [p.name, p]));

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

const registry = { assetsHost: "", plugins: {} };
const manifests = findManifests(distDir, distDir);
if (manifests.length === 0) {
  throw new Error(`No plugin manifests found under ${distDir}`);
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
    manifestPath: "/" + rel.split(sep).join("/"),
    pluginManifest: manifest,
  };
}

const registryPath = resolve(outputDir, "plugin-registry.json");
writeFileSync(registryPath, JSON.stringify(registry, null, 2));
console.log(`Plugin registry written (${Object.keys(registry.plugins).length} plugins) → ${registryPath}`);
