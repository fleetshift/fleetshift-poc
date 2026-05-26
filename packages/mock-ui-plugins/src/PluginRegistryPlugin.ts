import fs from "fs";
import path from "path";
import type { Compiler } from "webpack";

interface PluginMeta {
  name: string;
  key: string;
  label: string;
  persona: "ops" | "dev";
}

interface PluginRegistryPluginOptions {
  assetsHost: string;
  plugins: PluginMeta[];
}

function findManifests(
  dir: string,
  base: string,
): { path: string; rel: string }[] {
  const results: { path: string; rel: string }[] = [];
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return results;
  }
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...findManifests(full, base));
    } else if (entry.name.endsWith("-manifest.json")) {
      results.push({ path: full, rel: path.relative(base, full) });
    }
  }
  return results;
}

export class PluginRegistryPlugin {
  private options: PluginRegistryPluginOptions;

  constructor(options: PluginRegistryPluginOptions) {
    this.options = options;
  }

  apply(compiler: Compiler) {
    compiler.hooks.afterEmit.tapAsync(
      "PluginRegistryPlugin",
      (compilation, callback) => {
        const outputPath = compilation.outputOptions.path;
        if (!outputPath) {
          callback();
          return;
        }

        const metaByName = new Map(
          this.options.plugins.map((p) => [p.name, p]),
        );

        const registry: Record<string, unknown> = {
          assetsHost: this.options.assetsHost,
          plugins: {} as Record<string, unknown>,
        };

        const plugins = registry.plugins as Record<string, unknown>;
        const manifests = findManifests(outputPath, outputPath);

        for (const { path: filePath, rel } of manifests) {
          let manifest: Record<string, unknown>;
          try {
            manifest = JSON.parse(fs.readFileSync(filePath, "utf-8"));
          } catch {
            continue;
          }

          const name = manifest.name as string | undefined;
          if (!name) continue;

          // Skip non-SDK manifests (e.g. mf-manifest.json from Module Federation)
          if (!Array.isArray(manifest.loadScripts)) continue;

          const meta = metaByName.get(name);
          if (!meta) continue;

          plugins[name] = {
            name: meta.name,
            key: meta.key,
            label: meta.label,
            persona: meta.persona,
            manifestPath: "/" + rel.split(path.sep).join("/"),
            pluginManifest: manifest,
          };
        }

        const registryPath = path.join(outputPath, "plugin-registry.json");
        try {
          fs.writeFileSync(registryPath, JSON.stringify(registry, null, 2));
        } catch (err) {
          console.error("PluginRegistryPlugin: failed to write registry", err);
        }

        callback();
      },
    );
  }
}
