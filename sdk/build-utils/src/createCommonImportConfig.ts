import type { RspackPluginInstance } from "@rspack/core";
import fs from "fs";
import { createRequire } from "module";
import path from "path";

interface TransformImportEntry {
  libraryName: string;
  libraryDirectory?: string;
  customName?: string;
  camelToDashComponentName?: boolean;
  transformToDefaultImport?: boolean;
}

interface CommonModuleEntry {
  path: string;
  sourceExport: string;
  type: boolean;
}

function loadCommonModulesMap(): Record<string, CommonModuleEntry> {
  const req = createRequire(import.meta.url);
  const commonPkgDir = path.dirname(
    req.resolve("@fleetshift/common/package.json"),
  );
  const mapPath = path.resolve(commonPkgDir, "dist/common-modules.json");
  return JSON.parse(fs.readFileSync(mapPath, "utf-8"));
}

/**
 * Returns a `transformImport` entry for `builtin:swc-loader` that
 * rewrites barrel imports from `@fleetshift/common` into per-file
 * deep imports under `dist/dynamic/`.
 *
 * The naive `{{ member }}` template is corrected at resolution time
 * by {@link createCommonModuleReplacementPlugin}.
 */
export function createCommonTransformImport(): TransformImportEntry {
  return {
    libraryName: "@fleetshift/common",
    customName: "@fleetshift/common/dist/dynamic/{{ member }}",
    transformToDefaultImport: false,
  };
}

/**
 * Creates an rspack plugin that corrects the naive member-name paths
 * produced by {@link createCommonTransformImport} using the generated
 * `common-modules.json`.
 *
 * The transform emits paths like:
 *   `@fleetshift/common/dist/dynamic/loadPfIcon`
 * but `loadPfIcon` lives in `dist/dynamic/pfIconLoader`.
 * This plugin intercepts those requests and rewrites them.
 */
export function createCommonModuleReplacementPlugin(): RspackPluginInstance {
  const moduleMap = loadCommonModulesMap();

  const corrections = new Map<string, string>();
  for (const [exportName, entry] of Object.entries(moduleMap)) {
    const naivePath = `@fleetshift/common/dist/dynamic/${exportName}`;
    const actualPath = `@fleetshift/common/${entry.path}`;
    if (naivePath !== actualPath) {
      corrections.set(naivePath, actualPath);
    }
  }

  return {
    apply(compiler) {
      compiler.hooks.normalModuleFactory.tap(
        "CommonModuleReplacementPlugin",
        (nmf) => {
          nmf.hooks.beforeResolve.tap(
            "CommonModuleReplacementPlugin",
            (result) => {
              if (!result) return;
              const corrected = corrections.get(result.request);
              if (corrected) {
                result.request = corrected;
              }
            },
          );
        },
      );
    },
  };
}
