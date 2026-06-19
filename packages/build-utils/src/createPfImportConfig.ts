import type { RspackPluginInstance } from "@rspack/core";
import fs from "fs";
import path from "path";

interface TransformImportEntry {
  libraryName: string;
  libraryDirectory?: string;
  customName?: string;
  camelToDashComponentName?: boolean;
  transformToDefaultImport?: boolean;
}

function loadDynamicModulesMap(
  nodeModulesRoot: string,
): Record<string, string> {
  const mapPath = path.resolve(
    nodeModulesRoot,
    "node_modules/@patternfly/react-core/dist/dynamic-modules.json",
  );
  return JSON.parse(fs.readFileSync(mapPath, "utf-8"));
}

/**
 * Returns `transformImport` entries for `builtin:swc-loader`.
 *
 * - PF core: naive `components/{{ member }}` template — corrected at
 *   resolution time by `createPfModuleReplacementPlugin`.
 * - PF icons: `kebabCase` template handles the standard naming.
 */
export function createPfTransformImport(): TransformImportEntry[] {
  return [
    {
      libraryName: "@patternfly/react-core",
      customName: "@patternfly/react-core/dist/dynamic/components/{{ member }}",
      transformToDefaultImport: false,
    },
    {
      libraryName: "@patternfly/react-icons",
      customName:
        "@patternfly/react-icons/dist/dynamic/icons/{{ kebabCase member }}",
      transformToDefaultImport: true,
    },
  ];
}

/**
 * Creates an rspack plugin that corrects the naive component paths
 * produced by `createPfTransformImport` using PF's `dynamic-modules.json`.
 *
 * The transform emits paths like:
 *   `@patternfly/react-core/dist/dynamic/components/ActionGroup`
 * but `ActionGroup` actually lives in `dist/dynamic/components/Form`.
 * This plugin intercepts those requests and rewrites them to the correct path.
 */
export function createPfModuleReplacementPlugin(
  monorepoRoot: string,
): RspackPluginInstance {
  const moduleMap = loadDynamicModulesMap(monorepoRoot);

  // Build a correction map: naivePath → actualPath
  // Only entries where the member name differs from the directory name
  const corrections = new Map<string, string>();
  for (const [exportName, dynamicPath] of Object.entries(moduleMap)) {
    const naivePath = `@patternfly/react-core/dist/dynamic/components/${exportName}`;
    const actualPath = `@patternfly/react-core/${dynamicPath}`;
    if (naivePath !== actualPath) {
      corrections.set(naivePath, actualPath);
    }
  }

  return {
    apply(compiler) {
      compiler.hooks.normalModuleFactory.tap(
        "PfModuleReplacementPlugin",
        (nmf) => {
          nmf.hooks.beforeResolve.tap("PfModuleReplacementPlugin", (result) => {
            if (!result) return;
            const corrected = corrections.get(result.request);
            if (corrected) {
              result.request = corrected;
            }
          });
        },
      );
    },
  };
}
