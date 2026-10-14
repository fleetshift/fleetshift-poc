/**
 * Parse common/src/index.ts and generate:
 *   1. dist/common-modules.json   — export-name → { path, sourceExport, type }
 *   2. dist/dynamic/<file>/       — per-file entry points for MF sharing
 *
 * Mirrors the pattern PatternFly uses with dist/dynamic/ and
 * dynamic-modules.json — every named export gets a deterministic
 * deep-import path so SWC transformImport can split barrel imports.
 *
 * Each dynamic entry uses explicit named re-exports (not `export *`)
 * because rspack's module-federation shared-module wrapping can drop
 * named exports that come through `export *`.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const indexPath = path.resolve(root, "src/index.ts");
const distDir = path.resolve(root, "dist");
const dynamicDir = path.resolve(distDir, "dynamic");

const src = fs.readFileSync(indexPath, "utf-8");

// Match: export { Name1, Name2 } from "./file.js";
// Match: export { default as Name } from "./file.js";
// Match: export * from "./file.js";
// Match: export type { ... } from "./file.js";
const reExport =
  /export\s+(type\s+)?(?:\{([^}]+)\}|\*)\s+from\s+["']\.\/([^"']+)["']/g;

/** @type {Record<string, { path: string, sourceExport: string, type: boolean }>} */
const moduleMap = {};
let match;
while ((match = reExport.exec(src)) !== null) {
  const [, typeKeyword, names, rawFile] = match;
  const file = rawFile.replace(/\.(js|ts|tsx)$/, "");
  const isTypeExport = !!typeKeyword;

  if (!names) {
    // export * — handled below by parsing the source file
    continue;
  }

  for (const raw of names.split(",")) {
    let trimmed = raw.trim();
    if (!trimmed) continue;

    // Handle inline type specifiers: export { type Foo, type Bar as Baz }
    const isInlineType = trimmed.startsWith("type ");
    if (isInlineType) trimmed = trimmed.slice(5);

    // Handle "default as Foo" → export name is "Foo", source export is "default"
    const aliasMatch = trimmed.match(/(?:(default|\w+))\s+as\s+(\w+)/);
    const exportName = aliasMatch ? aliasMatch[2] : trimmed;
    const sourceExport = aliasMatch ? aliasMatch[1] : trimmed;

    moduleMap[exportName] = {
      path: `dist/dynamic/${file}`,
      sourceExport,
      type: isTypeExport || isInlineType,
    };
  }
}

// Handle `export *` modules by parsing their files for export names
const starExportRe = /export\s+\*\s+from\s+["']\.\/([^"']+)["']/g;
let starMatch;
const srcCopy = src;
while ((starMatch = starExportRe.exec(srcCopy)) !== null) {
  const rawFile = starMatch[1];
  const file = rawFile.replace(/\.(js|ts|tsx)$/, "");
  const filePath = path.resolve(root, "src", rawFile.replace(/\.js$/, ".ts"));

  if (!fs.existsSync(filePath)) continue;

  const fileSrc = fs.readFileSync(filePath, "utf-8");

  // Match named exports: export const/let/var/function/class Name
  const namedExportRe =
    /export\s+(?:const|let|var|function|class)\s+(\w+)/g;
  let namedMatch;
  while ((namedMatch = namedExportRe.exec(fileSrc)) !== null) {
    moduleMap[namedMatch[1]] = {
      path: `dist/dynamic/${file}`,
      sourceExport: namedMatch[1],
      type: false,
    };
  }

  // Match type/interface/enum exports
  const typeExportRe =
    /export\s+(?:type|interface|enum)\s+(\w+)/g;
  let typeMatch;
  while ((typeMatch = typeExportRe.exec(fileSrc)) !== null) {
    moduleMap[typeMatch[1]] = {
      path: `dist/dynamic/${file}`,
      sourceExport: typeMatch[1],
      // enum is a value at runtime, type/interface are not
      type: !typeMatch[0].includes("enum"),
    };
  }

  // Match re-exports from within: export { Name } from ...
  const innerReExportRe = /export\s+(type\s+)?\{([^}]+)\}/g;
  let innerMatch;
  while ((innerMatch = innerReExportRe.exec(fileSrc)) !== null) {
    const innerType = !!innerMatch[1];
    for (const raw of innerMatch[2].split(",")) {
      let trimmed = raw.trim();
      if (!trimmed) continue;

      const isInlineType = trimmed.startsWith("type ");
      if (isInlineType) trimmed = trimmed.slice(5);

      const aliasMatch = trimmed.match(/(\w+)\s+as\s+(\w+)/);
      const name = aliasMatch ? aliasMatch[2] : trimmed;
      const source = aliasMatch ? aliasMatch[1] : trimmed;
      moduleMap[name] = {
        path: `dist/dynamic/${file}`,
        sourceExport: source,
        type: innerType || isInlineType,
      };
    }
  }
}

// Write common-modules.json
fs.mkdirSync(distDir, { recursive: true });
fs.writeFileSync(
  path.resolve(distDir, "common-modules.json"),
  JSON.stringify(moduleMap, null, 2) + "\n",
);

// Generate dist/dynamic/<file>/ entry points with explicit re-exports
// Group exports by target file path
/** @type {Map<string, Array<{ exportName: string, sourceExport: string, type: boolean }>>} */
const byFile = new Map();
for (const [exportName, entry] of Object.entries(moduleMap)) {
  const list = byFile.get(entry.path) ?? [];
  list.push({ exportName, sourceExport: entry.sourceExport, type: entry.type });
  byFile.set(entry.path, list);
}

for (const [dynamicPath, exports] of byFile) {
  const file = dynamicPath.replace("dist/dynamic/", "");
  const dir = path.resolve(dynamicDir, file);
  fs.mkdirSync(dir, { recursive: true });

  const depth = file.split("/").length;
  const upPrefix = "../".repeat(depth + 1); // dynamic/<file>/ → dist/esm/
  const esmPath = `${upPrefix}esm/${file}.js`;

  // Build explicit re-export statements (skip type-only exports)
  const runtimeExports = exports.filter((e) => !e.type);

  const reExports = runtimeExports.map((e) => {
    if (e.sourceExport === e.exportName) {
      return e.exportName;
    }
    return `${e.sourceExport} as ${e.exportName}`;
  });

  const lines = [];
  if (reExports.length > 0) {
    lines.push(`export { ${reExports.join(", ")} } from "${esmPath}";`);
  }

  fs.writeFileSync(path.resolve(dir, "index.js"), lines.join("\n") + "\n");

  // package.json so MF shared resolution finds the module
  fs.writeFileSync(
    path.resolve(dir, "package.json"),
    JSON.stringify({ module: "./index.js", main: "./index.js" }, null, 2) +
      "\n",
  );
}

const count = Object.keys(moduleMap).length;
const files = byFile.size;
console.log(
  `@fleetshift/common: generated ${count} module entries across ${files} dynamic entry points`,
);
