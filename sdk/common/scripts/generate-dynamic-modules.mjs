/**
 * Parse common/src/index.ts and generate:
 *   1. dist/common-modules.json   — export-name → source-file mapping
 *   2. dist/dynamic/<file>/       — per-file entry points for MF sharing
 *
 * Mirrors the pattern PatternFly uses with dist/dynamic/ and
 * dynamic-modules.json — every named export gets a deterministic
 * deep-import path so SWC transformImport can split barrel imports.
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
  /export\s+(?:type\s+)?(?:\{([^}]+)\}|\*)\s+from\s+["']\.\/([^"']+)["']/g;

const moduleMap = {};
let match;
while ((match = reExport.exec(src)) !== null) {
  const [, names, rawFile] = match;
  // Normalize: strip .js/.ts extension, keep path relative
  const file = rawFile.replace(/\.(js|ts|tsx)$/, "");

  if (!names) {
    // export * — we can't enumerate at parse time, skip
    // These modules will need manual entries or a runtime fallback
    continue;
  }

  for (const raw of names.split(",")) {
    const trimmed = raw.trim();
    if (!trimmed) continue;

    // Handle "default as Foo" → export name is "Foo"
    const aliasMatch = trimmed.match(/(?:default|(\w+))\s+as\s+(\w+)/);
    const exportName = aliasMatch ? aliasMatch[2] : trimmed;

    // Skip type-only re-exports — they don't produce runtime modules
    // (the regex already matches `export type { ... }` lines but
    //  those names are fine to include: transform will rewrite them
    //  and the deep path just needs to resolve. Including types in the
    //  map is harmless and keeps the correction plugin accurate.)
    moduleMap[exportName] = `dist/dynamic/${file}`;
  }
}

// Handle `export *` modules by parsing their files for export names
const starExportRe =
  /export\s+\*\s+from\s+["']\.\/([^"']+)["']/g;
let starMatch;
const srcCopy = src;
while ((starMatch = starExportRe.exec(srcCopy)) !== null) {
  const rawFile = starMatch[1];
  const file = rawFile.replace(/\.(js|ts|tsx)$/, "");
  const filePath = path.resolve(root, "src", rawFile.replace(/\.js$/, ".ts"));

  if (!fs.existsSync(filePath)) continue;

  const fileSrc = fs.readFileSync(filePath, "utf-8");
  // Match named exports: export const/function/class/type/interface/enum Name
  const namedExportRe =
    /export\s+(?:const|let|var|function|class|type|interface|enum)\s+(\w+)/g;
  let namedMatch;
  while ((namedMatch = namedExportRe.exec(fileSrc)) !== null) {
    moduleMap[namedMatch[1]] = `dist/dynamic/${file}`;
  }
  // Match re-exports from within: export { Name } from ...
  const innerReExportRe = /export\s+(?:type\s+)?\{([^}]+)\}/g;
  let innerMatch;
  while ((innerMatch = innerReExportRe.exec(fileSrc)) !== null) {
    for (const raw of innerMatch[1].split(",")) {
      const trimmed = raw.trim();
      if (!trimmed) continue;
      const aliasMatch = trimmed.match(/\w+\s+as\s+(\w+)/);
      const name = aliasMatch ? aliasMatch[1] : trimmed;
      moduleMap[name] = `dist/dynamic/${file}`;
    }
  }
}

// Write common-modules.json
fs.mkdirSync(distDir, { recursive: true });
fs.writeFileSync(
  path.resolve(distDir, "common-modules.json"),
  JSON.stringify(moduleMap, null, 2) + "\n",
);

// Generate dist/dynamic/<file>/ entry points
const uniqueFiles = [...new Set(Object.values(moduleMap))];
for (const dynamicPath of uniqueFiles) {
  const file = dynamicPath.replace("dist/dynamic/", "");
  const dir = path.resolve(dynamicDir, file);
  fs.mkdirSync(dir, { recursive: true });

  // ESM index re-exporting from the built ESM output
  const depth = file.split("/").length;
  const upPrefix = "../".repeat(depth + 1); // dynamic/<file>/ → dist/esm/
  fs.writeFileSync(
    path.resolve(dir, "index.js"),
    `export * from "${upPrefix}esm/${file}.js";\n`,
  );

  // package.json so MF shared resolution finds the module
  fs.writeFileSync(
    path.resolve(dir, "package.json"),
    JSON.stringify({ module: "./index.js", main: "./index.js" }, null, 2) +
      "\n",
  );
}

const count = Object.keys(moduleMap).length;
const files = uniqueFiles.length;
console.log(
  `@fleetshift/common: generated ${count} module entries across ${files} dynamic entry points`,
);
