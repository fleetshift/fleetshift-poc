/**
 * Parse common/src/index.ts and generate:
 *   1. dist/common-modules.json — export-name to source-module metadata
 *   2. virtual dist/dynamic/<file> package descriptors
 *
 * Re-export graphs are followed recursively so each named export points at
 * its actual compiled source file instead of a barrel index.
 */
import fs from "fs";
import path from "path";
import * as ts from "typescript";
import { fileURLToPath } from "url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sourceRoot = path.resolve(root, "src");
const indexPath = path.resolve(sourceRoot, "index.ts");
const distDir = path.resolve(root, "dist");
const dynamicDir = path.resolve(distDir, "dynamic");

function normalizeEsmImports(directory) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const filePath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      normalizeEsmImports(filePath);
      continue;
    }
    if (!entry.name.endsWith(".js")) continue;
    const source = fs.readFileSync(filePath, "utf8");
    const normalized = source.replace(
      /(from\s+["']|import\s*\(\s*["'])(\.{1,2}\/[^"']+)(["'])/g,
      (match, prefix, specifier, suffix) => {
        if (/\.(?:js|json)$/.test(specifier)) {
          return match;
        }
        const target = path.resolve(path.dirname(filePath), specifier);
        const targetSpecifier = fs.existsSync(path.join(target, "index.js"))
          ? `${specifier}/index.js`
          : `${specifier}.js`;
        return `${prefix}${targetSpecifier}${suffix}`;
      },
    );
    if (normalized !== source) fs.writeFileSync(filePath, normalized);
  }
}

/** @typedef {{ path: string, sourceExport: string, type: boolean }} ModuleEntry */

const moduleCache = new Map();

function resolveModule(fromFile, rawImport) {
  const raw = rawImport.replace(/\.(js|jsx|ts|tsx)$/, "");
  const base = path.resolve(path.dirname(fromFile), raw);
  for (const candidate of [
    base,
    `${base}.ts`,
    `${base}.tsx`,
    path.join(base, "index.ts"),
  ]) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return undefined;
}

function parseModule(filePath, stack = new Set()) {
  const cached = moduleCache.get(filePath);
  if (cached) return cached;
  if (stack.has(filePath)) return new Map();

  const nextStack = new Set(stack).add(filePath);
  const sourceFile = ts.createSourceFile(
    filePath,
    fs.readFileSync(filePath, "utf8"),
    ts.ScriptTarget.Latest,
    true,
  );
  const entries = new Map();
  const relative = path
    .relative(sourceRoot, filePath)
    .replace(/\.(js|jsx|ts|tsx)$/, "");
  const dynamicPath = `dist/dynamic/${relative}`;

  const addLocal = (name, type) => {
    entries.set(name, { path: dynamicPath, sourceExport: name, type });
  };

  for (const statement of sourceFile.statements) {
    const hasModifier = (kind) =>
      statement.modifiers?.some((modifier) => modifier.kind === kind) ?? false;

    if (ts.isExportDeclaration(statement)) {
      const moduleSpecifier = statement.moduleSpecifier;
      if (!moduleSpecifier || !ts.isStringLiteral(moduleSpecifier)) continue;
      const targetPath = resolveModule(filePath, moduleSpecifier.text);
      if (!targetPath) continue;
      const target = parseModule(targetPath, nextStack);
      if (!statement.exportClause) {
        for (const [name, entry] of target) {
          if (name !== "default" && !entries.has(name))
            entries.set(name, entry);
        }
        continue;
      }
      if (!ts.isNamedExports(statement.exportClause)) continue;
      for (const specifier of statement.exportClause.elements) {
        const sourceName = specifier.propertyName?.text ?? specifier.name.text;
        const targetEntry = target.get(sourceName);
        if (!targetEntry) continue;
        entries.set(specifier.name.text, {
          ...targetEntry,
          type:
            statement.isTypeOnly || specifier.isTypeOnly || targetEntry.type,
        });
      }
      continue;
    }

    if (ts.isExportAssignment(statement) && !statement.isExportEquals) {
      entries.set("default", {
        path: dynamicPath,
        sourceExport: "default",
        type: false,
      });
      continue;
    }

    if (!hasModifier(ts.SyntaxKind.ExportKeyword)) continue;
    const isType =
      ts.isTypeAliasDeclaration(statement) ||
      ts.isInterfaceDeclaration(statement);
    if (ts.isVariableStatement(statement)) {
      for (const declaration of statement.declarationList.declarations) {
        if (ts.isIdentifier(declaration.name))
          addLocal(declaration.name.text, false);
      }
    } else if (
      "name" in statement &&
      statement.name &&
      ts.isIdentifier(statement.name)
    ) {
      addLocal(statement.name.text, isType);
    }
    if (hasModifier(ts.SyntaxKind.DefaultKeyword)) {
      entries.set("default", {
        path: dynamicPath,
        sourceExport: "default",
        type: false,
      });
    }
  }

  moduleCache.set(filePath, entries);
  return entries;
}

const moduleMap = Object.fromEntries(parseModule(indexPath));

fs.mkdirSync(distDir, { recursive: true });
fs.rmSync(dynamicDir, { recursive: true, force: true });
fs.writeFileSync(
  path.resolve(distDir, "common-modules.json"),
  JSON.stringify(moduleMap, null, 2) + "\n",
);

function compiledFiles(directory, prefix = "") {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const relative = path.join(prefix, entry.name);
    if (entry.isDirectory()) {
      return compiledFiles(path.join(directory, entry.name), relative);
    }
    return entry.name.endsWith(".js") ? [relative.slice(0, -3)] : [];
  });
}

const compiledRoot = path.resolve(distDir, "esm");
normalizeEsmImports(compiledRoot);
const compiledModules = compiledFiles(compiledRoot);

function relativeCompiledPath(dir, format, file) {
  let relative = path.relative(
    dir,
    path.resolve(distDir, format, `${file}.js`),
  );
  if (!relative.startsWith(".")) relative = `./${relative}`;
  return relative.split(path.sep).join("/");
}

function writeDescriptor(dir, file) {
  fs.mkdirSync(dir, { recursive: true });
  const esmPath = relativeCompiledPath(dir, "esm", file);
  const cjsPath = relativeCompiledPath(dir, "cjs", file);
  const typePath = esmPath.replace(/\.js$/, ".d.ts");
  fs.writeFileSync(
    path.resolve(dir, "package.json"),
    JSON.stringify(
      { module: esmPath, main: cjsPath, types: typePath },
      null,
      2,
    ) + "\n",
  );
}

for (const file of compiledModules) {
  writeDescriptor(path.resolve(dynamicDir, file), file);
}

console.log(
  `@fleetshift/common: generated ${Object.keys(moduleMap).length} module entries across ${compiledModules.length} compiled files`,
);
