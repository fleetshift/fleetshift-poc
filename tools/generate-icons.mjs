#!/usr/bin/env node
/**
 * Build-time script: reads @patternfly/react-icons package exports and
 * generates a JSON manifest (name + kebab-case file stem + keywords).
 *
 * Output: common/src/generated/pf-icons.json
 * Run:    npm run generate:icons
 */

import { mkdirSync, readdirSync, writeFileSync } from "fs";
import { dirname, join, resolve } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const monorepoRoot = resolve(__dirname, "..");

import { createRequire } from "module";
const require = createRequire(import.meta.url);
const pfIconsPkg = dirname(require.resolve("@patternfly/react-icons/package.json"));
const iconsDir = join(pfIconsPkg, "dist/esm/icons");

/** Convert kebab-case file stem to PascalCase component name. */
function toPascalCase(kebab) {
  return kebab
    .split("-")
    .map((s) => s.charAt(0).toUpperCase() + s.slice(1))
    .join("");
}

/** Derive search keywords from the kebab-case stem. */
function deriveKeywords(stem) {
  // Remove trailing "-icon" suffix, split remaining parts
  const base = stem.replace(/-icon$/, "");
  return base.split("-").filter((w) => w.length > 0);
}

const files = readdirSync(iconsDir).filter(
  (f) => f.endsWith(".js") && !f.endsWith(".d.ts"),
);

const manifest = [];

for (const file of files) {
  const stem = file.replace(/\.js$/, "");
  const name = toPascalCase(stem);

  // Skip the createIcon utility
  if (name === "CreateIcon") continue;

  // Just the component name — keywords derived at runtime from name parts
  manifest.push(name);
}

// Sort alphabetically by name for deterministic output
manifest.sort((a, b) => a.localeCompare(b));

const outPath = join(
  monorepoRoot,
  "sdk/common/src/generated/pf-icons.json",
);

mkdirSync(dirname(outPath), { recursive: true });
writeFileSync(outPath, JSON.stringify(manifest) + "\n");

console.log(`Generated ${manifest.length} icons → ${outPath}`);
