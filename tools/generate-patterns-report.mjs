#!/usr/bin/env node
/**
 * Generates a Markdown report from resource-patterns.json.
 *
 * Usage: node scripts/generate-patterns-report.mjs [input] [output]
 *   input  — path to patterns JSON (default: resource-patterns.json)
 *   output — path to write MD report (default: resource-patterns-report.md)
 */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const input = resolve(process.argv[2] ?? "resource-patterns.json");
const output = resolve(process.argv[3] ?? "resource-patterns-report.md");

const d = JSON.parse(readFileSync(input, "utf8"));
const lines = [];
const w = (s = "") => lines.push(s);

// ── Header ───────────────────────────────────────────────────────────

w("# Resource Patterns Report");
w();
w(`> Auto-generated from \`resource-patterns.json\` — **${d.totalCount}** resources analyzed.`);
w();

// ── Resource Type Summary ────────────────────────────────────────────

w("## Resource Types");
w();
w("| Type | Count | Name Pattern |");
w("|------|------:|--------------|");
for (const [rt, info] of Object.entries(d.resourceTypes)) {
  w(`| \`${rt}\` | ${info.count} | \`${info.namePattern}\` |`);
}
w();

// ── Per-type details ─────────────────────────────────────────────────

for (const [rt, info] of Object.entries(d.resourceTypes)) {
  w(`### ${rt}`);
  w();
  w(`**Count:** ${info.count}`);
  w();

  if (info.nameExamples?.length) {
    w("**Name examples:**");
    for (const ex of info.nameExamples.slice(0, 3)) {
      w(`- \`${ex}\``);
    }
    w();
  }

  // resource shape — only show fields with distinctValues or interesting types
  const shapeEntries = Object.entries(info.resourceShape ?? {});
  if (shapeEntries.length > 0) {
    w("**Fields:**");
    w();
    w("| Path | Type | Prevalence | Distinct Values |");
    w("|------|------|------------|-----------------|");
    for (const [path, field] of shapeEntries) {
      let vals = "";
      if (field.distinctValues && field.distinctValues.length <= 10) {
        vals = field.distinctValues.map((v) => `\`${v}\``).join(", ");
      } else if (field.distinctCount) {
        vals = `${field.distinctCount} values`;
      }
      w(`| \`${path}\` | ${field.type} | ${field.prevalence} | ${vals} |`);
    }
    w();
  }

  if (info.conditions?.length) {
    w(`**Conditions:** ${info.conditions.map((c) => `\`${c}\``).join(", ")}`);
    w();
  }
}

// ── K8s Kind Distribution ────────────────────────────────────────────

const kinds = d.crossTypePatterns?.observationKindDistribution;
if (kinds) {
  w("## Kubernetes Object Kinds");
  w();
  const entries = Object.entries(kinds);
  w(`**${entries.length} distinct kinds** across ${d.resourceTypes["kubernetes.fleetshift.io/Object"]?.count ?? "?"} objects.`);
  w();
  w("| Kind | Count |");
  w("|------|------:|");
  for (const [kind, count] of entries) {
    w(`| ${kind} | ${count} |`);
  }
  w();
}

// ── Namespace Distribution ───────────────────────────────────────────

const namespaces = d.crossTypePatterns?.namespaceDistribution;
if (namespaces) {
  const nsEntries = Object.entries(namespaces);
  w("## Namespaces");
  w();
  w(`**${nsEntries.length} namespaces.**`);
  w();
  w("| Namespace | Count |");
  w("|-----------|------:|");
  for (const [ns, count] of nsEntries) {
    w(`| \`${ns}\` | ${count} |`);
  }
  w();
}

// ── API Groups ───────────────────────────────────────────────────────

const apiGroups = d.crossTypePatterns?.apiGroupDistribution;
if (apiGroups) {
  const agEntries = Object.entries(apiGroups);
  w("## API Groups");
  w();
  w(`**${agEntries.length} API groups.**`);
  w();
  w("| Group | Count |");
  w("|-------|------:|");
  for (const [group, count] of agEntries) {
    w(`| \`${group}\` | ${count} |`);
  }
  w();
}

// ── Conditions ───────────────────────────────────────────────────────

const conditions = d.crossTypePatterns?.conditions;
if (conditions) {
  const condEntries = Object.entries(conditions);
  w("## Conditions");
  w();
  w(`**${condEntries.length} condition types** observed.`);
  w();
  w("| Condition | Statuses | Reasons |");
  w("|-----------|----------|---------|");
  for (const [name, info] of condEntries) {
    const statuses = info.statuses?.map((s) => `\`${s}\``).join(", ") ?? "";
    const reasons = info.reasons?.length
      ? info.reasons.length <= 5
        ? info.reasons.map((r) => `\`${r}\``).join(", ")
        : `${info.reasons.length} reasons`
      : "";
    w(`| ${name} | ${statuses} | ${reasons} |`);
  }
  w();
}

// ── Extracted Fields by Kind ─────────────────────────────────────────

const extracted = d.crossTypePatterns?.extractedFieldsByKind;
if (extracted) {
  w("## Extracted Fields by Kind");
  w();
  w("These are kind-specific fields surfaced by the backend into `resource.observation.extracted`.");
  w();

  for (const [kind, fields] of Object.entries(extracted)) {
    w(`### ${kind}`);
    w();
    w("| Field | Values |");
    w("|-------|--------|");
    for (const [field, vals] of Object.entries(fields)) {
      let display;
      if (Array.isArray(vals)) {
        display = vals.length <= 8
          ? vals.map((v) => `\`${v}\``).join(", ")
          : vals.slice(0, 6).map((v) => `\`${v}\``).join(", ") + `, ... (${vals.length} total)`;
      } else {
        display = `${vals.distinctCount} distinct values`;
      }
      w(`| \`${field}\` | ${display} |`);
    }
    w();
  }
}

// ── Filterable Dimensions ────────────────────────────────────────────

const dims = d.crossTypePatterns?.filterableDimensions;
if (dims) {
  w("## Filterable Dimensions");
  w();

  for (const [level, items] of Object.entries(dims)) {
    const label = level.replace(/([A-Z])/g, " $1").trim();
    w(`### ${label}`);
    w();
    w("| Field | Info |");
    w("|-------|------|");
    for (const item of items) {
      let info = "";
      if (item.values) info = item.values.map((v) => `\`${v}\``).join(", ");
      else if (item.distinctCount) info = `${item.distinctCount} distinct values`;
      if (item.description) info += info ? ` — ${item.description}` : item.description;
      if (item.forKinds) info += ` (${item.forKinds.join(", ")})`;
      w(`| \`${item.field}\` | ${info} |`);
    }
    w();
  }
}

// ── Label Keys ───────────────────────────────────────────────────────

const labelKeys = d.crossTypePatterns?.searchAutocompleteHints?.byLabelKey;
if (labelKeys?.length) {
  w("## Common Label Keys");
  w();
  w(`**${labelKeys.length} label keys** found. Top ones:`);
  w();
  for (const key of labelKeys.slice(0, 40)) {
    w(`- \`${key}\``);
  }
  if (labelKeys.length > 40) w(`- ... and ${labelKeys.length - 40} more`);
  w();
}

// ── Suggested CEL Filters ────────────────────────────────────────────

const celFilters = d.crossTypePatterns?.suggestedCelFilters;
if (celFilters?.length) {
  w("## Suggested CEL Filters");
  w();
  w("Ready-to-use filter templates for the advanced search.");
  w();
  w("| Expression | Description |");
  w("|------------|-------------|");
  for (const f of celFilters) {
    w(`| \`${f.expression}\` | ${f.description} |`);
  }
  w();
}

// ── Write ────────────────────────────────────────────────────────────

writeFileSync(output, lines.join("\n") + "\n");
console.log(`Report written → ${output} (${lines.length} lines)`);
