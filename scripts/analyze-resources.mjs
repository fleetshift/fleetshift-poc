#!/usr/bin/env node
/**
 * Analyzes sample-response.json and produces resource-patterns.json
 * with structural patterns for search autocomplete and CEL filter hints.
 *
 * Usage: node scripts/analyze-resources.mjs [input] [output]
 *   input  — path to resource array JSON (default: sample-response.json)
 *   output — path to write patterns (default: resource-patterns.json)
 */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const input = resolve(process.argv[2] ?? "sample-response.json");
const output = resolve(process.argv[3] ?? "resource-patterns.json");

const resources = JSON.parse(readFileSync(input, "utf8"));
const items = Array.isArray(resources) ? resources : resources.resources ?? [];

// ── helpers ──────────────────────────────────────────────────────────

function collectPaths(obj, prefix = "", out = {}) {
  if (obj == null || typeof obj !== "object") return out;
  for (const [k, v] of Object.entries(obj)) {
    const path = prefix ? `${prefix}.${k}` : k;
    if (Array.isArray(v)) {
      out[path] = { type: "array", values: new Set() };
      for (const el of v.slice(0, 20)) {
        if (typeof el === "object" && el !== null) {
          collectPaths(el, `${path}[]`, out);
        } else if (el != null) {
          (out[path].values ??= new Set()).add(String(el));
        }
      }
    } else if (typeof v === "object") {
      out[path] = { type: "object" };
      collectPaths(v, path, out);
    } else {
      if (!out[path]) out[path] = { type: typeof v, values: new Set() };
      out[path].values.add(String(v));
    }
  }
  return out;
}

function prevalence(seen, total) {
  if (seen >= total * 0.95) return "always";
  if (seen >= total * 0.5) return "common";
  return "rare";
}

// ── group by resourceType ────────────────────────────────────────────

const byType = new Map();
for (const item of items) {
  const rt = item.resourceType;
  if (!byType.has(rt)) byType.set(rt, []);
  byType.get(rt).push(item);
}

// ── per-type analysis ────────────────────────────────────────────────

const resourceTypes = {};

for (const [rt, group] of byType) {
  const count = group.length;
  const names = group.map((r) => r.name);

  // name patterns
  const prefixes = [...new Set(names.map((n) => {
    const match = n.match(/^(\/\/[^/]+\/)/);
    return match ? match[1] : n.split("/").slice(0, 3).join("/") + "/";
  }))];

  // resource shape — merge paths from a sample (up to 200)
  const sample = group.slice(0, 200);
  const fieldCounts = {};
  const fieldValues = {};
  const fieldTypes = {};

  for (const item of sample) {
    if (!item.resource) continue;
    const paths = collectPaths(item.resource);
    for (const [path, info] of Object.entries(paths)) {
      fieldCounts[path] = (fieldCounts[path] ?? 0) + 1;
      fieldTypes[path] = info.type;
      if (info.values) {
        if (!fieldValues[path]) fieldValues[path] = new Set();
        for (const v of info.values) fieldValues[path].add(v);
      }
    }
  }

  const resourceShape = {};
  for (const [path, cnt] of Object.entries(fieldCounts)) {
    const entry = {
      type: fieldTypes[path],
      prevalence: prevalence(cnt, sample.length),
    };
    const vals = fieldValues[path];
    if (vals && vals.size > 0 && vals.size <= 50) {
      entry.distinctValues = [...vals].sort();
      entry.distinctCount = vals.size;
    } else if (vals) {
      entry.distinctCount = vals.size;
    }
    resourceShape[path] = entry;
  }

  // conditions
  const conditionNames = new Set();
  for (const item of sample) {
    if (item.resource?.conditions) {
      for (const key of Object.keys(item.resource.conditions)) {
        conditionNames.add(key);
      }
    }
  }

  // labels
  const labelKeys = new Set();
  for (const item of sample) {
    if (item.resource?.localLabels) {
      for (const key of Object.keys(item.resource.localLabels)) {
        labelKeys.add(key);
      }
    }
  }

  resourceTypes[rt] = {
    count,
    namePattern: inferNamePattern(rt, names),
    nameExamples: names.slice(0, 5),
    namePrefixes: prefixes,
    resourceShape,
    conditions: [...conditionNames].sort(),
    labels: [...labelKeys].sort(),
  };
}

function inferNamePattern(rt, names) {
  if (names.length === 0) return "";
  const sample = names[0];
  if (rt === "kubernetes.fleetshift.io/Object") {
    return "//kubernetes.fleetshift.io/clusters/{cluster}/apiResources/{group~version~resource}/objects/{uuid}";
  }
  if (rt.endsWith("/Cluster")) {
    const domain = rt.split("/")[0];
    return `//${domain}/clusters/{clusterName}`;
  }
  if (rt.endsWith("/Node")) {
    const domain = rt.split("/")[0];
    return `//${domain}/clusters/{clusterName}/nodes/{nodeName}`;
  }
  return sample.replace(/[^/]+$/, "{id}");
}

// ── kubernetes.fleetshift.io/Object deep analysis ────────────────────

const k8sObjects = byType.get("kubernetes.fleetshift.io/Object") ?? [];

// kind distribution
const kindCounts = {};
const namespaceCounts = {};
const gvrCounts = {};
const apiGroupCounts = {};
const conditionStatusMap = {};
const allConditionNames = new Set();
const allLabelKeys = new Set();

// per-kind extracted fields
const extractedByKind = {};

for (const item of k8sObjects) {
  const obs = item.resource?.observation;
  if (!obs) continue;

  const kind = obs.kind ?? "unknown";
  kindCounts[kind] = (kindCounts[kind] ?? 0) + 1;

  const ns = obs.metadata?.namespace;
  if (ns) namespaceCounts[ns] = (namespaceCounts[ns] ?? 0) + 1;

  const gvr = obs.gvr;
  if (gvr) {
    const gvrKey = `${gvr.group || "core"}/${gvr.version}/${gvr.resource}`;
    gvrCounts[gvrKey] = (gvrCounts[gvrKey] ?? 0) + 1;
    const group = gvr.group || "core";
    apiGroupCounts[group] = (apiGroupCounts[group] ?? 0) + 1;
  }

  // conditions
  const conds = item.resource?.conditions;
  if (conds) {
    for (const [name, val] of Object.entries(conds)) {
      allConditionNames.add(name);
      if (!conditionStatusMap[name]) conditionStatusMap[name] = { statuses: new Set(), reasons: new Set() };
      if (val.status) conditionStatusMap[name].statuses.add(val.status);
      if (val.reason) conditionStatusMap[name].reasons.add(val.reason);
    }
  }

  // labels
  if (item.resource?.localLabels) {
    for (const key of Object.keys(item.resource.localLabels)) {
      allLabelKeys.add(key);
    }
  }

  // extracted fields per kind
  const extracted = obs.extracted;
  if (extracted && typeof extracted === "object") {
    if (!extractedByKind[kind]) extractedByKind[kind] = {};
    for (const [k, v] of Object.entries(extracted)) {
      if (!extractedByKind[kind][k]) extractedByKind[kind][k] = new Set();
      if (v != null && typeof v !== "object") {
        extractedByKind[kind][k].add(String(v));
      }
    }
  }
}

// sort by count desc
const sortedKinds = Object.entries(kindCounts).sort((a, b) => b[1] - a[1]);
const sortedNamespaces = Object.entries(namespaceCounts).sort((a, b) => b[1] - a[1]);
const sortedGvrs = Object.entries(gvrCounts).sort((a, b) => b[1] - a[1]);
const sortedApiGroups = Object.entries(apiGroupCounts).sort((a, b) => b[1] - a[1]);

// ── cross-type patterns ─────────────────────────────────────────────

const crossTypePatterns = {
  commonFields: ["name", "resourceType", "resource.name", "resource.uid", "resource.etag", "resource.createTime", "resource.updateTime"],
  topLevelNameFormat: "//{domain}/{resourcePath}",
  nameSegmentConventions: "K8s objects use GVR format: {group}~{version}~{resource} with ~ separator. Names are URIs: //kubernetes.fleetshift.io/clusters/{cluster}/apiResources/{gvr}/objects/{uuid}",

  observationKindDistribution: Object.fromEntries(sortedKinds),
  namespaceDistribution: Object.fromEntries(sortedNamespaces),
  gvrDistribution: Object.fromEntries(sortedGvrs),
  apiGroupDistribution: Object.fromEntries(sortedApiGroups),

  conditions: Object.fromEntries(
    [...allConditionNames].sort().map((name) => [
      name,
      {
        statuses: [...(conditionStatusMap[name]?.statuses ?? [])].sort(),
        reasons: [...(conditionStatusMap[name]?.reasons ?? [])].sort(),
      },
    ])
  ),

  extractedFieldsByKind: Object.fromEntries(
    Object.entries(extractedByKind)
      .filter(([, fields]) => Object.keys(fields).length > 0)
      .map(([kind, fields]) => [
        kind,
        Object.fromEntries(
          Object.entries(fields).map(([k, vals]) => [
            k,
            vals.size <= 30 ? [...vals].sort() : { distinctCount: vals.size },
          ])
        ),
      ])
  ),

  filterableDimensions: {
    lowCardinality: [
      { field: "resourceType", distinctCount: byType.size, values: [...byType.keys()] },
      { field: "resource.observation.gvr.scope", distinctCount: 2, values: ["cluster", "namespaced"] },
      { field: "resource.conditions.*.status", distinctCount: 3, values: ["True", "False", "Unknown"] },
    ],
    mediumCardinality: [
      { field: "resource.observation.kind", distinctCount: Object.keys(kindCounts).length },
      { field: "resource.observation.metadata.namespace", distinctCount: Object.keys(namespaceCounts).length },
      { field: "resource.observation.gvr.group", distinctCount: Object.keys(apiGroupCounts).length },
    ],
    highCardinality: [
      { field: "resource.observation.metadata.name", description: "K8s object name" },
      { field: "name", description: "Full resource URI" },
      { field: "resource.uid", description: "Unique resource ID" },
    ],
  },

  suggestedCelFilters: [
    { expression: 'resource.observation.kind == "Pod"', description: "Find all Pods" },
    { expression: 'resource.observation.kind == "Deployment"', description: "Find all Deployments" },
    { expression: 'resource.observation.kind == "Node"', description: "Find all Nodes" },
    { expression: 'resource.observation.kind == "Service"', description: "Find all Services" },
    { expression: 'resource.observation.kind in ["Pod", "Deployment", "DaemonSet", "StatefulSet", "ReplicaSet"]', description: "Find workload resources" },
    { expression: 'resource.observation.kind in ["Service", "NetworkPolicy", "Ingress"]', description: "Find networking resources" },
    { expression: 'resource.observation.kind in ["ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "ServiceAccount"]', description: "Find RBAC resources" },
    { expression: 'resource.observation.gvr.scope == "namespaced"', description: "Find namespace-scoped resources" },
    { expression: 'resource.observation.gvr.scope == "cluster"', description: "Find cluster-scoped resources" },
    { expression: 'resource.observation.metadata.namespace == "kube-system"', description: "Find resources in kube-system" },
    { expression: 'resource.conditions.Ready.status == "True"', description: "Find resources that are Ready" },
    { expression: 'resource.conditions.Ready.status == "False"', description: "Find resources that are NOT ready" },
    { expression: 'resource.conditions.Available.status == "True"', description: "Find Available resources" },
    { expression: 'resource.conditions.Degraded.status != "False"', description: "Find potentially degraded resources" },
    { expression: 'resource.observation.gvr.group == "apps"', description: "Find apps API group (Deployments, DaemonSets, etc.)" },
    { expression: 'resource.observation.extracted.status == "Running"', description: "Find running Pods" },
    { expression: 'resource.observation.extracted.phase == "Active"', description: "Find active Namespaces" },
    { expression: 'resource.observation.kind == "CustomResourceDefinition"', description: "Find all CRDs" },
    { expression: 'resourceType == "kind.fleetshift.io/Cluster"', description: "Find Kind clusters" },
    { expression: 'resourceType == "gcphcp.fleetshift.io/Cluster"', description: "Find GCP HCP clusters" },
    { expression: 'name.startsWith("//kubernetes.fleetshift.io/clusters/")', description: "Find all K8s objects" },
  ],

  searchAutocompleteHints: {
    byResourceType: [...byType.keys()].sort(),
    byObservationKind: sortedKinds.map(([k]) => k),
    byNamespace: sortedNamespaces.map(([ns]) => ns),
    byApiGroup: sortedApiGroups.map(([g]) => g),
    byConditionName: [...allConditionNames].sort(),
    byLabelKey: [...allLabelKeys].sort(),
  },
};

// ── write output ─────────────────────────────────────────────────────

const result = {
  totalCount: items.length,
  resourceTypes,
  crossTypePatterns,
};

writeFileSync(output, JSON.stringify(result, null, 2) + "\n");
console.log(`Analyzed ${items.length} resources → ${output}`);
console.log(`  ${byType.size} resource types, ${Object.keys(kindCounts).length} K8s kinds, ${Object.keys(namespaceCounts).length} namespaces`);
