#!/usr/bin/env node
import { $ } from "zx";
import { resolve } from "node:path";
import { importKeyValueArgs, requireOcCluster } from "../../scripts/common.mjs";
importKeyValueArgs(process.argv.slice(2));
const k8sDir = resolve(import.meta.dirname, "..");

// Preconditions: fail early with actionable errors before changing resources.
console.log("=== FleetShift Kubernetes Teardown ===");
await $`command -v oc`;
await requireOcCluster("OME_CLUSTER_API");
if (!(await $`oc get namespace fleetshift`.nothrow()).ok) {
  console.log("Namespace 'fleetshift' not found. Nothing to tear down.");
  process.exit(0);
}

// Step 1: delete all Kustomize-managed resources.
console.log("Removing resources via Kustomize...");
await $`oc delete -k ${k8sDir} --ignore-not-found=true`;

// Step 2: delete namespace after its workloads and services are removed.
console.log("Deleting namespace...");
await $`oc delete namespace fleetshift --ignore-not-found=true`;
console.log("\n=== Teardown Complete ===");
