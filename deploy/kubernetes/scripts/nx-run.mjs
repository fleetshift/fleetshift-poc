#!/usr/bin/env node
import { $ } from "zx";
import { resolve } from "node:path";
import { importKeyValueArgs, requireOcCluster } from "../../scripts/common.mjs";
const [command, ...rest] = importKeyValueArgs(process.argv.slice(2));
await requireOcCluster("OME_CLUSTER_API");
const script = (name) => resolve(import.meta.dirname, `${name}.mjs`);

// Keep target dispatch in one place while deployment steps stay independently runnable.
if (["deploy", "teardown"].includes(command)) {
  await $`node ${script(command)} ${rest}`;
  process.exit(0);
}
if (command === "status") {
  await $`oc get all -n ${process.env.NAMESPACE || "fleetshift"}`;
  process.exit(0);
}
if (command === "logs") {
  await $`oc logs -f deployment/fleetshift-server -n ${process.env.NAMESPACE || "fleetshift"} --all-containers`;
  process.exit(0);
}
if (command === "set-image") {
  if (!process.env.TAG)
    throw new Error(
      "TAG is required. Usage: npx nx run k8s:set-image -- TAG=<image-tag>",
    );
  await $`oc tag quay.io/stolostron/fleetshift:${process.env.TAG} fleetshift-server:latest -n ${process.env.NAMESPACE || "fleetshift"}`;
  process.exit(0);
}
if (command === "reset-image") {
  await $`oc tag --scheduled quay.io/stolostron/fleetshift-server:latest fleetshift-server:latest -n ${process.env.NAMESPACE || "fleetshift"}`;
  process.exit(0);
}
if (command === "import-images") {
  await $`oc import-image fleetshift-server:latest -n ${process.env.NAMESPACE || "fleetshift"} --confirm`;
  await $`oc import-image fleetshift-web:latest -n ${process.env.NAMESPACE || "fleetshift"} --confirm`;
  process.exit(0);
}
if (command === "register-redirect") {
  if (!process.env.USER || !process.env.PASSWORD)
    throw new Error("USER and PASSWORD are required");
  await $`node ${script("register-redirect")} ${process.env.USER} ${process.env.PASSWORD}`;
  process.exit(0);
}
if (command === "grpc-route-cert-deploy") {
  await $`node ${resolve(import.meta.dirname, "../grpc-route-cert/scripts/deploy.mjs")} --acme-email ${process.env.ACME_EMAIL} ${process.env.FRESH_CERT === "true" ? ["--fresh-cert"] : []} ${rest}`;
  process.exit(0);
}
if (command === "grpc-route-cert-teardown") {
  await $`node ${resolve(import.meta.dirname, "../grpc-route-cert/scripts/teardown.mjs")} ${rest}`;
  process.exit(0);
}
throw new Error(`Unknown Kubernetes command: ${command || ""}`);
