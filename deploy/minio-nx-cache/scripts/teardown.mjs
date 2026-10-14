#!/usr/bin/env node
import { $, question } from "zx";
import { requireOcLogin } from "../../scripts/common.mjs";
await requireOcLogin();
console.log(
  "This will remove MinIO Nx cache deployment from namespace 'minio-nx-cache'.",
);
console.log("All cached data will be PERMANENTLY DELETED.");
if (!/^y$/i.test(await question("Are you sure? (y/N): "))) {
  console.log("Aborted.");
  process.exit(0);
}

// Delete proxy and bucket job before MinIO storage resources.
console.log("==> Deleting nx-cache-proxy...");
for (const [kind, name] of [
  ["deployment", "nx-cache-proxy"],
  ["service", "nx-cache-proxy"],
  ["route", "nx-cache-proxy"],
  ["job", "minio-create-bucket"],
  ["route", "minio-api"],
  ["route", "minio-console"],
  ["deployment", "minio"],
  ["service", "minio"],
]) {
  if (name === "minio-create-bucket")
    console.log("==> Deleting bucket creation job...");
  if (name === "minio-api") console.log("==> Deleting MinIO routes...");
  if (name === "minio")
    console.log("==> Deleting MinIO deployment and service...");
  await $`oc delete ${kind} ${name} -n minio-nx-cache --ignore-not-found`;
}

// Wait before deleting secrets and PVC, so active pods release storage.
console.log("==> Waiting for MinIO pods to terminate...");
await $`oc wait --for=delete pod -l app=minio -n minio-nx-cache --timeout=60s`.nothrow();
await $`oc wait --for=delete pod -l app=nx-cache-proxy -n minio-nx-cache --timeout=60s`.nothrow();
console.log("==> Deleting secrets and PVC...");
for (const [kind, name] of [
  ["secret", "minio-credentials"],
  ["secret", "nx-cache-tokens"],
  ["pvc", "minio-data"],
])
  await $`oc delete ${kind} ${name} -n minio-nx-cache --ignore-not-found`;
console.log("==> Deleting namespace minio-nx-cache...");
await $`oc delete namespace minio-nx-cache --ignore-not-found`;
console.log("==> Waiting for namespace deletion...");
await $`oc wait --for=delete namespace/minio-nx-cache --timeout=120s`.nothrow();
console.log("\n==> Teardown complete.");
