#!/usr/bin/env node
import { $ } from "zx";
import { importKeyValueArgs, requireOcCluster } from "../../scripts/common.mjs";
const [command, ...args] = importKeyValueArgs(process.argv.slice(2));
await requireOcCluster("KC_CLUSTER_API");

// MinIO commands share cluster validation and namespace conventions.
if (command === "deploy" || command === "teardown") {
  await $`node ${new URL(`./${command}.mjs`, import.meta.url).pathname} ${args}`;
  process.exit(0);
}
if (command === "status") {
  await $`oc get all -n minio-nx-cache`;
  process.exit(0);
}
if (command === "credentials") {
  const user = (
    await $`oc get secret minio-credentials -n minio-nx-cache -o jsonpath={.data.MINIO_ROOT_USER} | base64 -d`
  ).stdout.trim();
  const pass = (
    await $`oc get secret minio-credentials -n minio-nx-cache -o jsonpath={.data.MINIO_ROOT_PASSWORD} | base64 -d`
  ).stdout.trim();
  const host = (
    await $`oc get route minio-api -n minio-nx-cache -o jsonpath={.spec.host}`
  ).stdout.trim();
  console.log(
    `\n  S3 API: https://${host}\n  Bucket: nx-cache\n\n  Root User: ${user}\n  Root Password: ${pass}\n`,
  );
  process.exit(0);
}
throw new Error(`Unknown MinIO command: ${command || ""}`);
