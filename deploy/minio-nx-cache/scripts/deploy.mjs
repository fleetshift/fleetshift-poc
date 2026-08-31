#!/usr/bin/env node
import { $ } from "zx";
import { resolve } from "node:path";
import { requireOcLogin } from "../../scripts/common.mjs";
const dir = resolve(import.meta.dirname, "..");
await $`command -v oc`;
await $`command -v openssl`;
await requireOcLogin();

// Create credentials once, then reconcile MinIO, bucket, proxy, and TLS resources.
await $`oc apply -f ${resolve(dir, "manifests/namespace.yaml")}`;
if (!(await $`oc get secret minio-credentials -n minio-nx-cache`.nothrow()).ok)
  await $`sh -c ${`oc create secret generic minio-credentials --from-literal=MINIO_ROOT_USER=minio-admin --from-literal=MINIO_ROOT_PASSWORD=$(openssl rand -base64 48 | tr -dc a-zA-Z0-9 | head -c 32) -n minio-nx-cache --dry-run=client -o yaml | oc apply -f -`}`;
if (!(await $`oc get secret nx-cache-tokens -n minio-nx-cache`.nothrow()).ok)
  await $`sh -c ${`oc create secret generic nx-cache-tokens --from-literal=read-token=$(openssl rand -hex 32) --from-literal=write-token=$(openssl rand -hex 32) -n minio-nx-cache --dry-run=client -o yaml | oc apply -f -`}`;
for (const file of ["deployment.yaml", "service.yaml"])
  await $`oc apply -f ${resolve(dir, "manifests", file)} -n minio-nx-cache`;
await $`oc rollout status deployment/minio -n minio-nx-cache --timeout=180s`;
for (const file of ["route-api.yaml", "route-console.yaml"])
  await $`oc apply -f ${resolve(dir, "manifests", file)} -n minio-nx-cache`;
await $`oc delete job minio-create-bucket -n minio-nx-cache --ignore-not-found`;
await $`oc apply -f ${resolve(dir, "manifests/bucket-job.yaml")} -n minio-nx-cache`;
await $`oc wait -n minio-nx-cache job/minio-create-bucket --for=condition=Complete --timeout=120s`;
await $`oc apply -f ${resolve(dir, "manifests/proxy-deployment.yaml")} -n minio-nx-cache`;
await $`oc rollout status deployment/nx-cache-proxy -n minio-nx-cache --timeout=120s`;
const host = (
  await $`oc get route nx-cache-proxy -n minio-nx-cache -o jsonpath={.spec.host}`
).stdout.trim();
const consoleHost = (
  await $`oc get route minio-console -n minio-nx-cache -o jsonpath={.spec.host}`
).stdout.trim();
const issuer = process.env.CERT_ISSUER || "zerossl-prod";
await $`sh -c ${`sed -e 's|__PROXY_HOST__|${host}|' -e 's|__CERT_ISSUER__|${issuer}|' ${resolve(dir, "manifests/certificate.yaml")} | oc apply -n minio-nx-cache -f -`}`;
await $`oc wait -n minio-nx-cache certificate/nx-cache-proxy-tls --for=condition=Ready --timeout=180s`;
for (const file of [
  "router-secret-reader-role.yaml",
  "router-secret-reader-binding.yaml",
])
  await $`oc apply -n minio-nx-cache -f ${resolve(dir, "manifests", file)}`;
await $`oc patch route nx-cache-proxy -n minio-nx-cache --type=merge -p ${'{"spec":{"tls":{"certificate":null,"key":null,"caCertificate":null,"externalCertificate":{"name":"nx-cache-proxy-tls"}}}}'}`;
console.log(
  `\n==========================================\n  Nx Remote Cache Deployment Complete\n==========================================\n\n  Cache Proxy: https://${host}\n  Console:     https://${consoleHost}\n  Bucket:      nx-cache\n\n  Run 'npx nx run minio:credentials' to retrieve bearer tokens.\n\n==========================================`,
);
