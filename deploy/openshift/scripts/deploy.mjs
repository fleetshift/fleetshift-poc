#!/usr/bin/env node
import { $ } from "zx";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import {
  importKeyValueArgs,
  loadDotenv,
  requireOcCluster,
} from "../../scripts/common.mjs";

importKeyValueArgs(process.argv.slice(2));
const openshiftDir = resolve(import.meta.dirname, "..");
const rootDir = resolve(openshiftDir, "../..");
if (!(await $`command -v oc`.nothrow()).ok)
  throw new Error("'oc' CLI not found.");
if (!existsSync(resolve(rootDir, ".env")))
  throw new Error(`${rootDir}/.env not found. Copy from .env.template.`);
await loadDotenv(resolve(rootDir, ".env"));
await requireOcCluster("OCM_FLEET_CLUSTER_API");

// Generate deployment inputs before applying manifests; Kustomize consumes both files.
await $`sh -c ${`printf '%s\n' 'OIDC_ISSUER_URL=${process.env.OIDC_ISSUER_URL}' 'OIDC_UI_CLIENT_ID=${process.env.OIDC_UI_CLIENT_ID || "ocm-fleet-console"}' 'OIDC_UI_SCOPE=${process.env.OIDC_UI_SCOPE || "openid profile email"}' 'OIDC_CLI_CLIENT_ID=${process.env.OIDC_CLI_CLIENT_ID}' 'OIDC_RESOURCE_AUDIENCE=${process.env.OIDC_RESOURCE_AUDIENCE || process.env.OIDC_AUDIENCE || "resource-manager"}' 'OIDC_KEY_ENROLLMENT_AUDIENCE=${process.env.OIDC_KEY_ENROLLMENT_AUDIENCE || "resource-manager-signing"}' 'PUBLIC_KEY_CLAIM_EXPR=${process.env.PUBLIC_KEY_CLAIM_EXPR || ""}' 'KEY_REGISTRY_ID=${process.env.KEY_REGISTRY_ID}' 'KEY_REGISTRY_SUBJECT_EXPR=${process.env.KEY_REGISTRY_SUBJECT_EXPR}' 'OCM_RM_LOG_LEVEL=${process.env.OCM_RM_LOG_LEVEL || "info"}' 'OCM_RM_SERVER_ADDONS=kubernetes' > ${resolve(openshiftDir, "config.env")}`}`;
const password = encodeURIComponent(process.env.POSTGRES_PASSWORD);
await $`sh -c ${`printf '%s\n' 'POSTGRES_USER=${process.env.POSTGRES_USER}' 'POSTGRES_PASSWORD=${process.env.POSTGRES_PASSWORD}' 'POSTGRES_DB=${process.env.POSTGRES_DB}' 'DATABASE_URL=postgres://${process.env.POSTGRES_USER}:${password}@postgres:5432/${process.env.POSTGRES_DB}?sslmode=disable' > ${resolve(openshiftDir, "secrets.env")}`}`;
await $`oc apply -f ${resolve(openshiftDir, "namespace.yaml")}`;
await $`oc apply -k ${openshiftDir}`;
await $`oc wait -n ocm-fleet statefulset/postgres --for=jsonpath={.status.readyReplicas}=1 --timeout=120s`;
await $`oc import-image ocm-fleet-server:latest -n ocm-fleet --confirm`.nothrow();
await $`oc import-image ocm-fleet-web:latest -n ocm-fleet --confirm`.nothrow();
await $`oc set triggers deployment/resource-manager -n ocm-fleet --from-image=ocm-fleet-server:latest -c resource-manager`.nothrow();
await $`oc set triggers deployment/resource-manager -n ocm-fleet --from-image=ocm-fleet-web:latest -c web-builder --containers=web-builder`.nothrow();
await $`oc wait -n ocm-fleet deployment/resource-manager --for=condition=Available --timeout=300s`;
const http =
  (
    await $`oc get route web -n ocm-fleet -o jsonpath={.spec.host}`.nothrow()
  ).stdout.trim() || "<pending>";
const grpc =
  (
    await $`oc get route grpc -n ocm-fleet -o jsonpath={.spec.host}`.nothrow()
  ).stdout.trim() || "<pending>";
console.log(
  `\n=== Deployment Complete ===\n  Frontend + API: https://${http}\n  gRPC:           ${grpc}:443\n`,
);
