#!/usr/bin/env node
import { $ } from "zx";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import {
  importKeyValueArgs,
  isTruthy,
  loadDotenv,
  requireOcLogin,
} from "../../scripts/common.mjs";

importKeyValueArgs(process.argv.slice(2));
const k8sDir = resolve(import.meta.dirname, "..");
const rootDir = resolve(k8sDir, "../..");
if (!(await $`command -v oc`.nothrow()).ok)
  throw new Error("'oc' CLI not found.");
await requireOcLogin();
if (!existsSync(resolve(rootDir, ".env")))
  throw new Error(`${rootDir}/.env not found. Copy from .env.template.`);
await loadDotenv(resolve(rootDir, ".env"));

// Generate deployment inputs before applying manifests; Kustomize consumes both files.
const addons = isTruthy(process.env.GCPHCP_ENABLED)
  ? "kubernetes,gcphcp"
  : "kubernetes";
const gcphcpPath = isTruthy(process.env.GCPHCP_ENABLED)
  ? "/etc/fleetshift/gcphcp/gcphcp.yaml"
  : "";
await $`node ${resolve(rootDir, "deploy/scripts/render-gcphcp-config.mjs")} --output ${resolve(k8sDir, "gcphcp.yaml")}`;
await $`sh -c ${`printf '%s\n' 'OIDC_ISSUER_URL=${process.env.OIDC_ISSUER_URL}' 'OIDC_UI_CLIENT_ID=${process.env.OIDC_UI_CLIENT_ID || "fleetshift-ui"}' 'OIDC_UI_SCOPE=${process.env.OIDC_UI_SCOPE || "openid profile email"}' 'OIDC_CLI_CLIENT_ID=${process.env.OIDC_CLI_CLIENT_ID}' 'OIDC_RESOURCE_AUDIENCE=${process.env.OIDC_RESOURCE_AUDIENCE || process.env.OIDC_AUDIENCE || "fleetshift"}' 'OIDC_KEY_ENROLLMENT_AUDIENCE=${process.env.OIDC_KEY_ENROLLMENT_AUDIENCE || "fleetshift-signing"}' 'PUBLIC_KEY_CLAIM_EXPR=${process.env.PUBLIC_KEY_CLAIM_EXPR || ""}' 'KEY_REGISTRY_ID=${process.env.KEY_REGISTRY_ID}' 'KEY_REGISTRY_SUBJECT_EXPR=${process.env.KEY_REGISTRY_SUBJECT_EXPR}' 'FLEETSHIFT_LOG_LEVEL=${process.env.FLEETSHIFT_LOG_LEVEL || "info"}' 'FLEETSHIFT_SERVER_ADDONS=${addons}' 'GCPHCP_CONFIG_PATH=${gcphcpPath}' > ${resolve(k8sDir, "config.env")}`}`;
const password = encodeURIComponent(process.env.POSTGRES_PASSWORD);
await $`sh -c ${`printf '%s\n' 'POSTGRES_USER=${process.env.POSTGRES_USER}' 'POSTGRES_PASSWORD=${process.env.POSTGRES_PASSWORD}' 'POSTGRES_DB=${process.env.POSTGRES_DB}' 'DATABASE_URL=postgres://${process.env.POSTGRES_USER}:${password}@postgres:5432/${process.env.POSTGRES_DB}?sslmode=disable' > ${resolve(k8sDir, "secrets.env")}`}`;
await $`oc apply -f ${resolve(k8sDir, "namespace.yaml")}`;
await $`oc apply -k ${k8sDir}`;
await $`oc wait -n fleetshift statefulset/postgres --for=jsonpath={.status.readyReplicas}=1 --timeout=120s`;
await $`oc import-image fleetshift-server:latest -n fleetshift --confirm`.nothrow();
await $`oc import-image fleetshift-web:latest -n fleetshift --confirm`.nothrow();
await $`oc set triggers deployment/fleetshift-server -n fleetshift --from-image=fleetshift-server:latest -c fleetshift-server`.nothrow();
await $`oc set triggers deployment/fleetshift-server -n fleetshift --from-image=fleetshift-web:latest -c web-builder --containers=web-builder`.nothrow();
await $`oc wait -n fleetshift deployment/fleetshift-server --for=condition=Available --timeout=300s`;
const http =
  (
    await $`oc get route web -n fleetshift -o jsonpath={.spec.host}`.nothrow()
  ).stdout.trim() || "<pending>";
const grpc =
  (
    await $`oc get route grpc -n fleetshift -o jsonpath={.spec.host}`.nothrow()
  ).stdout.trim() || "<pending>";
console.log(
  `\n=== Deployment Complete ===\n  Frontend + API: https://${http}\n  gRPC:           ${grpc}:443\n`,
);
