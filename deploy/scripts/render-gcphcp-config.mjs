#!/usr/bin/env node
import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

function usage() {
  console.error(`Usage: render-gcphcp-config.mjs --output <path>

Renders a gcphcp YAML config file from environment variables.
When GCPHCP_ENABLED is not truthy, writes a disabled placeholder file.

Only GCPHCP_GATEWAY_URL is required when enabled. The remaining values use
built-in POC defaults unless overridden by a nonempty environment variable.`);
  process.exit(1);
}

function isTruthy(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").toLowerCase());
}

function yamlEscape(value) {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll('"', '\\"')
    .replaceAll("\n", "\\n")
    .replaceAll("\r", "\\r")
    .replaceAll("\t", "\\t");
}

const args = process.argv.slice(2);
let outputPath = "";
for (let index = 0; index < args.length; index += 1) {
  if (args[index] !== "--output") usage();
  if (index + 1 >= args.length) usage();
  outputPath = args[++index];
}
if (!outputPath) usage();

// Always write a valid placeholder so manifests can reference this file safely.
await mkdir(dirname(outputPath), { recursive: true });
if (!isTruthy(process.env.GCPHCP_ENABLED || "false")) {
  await writeFile(outputPath, "# gcphcp is disabled for this deployment.\n");
  process.exit(0);
}

if (!process.env.GCPHCP_GATEWAY_URL) {
  console.error(`ERROR: GCPHCP_GATEWAY_URL is required when GCPHCP_ENABLED=true.
Set it in .env (for example GCPHCP_GATEWAY_URL=https://your-cls-gateway)
or pass it with podman -e GCPHCP_GATEWAY_URL=https://your-cls-gateway.`);
  process.exit(1);
}

const values = {
  gatewayAudience:
    process.env.GCPHCP_GATEWAY_AUDIENCE ||
    "32555940559.apps.googleusercontent.com",
  targetId: process.env.GCPHCP_TARGET_ID || "gcphcp",
  gcpProject: process.env.GCPHCP_GCP_PROJECT || "gcp-ome-poc",
  gcpRegion: process.env.GCPHCP_GCP_REGION || "us-central1",
  workforcePool: process.env.GCPHCP_WORKFORCE_POOL || "ome-hcp",
  workforceProvider: process.env.GCPHCP_WORKFORCE_PROVIDER || "ome-oidc",
  brokerSaEmail:
    process.env.GCPHCP_BROKER_SA_EMAIL ||
    "hcp-idtoken-broker@gcp-ome-poc.iam.gserviceaccount.com",
};
await writeFile(
  outputPath,
  `gateway:
  url: "${yamlEscape(process.env.GCPHCP_GATEWAY_URL)}"
  audience: "${yamlEscape(values.gatewayAudience)}"
targets:
  - id: "${yamlEscape(values.targetId)}"
    gcp_project: "${yamlEscape(values.gcpProject)}"
    region: "${yamlEscape(values.gcpRegion)}"
    workforce_pool: "${yamlEscape(values.workforcePool)}"
    workforce_provider: "${yamlEscape(values.workforceProvider)}"
    broker_sa_email: "${yamlEscape(values.brokerSaEmail)}"
`,
);
