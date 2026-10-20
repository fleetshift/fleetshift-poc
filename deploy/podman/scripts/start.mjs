#!/usr/bin/env node
import { $, cd } from "zx";
import {
  compose,
  composeDir,
  copySandboxCA,
  ensurePodmanReady,
  importKeyValueArgs,
  rootDir,
} from "./common.mjs";

importKeyValueArgs(process.argv.slice(2));
await ensurePodmanReady();

// Kind network is shared with locally provisioned clusters.
try {
  await $`podman network inspect kind`;
} catch {
  await $`podman network create kind`;
}

if (process.env.DEV === "true" || process.env.BUILD === "true") {
  // Build through Nx so image dependencies use the same workspace targets.
  console.log("==> Building all-in-one image from source (Nx image:aio)");
  cd(rootDir);
  await $`npx nx run image:aio`;
}

console.log("==> Starting OCM Fleet stack");
const upArgs = ["up", "-d"];
if (process.env.DEV === "true" || process.env.BUILD === "true")
  upArgs.push("--build");
await compose(...upArgs);

const httpPort = process.env.OCM_RM_SERVER_HTTP_PORT || "8085";
const publicOrigin = `https://ocm-fleet.localhost:${httpPort}`;
console.log(
  `\n==> OCM Fleet stack is running!\n    Console:               ${publicOrigin}  (opens /app after the certificate warning)`,
);

if (!process.env.OIDC_ISSUER_URL) {
  // Peer Dex is enabled when no external issuer is configured.
  console.log("==> Copying Dex sandbox CA to .certs/ca.crt (for fleetctl)");
  try {
    await copySandboxCA();
  } catch {
    console.error(
      `    WARN: sandbox CA not ready yet. Copy it later with:\n      podman compose cp ocm-fleet:/data/sandbox/pki/ca.crt ${composeDir}/.certs/ca.crt`,
    );
  }
  console.log(`
  Built-in Dex sandbox IdP (no OIDC_ISSUER_URL set):
    Issuer:  ${publicOrigin}/idp
    Users:   ops@ocm-fleet.local / ocm-fleet-ops
             dev@ocm-fleet.local / ocm-fleet-dev

  Open ${publicOrigin} and accept the browser certificate warning
  (unknown sandbox CA). Dex is same-origin under /idp; port 5556 is not published.

  If this volume previously ran the old :5556 Dex issuer, reset it first:
    npx nx run deploy:podman:clean

  Configure fleetctl:
    bin/fleetctl auth setup \\
      --issuer-url ${publicOrigin}/idp \\
      --client-id fleetctl \\
      --key-enrollment-client-id resource-manager-signing \\
      --oidc-ca-file ${composeDir}/.certs/ca.crt \\
      --scopes 'openid,profile,email,audience:server:client_id:resource-manager'
    bin/fleetctl auth login
`);
} else {
  console.log(`\n    External OIDC issuer: ${process.env.OIDC_ISSUER_URL}
    (peer Dex is parked; the container uses your external issuer)
    Register ${publicOrigin}, /app/auth/callback, and /app/silent-renew.html on that IdP.`);
}

console.log(
  "\n    Run 'npx nx run deploy:podman:logs' to tail container output.\n    Run 'npx nx run deploy:podman:status' to list containers.\n    Run 'npx nx --help' to see all available commands.",
);
