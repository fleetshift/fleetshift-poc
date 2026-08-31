#!/usr/bin/env node
import { $ } from "zx";
import { fileURLToPath } from "node:url";
import { compose, ensurePodmanReady, importKeyValueArgs } from "./common.mjs";

const args = importKeyValueArgs(process.argv.slice(2));
await ensurePodmanReady();

// Include every overlay so cleanup works regardless of how stack was started.
process.env.DEV = "true";
process.env.LOCAL_WEB = "true";
process.env.NX_CACHE = "true";

if (args[0] === "--clean") {
  console.log("==> Stopping stack and removing volumes and .certs");
  await compose("down", "-v");
  await $`rm -rf ${fileURLToPath(new URL("../.certs", import.meta.url))}`;
} else {
  console.log("==> Stopping stack (preserving data)");
  await compose("down");
}
console.log("==> Done.");
