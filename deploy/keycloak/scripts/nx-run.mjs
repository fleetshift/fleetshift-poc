#!/usr/bin/env node
import { $ } from "zx";
import { importKeyValueArgs, requireOcCluster } from "../../scripts/common.mjs";
const [command, ...args] = importKeyValueArgs(process.argv.slice(2));
await requireOcCluster("KC_CLUSTER_API");

// Validate command names before constructing a script path from user input.
if (
  ![
    "deploy",
    "teardown",
    "reset-realm",
    "add-user",
    "add-base-domain",
  ].includes(command)
)
  throw new Error(`Unknown Keycloak command: ${command || ""}`);
await $`node ${new URL(`./${command}.mjs`, import.meta.url).pathname} ${args}`;
