#!/usr/bin/env node
import { $ } from "zx";
import { fileURLToPath } from "node:url";
import { importKeyValueArgs } from "./common.mjs";

importKeyValueArgs(process.argv.slice(2));
// Stop first so image and mounted assets are picked up by fresh containers.
await $`${fileURLToPath(new URL("./stop.mjs", import.meta.url))}`;
process.env.BUILD = "true";
await $`${fileURLToPath(new URL("./start.mjs", import.meta.url))}`;
