#!/usr/bin/env node
import { compose, ensurePodmanReady, importKeyValueArgs } from "./common.mjs";

const args = importKeyValueArgs(process.argv.slice(2));
await ensurePodmanReady();
await compose(...args);
