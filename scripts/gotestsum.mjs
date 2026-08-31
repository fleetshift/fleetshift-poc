#!/usr/bin/env node
// Shared gotestsum wrapper for Nx Go test targets.

import { mkdir } from "node:fs/promises";
import { constants as osConstants } from "node:os";
import { dirname } from "node:path";
import { $ } from "zx";

const help = `Shared gotestsum wrapper for Nx Go test targets.

Usage:
  node "\${NX_WORKSPACE_ROOT}/scripts/gotestsum.mjs" [options] [--] [go test args...]

Options:
  --jsonfile PATH     Write gotestsum JSON events
  --junitfile PATH    Write JUnit XML (--junitfile-hide-empty-pkg)
  --junit-name NAME   --junitfile-project-name (default: \$NX_TASK_TARGET_PROJECT)
  --slowest DURATION  After the run, print tests slower than DURATION
                      from --jsonfile (requires --jsonfile)
  --live PKG          Locally: build PKG to tmp/gotestlive and pipe
                      standard-json into it. Ignored when GITHUB_ACTIONS is set.

Env:
  GOTESTSUM_FORMAT        default pkgname (CI sets github-actions)
  GITHUB_ACTIONS          if unset, --hide-summary=skipped

Always passes -count=1 (disables Go's test cache). A later -count wins.`;

const args = process.argv.slice(2);
let jsonfile = "";
let junitfile = "";
let junitName = process.env.NX_TASK_TARGET_PROJECT ?? "";
let slowest = "";
let live = "";
const gotestArgs = [];

for (let index = 0; index < args.length; index += 1) {
  const arg = args[index];
  if (arg === "-h" || arg === "--help") {
    console.log(help);
    process.exit(0);
  }
  if (arg === "--") {
    gotestArgs.push(...args.slice(index + 1));
    break;
  }
  if (
    arg === "--jsonfile" ||
    arg === "--junitfile" ||
    arg === "--junit-name" ||
    arg === "--slowest" ||
    arg === "--live"
  ) {
    if (index + 1 >= args.length) {
      const messages = {
        "--jsonfile": "--jsonfile requires a path",
        "--junitfile": "--junitfile requires a path",
        "--junit-name": "--junit-name requires a name",
        "--slowest": "--slowest requires a duration (e.g. 10s)",
        "--live": "--live requires a Go package path",
      };
      console.error(messages[arg]);
      process.exit(2);
    }
    const value = args[++index];
    if (arg === "--jsonfile") jsonfile = value;
    if (arg === "--junitfile") junitfile = value;
    if (arg === "--junit-name") junitName = value;
    if (arg === "--slowest") slowest = value;
    if (arg === "--live") live = value;
    continue;
  }
  gotestArgs.push(...args.slice(index));
  break;
}

if (slowest && !jsonfile) {
  console.error("gotestsum.mjs: --slowest requires --jsonfile");
  process.exit(2);
}

const inCi = Boolean(process.env.GITHUB_ACTIONS);
const useLive = Boolean(live) && !inCi;
// Local live mode streams standard JSON into gotestlive; CI keeps normal reports.
const format = useLive
  ? "standard-json"
  : (process.env.GOTESTSUM_FORMAT ?? "pkgname");
const gotestsumArgs = [
  "--format",
  format,
  "--format-icons",
  "hivis",
  "--format-hide-empty-pkg",
];
if (!inCi) gotestsumArgs.push("--hide-summary=skipped");
if (jsonfile) {
  await mkdir(dirname(jsonfile), { recursive: true });
  gotestsumArgs.push("--jsonfile", jsonfile);
}
if (junitfile) {
  await mkdir(dirname(junitfile), { recursive: true });
  gotestsumArgs.push("--junitfile", junitfile, "--junitfile-hide-empty-pkg");
  if (junitName) gotestsumArgs.push("--junitfile-project-name", junitName);
}
gotestsumArgs.push("--", "-count=1", ...gotestArgs);

async function printSlowest() {
  if (!slowest) return;
  const result =
    await $`go tool gotestsum tool slowest --jsonfile ${jsonfile} --threshold ${slowest}`.nothrow();
  if (result.exitCode === 0 && result.stdout.trim()) {
    console.log();
    console.log(`Slowest tests (≥ ${slowest})`);
    console.log(result.stdout.trimEnd());
  }
}

function exitStatus(result) {
  return (
    result.exitCode ??
    (result.signal ? 128 + osConstants.signals[result.signal] : 1)
  );
}

let status = 0;
if (useLive) {
  await mkdir("tmp", { recursive: true });
  const build = await $`go build -o tmp/gotestlive ${live}`.nothrow();
  if (!build.ok) {
    process.exit(exitStatus(build));
  } else {
    const gotestsum = $({
      stdio: ["pipe", "pipe", "inherit"],
    })`go tool gotestsum ${gotestsumArgs}`.nothrow();
    const gotestlive = $({
      stdio: ["pipe", "inherit", "inherit"],
    })`./tmp/gotestlive`.nothrow();
    const piped = gotestsum.pipe(gotestlive);
    const [gotestsumResult, liveResult] = await Promise.all([gotestsum, piped]);
    status = liveResult.ok
      ? exitStatus(gotestsumResult)
      : exitStatus(liveResult);
  }
} else {
  const result = await $({
    stdio: "inherit",
  })`go tool gotestsum ${gotestsumArgs}`.nothrow();
  status = exitStatus(result);
}

await printSlowest();
process.exit(status);
