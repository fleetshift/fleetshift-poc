#!/usr/bin/env node
import { $ } from "zx";

$.verbose = false;

function usage() {
  console.log(`Usage:
  check-clock-drift.mjs [--machine NAME] [--threshold SECONDS]

Checks clock drift between the host and a Podman machine.

Options:
  -m, --machine NAME       Podman machine name. Defaults to the current
                           default machine, then podman-machine-default.
  -t, --threshold SECONDS  Exit non-zero when absolute drift exceeds this
                           many seconds. Default: 5.
  -h, --help               Show this help.

Environment:
  PODMAN_MACHINE_NAME                    Default machine name override.
  PODMAN_CLOCK_DRIFT_THRESHOLD_SECONDS   Default threshold override.`);
}

function die(message) {
  console.error(`ERROR: ${message}`);
  process.exit(1);
}

function formatMs(value) {
  const sign = value < 0 ? "-" : "";
  const absolute = Math.abs(value);
  return `${sign}${Math.floor(absolute / 1000)}.${String(absolute % 1000).padStart(3, "0")}s`;
}

async function resolveDefaultMachine() {
  const result = await $`podman machine inspect --format {{.Name}}`.nothrow();
  if (result.ok) {
    const name = result.stdout.split(/\s+/).find(Boolean);
    if (name) return name;
  }
  return "podman-machine-default";
}

async function machineEpochMs(machine) {
  const remoteDateCommand =
    'ms=$(date -u +%s%3N 2>/dev/null || true); case "$ms" in ""|*N*) printf "%s000\\n" "$(date -u +%s)" ;; *) printf "%s\\n" "$ms" ;; esac';
  const result =
    await $`podman machine ssh ${machine} ${remoteDateCommand}`.nothrow();
  if (!result.ok) {
    console.error(result.stdout + result.stderr);
    return null;
  }
  const values = result.stdout.match(/^\d+$/gm);
  return values ? Number(values.at(-1)) : null;
}

let machine = process.env.PODMAN_MACHINE_NAME || "";
let thresholdSeconds = process.env.PODMAN_CLOCK_DRIFT_THRESHOLD_SECONDS || "5";
const args = process.argv.slice(2);
for (let index = 0; index < args.length;) {
  switch (args[index]) {
    case "-m":
    case "--machine":
      if (index + 1 >= args.length)
        die(`${args[index]} requires a machine name`);
      machine = args[index + 1];
      index += 2;
      break;
    case "-t":
    case "--threshold":
      if (index + 1 >= args.length)
        die(`${args[index]} requires a number of seconds`);
      thresholdSeconds = args[index + 1];
      index += 2;
      break;
    case "-h":
    case "--help":
      usage();
      process.exit(0);
      break;
    default:
      die(`unknown argument: ${args[index]}`);
  }
}

if (!/^\d+$/.test(thresholdSeconds))
  die("threshold must be a non-negative integer number of seconds");

if (!(await $`command -v podman`.nothrow()).ok) die("podman is not installed");
if (!machine) machine = await resolveDefaultMachine();

const inspect = await $`podman machine inspect ${machine}`.nothrow();
if (!inspect.ok)
  die(
    `could not inspect Podman machine '${machine}': ${inspect.stderr.trim()}`,
  );

const beforeMs = Date.now();
const podmanMs = await machineEpochMs(machine);
const afterMs = Date.now();
if (podmanMs === null)
  die(
    `could not read clock from Podman machine '${machine}'. Is it running? Try: podman machine start ${machine}`,
  );

const midMs = Math.floor((beforeMs + afterMs) / 2);
const roundTripMs = afterMs - beforeMs;
const driftMs = podmanMs - midMs;
const absoluteDriftMs = Math.abs(driftMs);
const thresholdMs = Number(thresholdSeconds) * 1000;

console.log(`Podman machine: ${machine}`);
console.log(`Round trip:     ${formatMs(roundTripMs)}`);
if (driftMs > 0)
  console.log(`Drift:          +${formatMs(driftMs)} (machine ahead of host)`);
else if (driftMs < 0)
  console.log(`Drift:          ${formatMs(driftMs)} (machine behind host)`);
else console.log("Drift:          0.000s");

if (absoluteDriftMs > thresholdMs) {
  console.log(`Status:         FAIL (threshold: ${formatMs(thresholdMs)})`);
  process.exit(1);
}
console.log(`Status:         OK (threshold: ${formatMs(thresholdMs)})`);
