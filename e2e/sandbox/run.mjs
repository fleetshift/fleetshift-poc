#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { chmod, mkdtemp, readFile, rm, stat } from "node:fs/promises";
import https from "node:https";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const IMAGE = "quay.io/stolostron/fleetshift:latest";
const UI_ORIGIN = "https://fleetshift-sandbox.localhost:8085";
const GRPC_TARGET = "127.0.0.1:50051";
const KIND_CLUSTER_LABEL = "io.x-k8s.kind.cluster";
const CA_IN_CONTAINER = "/data/sandbox/pki/ca.crt";
const COMMAND_TIMEOUT_MS = 10_000;
const SANDBOX_REMOVE_TIMEOUT_MS = 60_000;
const REPO_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../..",
);

export function parseCommand(argv) {
  if (argv[0] !== "--" || !argv[1]) {
    throw new Error("usage: node e2e/sandbox/run.mjs -- <command> [args...]");
  }
  return { command: argv[1], args: argv.slice(2) };
}

export function buildTestEnvironment(base, sandbox) {
  return {
    ...base,
    BASE_URL: UI_ORIGIN,
    FLEETSHIFT_GRPC_TARGET: GRPC_TARGET,
    FLEETSHIFT_CA_FILE: sandbox.caFile,
    FLEETSHIFT_E2E_RUN_ID: sandbox.runId,
    FLEETSHIFT_E2E_WORK_DIR: sandbox.workDir,
    FLEETSHIFT_KIND_PREFIX: sandbox.kindPrefix,
  };
}

export function kindNodeBelongsToRun(hostClusterName, kindPrefix) {
  return hostClusterName.startsWith(`fs--${kindPrefix}`);
}

export function usesPrebuiltImage(env) {
  return env.FLEETSHIFT_E2E_AIO_PREBUILT === "1";
}

export function normalizeSocketPath(value) {
  const candidate = value.trim().replace(/^unix:\/\//, "");
  return candidate && !candidate.includes("://") ? candidate : "";
}

function prepareImage() {
  if (usesPrebuiltImage(process.env)) return;
  console.error(`building ${IMAGE}`);
  const result = spawnSync("npx", ["nx", "run", "fleetshift-poc:image:aio"], {
    cwd: REPO_ROOT,
    env: { ...process.env, NX_DAEMON: "false" },
    stdio: "inherit",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error("AIO image build failed");
}

function podman(
  args,
  { allowFailure = false, timeout = COMMAND_TIMEOUT_MS } = {},
) {
  const result = spawnSync("podman", args, {
    encoding: "utf8",
    maxBuffer: 1024 * 1024,
    timeout,
  });
  const stdout = (result.stdout ?? "").trim();
  const output =
    `${stdout}${result.stderr ?? ""}${result.error?.message ?? ""}`.trim();
  if (result.error && !allowFailure) throw result.error;
  if (!allowFailure && result.status !== 0) {
    throw new Error(`podman ${args[0]} failed${output ? `\n${output}` : ""}`);
  }
  return { output, status: result.status ?? -1, stdout };
}

async function engineSocket() {
  const candidates = [
    normalizeSocketPath(process.env.PODMAN_SOCKET ?? ""),
    process.env.XDG_RUNTIME_DIR
      ? path.join(process.env.XDG_RUNTIME_DIR, "podman/podman.sock")
      : "",
    "/var/run/docker.sock",
  ];
  for (const candidate of candidates) {
    if (
      candidate &&
      (await stat(candidate).catch(() => undefined))?.isSocket()
    ) {
      return candidate;
    }
  }

  const reported = normalizeSocketPath(
    podman(["info", "--format", "{{.Host.RemoteSocket.Path}}"], {
      allowFailure: true,
    }).stdout,
  );
  if (reported && (await stat(reported).catch(() => undefined))?.isSocket()) {
    return reported;
  }
  throw new Error(
    "no Podman/Docker Unix socket found; set PODMAN_SOCKET or start the Podman machine/socket",
  );
}

async function poll(label, timeoutMs, action, signal) {
  const deadline = Date.now() + timeoutMs;
  let cause;
  while (Date.now() < deadline) {
    signal?.throwIfAborted();
    try {
      await action();
      return;
    } catch (error) {
      cause = error;
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  }
  throw new Error(`${label} timed out`, { cause });
}

function waitForReady(ca) {
  return new Promise((resolve, reject) => {
    const request = https.get(`${UI_ORIGIN}/readyz`, {
      ca,
      family: 4,
      rejectUnauthorized: true,
      timeout: 5_000,
    });
    request.once("timeout", () =>
      request.destroy(new Error("ready request timed out")),
    );
    request.once("error", reject);
    request.once("response", (response) => {
      response.resume();
      response.once("end", () => {
        if (response.statusCode === 200) resolve();
        else reject(new Error(`/readyz returned ${response.statusCode ?? 0}`));
      });
    });
  });
}

export function sanitize(value) {
  return value
    .replace(/Bearer\s+\S+/gi, "Bearer [REDACTED]")
    .replace(/https:\/\/\S+\/auth\?\S+/gi, "[AUTH URL REDACTED]")
    .replace(/https?:\/\/\S+\/callback\?\S+/gi, "[CALLBACK URL REDACTED]")
    .replace(/([?&](?:code|state|code_challenge)=)[^&\s]+/gi, "$1[REDACTED]")
    .replace(/("access_token"\s*:\s*")[^"]+/gi, "$1[REDACTED]");
}

function ownedKindNodes(kindPrefix) {
  const result = podman(
    [
      "ps",
      "-a",
      "--filter",
      `label=${KIND_CLUSTER_LABEL}`,
      "--format",
      `{{.ID}}\t{{.Label \"${KIND_CLUSTER_LABEL}\"}}`,
    ],
    { allowFailure: true },
  );
  if (result.status !== 0) return [];
  return result.output
    .split("\n")
    .map((line) => line.split("\t", 2).map((part) => part.trim()))
    .filter(
      ([id, cluster]) => id && kindNodeBelongsToRun(cluster ?? "", kindPrefix),
    );
}

async function createSandbox() {
  const runId = randomBytes(6).toString("hex");
  const workDir = await mkdtemp(
    path.join(os.tmpdir(), `fleetshift-e2e-${runId}-`),
  );
  await chmod(workDir, 0o700);
  return {
    caFile: path.join(workDir, "ca.crt"),
    containerName: `fleetshift-e2e-${runId}`,
    kindPrefix: `kind-e2e-${runId}-`,
    runId,
    workDir,
  };
}

async function startSandbox(sandbox, signal) {
  podman(["image", "exists", IMAGE]);
  if (
    podman(["network", "exists", "kind"], { allowFailure: true }).status !== 0
  ) {
    podman(["network", "create", "kind"]);
  }
  const socket = await engineSocket();
  signal.throwIfAborted();
  podman(
    [
      "run",
      "-d",
      "--pull=never",
      "--privileged",
      "--name",
      sandbox.containerName,
      "--label",
      `fleetshift.e2e.run=${sandbox.runId}`,
      "--network",
      "kind:alias=fleetshift",
      "-p",
      "127.0.0.1:8085:8085",
      "-p",
      "127.0.0.1:50051:50051",
      "-v",
      `${socket}:/var/run/docker.sock`,
      "-v",
      "/tmp:/tmp",
      IMAGE,
    ],
    { timeout: 30_000 },
  );
  await poll(
    "AIO exec readiness",
    30_000,
    () => podman(["exec", sandbox.containerName, "true"]),
    signal,
  );
  podman(
    [
      "exec",
      sandbox.containerName,
      "podman",
      "ps",
      "-a",
      "--filter",
      `label=${KIND_CLUSTER_LABEL}`,
    ],
    { timeout: 30_000 },
  );
  await poll(
    "copy sandbox CA",
    20_000,
    async () => {
      podman([
        "cp",
        `${sandbox.containerName}:${CA_IN_CONTAINER}`,
        sandbox.caFile,
      ]);
      const ca = await readFile(sandbox.caFile);
      if (!ca.includes("BEGIN CERTIFICATE"))
        throw new Error("invalid sandbox CA");
      await chmod(sandbox.caFile, 0o600);
    },
    signal,
  );
  const ca = await readFile(sandbox.caFile);
  await poll("sandbox readiness", 30_000, () => waitForReady(ca), signal);
}

function dumpDiagnostics(sandbox) {
  const show = (label, args) => {
    const output = podman(args, { allowFailure: true }).output;
    console.error(
      `===== ${label} =====\n${sanitize(output).slice(0, 64 * 1024) || "(no output)"}`,
    );
  };
  if (
    podman(["container", "exists", sandbox.containerName], {
      allowFailure: true,
    }).status === 0
  ) {
    show("AIO logs", ["logs", "--tail", "400", sandbox.containerName]);
    show("AIO Kind engine", [
      "exec",
      sandbox.containerName,
      "podman",
      "ps",
      "-a",
      "--filter",
      `label=${KIND_CLUSTER_LABEL}`,
    ]);
  }
  const nodes = ownedKindNodes(sandbox.kindPrefix);
  console.error(
    `===== host Kind nodes for ${sandbox.runId} =====\n${nodes.map(([id, cluster]) => `${id}\t${cluster}`).join("\n") || "(no nodes)"}`,
  );
}

function removeContainer(id) {
  return new Promise((resolve) => {
    const child = spawn("podman", ["rm", "-f", id], { stdio: "ignore" });
    const timer = setTimeout(() => child.kill("SIGKILL"), COMMAND_TIMEOUT_MS);
    const done = () => {
      clearTimeout(timer);
      resolve();
    };
    child.once("error", done);
    child.once("close", done);
  });
}

async function stopSandbox(sandbox) {
  const removed = podman(["rm", "-f", sandbox.containerName], {
    allowFailure: true,
    timeout: SANDBOX_REMOVE_TIMEOUT_MS,
  });
  if (removed.status !== 0) {
    console.error(
      `failed to remove sandbox ${sandbox.containerName}${removed.output ? `\n${sanitize(removed.output)}` : ""}`,
    );
  }
  await Promise.all(
    ownedKindNodes(sandbox.kindPrefix).map(([id]) => removeContainer(id)),
  );
  await rm(sandbox.workDir, { force: true, recursive: true });
}

function runCommand({ command, args }, env, signal) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env, signal, stdio: "inherit" });
    child.once("error", reject);
    child.once("close", (code, signal) => {
      resolve({ code: code ?? 1, signal });
    });
  });
}

async function main() {
  const command = parseCommand(process.argv.slice(2));
  prepareImage();
  const sandbox = await createSandbox();
  const controller = new AbortController();
  const cancel = () => controller.abort();
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  try {
    await startSandbox(sandbox, controller.signal);
    console.error(`sandbox ${sandbox.runId} ready at ${UI_ORIGIN}`);
    const result = await runCommand(
      command,
      buildTestEnvironment(process.env, sandbox),
      controller.signal,
    );
    if (result.code !== 0 || result.signal !== null) dumpDiagnostics(sandbox);
    return result.code;
  } catch (error) {
    console.error(
      error instanceof Error ? (error.stack ?? error.message) : error,
    );
    dumpDiagnostics(sandbox);
    return 1;
  } finally {
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
    if (process.env.FLEETSHIFT_E2E_KEEP === "1") {
      console.error(
        `keeping sandbox ${sandbox.containerName}, Kind prefix ${sandbox.kindPrefix}, and ${sandbox.workDir}`,
      );
    } else {
      await stopSandbox(sandbox);
    }
  }
}

if (
  process.argv[1] &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  try {
    process.exitCode = await main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  }
}
