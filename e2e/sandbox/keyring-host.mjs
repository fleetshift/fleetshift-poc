import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import os from "node:os";

export const REQUIRED_MAXKEYS = 2000;
export const REQUIRED_MAXBYTES = 200000;
export const ALLOW_LOW_KEYRING_ENV = "FLEETSHIFT_E2E_ALLOW_LOW_KEYRING";
export const KEYRING_DOCS_PATH = "docs/testing/end-to-end.md";
export const SYSCTL_D_BASENAME = "zz-fleetshift-e2e-keys.conf";
export const SYSCTL_D_FILE = `/etc/sysctl.d/${SYSCTL_D_BASENAME}`;
export const MACHINE_KEYRING_SEPARATOR = "FLEETSHIFT-E2E-KEYRING";

const MAXKEYS_SYSCTL = "kernel.keys.maxkeys";
const MAXBYTES_SYSCTL = "kernel.keys.maxbytes";
const PROC_MAXKEYS = "/proc/sys/kernel/keys/maxkeys";
const PROC_MAXBYTES = "/proc/sys/kernel/keys/maxbytes";
const PROC_KEY_USERS = "/proc/key-users";

/** Parse a /proc sysctl file or `sysctl` assignment into a non-negative integer. */
export function parseSysctlValue(raw) {
  const text = String(raw ?? "").trim();
  const valuePart = text.includes("=")
    ? text.slice(text.indexOf("=") + 1).trim()
    : text;
  if (!/^[0-9]+$/.test(valuePart)) {
    throw new Error(`invalid sysctl value ${JSON.stringify(raw)}`);
  }
  return Number.parseInt(valuePart, 10);
}

/** True when both quotas are at least the required minimums. */
export function hasSufficientKeyringLimits(limits) {
  return (
    limits.maxkeys >= REQUIRED_MAXKEYS && limits.maxbytes >= REQUIRED_MAXBYTES
  );
}

/**
 * Sysctl assignments needed to reach the required minimums.
 * Already-higher values are left unchanged.
 */
export function sysctlWritesNeeded(current) {
  const writes = [];
  if (current.maxkeys < REQUIRED_MAXKEYS) {
    writes.push({ key: MAXKEYS_SYSCTL, value: REQUIRED_MAXKEYS });
  }
  if (current.maxbytes < REQUIRED_MAXBYTES) {
    writes.push({ key: MAXBYTES_SYSCTL, value: REQUIRED_MAXBYTES });
  }
  return writes;
}

/** Local override is ignored when CI or GitHub Actions is set. */
export function honorsLowKeyringOverride(env = {}) {
  if (env.CI === "true" || env.GITHUB_ACTIONS === "true") return false;
  return env[ALLOW_LOW_KEYRING_ENV] === "1";
}

/** True when captured output is kernel keyring quota exhaustion. */
export function isKeyringQuotaError(text) {
  const raw = String(text ?? "");
  return /keyctl/i.test(raw) && /Disk quota exceeded/i.test(raw);
}

/** The `/proc/key-users` line for `uid`, or empty when absent. */
export function keyUsersLine(raw, uid) {
  const prefix = `${uid}:`;
  for (const line of String(raw ?? "").split("\n")) {
    if (line.trim().startsWith(prefix)) return line.trim();
  }
  return "";
}

/** True when `candidate` would be applied after `reference` in sysctl.d order. */
export function sysctlDBasenameSortsLater(candidate, reference) {
  return candidate.localeCompare(reference) > 0;
}

/** Parse `key = value` assignments from a sysctl.conf-style file. */
export function parseSysctlAssignments(raw, path = "") {
  const assignments = [];
  for (const line of String(raw ?? "").split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#") || trimmed.startsWith(";")) {
      continue;
    }
    const match = /^([A-Za-z0-9._-]+)\s*=\s*(.*)$/.exec(trimmed);
    if (!match) continue;
    assignments.push({
      key: match[1],
      path,
      value: match[2].trim(),
    });
  }
  return assignments;
}

function sysctlDirRank(path) {
  if (path.startsWith("/etc/")) return 3;
  if (path.startsWith("/run/")) return 2;
  return 1;
}

/** Keyring assignments from a map of file path to file contents. */
export function keyringSysctlDefinitions(files) {
  const definitions = [];
  for (const [path, content] of Object.entries(files)) {
    const basename = path.split("/").pop() ?? path;
    for (const assignment of parseSysctlAssignments(content, path)) {
      if (
        assignment.key === MAXKEYS_SYSCTL ||
        assignment.key === MAXBYTES_SYSCTL
      ) {
        definitions.push({ ...assignment, basename });
      }
    }
  }
  return definitions;
}

/** The definition that wins for `key` under sysctl.d lexicographic order. */
export function winningKeyringDefinition(definitions, key) {
  const matches = definitions.filter((item) => item.key === key);
  if (matches.length === 0) return undefined;
  return [...matches]
    .sort((left, right) => {
      const byName = left.basename.localeCompare(right.basename);
      if (byName !== 0) return byName;
      return sysctlDirRank(left.path) - sysctlDirRank(right.path);
    })
    .at(-1);
}

/** Keyring definitions whose filenames sort after our late-sorting drop-in. */
export function laterConflictingKeyringDefinitions(
  definitions,
  ourBasename = SYSCTL_D_BASENAME,
) {
  return definitions.filter(
    (item) =>
      item.basename !== ourBasename &&
      sysctlDBasenameSortsLater(item.basename, ourBasename),
  );
}

/** Render competing sysctl sources; does not claim persistence succeeded. */
export function formatCompetingKeyringDefinitions(definitions) {
  if (definitions.length === 0) return "";
  return [
    "Competing kernel keyring definitions (later filenames win):",
    ...definitions.map((item) => `  ${item.path}: ${item.key} = ${item.value}`),
  ].join("\n");
}

function connectionMatchesMachine(connection, machine) {
  return (
    connection.name === machine.name ||
    connection.name === `${machine.name}-root`
  );
}

/**
 * Classify the Podman host that owns Kind node containers.
 * `skipped` means the non-root keyring check does not apply.
 */
export function classifyPodmanHost({
  connections = [],
  engine,
  machines = [],
  platform,
  rootless,
  uid,
}) {
  if (engine !== "podman") {
    return {
      kind: "skipped",
      label: "unrelated container engine",
      reason: "engine",
    };
  }
  if (uid === 0 || rootless === false) {
    return { kind: "skipped", label: "rootful Podman", reason: "rootful" };
  }
  if (rootless !== true) {
    return {
      kind: "skipped",
      label: "undetermined Podman host",
      reason: "unknown",
    };
  }

  const runningMachines = machines.filter((machine) => machine.running);
  const defaultConnection =
    connections.find((connection) => connection.isDefault) ?? connections[0];
  const matchedMachine = defaultConnection
    ? runningMachines.find((machine) =>
        connectionMatchesMachine(defaultConnection, machine),
      )
    : undefined;
  if (matchedMachine) {
    return {
      kind: "podman-machine",
      label: `Podman machine ${matchedMachine.name}`,
      machineName: matchedMachine.name,
    };
  }

  if (platform === "darwin" && runningMachines.length > 0) {
    const machine =
      runningMachines.find((item) => item.isDefault) ?? runningMachines[0];
    return {
      kind: "podman-machine",
      label: `Podman machine ${machine.name}`,
      machineName: machine.name,
    };
  }

  const uri = defaultConnection?.uri ?? "";
  if (/^ssh:\/\//i.test(uri)) {
    return {
      kind: "unmanaged-remote",
      label: `unmanaged remote Podman (${uri})`,
      uri,
    };
  }

  if (platform === "linux") {
    return { kind: "native-linux", label: "native Linux rootless Podman" };
  }

  return {
    kind: "skipped",
    label: "unsupported Podman host",
    reason: "unsupported",
  };
}

function formatCurrentValues(limits) {
  if (!limits) {
    return [
      `  ${MAXKEYS_SYSCTL}:  (unknown; required >= ${REQUIRED_MAXKEYS})`,
      `  ${MAXBYTES_SYSCTL}: (unknown; required >= ${REQUIRED_MAXBYTES})`,
    ].join("\n");
  }
  return [
    `  ${MAXKEYS_SYSCTL}:  ${limits.maxkeys} (required >= ${REQUIRED_MAXKEYS})`,
    `  ${MAXBYTES_SYSCTL}: ${limits.maxbytes} (required >= ${REQUIRED_MAXBYTES})`,
  ].join("\n");
}

function nativeLinuxCommands() {
  return [
    "Temporary (until reboot):",
    `  sudo sysctl -w ${MAXKEYS_SYSCTL}=${REQUIRED_MAXKEYS}`,
    `  sudo sysctl -w ${MAXBYTES_SYSCTL}=${REQUIRED_MAXBYTES}`,
    "",
    `Persistent (late-sorting ${SYSCTL_D_BASENAME}; sysctl.d is lexicographic, so 99-keys.conf would override a 99-... file):`,
    `  printf '%s\\n' '${MAXKEYS_SYSCTL} = ${REQUIRED_MAXKEYS}' '${MAXBYTES_SYSCTL} = ${REQUIRED_MAXBYTES}' | sudo tee ${SYSCTL_D_FILE} >/dev/null`,
    `  sudo sysctl -p ${SYSCTL_D_FILE}`,
    `  sysctl -n ${MAXKEYS_SYSCTL}`,
    `  sysctl -n ${MAXBYTES_SYSCTL}`,
    "",
    "On a shared Linux system, ask an administrator rather than changing system-wide limits casually.",
  ].join("\n");
}

function machineSsh(machineName, command) {
  return `podman machine ssh ${machineName} -- ${command}`;
}

/**
 * Remote script for `podman machine ssh`.
 * Passed as one argv after `--` because Podman joins args with spaces and the
 * remote shell then parses `#` as a comment. A `sh -c` plus `###...###`
 * separator therefore prints no separators and looks like empty keyring output.
 */
export function machineKeyringRemoteCommand() {
  return [
    `cat ${PROC_MAXKEYS}`,
    `echo ${MACHINE_KEYRING_SEPARATOR}`,
    `cat ${PROC_MAXBYTES}`,
    `echo ${MACHINE_KEYRING_SEPARATOR}`,
    `cat ${PROC_KEY_USERS}`,
  ].join("; ");
}

function podmanMachineCommands(machineName) {
  const sysctlMaxkeys = `sudo sysctl -w ${MAXKEYS_SYSCTL}=${REQUIRED_MAXKEYS}`;
  const sysctlMaxbytes = `sudo sysctl -w ${MAXBYTES_SYSCTL}=${REQUIRED_MAXBYTES}`;
  const apply = `sudo sysctl -p ${SYSCTL_D_FILE}`;
  const verifyMaxkeys = `sysctl -n ${MAXKEYS_SYSCTL}`;
  const verifyMaxbytes = `sysctl -n ${MAXBYTES_SYSCTL}`;
  const lines = [
    "Temporary (until VM reboot):",
    `  ${machineSsh(machineName, sysctlMaxkeys)}`,
    `  ${machineSsh(machineName, sysctlMaxbytes)}`,
    "",
    "Custom machine-name syntax:",
    `  podman machine ssh <machine-name> -- ${sysctlMaxkeys}`,
    `  podman machine ssh <machine-name> -- ${sysctlMaxbytes}`,
    "",
    "Persistent (survives machine restart, not deletion or reset):",
    `  printf '%s\\n' '${MAXKEYS_SYSCTL} = ${REQUIRED_MAXKEYS}' '${MAXBYTES_SYSCTL} = ${REQUIRED_MAXBYTES}' | ${machineSsh(machineName, `sudo tee ${SYSCTL_D_FILE}`)} >/dev/null`,
    `  ${machineSsh(machineName, apply)}`,
    `  ${machineSsh(machineName, verifyMaxkeys)}`,
    `  ${machineSsh(machineName, verifyMaxbytes)}`,
  ];
  return lines.join("\n");
}

function remoteHostCommands() {
  return [
    "Kernel keyring quotas must be checked and changed on that host, not on this machine.",
    "",
    nativeLinuxCommands(),
  ].join("\n");
}

function explanation() {
  return [
    'The error "crun: join keyctl: Disk quota exceeded" is kernel keyring quota exhaustion, not filesystem capacity.',
    "These per-user quotas limit how many simultaneous rootless Kind node containers can exist (one container consumed per Kind cluster node).",
  ].join("\n");
}

/** Diagnosis, limits, and exact remediation for the detected Podman host. */
export function formatKeyringGuidance(host, limits, options = {}) {
  const commands =
    host.kind === "podman-machine"
      ? podmanMachineCommands(host.machineName || "<machine-name>")
      : host.kind === "unmanaged-remote"
        ? remoteHostCommands()
        : nativeLinuxCommands();
  const competing = formatCompetingKeyringDefinitions(options.competing ?? []);
  return [
    `Kernel keyring quotas on ${host.label} are too low for rootless Kind E2E.`,
    "",
    `Detected Podman host: ${host.label}`,
    formatCurrentValues(limits),
    "",
    explanation(),
    "",
    commands,
    ...(competing ? ["", competing] : []),
    "",
    "To continue anyway on a local machine (ignored in CI):",
    `  ${ALLOW_LOW_KEYRING_ENV}=1 npx nx test:e2e e2e-cli`,
    "",
    `See ${KEYRING_DOCS_PATH}`,
  ].join("\n");
}

/**
 * Snapshot of current quotas plus remediation when captured logs show a
 * keyctl disk-quota error.
 */
export function formatKeyringDiagnostics({
  captured = "",
  host,
  keyUsers = "",
  limits,
  uid,
}) {
  const lines = ["===== kernel keyring ====="];
  if (host?.label) lines.push(`host: ${host.label}`);
  if (limits) {
    lines.push(
      `${MAXKEYS_SYSCTL}=${limits.maxkeys} ${MAXBYTES_SYSCTL}=${limits.maxbytes}`,
    );
  } else {
    lines.push("(limits not read)");
  }
  const users = uid != null ? keyUsersLine(keyUsers, uid) : "";
  lines.push(`/proc/key-users uid=${uid ?? "?"}: ${users || "(no entry)"}`);
  if (host && isKeyringQuotaError(captured)) {
    lines.push("", formatKeyringGuidance(host, limits));
  }
  return lines.join("\n");
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value && typeof value === "object") return Object.values(value);
  return [];
}

function parseJsonList(stdout) {
  const text = String(stdout ?? "").trim();
  if (!text) return [];
  let value;
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error(`invalid JSON: ${JSON.stringify(text.slice(0, 80))}`);
  }
  return asArray(value);
}

function parseEngine(stdout, status) {
  if (status !== 0) return { engine: "unknown", rootless: null };
  const text = String(stdout ?? "").trim();
  if (!text) return { engine: "unknown", rootless: null };
  try {
    const info = JSON.parse(text);
    if (!info?.host) return { engine: "unknown", rootless: null };
    const rootless = info.host?.security?.rootless;
    return {
      engine: "podman",
      rootless: typeof rootless === "boolean" ? rootless : null,
    };
  } catch {
    return { engine: "unknown", rootless: null };
  }
}

function parseMachines(stdout) {
  return parseJsonList(stdout)
    .map((machine) => ({
      isDefault: Boolean(machine.Default ?? machine.default),
      name: String(machine.Name ?? machine.name ?? ""),
      running: Boolean(machine.Running ?? machine.running),
    }))
    .filter((machine) => machine.name);
}

function parseConnections(stdout) {
  return parseJsonList(stdout).map((connection) => ({
    isDefault: Boolean(connection.Default ?? connection.default),
    name: String(connection.Name ?? connection.name ?? ""),
    uri: String(connection.URI ?? connection.Uri ?? connection.uri ?? ""),
  }));
}

function defaultIO(overrides = {}) {
  return {
    env: process.env,
    error(message) {
      console.error(message);
    },
    log(message) {
      console.error(message);
    },
    platform: process.platform,
    readFile(path) {
      return readFileSync(path, "utf8");
    },
    run(command, args) {
      const result = spawnSync(command, args, {
        encoding: "utf8",
        maxBuffer: 1024 * 1024,
        timeout: 15_000,
      });
      return {
        status: result.status ?? (result.error ? -1 : 0),
        stderr: result.stderr ?? "",
        stdout: result.stdout ?? "",
      };
    },
    uid:
      typeof process.getuid === "function"
        ? process.getuid()
        : os.userInfo().uid,
    ...overrides,
  };
}

function runPodman(io, args) {
  return io.run("podman", args);
}

function readProc(io, path) {
  try {
    return io.readFile(path);
  } catch (error) {
    throw new Error(
      `failed to read ${path}: ${error instanceof Error ? error.message : error}`,
    );
  }
}

function clipCommandOutput(text) {
  const trimmed = String(text ?? "").trim();
  if (!trimmed) return "(empty)";
  return trimmed.length > 2048 ? `${trimmed.slice(0, 2048)}…` : trimmed;
}

function formatCommandOutput(result) {
  return `: stdout: ${clipCommandOutput(result.stdout)}; stderr: ${clipCommandOutput(result.stderr)}`;
}

function lastNonEmptyLine(raw) {
  const lines = String(raw ?? "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.at(-1) ?? "";
}

function parseMachineKeyringOutput(stdout) {
  const parts = String(stdout)
    .split(MACHINE_KEYRING_SEPARATOR)
    .map((part) => part.trim());
  if (parts.length < 3) {
    throw new Error("missing keyring separators");
  }
  return {
    keyUsers: parts.slice(2).join(MACHINE_KEYRING_SEPARATOR),
    limits: {
      maxbytes: parseSysctlValue(lastNonEmptyLine(parts[1])),
      maxkeys: parseSysctlValue(lastNonEmptyLine(parts[0])),
    },
  };
}

function machineReadKeyring(io, machineName) {
  const result = runPodman(io, [
    "machine",
    "ssh",
    machineName,
    "--",
    machineKeyringRemoteCommand(),
  ]);
  if (result.status !== 0) {
    throw new Error(
      `podman machine ssh ${machineName} failed to read keyring limits${formatCommandOutput(result)}`,
    );
  }
  try {
    return parseMachineKeyringOutput(result.stdout);
  } catch (error) {
    throw new Error(
      `podman machine ssh ${machineName} returned unexpected keyring output: ${
        error instanceof Error ? error.message : error
      }${formatCommandOutput(result)}`,
    );
  }
}

function readLimits(host, io) {
  if (host.kind === "native-linux") {
    return {
      keyUsers: (() => {
        try {
          return io.readFile(PROC_KEY_USERS);
        } catch {
          return "";
        }
      })(),
      limits: {
        maxbytes: parseSysctlValue(readProc(io, PROC_MAXBYTES)),
        maxkeys: parseSysctlValue(readProc(io, PROC_MAXKEYS)),
      },
    };
  }
  if (host.kind === "podman-machine") {
    return machineReadKeyring(io, host.machineName);
  }
  return { keyUsers: "", limits: undefined };
}

/** Detect the Podman host and read current keyring quotas when possible. */
export function inspectKeyring(overrides = {}) {
  const io = defaultIO(overrides);
  const info = runPodman(io, ["info", "--format", "json"]);
  const { engine, rootless } = parseEngine(info.stdout, info.status);
  const machines = parseMachines(
    runPodman(io, ["machine", "list", "--format", "json"]).stdout,
  );
  const connections = parseConnections(
    runPodman(io, ["system", "connection", "list", "--format", "json"]).stdout,
  );
  const host = classifyPodmanHost({
    connections,
    engine,
    machines,
    platform: io.platform,
    rootless,
    uid: io.uid,
  });
  const { keyUsers, limits } = readLimits(host, io);
  return { host, keyUsers, limits, uid: io.uid };
}

/**
 * Local read-only preflight. Never invokes sudo or writes sysctls.
 * Throws when quotas are too low unless the local override is set.
 */
export function preflightKeyring(overrides = {}) {
  const io = defaultIO(overrides);
  const inspected = inspectKeyring(io);
  if (inspected.host.kind === "skipped") return { action: "skipped" };
  if (inspected.host.kind === "unmanaged-remote") {
    io.error(formatKeyringGuidance(inspected.host, inspected.limits));
    return { action: "reported-remote" };
  }
  if (inspected.limits && hasSufficientKeyringLimits(inspected.limits)) {
    return { action: "ok" };
  }
  const guidance = formatKeyringGuidance(inspected.host, inspected.limits);
  if (honorsLowKeyringOverride(io.env)) {
    io.error(
      `WARNING: ${ALLOW_LOW_KEYRING_ENV}=1 is set; continuing with insufficient kernel keyring quotas.\n\n${guidance}`,
    );
    return { action: "warned" };
  }
  throw new Error(guidance);
}

/**
 * CI-only: raise local sysctls to at least the required minimums, verify, and
 * log the effective values plus this uid's `/proc/key-users` line.
 * Never lowers a higher value. Ignores the local override.
 */
export function ensureCiKeyringLimits(overrides = {}) {
  const io = defaultIO(overrides);
  const read = () => ({
    maxbytes: parseSysctlValue(readProc(io, PROC_MAXBYTES)),
    maxkeys: parseSysctlValue(readProc(io, PROC_MAXKEYS)),
  });
  let current = read();
  for (const write of sysctlWritesNeeded(current)) {
    const assignment = `${write.key}=${write.value}`;
    const result = io.run("sudo", ["sysctl", "-w", assignment]);
    if (result.status !== 0) {
      throw new Error(
        `failed to configure ${write.key}: sudo sysctl -w ${assignment} failed${result.stderr ? `\n${result.stderr}` : ""}`,
      );
    }
  }
  current = read();
  if (!hasSufficientKeyringLimits(current)) {
    throw new Error(
      `failed to configure kernel keyring quotas: ${MAXKEYS_SYSCTL} is ${current.maxkeys}, required >= ${REQUIRED_MAXKEYS}; ${MAXBYTES_SYSCTL} is ${current.maxbytes}, required >= ${REQUIRED_MAXBYTES}`,
    );
  }
  let keyUsers = "";
  try {
    keyUsers = io.readFile(PROC_KEY_USERS);
  } catch {
    keyUsers = "";
  }
  io.log(
    `kernel keyring: ${MAXKEYS_SYSCTL}=${current.maxkeys} ${MAXBYTES_SYSCTL}=${current.maxbytes}`,
  );
  io.log(
    `kernel keyring: uid=${io.uid} /proc/key-users: ${keyUsersLine(keyUsers, io.uid) || "(no entry)"}`,
  );
}
