import { $, sleep } from "zx";
import { readFile } from "node:fs/promises";

$.verbose = true;

export async function loadDotenv(path) {
  const values = {};
  for (const line of await readFile(path, "utf8").then((text) =>
    text.split(/\r?\n/),
  )) {
    const match = line.match(
      /^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)\s*$/,
    );
    if (!match || process.env[match[1]] !== undefined) continue;
    values[match[1]] = match[2].replace(/^(['"])(.*)\1$/, "$2");
  }
  Object.assign(process.env, values);
}

export async function requireOcLogin() {
  try {
    await $`oc whoami`;
  } catch {
    throw new Error("Not logged in to OpenShift. Run 'oc login' first.");
  }
}

export async function requireOcCluster(variable) {
  await requireOcLogin();
  if (!process.env[variable])
    throw new Error(
      `Wrong cluster or ${variable} not set. Set ${variable} and log in with oc.`,
    );
  const server = (await $`oc whoami --show-server`).stdout;
  if (!server.includes(process.env[variable]))
    throw new Error(
      `Wrong cluster. Current oc server does not match ${variable}.`,
    );
}

export async function waitFor(predicate, timeout, interval) {
  const deadline = Date.now() + timeout * 1000;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await sleep(interval * 1000);
  }
  return false;
}

export function importKeyValueArgs(args) {
  return args.filter((arg) => {
    if (!arg.includes("=")) return true;
    const index = arg.indexOf("=");
    process.env[arg.slice(0, index)] = arg.slice(index + 1);
    return false;
  });
}

export function isTruthy(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").toLowerCase());
}
