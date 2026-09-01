import { $, sleep } from "zx";
import { readFile } from "node:fs/promises";

$.verbose = true;

export async function loadDotenv(path) {
  const values = {};
  for (const line of await readFile(path, "utf8").then((text) =>
    text.split(/\r?\n/),
  )) {
    const match = line.match(
      /^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/,
    );
    if (!match || process.env[match[1]] !== undefined) continue;
    let value = match[2].trim();
    const quote = value[0];
    if (quote === '"' || quote === "'") {
      const closing = value.lastIndexOf(quote);
      if (
        closing === 0 ||
        value
          .slice(closing + 1)
          .trim()
          .replace(/^#.*$/, "")
      )
        throw new Error(`Unclosed quote in dotenv entry ${match[1]}`);
      value = value.slice(1, closing);
      if (quote === '"')
        value = value.replace(
          /\\([\\"nrt])/g,
          (_, escaped) =>
            ({ "\\": "\\", '"': '"', n: "\n", r: "\r", t: "\t" })[escaped],
        );
    } else {
      value = value.replace(/\s+#.*$/, "");
    }
    values[match[1]] = value;
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
  const normalize = (value) => {
    try {
      const url = new URL(value.trim());
      return `${url.protocol}//${url.host}${url.pathname.replace(/\/+$/, "")}`;
    } catch {
      throw new Error(`${variable} must be a valid cluster URL.`);
    }
  };
  const server = normalize((await $`oc whoami --show-server`).stdout);
  if (server !== normalize(process.env[variable]))
    throw new Error(
      `Wrong cluster. Current oc server does not match ${variable}.`,
    );
}

export async function waitFor(predicate, timeout, interval = 1) {
  const deadline = Date.now() + timeout * 1000;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await sleep(Math.min(interval * 1000, Math.max(0, deadline - Date.now())));
  }
  return false;
}

export function importKeyValueArgs(args) {
  const keys = new Set([
    "OME_CLUSTER_API",
    "KC_CLUSTER_API",
    "CLUSTER_NAME",
    "ACME_EMAIL",
    "GCPHCP_ENABLED",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "OIDC_ISSUER_URL",
    "OIDC_UI_CLIENT_ID",
    "OIDC_UI_SCOPE",
    "OIDC_CLI_CLIENT_ID",
    "OIDC_RESOURCE_AUDIENCE",
    "OIDC_AUDIENCE",
    "OIDC_KEY_ENROLLMENT_AUDIENCE",
    "PUBLIC_KEY_CLAIM_EXPR",
    "KEY_REGISTRY_ID",
    "KEY_REGISTRY_SUBJECT_EXPR",
    "FLEETSHIFT_LOG_LEVEL",
    "NAMESPACE",
    "TAG",
    "USER",
    "PASSWORD",
    "FRESH_CERT",
  ]);
  return args.filter((arg) => {
    const index = arg.indexOf("=");
    if (index <= 0 || !keys.has(arg.slice(0, index))) return true;
    process.env[arg.slice(0, index)] = arg.slice(index + 1);
    return false;
  });
}

export function isTruthy(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").toLowerCase());
}
