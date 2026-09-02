import { isIP } from "node:net";

import { commandFailure, requireCommandSuccess, runCommand } from "./command";
import { type HTTPSResponse, requestHTTPS } from "./https";

const KIND_API_PORT = "6443";
const KIND_CLUSTER_LABEL = "io.x-k8s.kind.cluster";
const KIND_ROLE_LABEL = "io.x-k8s.kind.role";
const KIND_CONTROL_PLANE_ROLE = "control-plane";

function parseAddress(value: string): { host: string; port: string } | null {
  const ipv6 = /^\[([^\]]+)]:(\d+)$/.exec(value);
  if (ipv6) return { host: ipv6[1], port: ipv6[2] };
  const ipv4 = /^([^:]+):(\d+)$/.exec(value);
  if (ipv4) return { host: ipv4[1], port: ipv4[2] };
  return null;
}

/** Parse `podman port` output into a host-reachable Kubernetes API URL. */
export function parsePodmanPort(raw: string): string {
  const candidates: Array<{ preferIPv4: boolean; url: string }> = [];
  for (const rawLine of raw.split("\n")) {
    let line = rawLine.trim();
    if (!line) continue;
    if (line.includes("->")) line = line.slice(line.indexOf("->") + 2).trim();
    const parsed = parseAddress(line);
    if (!parsed) continue;
    const originalHost = parsed.host;
    const host = ["", "0.0.0.0", "*", "::"].includes(originalHost)
      ? "127.0.0.1"
      : originalHost;
    const url =
      isIP(host) === 6
        ? `https://[${host}]:${parsed.port}`
        : `https://${host}:${parsed.port}`;
    candidates.push({
      preferIPv4: isIP(originalHost) === 4 || originalHost === "0.0.0.0",
      url,
    });
  }
  if (candidates.length === 0) {
    throw new Error(
      `cannot parse podman port output ${JSON.stringify(raw.trim())}`,
    );
  }
  return (
    candidates.find(({ preferIPv4 }) => preferIPv4)?.url ?? candidates[0].url
  );
}

export function hostKindClusterName(clusterId: string): string {
  return `fs--${clusterId}`;
}

async function containerIDs(filters: readonly string[]): Promise<string[]> {
  const result = requireCommandSuccess(
    "podman ps",
    await runCommand("podman", ["ps", "-a", ...filters, "--format", "{{.ID}}"]),
  );
  return result.stdout.trim().split(/\s+/).filter(Boolean);
}

export async function kindNodeIDs(hostName: string): Promise<string[]> {
  return containerIDs(["--filter", `label=${KIND_CLUSTER_LABEL}=${hostName}`]);
}

async function kindControlPlaneID(hostName: string): Promise<string> {
  const ids = await containerIDs([
    "--filter",
    `label=${KIND_CLUSTER_LABEL}=${hostName}`,
    "--filter",
    `label=${KIND_ROLE_LABEL}=${KIND_CONTROL_PLANE_ROLE}`,
  ]);
  if (!ids[0])
    throw new Error(`no Kind control-plane container for ${hostName}`);
  return ids[0];
}

interface KindHostAPI {
  ca: Buffer;
  url: string;
}

async function kindHostAPI(hostName: string): Promise<KindHostAPI> {
  const id = await kindControlPlaneID(hostName);
  const port = requireCommandSuccess(
    "podman port",
    await runCommand("podman", ["port", id, KIND_API_PORT]),
  );
  const ca = requireCommandSuccess(
    "read Kind CA",
    await runCommand("podman", [
      "exec",
      id,
      "cat",
      "/etc/kubernetes/pki/ca.crt",
    ]),
  ).stdout;
  if (!ca.includes("BEGIN CERTIFICATE")) {
    throw new Error("Kind CA is not a PEM certificate");
  }
  return { ca: Buffer.from(ca), url: parsePodmanPort(port.stdout) };
}

export async function kindAPIRequest(
  hostName: string,
  token: string,
  method: string,
  path: string,
  body?: string,
): Promise<HTTPSResponse> {
  if (!token.trim()) throw new Error("empty access token");
  const api = await kindHostAPI(hostName);
  return requestHTTPS(`${api.url}${path}`, {
    body,
    ca: api.ca,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(body === undefined ? {} : { "Content-Type": "application/json" }),
    },
    method,
  });
}

export async function kubectlOnKind(
  hostName: string,
  args: readonly string[],
): Promise<{ exitCode: number; stderr: string; stdout: string }> {
  const id = await kindControlPlaneID(hostName);
  const result = await runCommand("podman", [
    "exec",
    id,
    "kubectl",
    "--kubeconfig=/etc/kubernetes/admin.conf",
    ...args,
  ]);
  if (result.timedOut) throw commandFailure("kubectl", result);
  return result;
}
