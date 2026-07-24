import type { ResourceResult } from "@fleetshift/common";

export interface ClusterCondition {
  status: string;
  reason?: string;
  message?: string;
  lastTransitionTime?: string;
}

export interface NodepoolSpec {
  id: string;
  replicas: number;
  instanceType: string;
  rootVolumeSize?: number;
  rootVolumeType?: string;
  autoRepair?: boolean;
  upgradeType?: string;
}

export interface ClusterResource {
  name: string;
  uid: string;
  state?: string;
  reconciling?: boolean;
  createTime?: string;
  updateTime?: string;
  pauseReason?: string;
  conditions?: Record<string, ClusterCondition>;
  observation?: Record<string, unknown>;
  spec?: {
    releaseVersion?: string;
    nodepools?: NodepoolSpec[];
    endpointAccess?: string;
    channelGroup?: string;
  };
}

export interface ClusterRow {
  result: ResourceResult<ClusterResource>;
  id: string;
  service: string;
  nodePoolCount: number;
}

const STATE_LABELS: Record<
  string,
  { text: string; color: "blue" | "green" | "orange" | "red" | "grey" }
> = {
  CREATING: { text: "Creating", color: "blue" },
  ACTIVE: { text: "Active", color: "green" },
  DELETING: { text: "Deleting", color: "orange" },
  FAILED: { text: "Failed", color: "red" },
  PAUSED_AUTH: { text: "Paused (Auth)", color: "orange" },
  RUNNING: { text: "Running", color: "green" },
  PROVISIONING: { text: "Provisioning", color: "blue" },
  ERROR: { text: "Error", color: "red" },
  DEGRADED: { text: "Degraded", color: "orange" },
};

const DEFAULT_STATE_LABEL = { text: "Unknown", color: "grey" } as const;

const HEALTHY_STATES = new Set(["ACTIVE", "RUNNING"]);
const FAILURE_STATES = new Set(["ERROR", "DEGRADED", "FAILED"]);
const TRANSIENT_STATES = new Set(["CREATING", "DELETING", "PROVISIONING"]);

export function stateLabel(state: string | undefined) {
  if (!state) return DEFAULT_STATE_LABEL;
  return STATE_LABELS[state] ?? DEFAULT_STATE_LABEL;
}

export function isFailureState(state: string | undefined): boolean {
  return !!state && FAILURE_STATES.has(state);
}

export function isTransientState(state: string | undefined): boolean {
  return !!state && TRANSIENT_STATES.has(state);
}

export function formatTime(iso: string | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

export function extractClusterId(resourceName: string): string {
  return resourceName.replace(/^clusters\//, "");
}

export function extractService(canonicalName: string): string {
  const match = canonicalName.match(/^\/\/([^/]+)\//);
  return match?.[1] ?? "";
}

const SERVICE_LABELS: Record<string, string> = {
  "gcphcp.fleetshift.io": "GCP HCP",
  "kind.fleetshift.io": "Kind",
  "kubernetes.fleetshift.io": "Kubernetes",
};

export function serviceLabel(service: string): string {
  return SERVICE_LABELS[service] ?? service;
}

export function isHealthyState(state: string | undefined): boolean {
  return !!state && HEALTHY_STATES.has(state);
}

export function buildAddonBasePath(service: string): string {
  if (!Object.prototype.hasOwnProperty.call(SERVICE_LABELS, service)) {
    throw new Error(`Unsupported cluster service: ${service}`);
  }
  return `/apis/${service}/v1`;
}

export function deriveClusterState(
  resource: ClusterResource,
): string | undefined {
  if (resource.pauseReason) return "PAUSED_AUTH";
  if (resource.state) return resource.state;
  const ready = resource.conditions?.Ready;
  if (ready?.status === "True") return "RUNNING";
  if (ready?.status === "False") return "ERROR";
  return undefined;
}
