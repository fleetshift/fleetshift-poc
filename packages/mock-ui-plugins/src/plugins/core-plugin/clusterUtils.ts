import type { MgmtDeployment, DeploymentState } from "../management-plugin/api";

export interface KindClusterSpec {
  name: string;
  nodes?: Array<{ role: string; image?: string }>;
  networking?: {
    apiServerPort?: number;
    podSubnet?: string;
    serviceSubnet?: string;
  };
}

export interface ClusterRow {
  deployment: MgmtDeployment;
  clusterName: string;
  nodeCount: number;
  spec: KindClusterSpec | null;
}

export const STATE_LABELS: Record<
  DeploymentState,
  { text: string; color: "blue" | "green" | "red" | "orange" | "grey" }
> = {
  STATE_UNSPECIFIED: { text: "Unknown", color: "grey" },
  STATE_CREATING: { text: "Creating", color: "blue" },
  STATE_ACTIVE: { text: "Healthy", color: "green" },
  STATE_DELETING: { text: "Deleting", color: "orange" },
  STATE_FAILED: { text: "Failed", color: "red" },
  STATE_PAUSED_AUTH: { text: "Paused", color: "orange" },
};

export function decodeSpec(deployment: MgmtDeployment): KindClusterSpec | null {
  const manifest = deployment.manifestStrategy?.manifests?.find(
    (m) => m.resourceType === "api.kind.cluster",
  );
  if (!manifest?.raw) return null;
  try {
    return JSON.parse(atob(manifest.raw));
  } catch {
    return null;
  }
}

export function toClusterRow(dep: MgmtDeployment): ClusterRow {
  const spec = decodeSpec(dep);
  const shortName = dep.name.replace(/^deployments\//, "");
  return {
    deployment: dep,
    clusterName: spec?.name ?? shortName,
    nodeCount: spec?.nodes?.length ?? 0,
    spec,
  };
}

export function formatTime(iso: string): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}
