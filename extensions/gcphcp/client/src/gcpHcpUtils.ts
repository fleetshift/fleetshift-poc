import type { GcpHcpCluster } from "./api";

export const GCP_HCP_STATE_LABELS: Record<
  string,
  { text: string; color: "blue" | "green" | "orange" | "red" | "grey" }
> = {
  CREATING: { text: "Creating", color: "blue" },
  ACTIVE: { text: "Active", color: "green" },
  DELETING: { text: "Deleting", color: "orange" },
  FAILED: { text: "Failed", color: "red" },
  PAUSED_AUTH: { text: "Paused (Auth)", color: "orange" },
};

export const DEFAULT_STATE_LABEL = { text: "Unknown", color: "grey" } as const;

export function stateLabel(state: string) {
  return GCP_HCP_STATE_LABELS[state] ?? DEFAULT_STATE_LABEL;
}

export function formatTime(iso: string): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

export interface GcpHcpClusterRow {
  cluster: GcpHcpCluster;
  id: string;
  nodePoolCount: number;
}
