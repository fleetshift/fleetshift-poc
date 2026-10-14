export function extractClusterId(resourceName: string): string {
  const match = resourceName.match(/(?:^|\/)clusters\/([^/]+)/);
  return match?.[1] ?? resourceName;
}

export interface ClusterCondition {
  status: string;
  reason?: string;
  message?: string;
  lastTransitionTime?: string;
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
}
