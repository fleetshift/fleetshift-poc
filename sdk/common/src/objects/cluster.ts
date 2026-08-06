export function extractClusterId(resourceName: string): string {
  return resourceName.replace(/^clusters\//, "");
}

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
