export type DeploymentState =
  | "STATE_UNSPECIFIED"
  | "STATE_CREATING"
  | "STATE_ACTIVE"
  | "STATE_DELETING"
  | "STATE_FAILED";

/** JSON shape returned by DeploymentService.ListDeployments. */
export interface DeploymentListItem {
  name: string;
  state?: DeploymentState;
  reconciling?: boolean;
  resolvedTargetIds?: string[];
  createTime?: string;
  updateTime?: string;
  pauseReason?: string;
}

export interface DeploymentListResponse {
  deployments?: DeploymentListItem[];
  nextPageToken?: string;
}
