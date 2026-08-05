import { createApiClient } from "@fleetshift/common";

const client = createApiClient("/apis/kind.fleetshift.io/v1");

export interface KindNodeSpec {
  role: "control-plane" | "worker";
  image?: string;
}

export interface KindNetworkingSpec {
  apiServerPort?: number;
  podSubnet?: string;
  serviceSubnet?: string;
}

export interface KindClusterSpec {
  name?: string;
  nodes?: KindNodeSpec[];
  networking?: KindNetworkingSpec;
}

export interface KindCluster {
  name: string;
  uid: string;
  spec: KindClusterSpec;
  state: string;
  createTime: string;
  updateTime: string;
  etag: string;
  generation: string;
}

export async function createKindCluster(
  clusterId: string,
  spec: KindClusterSpec,
): Promise<KindCluster> {
  try {
    return await client.post<KindCluster>(
      `/clusters?cluster_id=${encodeURIComponent(clusterId)}`,
      { spec },
    );
  } catch (error) {
    console.error(error);
    throw new Error("Kind cluster creation failed");
  }
}
