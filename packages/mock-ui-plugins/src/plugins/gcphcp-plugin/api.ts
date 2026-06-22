const BASE = "/apis/gcphcp.fleetshift.io/v1";

export interface NodepoolSpec {
  id: string;
  replicas: number;
  instanceType: string;
  rootVolumeSize: number;
  rootVolumeType: string;
  autoRepair: boolean;
  upgradeType: string;
}

export interface GcpHcpClusterSpec {
  endpointAccess: string;
  releaseVersion: string;
  channelGroup: string;
  nodepools: NodepoolSpec[];
}

export interface GcpHcpCluster {
  name: string;
  uid: string;
  spec: GcpHcpClusterSpec;
  intentVersion: string;
  state: string;
  createTime: string;
  updateTime: string;
  etag: string;
  generation: string;
  pauseReason?: string;
}

export const GCPHCP_ENTITIES = {
  clusters: "clusters",
} as const;
export type GCHCP_ENTITY =
  (typeof GCPHCP_ENTITIES)[keyof typeof GCPHCP_ENTITIES];

const ALLOWED_ENTITIES = Object.values(GCPHCP_ENTITIES);

interface EntityResponseMap {
  clusters: {
    list: {
      clusters: GcpHcpCluster[];
    };
    get: GcpHcpCluster;
    delete: GcpHcpCluster;
    resume: GcpHcpCluster;
    create: GcpHcpCluster;
  };
}

const createParamMap: Record<GCHCP_ENTITY, string> = {
  clusters: "cluster_id",
};

declare global {
  interface URL {
    appendPathname(fragment: string): URL;
  }
}

URL.prototype.appendPathname = function (this: URL, fragment: string) {
  if (!this.pathname.endsWith("/") && !fragment.startsWith("/")) {
    this.pathname += "/";
  }
  this.pathname += fragment;
  return this;
};

function getEntityURL(entity: GCHCP_ENTITY) {
  // in case a string somehows makes its way there
  if (!ALLOWED_ENTITIES.includes(entity)) {
    throw new Error(
      `Invalid entity value: Expected one of ${ALLOWED_ENTITIES.join()}, got ${entity}.`,
    );
  }
  return new URL(`${BASE}/${entity}`, window.location.origin);
}

async function makeRequest(...args: Parameters<typeof fetch>) {
  const resp = await fetch(...args);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || resp.statusText);
  }
  return resp.json();
}

async function list<T extends keyof EntityResponseMap>(
  entity: T,
): Promise<EntityResponseMap[T]["list"]> {
  return makeRequest(getEntityURL(entity));
}

async function get<T extends keyof EntityResponseMap>(
  entity: T,
  id: string,
): Promise<EntityResponseMap[T]["get"]> {
  const url = getEntityURL(entity);
  return makeRequest(url.appendPathname(id));
}

async function deleteEntity<T extends keyof EntityResponseMap>(
  entity: T,
  id: string,
): Promise<EntityResponseMap[T]["delete"]> {
  const url = getEntityURL(entity);
  return makeRequest(url.appendPathname(id), { method: "DELETE" });
}

async function resume<T extends keyof EntityResponseMap>(
  entity: T,
  id: string,
): Promise<EntityResponseMap[T]["resume"]> {
  const url = getEntityURL(entity);
  return makeRequest(url.appendPathname(`${id}:resume`), { method: "POST" });
}

async function create<T extends keyof EntityResponseMap, P = unknown>(
  entity: T,
  id: string,
  payload: P,
): Promise<EntityResponseMap[T]["create"]> {
  const url = getEntityURL(entity);
  url.searchParams.append(createParamMap[entity], id);
  return makeRequest(url, {
    method: "POST",
    body: JSON.stringify(payload),
    headers: {
      "Content-Type": "application/json",
    },
  });
}

export function extractClusterId(name: string): string {
  return name.replace(/^clusters\//, "");
}

export async function listGcpHcpClusters(): Promise<GcpHcpCluster[]> {
  const res = await list("clusters");
  return res.clusters ?? [];
}

export async function getGcpHcpCluster(id: string): Promise<GcpHcpCluster> {
  try {
    return await get("clusters", id);
  } catch (error) {
    console.error(error);
    throw new Error("Unable to retrieve GCPHCP clusters");
  }
}

export async function createGcpHcpCluster(
  clusterId: string,
  spec: GcpHcpClusterSpec,
): Promise<GcpHcpCluster> {
  try {
    return await create("clusters", clusterId, { spec });
  } catch (error) {
    console.error(error);
    throw new Error("GCP HCP create failed");
  }
}

export async function resumeGcpHcpCluster(id: string) {
  try {
    return await resume("clusters", id);
  } catch (error) {
    console.error(error);
    throw new Error("Resume GCP HCP cluster failed");
  }
}

export async function deleteGcpHcpCluster(id: string) {
  try {
    return await deleteEntity("clusters", id);
  } catch (error) {
    console.error(error);
    throw new Error("Delete GCP HCP cluster failed");
  }
}
