import { expect } from "@playwright/test";

import { type FleetctlClient } from "../support/fleetctl";
import { parseJSON } from "../support/json";
import { CONFIG_MAP_NAME } from "./deployments";
import { clusterResourceName, KIND_CLUSTER_TYPE, parseCluster } from "./kind";

const KIND_CLUSTER_QUERY_TYPE = "kind.fleetshift.io/Cluster";
const KIND_NODE_QUERY_TYPE = "kind.fleetshift.io/Node";
const KUBERNETES_OBJECT_QUERY_TYPE = "kubernetes.fleetshift.io/Object";
const INSPECT_PAGE_SIZE = 50;
const PAGINATION_PAGE_SIZE = 10;
const PAGINATION_ORDER = "resource_type,name";
const MAX_PAGES = 100;
const WAIT_TIMEOUT_MS = 2 * 60_000;
const INDEXED_KIND_CLUSTER_GONE_TIMEOUT_MS = 1 * 60_000;
const POLL_INTERVAL_MS = 2_000;
const KIND_NODE_PREFIX = "//kind.fleetshift.io/nodes/";

export interface QueryHit {
  name: string;
  resource: unknown;
  resourceType: string;
}

interface QueryPage {
  nextPageToken: string;
  resources: QueryHit[];
}

interface QueryRequest {
  filter?: string;
  orderBy?: string;
  pageSize?: number;
  pageToken?: string;
}

interface QueryObservation {
  cluster: string;
  extracted: Record<string, unknown>;
  kind: string;
  metadata: { name: string; namespace: string };
}

function queryHit(item: Partial<QueryHit> | null | undefined): QueryHit {
  return {
    name: item?.name ?? "",
    resource: item?.resource ?? null,
    resourceType: item?.resourceType ?? "",
  };
}

export function parseQueryPage(raw: string): QueryPage {
  const page = parseJSON<{
    nextPageToken?: string;
    resources?: Array<Partial<QueryHit> | null>;
  } | null>(raw);
  const resources = Array.isArray(page?.resources) ? page.resources : [];
  return {
    nextPageToken: page?.nextPageToken ?? "",
    resources: resources.map(queryHit),
  };
}

export function observation(hit: QueryHit): QueryObservation {
  const body = hit.resource as
    | {
        observation?: {
          cluster?: string;
          extracted?: Record<string, unknown>;
          kind?: string;
          metadata?: { name?: string; namespace?: string };
        } | null;
      }
    | null
    | undefined;
  const value = body?.observation;
  if (value == null || typeof value !== "object") {
    throw new Error(`query hit ${hit.name} has no observation`);
  }
  return {
    cluster: value.cluster ?? "",
    extracted: value.extracted ?? {},
    kind: value.kind ?? "",
    metadata: {
      name: value.metadata?.name ?? "",
      namespace: value.metadata?.namespace ?? "",
    },
  };
}

function queryArgs(request: QueryRequest): string[] {
  const args = ["resource", "query"];
  if (request.filter) args.push("--filter", request.filter);
  if (request.pageSize) args.push("--page-size", String(request.pageSize));
  if (request.pageToken) args.push("--page-token", request.pageToken);
  if (request.orderBy) args.push("--order-by", request.orderBy);
  return args;
}

function kubernetesObjectNamePrefix(clusterId: string): string {
  return `//kubernetes.fleetshift.io/clusters/${encodeURIComponent(clusterId)}/`;
}

export function kubernetesObjectsInClusterFilter(clusterId: string): string {
  return `resourceType == ${JSON.stringify(KUBERNETES_OBJECT_QUERY_TYPE)} && name.startsWith(${JSON.stringify(kubernetesObjectNamePrefix(clusterId))})`;
}

export function kubernetesObjectKindFilter(
  clusterId: string,
  kind: string,
): string {
  return `${kubernetesObjectsInClusterFilter(clusterId)} && resource.observation.kind == ${JSON.stringify(kind)}`;
}

export function indexedConfigMapFilter(
  clusterId: string,
  namespace: string,
  name = CONFIG_MAP_NAME,
): string {
  return `${kubernetesObjectKindFilter(clusterId, "ConfigMap")} && resource.observation.metadata.namespace == ${JSON.stringify(namespace)} && resource.observation.metadata.name == ${JSON.stringify(name)}`;
}

export function kindClusterIdentityFilter(clusterId: string): string {
  return `resourceType == ${JSON.stringify(KIND_CLUSTER_QUERY_TYPE)} && resource.name == ${JSON.stringify(clusterResourceName(clusterId))}`;
}

function kindClusterReadyFilter(clusterId: string): string {
  return `${kindClusterIdentityFilter(clusterId)} && resource.state == "ACTIVE" && resource.conditions["Ready"].status == "True"`;
}

export function kindNodeInClusterFilter(clusterId: string): string {
  return `resourceType == ${JSON.stringify(KIND_NODE_QUERY_TYPE)} && resource.observation.cluster == ${JSON.stringify(clusterResourceName(clusterId))}`;
}

function deniedGVRFilter(): string {
  return `resourceType == ${JSON.stringify(KUBERNETES_OBJECT_QUERY_TYPE)} && resource.observation.gvr.resource in ["events","leases","endpoints","endpointslices","componentstatuses"]`;
}

export function kubernetesObjectInCluster(
  hit: QueryHit,
  clusterId: string,
): boolean {
  const leaf = "/objects/";
  const index = hit.name.lastIndexOf(leaf);
  return (
    hit.resourceType === KUBERNETES_OBJECT_QUERY_TYPE &&
    hit.name.startsWith(kubernetesObjectNamePrefix(clusterId)) &&
    hit.name.includes("/apiResources/") &&
    index >= 0 &&
    index + leaf.length < hit.name.length
  );
}

function namesOutsideCluster(
  hits: readonly QueryHit[],
  clusterId: string,
): string[] {
  return hits
    .filter((hit) => !kubernetesObjectInCluster(hit, clusterId))
    .map(({ name }) => name);
}

function kindNodeNames(hits: readonly QueryHit[]): string[] {
  return hits.map(({ name }) => name.slice(KIND_NODE_PREFIX.length)).sort();
}

function kubernetesNodeNames(hits: readonly QueryHit[]): string[] {
  return hits.map((hit) => observation(hit).metadata.name).sort();
}

function kindNodeScope(hits: readonly QueryHit[], clusterId: string) {
  return {
    allScoped: hits.every(
      (hit) =>
        hit.resourceType === KIND_NODE_QUERY_TYPE &&
        hit.name.startsWith(KIND_NODE_PREFIX) &&
        observation(hit).cluster === clusterResourceName(clusterId),
    ),
  };
}

function kubernetesNodeScope(hits: readonly QueryHit[], clusterId: string) {
  return {
    allNodes: hits.every((hit) => observation(hit).kind === "Node"),
    outside: namesOutsideCluster(hits, clusterId),
  };
}

type NodeCountExpectation = { count: number } | { hasNodes: true };

export class QuerySteps {
  readonly #client: FleetctlClient;

  constructor(client: FleetctlClient) {
    this.#client = client;
  }

  async #query(request: QueryRequest): Promise<QueryPage> {
    const result = await this.#client.succeed(queryArgs(request));
    return parseQueryPage(result.stdout);
  }

  async #waitForSummary<T>(
    request: QueryRequest,
    summarize: (hits: readonly QueryHit[]) => T,
    expected: T,
    timeoutMs = WAIT_TIMEOUT_MS,
  ): Promise<QueryHit[]> {
    let hits: QueryHit[] = [];
    await expect
      .poll(
        async () => {
          hits = (await this.#query(request)).resources;
          return summarize(hits);
        },
        { intervals: [POLL_INTERVAL_MS], timeout: timeoutMs },
      )
      .toEqual(expected);
    return hits;
  }

  async indexedKubernetesObjectsExist(clusterId: string): Promise<void> {
    await this.#waitForSummary(
      {
        filter: kubernetesObjectKindFilter(clusterId, "Node"),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (hits) => ({
        cluster: clusterId,
        hasNodes: hits.length > 0,
        missingVersions: hits.filter(
          (hit) =>
            typeof observation(hit).extracted["kubeletVersion"] !== "string",
        ).length,
        outside: namesOutsideCluster(hits, clusterId),
      }),
      { cluster: clusterId, hasNodes: true, missingVersions: 0, outside: [] },
    );

    await this.#waitForSummary(
      {
        filter: kubernetesObjectKindFilter(clusterId, "Namespace"),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (hits) => {
        const names = hits.map((hit) => observation(hit).metadata.name);
        return {
          cluster: clusterId,
          default: names.includes("default"),
          kubeSystem: names.includes("kube-system"),
          outside: namesOutsideCluster(hits, clusterId),
        };
      },
      {
        cluster: clusterId,
        default: true,
        kubeSystem: true,
        outside: [],
      },
    );

    await this.#waitForSummary(
      {
        filter: kubernetesObjectKindFilter(clusterId, "ConfigMap"),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (hits) => ({
        cluster: clusterId,
        hasConfigMaps: hits.length > 0,
        outside: namesOutsideCluster(hits, clusterId),
      }),
      { cluster: clusterId, hasConfigMaps: true, outside: [] },
    );

    expect(
      (
        await this.#query({
          filter: deniedGVRFilter(),
          pageSize: INSPECT_PAGE_SIZE,
        })
      ).resources,
    ).toEqual([]);
  }

  async indexedConfigMapExists(
    clusterId: string,
    namespace: string,
  ): Promise<void> {
    await this.#waitForSummary(
      {
        filter: indexedConfigMapFilter(clusterId, namespace),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (hits) => ({
        hits: hits.map((hit) => {
          const obs = observation(hit);
          return {
            kind: obs.kind,
            name: obs.metadata.name,
            namespace: obs.metadata.namespace,
          };
        }),
        outside: namesOutsideCluster(hits, clusterId),
      }),
      {
        hits: [{ kind: "ConfigMap", name: CONFIG_MAP_NAME, namespace }],
        outside: [],
      },
    );
  }

  async kindClusterQueryMatchesGet(clusterId: string): Promise<void> {
    const envelopeName = `//kind.fleetshift.io/${clusterResourceName(clusterId)}`;
    const hits = await this.#waitForSummary(
      {
        filter: kindClusterReadyFilter(clusterId),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (items) =>
        items.map(({ name, resourceType }) => ({ name, resourceType })),
      [{ name: envelopeName, resourceType: KIND_CLUSTER_QUERY_TYPE }],
    );
    const get = await this.#client.succeed([
      "resource",
      "get",
      KIND_CLUSTER_TYPE,
      clusterId,
    ]);
    const got = parseCluster(get.stdout);
    expect(hits[0]?.resource).toMatchObject({
      conditions: { Ready: { status: got.conditions["Ready"]?.status } },
      name: got.name,
      state: got.state,
    });
  }

  async kubernetesObjectQueryPaginates(clusterId: string): Promise<void> {
    const base: QueryRequest = {
      filter: kubernetesObjectsInClusterFilter(clusterId),
      orderBy: PAGINATION_ORDER,
      pageSize: PAGINATION_PAGE_SIZE,
    };
    const seen = new Set<string>();
    let token = "";
    let firstToken = "";
    let exhausted = false;
    for (let pageNumber = 1; pageNumber <= MAX_PAGES; pageNumber += 1) {
      const page = await this.#query({ ...base, pageToken: token });
      if (pageNumber === 1) firstToken = page.nextPageToken;
      if (page.nextPageToken)
        expect(page.resources).toHaveLength(PAGINATION_PAGE_SIZE);
      else
        expect(page.resources.length).toBeLessThanOrEqual(PAGINATION_PAGE_SIZE);
      for (const hit of page.resources) {
        expect(seen.has(hit.name), `duplicate query name ${hit.name}`).toBe(
          false,
        );
        expect(kubernetesObjectInCluster(hit, clusterId)).toBe(true);
        seen.add(hit.name);
      }
      if (!page.nextPageToken) {
        exhausted = true;
        break;
      }
      token = page.nextPageToken;
    }
    expect(exhausted).toBe(true);
    expect(firstToken).not.toBe("");
    expect(seen.size).toBeGreaterThan(PAGINATION_PAGE_SIZE);

    const mismatch = await this.#client.run(
      queryArgs({
        filter: `resourceType == ${JSON.stringify(KIND_CLUSTER_QUERY_TYPE)}`,
        orderBy: PAGINATION_ORDER,
        pageSize: PAGINATION_PAGE_SIZE,
        pageToken: firstToken,
      }),
    );
    expect(mismatch.exitCode).not.toBe(0);
    expect(mismatch.stderr.toLowerCase()).toContain("does not match");
  }

  async bothNodeTypesAreIndexed(clusterId: string): Promise<void> {
    const kindHits = await this.#kindNodeHits(clusterId, { hasNodes: true });
    const kubernetesHits = await this.#kubernetesNodeHits(clusterId, {
      hasNodes: true,
    });
    expect(kubernetesNodeNames(kubernetesHits)).toEqual(
      kindNodeNames(kindHits),
    );
  }

  async indexedConfigMapGone(
    clusterId: string,
    namespace: string,
  ): Promise<void> {
    await this.#waitForSummary(
      {
        filter: indexedConfigMapFilter(clusterId, namespace),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (hits) => ({
        names: hits.map((hit) => observation(hit).metadata.name),
        outside: namesOutsideCluster(hits, clusterId),
      }),
      { names: [], outside: [] },
    );
  }

  async indexedKindClusterExists(clusterId: string): Promise<void> {
    await this.#waitForSummary(
      {
        filter: kindClusterIdentityFilter(clusterId),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (hits) => ({
        count: hits.filter(
          (hit) => hit.resourceType === KIND_CLUSTER_QUERY_TYPE,
        ).length,
      }),
      { count: 1 },
    );
  }

  async indexedKindClusterGone(clusterId: string): Promise<void> {
    await this.#waitForSummary(
      {
        filter: kindClusterIdentityFilter(clusterId),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (hits) => hits.map(({ name }) => name),
      [],
      INDEXED_KIND_CLUSTER_GONE_TIMEOUT_MS,
    );
  }

  async nodeIdentitiesMatch(
    clusterId: string,
    expectedNames: readonly string[],
  ): Promise<void> {
    const expected = [...expectedNames].sort();
    const [kindNames, kubernetesNames] = await Promise.all([
      this.#kindNodeIdentities(clusterId, expected.length),
      this.#kubernetesNodeIdentities(clusterId, expected.length),
    ]);
    expect(kindNames).toEqual(expected);
    expect(kubernetesNames).toEqual(expected);
  }

  async #kindNodeHits(
    clusterId: string,
    expected: NodeCountExpectation,
  ): Promise<QueryHit[]> {
    return this.#waitForSummary(
      {
        filter: kindNodeInClusterFilter(clusterId),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (hits) => ({
        ...kindNodeScope(hits, clusterId),
        ...("count" in expected
          ? { count: hits.length }
          : { hasNodes: hits.length > 0 }),
      }),
      { allScoped: true, ...expected },
    );
  }

  async #kubernetesNodeHits(
    clusterId: string,
    expected: NodeCountExpectation,
  ): Promise<QueryHit[]> {
    return this.#waitForSummary(
      {
        filter: kubernetesObjectKindFilter(clusterId, "Node"),
        pageSize: INSPECT_PAGE_SIZE,
      },
      (hits) => ({
        ...kubernetesNodeScope(hits, clusterId),
        ...("count" in expected
          ? { count: hits.length }
          : { hasNodes: hits.length > 0 }),
      }),
      { allNodes: true, outside: [], ...expected },
    );
  }

  async #kindNodeIdentities(
    clusterId: string,
    expectedCount: number,
  ): Promise<string[]> {
    return kindNodeNames(
      await this.#kindNodeHits(clusterId, { count: expectedCount }),
    );
  }

  async #kubernetesNodeIdentities(
    clusterId: string,
    expectedCount: number,
  ): Promise<string[]> {
    return kubernetesNodeNames(
      await this.#kubernetesNodeHits(clusterId, { count: expectedCount }),
    );
  }
}
