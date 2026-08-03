/**
 * Base URL for the resource query API.
 * Matches the proto: `/apis/fleetshift.io/v1/{scope}:queryResources`
 */
const QUERY_BASE = "/apis/fleetshift.io/v1";

// ---------------------------------------------------------------------------
// Response / result types (from proto: fleetshift/v1/resource_result.proto)
// ---------------------------------------------------------------------------

/**
 * Common envelope fields always present inside the `resource` Struct,
 * regardless of which capabilities the addon declares.
 */
export interface ResourceCommon {
  name: string;
  uid: string;
  labels: Record<string, string>;
  createTime: string;
  updateTime: string;
  etag: string;
}

/**
 * A single resource returned by QueryResources.
 *
 * The generic `Props` parameter types the addon-specific fields inside
 * the `resource` Struct (spec, state, etc.). The `resource` body always
 * includes {@link ResourceCommon} fields plus whatever the addon schema
 * defines.
 */
export interface ResourceResult<Props = Record<string, unknown>> {
  /** Canonical full resource name, e.g. `"//gcphcp.fleetshift.io/clusters/prod"`. */
  name: string;
  /** Stable type identity, e.g. `"gcphcp.fleetshift.io/Cluster"`. */
  resourceType: string;
  /** Dynamic resource body (protojson-encoded Struct). */
  resource: ResourceCommon & Props;
}

/** Paginated response from QueryResources. */
export interface QueryResourcesResponse<Props = Record<string, unknown>> {
  resources: ResourceResult<Props>[];
  /** Empty string when there are no more pages. */
  nextPageToken: string;
}

// ---------------------------------------------------------------------------
// Request parameters
// ---------------------------------------------------------------------------

/** Parameters accepted by {@link ResourceApi.search}. */
export interface QueryResourcesParams {
  /** CEL filter expression. Empty matches everything. */
  filter?: string;
  /** Max results per page (server default applies if unset). */
  pageSize?: number;
  /** Token from a previous response to fetch the next page. */
  pageToken?: string;
  /** Ordering. Empty for server default. v0 supports `"resource_type,name"`. */
  orderBy?: string;
}

// ---------------------------------------------------------------------------
// API error
// ---------------------------------------------------------------------------

/** Structured error body returned by the gRPC-gateway. */
export interface RpcStatus {
  code: number;
  message: string;
  details?: unknown[];
}

/** Error thrown when the query API returns a non-OK response. */
export class ResourceApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly rpcStatus: RpcStatus | null,
  ) {
    super(rpcStatus?.message ?? `Query API error ${status}`);
    this.name = "ResourceApiError";
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function buildQueryUrl(scope: string, params?: QueryResourcesParams): string {
  const qp = new URLSearchParams();
  if (params?.filter) qp.set("filter", params.filter);
  if (params?.pageSize != null) qp.set("pageSize", String(params.pageSize));
  if (params?.pageToken) qp.set("pageToken", params.pageToken);
  if (params?.orderBy) qp.set("orderBy", params.orderBy);
  const qs = qp.toString();
  const path = `${QUERY_BASE}/${encodeURIComponent(scope)}:queryResources`;
  return qs ? `${path}?${qs}` : path;
}

/** Escape a value for embedding in a CEL string literal (backslash + double-quote). */
function escapeCelString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

async function queryRequest<Props>(
  scope: string,
  params?: QueryResourcesParams,
): Promise<QueryResourcesResponse<Props>> {
  const url = buildQueryUrl(scope, params);
  const res = await fetch(url);
  if (!res.ok) {
    const rpcStatus = (await res.json().catch(() => null)) as RpcStatus | null;
    throw new ResourceApiError(res.status, rpcStatus);
  }
  return res.json() as Promise<QueryResourcesResponse<Props>>;
}

// ---------------------------------------------------------------------------
// Public API object
// ---------------------------------------------------------------------------

/** Typed client returned by {@link createResourceApi}. */
export interface ResourceApi<Props = Record<string, unknown>> {
  /**
   * Query resources with an optional CEL filter and pagination.
   * Maps directly to `ResourceQueryService.QueryResources`.
   */
  search(params?: QueryResourcesParams): Promise<QueryResourcesResponse<Props>>;

  /**
   * List all resources (no filter). Convenience wrapper around
   * {@link search} with only pagination parameters.
   */
  list(
    params?: Pick<QueryResourcesParams, "pageSize" | "pageToken">,
  ): Promise<QueryResourcesResponse<Props>>;

  /**
   * Fetch a single resource by its full resource name.
   *
   * Uses a CEL filter `name == "{resourceName}"` under the hood.
   * Returns `undefined` when no match is found.
   */
  get(resourceName: string): Promise<ResourceResult<Props> | undefined>;

  /**
   * Fetch all pages of results and return a flat array.
   * Automatically follows `nextPageToken` until exhausted.
   */
  searchAll(
    params?: Omit<QueryResourcesParams, "pageToken">,
  ): Promise<ResourceResult<Props>[]>;
}

/**
 * Create a typed resource-query API client scoped to a resource
 * hierarchy location.
 *
 * @example
 * ```ts
 * interface ClusterResource { spec: { releaseVersion: string } }
 * const clusterApi = createResourceApi<ClusterResource>("-");
 *
 * // query with a CEL filter
 * const { resources } = await clusterApi.search({
 *   filter: 'resource_type == "gcphcp.fleetshift.io/Cluster"',
 *   pageSize: 20,
 * });
 *
 * // auto-paginate
 * const all = await clusterApi.searchAll();
 * ```
 *
 * @param scope - Scope to search within (use `"-"` for global).
 */
export function createResourceApi<Props = Record<string, unknown>>(
  scope: string,
): ResourceApi<Props> {
  return {
    search(params) {
      return queryRequest<Props>(scope, params);
    },

    list(params) {
      return queryRequest<Props>(scope, params);
    },

    async get(resourceName) {
      const { resources } = await queryRequest<Props>(scope, {
        filter: `name == "${escapeCelString(resourceName)}"`,
        pageSize: 1,
      });
      return resources[0];
    },

    async searchAll(params) {
      const allResults: ResourceResult<Props>[] = [];
      let pageToken: string | undefined;
      const seenTokens = new Set<string>();
      do {
        const response = await queryRequest<Props>(scope, {
          ...params,
          pageToken,
        });
        allResults.push(...response.resources);
        pageToken = response.nextPageToken || undefined;
        if (pageToken && seenTokens.has(pageToken)) {
          throw new Error(
            `Cyclic pagination detected: repeated nextPageToken "${pageToken}"`,
          );
        }
        if (pageToken) seenTokens.add(pageToken);
      } while (pageToken);
      return allResults;
    },
  };
}

// ---------------------------------------------------------------------------
// Generic REST API client
// ---------------------------------------------------------------------------

/**
 * Generic REST client for non-search API calls (CRUD operations).
 *
 * Replaces per-plugin `makeRequest` / `mgmtFetch` wrappers with a shared,
 * typed client that uses the same structured {@link ResourceApiError} error
 * handling as the query client.
 */
export interface ApiClient {
  /** GET `basePath + path` with optional query params. */
  get<T>(path: string, params?: Record<string, string>): Promise<T>;
  /** POST `basePath + path` with optional JSON body. */
  post<T>(path: string, body?: unknown): Promise<T>;
  /** PUT `basePath + path` with optional JSON body. */
  put<T>(path: string, body?: unknown): Promise<T>;
  /** PATCH `basePath + path` with optional JSON body. */
  patch<T>(path: string, body?: unknown): Promise<T>;
  /** DELETE `basePath + path`. Returns `undefined` for 204 No Content. */
  delete<T = void>(path: string): Promise<T>;
}

async function apiRequest<T>(
  basePath: string,
  path: string,
  init?: RequestInit,
): Promise<T> {
  const res = await fetch(`${basePath}${path}`, init);
  if (!res.ok) {
    const rpcStatus = (await res.json().catch(() => null)) as RpcStatus | null;
    throw new ResourceApiError(res.status, rpcStatus);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

function jsonInit(method: string, body?: unknown): RequestInit {
  if (body == null) return { method };
  return {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

/**
 * Create a generic REST client scoped to `basePath`.
 *
 * @example
 * ```ts
 * const mgmt = createApiClient("/v1");
 * const deployments = await mgmt.get<ListDeploymentsResponse>("/deployments");
 *
 * const gcphcp = createApiClient("/apis/gcphcp.fleetshift.io/v1");
 * const cluster = await gcphcp.post<GcpHcpCluster>(
 *   "/clusters?cluster_id=my-cluster",
 *   { spec },
 * );
 * ```
 */
export function createApiClient(basePath: string): ApiClient {
  return {
    get<T>(path: string, params?: Record<string, string>): Promise<T> {
      const qs = params ? `?${new URLSearchParams(params).toString()}` : "";
      return apiRequest<T>(basePath, `${path}${qs}`);
    },
    post<T>(path: string, body?: unknown): Promise<T> {
      return apiRequest<T>(basePath, path, jsonInit("POST", body));
    },
    put<T>(path: string, body?: unknown): Promise<T> {
      return apiRequest<T>(basePath, path, jsonInit("PUT", body));
    },
    patch<T>(path: string, body?: unknown): Promise<T> {
      return apiRequest<T>(basePath, path, jsonInit("PATCH", body));
    },
    delete<T>(path: string): Promise<T> {
      return apiRequest<T>(basePath, path, { method: "DELETE" });
    },
  };
}

// ---------------------------------------------------------------------------
// Backward-compat type aliases (PR #39 names)
// ---------------------------------------------------------------------------

/** @deprecated Use {@link ResourceResult} instead. */
export type ResourceSearchResult<Props = Record<string, unknown>> =
  ResourceResult<Props>;

/** @deprecated Use {@link QueryResourcesResponse} instead. */
export type SearchResourcesResponse<Props = Record<string, unknown>> =
  QueryResourcesResponse<Props>;

/** @deprecated Use {@link QueryResourcesParams} instead. */
export type SearchResourcesParams = QueryResourcesParams;

/** @deprecated Use {@link ResourceCommon} instead. */
export type Condition = {
  type: string;
  status: string;
  reason: string;
  message: string;
  lastTransitionTime: string;
};
