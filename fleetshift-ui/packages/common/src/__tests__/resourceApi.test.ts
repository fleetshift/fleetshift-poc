import { afterEach, describe, expect, it, vi } from "vitest";

import type { QueryResourcesResponse, ResourceResult } from "../resourceApi";
import {
  createApiClient,
  createResourceApi,
  ResourceApiError,
} from "../resourceApi";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface TestProps {
  apiUrl: string;
}

function makeResult(
  overrides: Partial<ResourceResult<TestProps>> = {},
): ResourceResult<TestProps> {
  return {
    name: "//kind.fleetshift.io/clusters/prod",
    resourceType: "kind.fleetshift.io/Cluster",
    resource: {
      name: "clusters/prod",
      uid: "550e8400-e29b-41d4-a716-446655440000",
      labels: {},
      createTime: "2026-01-01T00:00:00Z",
      updateTime: "2026-01-01T00:00:00Z",
      etag: 'W/"abc"',
      apiUrl: "https://prod.example.com",
    },
    ...overrides,
  };
}

function mockFetch(body: unknown, status = 200) {
  return vi.spyOn(globalThis, "fetch").mockResolvedValue({
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? "OK" : "Error",
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  } as Response);
}

/** Parse a relative URL string into path + URLSearchParams. */
function parseRelativeUrl(url: string) {
  const [path, qs] = url.split("?");
  return { path, params: new URLSearchParams(qs ?? "") };
}

afterEach(() => {
  vi.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createResourceApi", () => {
  describe("search", () => {
    it("calls the correct URL with no params", async () => {
      const response: QueryResourcesResponse<TestProps> = {
        resources: [makeResult()],
        nextPageToken: "",
      };
      const spy = mockFetch(response);
      const api = createResourceApi<TestProps>("-");

      const res = await api.search();

      expect(res.resources).toHaveLength(1);
      expect(res.resources[0].resource.apiUrl).toBe("https://prod.example.com");

      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toBe("/apis/fleetshift.io/v1/-:queryResources");
    });

    it("passes filter, pageSize, and pageToken as query params", async () => {
      const spy = mockFetch({ resources: [], nextPageToken: "" });
      const api = createResourceApi("-");

      await api.search({
        filter: 'resource_type == "kind.fleetshift.io/Cluster"',
        pageSize: 25,
        pageToken: "tok123",
      });

      const { path, params } = parseRelativeUrl(spy.mock.calls[0][0] as string);
      expect(path).toBe("/apis/fleetshift.io/v1/-:queryResources");
      expect(params.get("filter")).toBe(
        'resource_type == "kind.fleetshift.io/Cluster"',
      );
      expect(params.get("pageSize")).toBe("25");
      expect(params.get("pageToken")).toBe("tok123");
    });

    it("passes orderBy as query param", async () => {
      const spy = mockFetch({ resources: [], nextPageToken: "" });
      const api = createResourceApi("-");

      await api.search({ orderBy: "resource_type,name" });

      const { params } = parseRelativeUrl(spy.mock.calls[0][0] as string);
      expect(params.get("orderBy")).toBe("resource_type,name");
    });

    it("encodes the scope in the URL", async () => {
      const spy = mockFetch({ resources: [], nextPageToken: "" });
      const api = createResourceApi("workspace/foo");

      await api.search();

      const calledUrl = spy.mock.calls[0][0] as string;
      expect(calledUrl).toBe(
        "/apis/fleetshift.io/v1/workspace%2Ffoo:queryResources",
      );
    });

    it("throws ResourceApiError on non-OK response", async () => {
      const rpcStatus = { code: 13, message: "Internal error", details: [] };
      mockFetch(rpcStatus, 500);
      const api = createResourceApi("-");

      await expect(api.search()).rejects.toThrow(ResourceApiError);
      await expect(api.search()).rejects.toMatchObject({
        status: 500,
        rpcStatus: { code: 13, message: "Internal error" },
      });
    });

    it("throws ResourceApiError with null rpcStatus when body is unparseable", async () => {
      vi.spyOn(globalThis, "fetch").mockResolvedValue({
        ok: false,
        status: 502,
        statusText: "Bad Gateway",
        json: () => Promise.reject(new SyntaxError("Unexpected token")),
      } as unknown as Response);
      const api = createResourceApi("-");

      const err = await api.search().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(ResourceApiError);
      expect((err as ResourceApiError).status).toBe(502);
      expect((err as ResourceApiError).rpcStatus).toBeNull();
    });
  });

  describe("list", () => {
    it("calls search without a filter", async () => {
      const spy = mockFetch({
        resources: [makeResult()],
        nextPageToken: "",
      });
      const api = createResourceApi<TestProps>("-");

      const res = await api.list({ pageSize: 10 });

      expect(res.resources).toHaveLength(1);
      const { params } = parseRelativeUrl(spy.mock.calls[0][0] as string);
      expect(params.has("filter")).toBe(false);
      expect(params.get("pageSize")).toBe("10");
    });
  });

  describe("get", () => {
    it("returns the first matching result", async () => {
      const result = makeResult();
      mockFetch({ resources: [result], nextPageToken: "" });
      const api = createResourceApi<TestProps>("-");

      const found = await api.get("//kind.fleetshift.io/clusters/prod");

      expect(found).toEqual(result);
    });

    it("returns undefined when no match", async () => {
      mockFetch({ resources: [], nextPageToken: "" });
      const api = createResourceApi("-");

      const found = await api.get("//nonexistent");

      expect(found).toBeUndefined();
    });

    it("uses name filter and pageSize 1", async () => {
      const spy = mockFetch({ resources: [], nextPageToken: "" });
      const api = createResourceApi("-");

      await api.get("//svc/res/id");

      const { params } = parseRelativeUrl(spy.mock.calls[0][0] as string);
      expect(params.get("filter")).toBe('name == "//svc/res/id"');
      expect(params.get("pageSize")).toBe("1");
    });

    it("escapes quotes and backslashes in resource name", async () => {
      const spy = mockFetch({ resources: [], nextPageToken: "" });
      const api = createResourceApi("-");

      await api.get('//svc/res/"tricky\\name"');

      const { params } = parseRelativeUrl(spy.mock.calls[0][0] as string);
      expect(params.get("filter")).toBe(
        'name == "//svc/res/\\"tricky\\\\name\\""',
      );
    });
  });

  describe("searchAll", () => {
    it("returns flat array following nextPageToken across pages", async () => {
      const page1: QueryResourcesResponse<TestProps> = {
        resources: [makeResult({ name: "//a" })],
        nextPageToken: "page2",
      };
      const page2: QueryResourcesResponse<TestProps> = {
        resources: [makeResult({ name: "//b" })],
        nextPageToken: "",
      };

      const spy = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          statusText: "OK",
          json: () => Promise.resolve(page1),
        } as Response)
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          statusText: "OK",
          json: () => Promise.resolve(page2),
        } as Response);

      const api = createResourceApi<TestProps>("-");
      const results = await api.searchAll({
        filter: 'resource_type == "kind.fleetshift.io/Cluster"',
      });

      expect(results).toHaveLength(2);
      expect(results[0].name).toBe("//a");
      expect(results[1].name).toBe("//b");

      // Verify second call includes pageToken
      const { params } = parseRelativeUrl(spy.mock.calls[1][0] as string);
      expect(params.get("pageToken")).toBe("page2");
    });

    it("returns results from a single page when nextPageToken is empty", async () => {
      mockFetch({ resources: [makeResult()], nextPageToken: "" });
      const api = createResourceApi("-");

      const results = await api.searchAll();

      expect(results).toHaveLength(1);
    });

    it("throws on repeated nextPageToken to prevent infinite loop", async () => {
      const page: QueryResourcesResponse<TestProps> = {
        resources: [makeResult({ name: "//a" })],
        nextPageToken: "same-token",
      };

      vi.spyOn(globalThis, "fetch").mockResolvedValue({
        ok: true,
        status: 200,
        statusText: "OK",
        json: () => Promise.resolve(page),
      } as Response);

      const api = createResourceApi<TestProps>("-");

      await expect(api.searchAll()).rejects.toThrow(
        /Cyclic pagination detected.*same-token/,
      );
    });
  });
});

// ---------------------------------------------------------------------------
// createApiClient
// ---------------------------------------------------------------------------

describe("createApiClient", () => {
  it("GET appends query params", async () => {
    const spy = mockFetch({ id: "1" });
    const client = createApiClient("/v1");

    const res = await client.get<{ id: string }>("/items", { foo: "bar" });

    expect(res).toEqual({ id: "1" });
    const calledUrl = spy.mock.calls[0][0] as string;
    expect(calledUrl).toBe("/v1/items?foo=bar");
  });

  it("GET without params omits query string", async () => {
    const spy = mockFetch({ ok: true });
    const client = createApiClient("/api");

    await client.get("/status");

    expect(spy.mock.calls[0][0]).toBe("/api/status");
  });

  it("POST sends JSON body", async () => {
    const spy = mockFetch({ name: "new" });
    const client = createApiClient("/v1");

    await client.post("/items", { name: "new" });

    const [url, init] = spy.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/v1/items");
    expect(init.method).toBe("POST");
    expect(init.headers).toEqual({ "Content-Type": "application/json" });
    expect(JSON.parse(init.body as string)).toEqual({ name: "new" });
  });

  it("POST without body omits Content-Type", async () => {
    const spy = mockFetch({});
    const client = createApiClient("/v1");

    await client.post("/actions/trigger");

    const [, init] = spy.mock.calls[0] as [string, RequestInit];
    expect(init.method).toBe("POST");
    expect(init.headers).toBeUndefined();
  });

  it("PUT sends JSON body", async () => {
    const spy = mockFetch({ updated: true });
    const client = createApiClient("/v1");

    await client.put("/items/1", { name: "updated" });

    const [url, init] = spy.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/v1/items/1");
    expect(init.method).toBe("PUT");
    expect(JSON.parse(init.body as string)).toEqual({ name: "updated" });
  });

  it("PATCH sends JSON body", async () => {
    const spy = mockFetch({ patched: true });
    const client = createApiClient("/v1");

    await client.patch("/items/1", { name: "patched" });

    const [url, init] = spy.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/v1/items/1");
    expect(init.method).toBe("PATCH");
    expect(JSON.parse(init.body as string)).toEqual({ name: "patched" });
  });

  it("DELETE sends method only", async () => {
    const spy = mockFetch({});
    const client = createApiClient("/v1");

    await client.delete("/items/1");

    const [url, init] = spy.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/v1/items/1");
    expect(init.method).toBe("DELETE");
  });

  it("DELETE returns undefined on 204 No Content", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue({
      ok: true,
      status: 204,
      statusText: "No Content",
      json: () => Promise.reject(new SyntaxError("Unexpected end of input")),
    } as unknown as Response);

    const client = createApiClient("/v1");
    const result = await client.delete("/items/1");

    expect(result).toBeUndefined();
  });

  it("throws ResourceApiError on non-OK response", async () => {
    mockFetch({ code: 5, message: "Not found" }, 404);
    const client = createApiClient("/v1");

    await expect(client.get("/missing")).rejects.toThrow(ResourceApiError);
    await expect(client.get("/missing")).rejects.toMatchObject({
      status: 404,
      rpcStatus: { code: 5, message: "Not found" },
    });
  });

  it("throws ResourceApiError with null rpcStatus on unparseable body", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      json: () => Promise.reject(new SyntaxError("Unexpected token")),
    } as unknown as Response);

    const client = createApiClient("/v1");
    const err = await client.get("/broken").catch((e: unknown) => e);

    expect(err).toBeInstanceOf(ResourceApiError);
    expect((err as ResourceApiError).status).toBe(500);
    expect((err as ResourceApiError).rpcStatus).toBeNull();
  });
});
