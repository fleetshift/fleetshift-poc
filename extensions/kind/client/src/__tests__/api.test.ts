import { afterEach, describe, expect, it, vi } from "vitest";

import type { KindCluster, KindClusterSpec } from "../api";
import { createKindCluster } from "../api";

function mockFetch(body: unknown, status = 200) {
  return vi.spyOn(globalThis, "fetch").mockResolvedValue({
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? "OK" : "Error",
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  } as Response);
}

function parseRelativeUrl(url: string) {
  const [path, qs] = url.split("?");
  return { path, params: new URLSearchParams(qs ?? "") };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("createKindCluster", () => {
  const clusterResponse: KindCluster = {
    name: "clusters/my-cluster",
    uid: "abc-123",
    spec: { nodes: [{ role: "control-plane" }] },
    state: "CREATING",
    createTime: "2026-07-21T00:00:00Z",
    updateTime: "2026-07-21T00:00:00Z",
    etag: 'W/"1"',
    generation: "1",
  };

  it("sends POST to kind managed resource API with cluster_id param", async () => {
    const spy = mockFetch(clusterResponse);

    const spec: KindClusterSpec = {
      nodes: [{ role: "control-plane" }],
    };

    const result = await createKindCluster("my-cluster", spec);

    expect(spy).toHaveBeenCalledOnce();
    const [url, init] = spy.mock.calls[0];
    const { path, params } = parseRelativeUrl(url as string);
    expect(path).toBe("/apis/kind.fleetshift.io/v1/clusters");
    expect(params.get("cluster_id")).toBe("my-cluster");
    expect(init?.method).toBe("POST");
    expect(JSON.parse(init?.body as string)).toEqual({ spec });
    expect(result).toEqual(clusterResponse);
  });

  it("includes networking in spec when provided", async () => {
    const spy = mockFetch(clusterResponse);

    const spec: KindClusterSpec = {
      nodes: [{ role: "control-plane" }, { role: "worker", image: "v1.30" }],
      networking: { apiServerPort: 6443, podSubnet: "10.244.0.0/16" },
    };

    await createKindCluster("test-cluster", spec);

    const body = JSON.parse(spy.mock.calls[0][1]?.body as string);
    expect(body.spec.networking.apiServerPort).toBe(6443);
    expect(body.spec.networking.podSubnet).toBe("10.244.0.0/16");
  });

  it("URL-encodes the cluster ID", async () => {
    const spy = mockFetch(clusterResponse);

    await createKindCluster("my cluster/special", {});

    const { params } = parseRelativeUrl(spy.mock.calls[0][0] as string);
    expect(params.get("cluster_id")).toBe("my cluster/special");
  });

  it("throws on API error", async () => {
    mockFetch({ message: "quota exceeded" }, 400);

    await expect(createKindCluster("fail", {})).rejects.toThrow(
      "Kind cluster creation failed",
    );
  });
});
