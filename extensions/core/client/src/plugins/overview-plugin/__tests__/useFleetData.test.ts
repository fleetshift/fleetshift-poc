import { describe, expect, it } from "vitest";

import { toDashboardCluster } from "../useFleetData";

describe("retained cluster dashboard", () => {
  it("identifies Kind and leaves unreported versions unknown", () => {
    const cluster = toDashboardCluster(
      {
        name: "//kind.open-cluster-management.io/clusters/local",
        resourceType: "kind.open-cluster-management.io/Cluster",
        resource: {
          name: "clusters/local",
          uid: "cluster-1",
          state: "RUNNING",
          labels: {},
          createTime: "",
          updateTime: "",
          etag: "1",
        },
      },
      {
        region: "demo-region",
        lat: 0,
        lng: 0,
        environment: "Development",
        cpuPercent: 40,
        memoryPercent: 50,
      },
    );
    expect(cluster.id).toBe("local");
    expect(cluster.provider).toBe("kind");
    expect(cluster.version).toBe("unknown");
    expect(cluster.status).toBe("healthy");
  });
});
