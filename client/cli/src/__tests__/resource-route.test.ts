/* eslint-disable playwright/no-standalone-expect */
import { describe, expect, it } from "vitest";

import { resourceRoute } from "../commands/resource/helpers";

describe("resource HTTP routes", () => {
  it("maps qualified resource types to dynamic API routes", () => {
    expect(resourceRoute("ocm.resource_manager.kind.v1alpha/clusters")).toEqual(
      {
        collection: "clusters",
        path: "/apis/kind.resource-manager.open-cluster-management.io/v1alpha/clusters",
        singular: "cluster",
      },
    );
    expect(
      resourceRoute("ocm.resource_manager.kubernetes.v1alpha/nodes"),
    ).toEqual({
      collection: "nodes",
      path: "/apis/kubernetes.resource-manager.open-cluster-management.io/v1alpha/nodes",
      singular: "node",
    });
  });

  it("requires qualified resource types", () => {
    expect(() => resourceRoute("clusters")).toThrow("qualified form");
  });

  it.each([
    "example.v1alpha/clusters",
    "ocm.resource_manager.kind.v2/clusters",
    "unrelated.ocm.resource_manager.kind.v1alpha/clusters",
    "ocm.resource_manager.kind.v1alpha.extra/clusters",
  ])("rejects unsupported qualified packages: %s", (type) => {
    expect(() => resourceRoute(type)).toThrow("unsupported resource package");
  });
});
