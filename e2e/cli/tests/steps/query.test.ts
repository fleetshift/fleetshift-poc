/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  indexedConfigMapFilter,
  kindClusterIdentityFilter,
  kindNodeInClusterFilter,
  kubernetesObjectInCluster,
  kubernetesObjectKindFilter,
  kubernetesObjectsInClusterFilter,
  observation,
  parseQueryPage,
} from "./query";

describe("resource query scoping", () => {
  it("builds an exact cluster-scoped Kubernetes filter", () => {
    expect(kubernetesObjectsInClusterFilter("kind/e2e")).toContain(
      "//kubernetes.fleetshift.io/clusters/kind%2Fe2e/",
    );
    expect(kubernetesObjectKindFilter("a", "Node")).toContain(
      'resource.observation.kind == "Node"',
    );
  });

  it("builds exact ConfigMap, Kind Cluster, and Kind Node filters", () => {
    expect(indexedConfigMapFilter("a", "ns-1")).toContain(
      'resource.observation.metadata.namespace == "ns-1"',
    );
    expect(indexedConfigMapFilter("a", "ns-1", "custom")).toContain(
      'resource.observation.metadata.name == "custom"',
    );
    expect(kindClusterIdentityFilter("kind-e2e")).toContain(
      'resource.name == "clusters/kind-e2e"',
    );
    expect(kindClusterIdentityFilter("kind-e2e")).not.toContain("ACTIVE");
    expect(kindNodeInClusterFilter("kind-e2e")).toContain(
      'resource.observation.cluster == "clusters/kind-e2e"',
    );
  });

  it("rejects envelope names that are not in-cluster Kubernetes objects", () => {
    const hit = {
      name: "//kubernetes.fleetshift.io/clusters/a/apiResources/v1/nodes/objects/node-a",
      resource: {},
      resourceType: "kubernetes.fleetshift.io/Object",
    };
    expect(kubernetesObjectInCluster(hit, "a")).toBe(true);
    expect(kubernetesObjectInCluster(hit, "b")).toBe(false);
    expect(
      kubernetesObjectInCluster({ ...hit, resourceType: "other" }, "a"),
    ).toBe(false);
    expect(
      kubernetesObjectInCluster(
        {
          ...hit,
          name: "//kubernetes.fleetshift.io/clusters/a/nodes/objects/node-a",
        },
        "a",
      ),
    ).toBe(false);
    expect(
      kubernetesObjectInCluster(
        {
          ...hit,
          name: "//kubernetes.fleetshift.io/clusters/a/apiResources/v1/nodes/objects/",
        },
        "a",
      ),
    ).toBe(false);
  });
});

describe("query JSON parsing", () => {
  it("decodes pagination and observation fields used by matching", () => {
    const page = parseQueryPage(`{
      "resources": [{
        "name": "//kubernetes.fleetshift.io/clusters/kind-e2e-abcd/apiResources/nodes/objects/node-uid",
        "resourceType": "kubernetes.fleetshift.io/Object",
        "resource": {
          "name": "unused-envelope-name",
          "observation": {
            "kind": "Node",
            "cluster": "clusters/kind-e2e-abcd",
            "gvr": {"resource": "nodes"},
            "metadata": {"name": "fs--kind-e2e-abcd-control-plane", "uid": "node-uid"},
            "extracted": {"kubeletVersion": "v1.31.0"}
          }
        }
      }],
      "nextPageToken": "tok"
    }`);
    expect(page.nextPageToken).toBe("tok");
    expect(page.resources).toHaveLength(1);
    const hit = page.resources[0];
    expect(hit.resourceType).toBe("kubernetes.fleetshift.io/Object");
    expect(kubernetesObjectInCluster(hit, "kind-e2e-abcd")).toBe(true);
    expect(observation(hit)).toEqual({
      cluster: "clusters/kind-e2e-abcd",
      extracted: { kubeletVersion: "v1.31.0" },
      kind: "Node",
      metadata: {
        name: "fs--kind-e2e-abcd-control-plane",
        namespace: "",
      },
    });
  });

  it("defaults missing pagination fields and ignores unused properties", () => {
    expect(parseQueryPage("{}")).toEqual({ nextPageToken: "", resources: [] });
    expect(parseQueryPage('{"resources":[],"unused":true}')).toEqual({
      nextPageToken: "",
      resources: [],
    });
    expect(
      parseQueryPage('{"resources":[{"extra":1}],"nextPageToken":"x"}'),
    ).toEqual({
      nextPageToken: "x",
      resources: [{ name: "", resource: null, resourceType: "" }],
    });
  });

  it("reports invalid JSON", () => {
    expect(() => parseQueryPage("{")).toThrow(/invalid JSON/);
  });

  it("fails when a query hit has no observation", () => {
    expect(() =>
      observation({ name: "broken", resource: {}, resourceType: "t" }),
    ).toThrow("query hit broken has no observation");
  });
});
