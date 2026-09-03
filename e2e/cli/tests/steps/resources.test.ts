/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import { parseResourceDescribe, parseResourceTypeRows } from "./resources";

describe("parseResourceTypeRows", () => {
  it("reads type, singular, and service from table rows", () => {
    expect(
      parseResourceTypeRows(`TYPE  SINGULAR  SERVICE
kind.fleetshift.v1/clusters  Cluster  kind.fleetshift.v1.ClusterService
kind.fleetshift.v1/nodes\tNode\tkind.fleetshift.v1.NodeService
`),
    ).toEqual([
      {
        service: "kind.fleetshift.v1.ClusterService",
        singular: "Cluster",
        type: "kind.fleetshift.v1/clusters",
      },
      {
        service: "kind.fleetshift.v1.NodeService",
        singular: "Node",
        type: "kind.fleetshift.v1/nodes",
      },
    ]);
  });

  it("skips the empty-types message and malformed lines", () => {
    expect(
      parseResourceTypeRows("No extension resource types available.\n"),
    ).toEqual([]);
  });
});

describe("parseResourceDescribe", () => {
  it("extracts methods, spec type, and nested field names from sections", () => {
    const described =
      parseResourceDescribe(`Type:     kind.fleetshift.v1/clusters
Singular: Cluster
Service:  kind.fleetshift.v1.ClusterService

Methods:
  CreateCluster
  GetCluster
  ListClusters
  DeleteCluster

Spec (addons.kind.v1.KindClusterSpec):
  string name = 1
  repeated Node nodes = 2 {
    string role = 1
    string image = 2
  }
  optional Networking networking = 3 {
    int32 api_server_port = 1
  }
  optional OIDC oidc = 4 {
    string username_claim = 1
  }
`);
    expect(described.type).toBe("kind.fleetshift.v1/clusters");
    expect(described.specType).toBe("addons.kind.v1.KindClusterSpec");
    expect(described.methods).toEqual([
      "CreateCluster",
      "GetCluster",
      "ListClusters",
      "DeleteCluster",
    ]);
    expect(described.fieldNames).toEqual(
      expect.arrayContaining(["nodes", "networking", "oidc", "role", "image"]),
    );
  });
});
