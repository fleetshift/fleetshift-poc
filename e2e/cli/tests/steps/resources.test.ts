/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import { parseResourceDescribe, parseResourceTypeRows } from "./resources";

describe("parseResourceTypeRows", () => {
  it("reads type, singular, and service from table rows", () => {
    expect(
      parseResourceTypeRows(`TYPE  SINGULAR  SERVICE
ocm.kind.v1alpha/clusters  Cluster  ocm.kind.v1alpha.ClusterService
ocm.kind.v1alpha/nodes\tNode\tocm.kind.v1alpha.NodeService
`),
    ).toEqual([
      {
        service: "ocm.kind.v1alpha.ClusterService",
        singular: "Cluster",
        type: "ocm.kind.v1alpha/clusters",
      },
      {
        service: "ocm.kind.v1alpha.NodeService",
        singular: "Node",
        type: "ocm.kind.v1alpha/nodes",
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
    const described = parseResourceDescribe(`Type:     ocm.kind.v1alpha/clusters
Singular: Cluster
Service:  ocm.kind.v1alpha.ClusterService

Methods:
  CreateCluster
  GetCluster
  ListClusters
  DeleteCluster

Spec (ocm.kind.v1alpha.KindClusterSpec):
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
    expect(described.type).toBe("ocm.kind.v1alpha/clusters");
    expect(described.specType).toBe("ocm.kind.v1alpha.KindClusterSpec");
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
