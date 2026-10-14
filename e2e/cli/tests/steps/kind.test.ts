/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  kubernetesNodeRole,
  parseCluster,
  parseClusterList,
  parseConfigMapData,
  parseKubernetesNodes,
  parseMetadataName,
} from "./kind";

describe("kind JSON parsing", () => {
  it("reads name, state, pauseReason, and Ready status", () => {
    const cluster = parseCluster(
      '{"name":"clusters/kind-e2e-abcd","state":"ACTIVE","pauseReason":"token expired","conditions":{"Ready":{"status":"True","message":"ignored"}},"uid":"ignored"}',
    );
    expect(cluster).toEqual({
      conditions: { Ready: { status: "True", message: "ignored" } },
      name: "clusters/kind-e2e-abcd",
      pauseReason: "token expired",
      spec: {},
      state: "ACTIVE",
    });
  });

  it("defaults missing optional fields", () => {
    expect(parseCluster("{}")).toEqual({
      conditions: {},
      name: "",
      pauseReason: "",
      spec: {},
      state: "",
    });
    expect(parseMetadataName("{}")).toBe("");
    expect(parseConfigMapData("{}")).toEqual({});
  });

  it("parses a cluster list, namespace name, and ConfigMap data", () => {
    expect(
      parseClusterList(
        '[{"name":"clusters/a","state":"CREATING","pauseReason":"waiting","conditions":{"Ready":{"status":"False"}}}]',
      ),
    ).toEqual([
      {
        conditions: { Ready: { status: "False" } },
        name: "clusters/a",
        pauseReason: "waiting",
        spec: {},
        state: "CREATING",
      },
    ]);
    expect(parseMetadataName('{"metadata":{"name":"e2e-abcd"}}')).toBe(
      "e2e-abcd",
    );
    expect(
      parseConfigMapData('{"data":{"from":"fleetshift-e2e-cli"}}'),
    ).toEqual({ from: "fleetshift-e2e-cli" });
  });

  it("reports invalid JSON", () => {
    expect(() => parseCluster("{")).toThrow(/invalid JSON/);
    expect(() => parseClusterList("{")).toThrow(/invalid JSON/);
    expect(() => parseClusterList("{}")).toThrow(/invalid JSON/);
    expect(() => parseMetadataName("{")).toThrow(/invalid JSON/);
    expect(() => parseConfigMapData("{")).toThrow(/invalid JSON/);
  });
});

describe("Kubernetes node parsing", () => {
  it("classifies control-plane and worker roles from labels", () => {
    expect(
      kubernetesNodeRole({ "node-role.kubernetes.io/control-plane": "" }),
    ).toBe("control-plane");
    expect(kubernetesNodeRole({ "node-role.kubernetes.io/master": "" })).toBe(
      "control-plane",
    );
    expect(kubernetesNodeRole({ "node-role.kubernetes.io/worker": "" })).toBe(
      "worker",
    );
    expect(kubernetesNodeRole({})).toBe("worker");
  });

  it("reads node names and roles from a Node list", () => {
    expect(
      parseKubernetesNodes(`{
        "items": [
          {
            "metadata": {
              "name": "fs--a-control-plane",
              "labels": { "node-role.kubernetes.io/control-plane": "" }
            }
          },
          { "metadata": { "name": "fs--a-worker", "labels": {} } }
        ]
      }`),
    ).toEqual([
      { name: "fs--a-control-plane", role: "control-plane" },
      { name: "fs--a-worker", role: "worker" },
    ]);
  });

  it("preserves a nested Kind node specification on resource get JSON", () => {
    expect(
      parseCluster(
        '{"name":"clusters/a","state":"ACTIVE","spec":{"nodes":[{"role":"control-plane"},{"role":"worker"}]}}',
      ).spec,
    ).toEqual({
      nodes: [{ role: "control-plane" }, { role: "worker" }],
    });
  });
});
