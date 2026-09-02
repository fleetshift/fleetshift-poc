/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  parseCluster,
  parseClusterList,
  parseConfigMapData,
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
      state: "ACTIVE",
    });
  });

  it("defaults missing optional fields", () => {
    expect(parseCluster("{}")).toEqual({
      conditions: {},
      name: "",
      pauseReason: "",
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
