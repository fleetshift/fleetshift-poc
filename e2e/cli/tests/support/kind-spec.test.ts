/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  isDefaultKindClusterSpec,
  kindClusterCreateSpecFromView,
  kindClusterSpecKey,
  kindClusterSpecsEqual,
  parseKindClusterCreateSpec,
} from "./kind-spec";

const MULTI_NODE = {
  nodes: [{ role: "control-plane" as const }, { role: "worker" as const }],
};

describe("kind cluster spec identity", () => {
  it("treats omitted nodes and {} as the default spec", () => {
    expect(kindClusterSpecKey({})).toBe("{}");
    expect(isDefaultKindClusterSpec({})).toBe(true);
    expect(isDefaultKindClusterSpec({ nodes: undefined })).toBe(true);
    expect(kindClusterSpecsEqual({}, {})).toBe(true);
  });

  it("treats an empty nodes list as distinct from default", () => {
    expect(isDefaultKindClusterSpec({ nodes: [] })).toBe(false);
    expect(kindClusterSpecsEqual({}, { nodes: [] })).toBe(false);
  });

  it("reads create spec from a cluster view and ignores extra fields", () => {
    expect(kindClusterCreateSpecFromView({ name: "clusters/a" })).toEqual({});
    expect(
      kindClusterCreateSpecFromView({
        name: "clusters/a",
        nodes: [{ role: "control-plane" }, { role: "worker" }],
      }),
    ).toEqual(MULTI_NODE);
    expect(
      kindClusterSpecsEqual(
        kindClusterCreateSpecFromView({
          name: "clusters/a",
          nodes: [{ role: "worker" }],
        }),
        {},
      ),
    ).toBe(false);
  });

  it("ignores property insertion order when comparing nodes", () => {
    expect(
      kindClusterSpecKey({
        nodes: [{ image: "kindest/node:v1.31.0", role: "worker" }],
      }),
    ).toBe(
      kindClusterSpecKey({
        nodes: [{ role: "worker", image: "kindest/node:v1.31.0" }],
      }),
    );
  });

  it("treats node order as significant", () => {
    expect(
      kindClusterSpecsEqual(MULTI_NODE, {
        nodes: [{ role: "worker" }, { role: "control-plane" }],
      }),
    ).toBe(false);
  });
});

describe("kind cluster spec parsing", () => {
  it("accepts default and nested node specs", () => {
    expect(parseKindClusterCreateSpec({})).toEqual({});
    expect(
      parseKindClusterCreateSpec({
        nodes: [
          { role: "control-plane" },
          { image: "kindest/node:v1.31.0", role: "worker" },
        ],
      }),
    ).toEqual({
      nodes: [
        { role: "control-plane" },
        { image: "kindest/node:v1.31.0", role: "worker" },
      ],
    });
  });

  it.each([
    { name: "null", value: null },
    { name: "array", value: [] },
    { name: "unknown field", value: { networking: {} } },
    { name: "non-array nodes", value: { nodes: {} } },
    {
      name: "unknown node field",
      value: { nodes: [{ role: "worker", extra: 1 }] },
    },
    { name: "invalid role", value: { nodes: [{ role: "ingress" }] } },
    {
      name: "non-string image",
      value: { nodes: [{ role: "worker", image: 1 }] },
    },
  ])("rejects $name", ({ value }) => {
    expect(() => parseKindClusterCreateSpec(value)).toThrow(/malformed/);
  });
});
