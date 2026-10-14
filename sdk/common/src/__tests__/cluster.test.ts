/* eslint-disable playwright/no-standalone-expect */

import { describe, expect, it } from "vitest";

import { extractClusterId } from "../objects/cluster.js";

describe("extractClusterId", () => {
  it.each([
    ["clusters/kind123", "kind123"],
    [
      "//kubernetes.fleetshift.io/clusters/kind123/apiResources/namespaces/objects/namespace-uid",
      "kind123",
    ],
    ["kind123", "kind123"],
  ])("extracts cluster ID from %s", (resourceName, expected) => {
    expect(extractClusterId(resourceName)).toBe(expected);
  });
});
