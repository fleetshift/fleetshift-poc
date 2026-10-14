/* eslint-disable playwright/no-standalone-expect */
import { describe, expect, it } from "vitest";

import { resourceRoute } from "../commands/resource/helpers";

describe("resource HTTP routes", () => {
  it("maps qualified resource types to dynamic API routes", () => {
    expect(resourceRoute("kind.fleetshift.v1/clusters")).toEqual({
      collection: "clusters",
      path: "/apis/kind.fleetshift.io/v1/clusters",
      singular: "cluster",
    });
  });

  it("requires qualified resource types", () => {
    expect(() => resourceRoute("clusters")).toThrow("qualified form");
  });
});
