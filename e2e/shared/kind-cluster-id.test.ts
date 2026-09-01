/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  MAX_KIND_RESOURCE_ID_LENGTH,
  uniqueKindClusterId,
  uniqueKindClusterIdFromEnv,
} from "./kind-cluster-id";

describe("uniqueKindClusterId", () => {
  it("leaves room for the Kind addon ownership prefix", () => {
    const id = uniqueKindClusterId("kind-e2e-54598f081769-");

    expect(id).toMatch(/^kind-e2e-54598f081769-[a-f0-9]{8}$/);
    expect(id.length).toBeLessThanOrEqual(MAX_KIND_RESOURCE_ID_LENGTH);
  });

  it("rejects a prefix that would exceed the provider name limit", () => {
    expect(() =>
      uniqueKindClusterId("a".repeat(MAX_KIND_RESOURCE_ID_LENGTH)),
    ).toThrow(/too long/);
  });

  it("rejects a prefix that is not RFC1123-safe", () => {
    expect(() => uniqueKindClusterId("Kind-e2e-")).toThrow(/RFC1123-safe/);
    expect(() => uniqueKindClusterId("kind_e2e-")).toThrow(/RFC1123-safe/);
    expect(() => uniqueKindClusterId("-kind")).toThrow(/RFC1123-safe/);
  });
});

describe("uniqueKindClusterIdFromEnv", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("reads FLEETSHIFT_KIND_PREFIX from the environment", () => {
    vi.stubEnv("FLEETSHIFT_KIND_PREFIX", "kind-e2e-env-");
    expect(uniqueKindClusterIdFromEnv()).toMatch(/^kind-e2e-env-[a-f0-9]{8}$/);
  });

  it("fails when FLEETSHIFT_KIND_PREFIX is unset", () => {
    vi.stubEnv("FLEETSHIFT_KIND_PREFIX", undefined);
    expect(() => uniqueKindClusterIdFromEnv()).toThrow(/unset/);
  });
});
