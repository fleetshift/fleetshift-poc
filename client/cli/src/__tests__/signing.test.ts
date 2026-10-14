/* eslint-disable playwright/no-standalone-expect */
import { describe, expect, it } from "vitest";

import { buildDeploymentEnvelope } from "../crypto/signing";

describe("deployment signing envelope", () => {
  it("matches canonical field names and omits zero generation", () => {
    const envelope = buildDeploymentEnvelope({
      deploymentID: "demo",
      manifestType: "kubernetes",
      manifest: { apiVersion: "v1", kind: "Namespace" },
      placement: { type: "all" },
      validUntil: new Date("2030-01-01T00:00:00Z"),
    });
    expect(JSON.parse(envelope)).toEqual({
      content: {
        name: "deployments/demo",
        manifest_strategy: {
          type: "inline",
          manifests: [
            {
              manifest_type: "kubernetes",
              content: { apiVersion: "v1", kind: "Namespace" },
            },
          ],
        },
        placement_strategy: { type: "all" },
      },
      output_constraints: [],
      valid_until: 1893456000,
    });
  });
});
