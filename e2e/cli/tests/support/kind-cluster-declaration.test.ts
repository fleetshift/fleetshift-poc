/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  encodeKindClusterDetails,
  KIND_CLUSTERS_ANNOTATION_TYPE,
  readKindClusterRequests,
} from "./kind-cluster-declaration";

const REQUESTS = [
  { access: "read-only" as const, state: "any" as const },
  { access: "modifiable" as const, state: "clean" as const },
];

describe("kindClusters annotation codec", () => {
  it("round-trips requests and preserves existing tags and annotations", () => {
    const encoded = encodeKindClusterDetails({
      annotation: { description: "https://example.test/issue", type: "issue" },
      kindClusters: REQUESTS,
      tag: "@smoke",
    });
    expect(encoded.tag).toBe("@smoke");
    expect(encoded).not.toHaveProperty("kindClusters");
    expect(readKindClusterRequests(annotations(encoded))).toEqual(REQUESTS);
    expect(annotations(encoded)).toEqual([
      { description: "https://example.test/issue", type: "issue" },
      {
        description: JSON.stringify(REQUESTS),
        type: KIND_CLUSTERS_ANNOTATION_TYPE,
      },
    ]);
  });

  it("rejects missing, duplicate, and malformed declarations", () => {
    expect(() => readKindClusterRequests([])).toThrow(/missing/);
    expect(() =>
      readKindClusterRequests([
        { description: "[]", type: KIND_CLUSTERS_ANNOTATION_TYPE },
        { description: "[]", type: KIND_CLUSTERS_ANNOTATION_TYPE },
      ]),
    ).toThrow(/duplicated/);
    expect(() =>
      readKindClusterRequests([
        { description: "{", type: KIND_CLUSTERS_ANNOTATION_TYPE },
      ]),
    ).toThrow(/malformed/);
    expect(() =>
      readKindClusterRequests([
        {
          description: '{"access":"read-only"}',
          type: KIND_CLUSTERS_ANNOTATION_TYPE,
        },
      ]),
    ).toThrow(/malformed/);
    expect(() =>
      readKindClusterRequests([
        {
          description: '[{"access":"write","state":"any"}]',
          type: KIND_CLUSTERS_ANNOTATION_TYPE,
        },
      ]),
    ).toThrow(/malformed/);
    expect(() =>
      readKindClusterRequests([
        {
          description:
            '[{"access":"read-only","state":"any","spec":{"nodes":[{"role":"ingress"}]}}]',
          type: KIND_CLUSTERS_ANNOTATION_TYPE,
        },
      ]),
    ).toThrow(/malformed/);
  });

  it("round-trips an omitted spec as unconstrained and {} as pinned default", () => {
    const unconstrained = [
      { access: "read-only" as const, state: "any" as const },
    ];
    const pinnedDefault = [
      { access: "read-only" as const, spec: {}, state: "any" as const },
    ];
    const multiNode = [
      {
        access: "read-only" as const,
        spec: {
          nodes: [
            { role: "control-plane" as const },
            { role: "worker" as const },
          ],
        },
        state: "any" as const,
      },
    ];
    expect(
      readKindClusterRequests(
        annotations(encodeKindClusterDetails({ kindClusters: unconstrained })),
      ),
    ).toEqual(unconstrained);
    expect(
      readKindClusterRequests(
        annotations(encodeKindClusterDetails({ kindClusters: pinnedDefault })),
      ),
    ).toEqual(pinnedDefault);
    expect(
      readKindClusterRequests(
        annotations(encodeKindClusterDetails({ kindClusters: multiNode })),
      ),
    ).toEqual(multiNode);
  });
});

function annotations(details: {
  annotation?:
    | { description?: string; type: string }
    | Array<{ description?: string; type: string }>;
}): Array<{ description?: string; type: string }> {
  if (details.annotation === undefined) return [];
  return Array.isArray(details.annotation)
    ? details.annotation
    : [details.annotation];
}
