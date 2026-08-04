import { describe, expect, it } from "vitest";

import type {
  ManifestStrategy,
  OutputConstraint,
  PlacementStrategy,
} from "../canonical";
import { buildSignedInputEnvelope, hashIntent } from "../canonical";

// Matches Go: time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)
const testValidUntil = new Date(Date.UTC(2026, 2, 11, 12, 0, 0));

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// Golden vectors generated from the Go implementation.
// Any drift between TS and Go will break attestation verification.

describe("buildSignedInputEnvelope", () => {
  it("vector 1: full envelope with manifests, targets, generation", () => {
    const ms: ManifestStrategy = {
      type: "inline",
      manifests: [
        { manifestType: "api.kind.cluster", content: { name: "c1" } },
      ],
    };
    const ps: PlacementStrategy = { type: "static", targets: ["target-a"] };
    const env = buildSignedInputEnvelope(
      "my-dep",
      ms,
      ps,
      testValidUntil,
      [],
      1,
    );

    expect(env).toBe(
      '{"content":{"name":"deployments/my-dep","manifest_strategy":{"type":"inline","manifests":[{"manifest_type":"api.kind.cluster","content":{"name":"c1"}}]},"placement_strategy":{"type":"static","targets":["target-a"]}},"output_constraints":[],"valid_until":1773230400,"expected_generation":1}',
    );
  });

  it("vector 2: minimal envelope, generation=0 omitted", () => {
    const ms: ManifestStrategy = { type: "inline" };
    const ps: PlacementStrategy = { type: "all" };
    const env = buildSignedInputEnvelope(
      "dep-1",
      ms,
      ps,
      testValidUntil,
      [],
      0,
    );

    expect(env).toBe(
      '{"content":{"name":"deployments/dep-1","manifest_strategy":{"type":"inline"},"placement_strategy":{"type":"all"}},"output_constraints":[],"valid_until":1773230400}',
    );
  });

  it("vector 3: constraints sorted by JSON serialization", () => {
    const ms: ManifestStrategy = { type: "inline" };
    const ps: PlacementStrategy = { type: "all" };
    const constraints: OutputConstraint[] = [
      { name: "z-constraint", expression: "output.foo == true" },
      { name: "a-constraint", expression: "output.bar == true" },
    ];
    const env = buildSignedInputEnvelope(
      "dep-1",
      ms,
      ps,
      testValidUntil,
      constraints,
      1,
    );

    expect(env).toBe(
      '{"content":{"name":"deployments/dep-1","manifest_strategy":{"type":"inline"},"placement_strategy":{"type":"all"}},"output_constraints":[{"expression":"output.bar == true","name":"a-constraint"},{"expression":"output.foo == true","name":"z-constraint"}],"valid_until":1773230400,"expected_generation":1}',
    );
  });

  it("vector 4: match_labels with sorted keys", () => {
    const ms: ManifestStrategy = { type: "inline" };
    const ps: PlacementStrategy = {
      type: "label",
      matchLabels: { zone: "us-east", env: "prod" },
    };
    const env = buildSignedInputEnvelope(
      "dep-2",
      ms,
      ps,
      testValidUntil,
      [],
      1,
    );

    expect(env).toBe(
      '{"content":{"name":"deployments/dep-2","manifest_strategy":{"type":"inline"},"placement_strategy":{"type":"label","match_labels":{"env":"prod","zone":"us-east"}}},"output_constraints":[],"valid_until":1773230400,"expected_generation":1}',
    );
  });

  it("vector 5: multiple manifests", () => {
    const ms: ManifestStrategy = {
      type: "inline",
      manifests: [
        { manifestType: "Deployment", content: { replicas: 3 } },
        { manifestType: "Service", content: { port: 80 } },
      ],
    };
    const ps: PlacementStrategy = { type: "static", targets: ["t1", "t2"] };
    const env = buildSignedInputEnvelope(
      "dep-3",
      ms,
      ps,
      testValidUntil,
      [],
      2,
    );

    expect(env).toBe(
      '{"content":{"name":"deployments/dep-3","manifest_strategy":{"type":"inline","manifests":[{"manifest_type":"Deployment","content":{"replicas":3}},{"manifest_type":"Service","content":{"port":80}}]},"placement_strategy":{"type":"static","targets":["t1","t2"]}},"output_constraints":[],"valid_until":1773230400,"expected_generation":2}',
    );
  });

  it("preserves full resource name when already prefixed", () => {
    const ms: ManifestStrategy = { type: "inline" };
    const ps: PlacementStrategy = { type: "all" };
    const env = buildSignedInputEnvelope(
      "deployments/already-prefixed",
      ms,
      ps,
      testValidUntil,
      [],
      1,
    );

    expect(env).toContain('"name":"deployments/already-prefixed"');
  });

  it("prefixes slash-containing non-resource IDs", () => {
    const ms: ManifestStrategy = { type: "inline" };
    const ps: PlacementStrategy = { type: "all" };
    const env = buildSignedInputEnvelope(
      "foo/bar",
      ms,
      ps,
      testValidUntil,
      [],
      1,
    );

    expect(env).toContain('"name":"deployments/foo/bar"');
  });

  it("is deterministic — same inputs produce same output", () => {
    const ms: ManifestStrategy = {
      type: "inline",
      manifests: [
        { manifestType: "api.kind.cluster", content: { name: "test-cluster" } },
      ],
    };
    const ps: PlacementStrategy = { type: "static", targets: ["t1", "t2"] };
    const a = buildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, [], 1);
    const b = buildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, [], 1);
    expect(a).toBe(b);
  });

  it("different deployment IDs produce different envelopes", () => {
    const ms: ManifestStrategy = { type: "inline" };
    const ps: PlacementStrategy = { type: "all" };
    const a = buildSignedInputEnvelope("dep-1", ms, ps, testValidUntil, [], 1);
    const b = buildSignedInputEnvelope("dep-2", ms, ps, testValidUntil, [], 1);
    expect(a).not.toBe(b);
  });
});

describe("hashIntent", () => {
  it("vector 1: matches Go SHA-256 hash", async () => {
    const envelope =
      '{"content":{"name":"deployments/my-dep","manifest_strategy":{"type":"inline","manifests":[{"manifest_type":"api.kind.cluster","content":{"name":"c1"}}]},"placement_strategy":{"type":"static","targets":["target-a"]}},"output_constraints":[],"valid_until":1773230400,"expected_generation":1}';
    const hash = await hashIntent(envelope);
    expect(toHex(hash)).toBe(
      "51b18238a9177eb2f75b491df63b5df43152f286e76aa1189188a834a485f261",
    );
  });

  it("vector 2: minimal envelope hash", async () => {
    const envelope =
      '{"content":{"name":"deployments/dep-1","manifest_strategy":{"type":"inline"},"placement_strategy":{"type":"all"}},"output_constraints":[],"valid_until":1773230400}';
    const hash = await hashIntent(envelope);
    expect(toHex(hash)).toBe(
      "7d14a987dd45a4be7a16ceed059ded1889d14eb5b7be79098444d806c2e58583",
    );
  });

  it("returns 32 bytes (SHA-256)", async () => {
    const hash = await hashIntent("test");
    expect(hash.length).toBe(32);
  });

  it("is deterministic", async () => {
    const a = await hashIntent("test-data");
    const b = await hashIntent("test-data");
    expect(toHex(a)).toBe(toHex(b));
  });
});
