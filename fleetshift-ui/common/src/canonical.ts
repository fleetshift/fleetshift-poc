/**
 * Canonical signed envelope construction for attestation signing.
 *
 * Port of fleetshift-poc/fleetshift-server/pkg/canonical/envelope.go.
 * Both the Go server (verifier) and the browser (signer) must produce
 * byte-identical JSON from identical inputs.
 */

export interface ManifestStrategy {
  type: string;
  manifests?: Manifest[];
}

export interface Manifest {
  manifestType: string;
  content: unknown;
}

export interface PlacementStrategy {
  type: string;
  targets?: string[];
  matchLabels?: Record<string, string>;
}

export interface OutputConstraint {
  name: string;
  expression: string;
}

/**
 * Build the canonical JSON envelope that gets hashed and signed.
 * Field order and serialization rules match the Go implementation
 * exactly so both sides produce identical bytes.
 */
export function buildSignedInputEnvelope(
  deploymentId: string,
  manifestStrategy: ManifestStrategy,
  placementStrategy: PlacementStrategy,
  validUntil: Date,
  outputConstraints: OutputConstraint[],
  expectedGeneration: number,
): string {
  const content: Record<string, unknown> = {
    name: deploymentId.startsWith("deployments/")
      ? deploymentId
      : `deployments/${deploymentId}`,
    manifest_strategy: marshalManifestStrategy(manifestStrategy),
    placement_strategy: marshalPlacementStrategy(placementStrategy),
  };

  const env: Record<string, unknown> = {
    content,
    output_constraints: marshalOutputConstraints(outputConstraints),
    valid_until: Math.floor(validUntil.getTime() / 1000),
  };

  if (expectedGeneration !== 0) {
    env.expected_generation = expectedGeneration;
  }

  return JSON.stringify(env);
}

/**
 * Compute the SHA-256 digest of canonical envelope bytes.
 */
export async function hashIntent(envelope: string): Promise<Uint8Array> {
  const data = new TextEncoder().encode(envelope);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return new Uint8Array(hash);
}

function marshalManifestStrategy(
  ms: ManifestStrategy,
): Record<string, unknown> {
  const out: Record<string, unknown> = { type: ms.type };
  if (ms.manifests && ms.manifests.length > 0) {
    out.manifests = ms.manifests.map((m) => ({
      manifest_type: m.manifestType,
      content: m.content,
    }));
  }
  return out;
}

function marshalPlacementStrategy(
  ps: PlacementStrategy,
): Record<string, unknown> {
  const out: Record<string, unknown> = { type: ps.type };
  if (ps.targets && ps.targets.length > 0) {
    out.targets = ps.targets;
  }
  if (ps.matchLabels && Object.keys(ps.matchLabels).length > 0) {
    // Go's encoding/json sorts map keys alphabetically
    const sorted: Record<string, string> = {};
    for (const key of Object.keys(ps.matchLabels).sort()) {
      sorted[key] = ps.matchLabels[key];
    }
    out.match_labels = sorted;
  }
  return out;
}

function marshalOutputConstraints(
  constraints: OutputConstraint[],
): Array<{ expression: string; name: string }> {
  if (!constraints || constraints.length === 0) {
    return [];
  }
  const docs = constraints.map((c) => ({
    // field order: expression before name (matches Go struct tag order)
    expression: c.expression,
    name: c.name,
  }));
  // Sort by JSON serialization of each entry, matching Go behavior
  docs.sort((a, b) => {
    const aJson = JSON.stringify(a);
    const bJson = JSON.stringify(b);
    return aJson < bJson ? -1 : aJson > bJson ? 1 : 0;
  });
  return docs;
}
