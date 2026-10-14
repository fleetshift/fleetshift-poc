import { createHash, createPrivateKey, sign } from "node:crypto";
import { readFile } from "node:fs/promises";

import { configDirectory } from "../config";

export interface CanonicalManifest {
  manifest_type: string;
  content: unknown;
}

export interface CanonicalPlacement {
  type: string;
  targets?: string[];
  match_labels?: Record<string, string>;
}

export function buildDeploymentEnvelope(input: {
  deploymentID: string;
  manifestType: string;
  manifest: unknown;
  placement: CanonicalPlacement;
  validUntil: Date;
  expectedGeneration?: number;
}): string {
  const content = {
    name: input.deploymentID.startsWith("deployments/")
      ? input.deploymentID
      : `deployments/${input.deploymentID}`,
    manifest_strategy: {
      type: "inline",
      manifests: [
        { manifest_type: input.manifestType, content: input.manifest },
      ],
    },
    placement_strategy: input.placement,
  };
  const envelope: Record<string, unknown> = {
    content,
    output_constraints: [],
    valid_until: Math.floor(input.validUntil.getTime() / 1000),
  };
  if (input.expectedGeneration)
    envelope.expected_generation = input.expectedGeneration;
  return JSON.stringify(envelope);
}

export async function signDeploymentEnvelope(
  directory: string | undefined,
  envelope: string,
): Promise<string> {
  const keyPEM = await readFile(
    `${configDirectory(directory)}/signing_key.pem`,
    "utf8",
  );
  const privateKey = createPrivateKey(keyPEM);
  const digest = createHash("sha256").update(envelope).digest();
  return sign(null, digest, privateKey).toString("base64");
}
