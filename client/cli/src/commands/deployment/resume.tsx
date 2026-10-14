import { flagString } from "../../argv";
import { configDirectory } from "../../config";
import {
  buildDeploymentEnvelope,
  signDeploymentEnvelope,
} from "../../crypto/signing";
import { JsonOutput } from "../../ui";
import type { CommandSpec } from "../types";
import { deploymentClient, deploymentName } from "./helpers";

export const resumeCommand: CommandSpec = {
  path: "deployment resume",
  description: "Resume deployment paused for authentication",
  implemented: true,
  run: async ({ args }) => {
    const name = args.positionals[0];
    if (!name) throw new Error("deployment name is required");
    const client = await deploymentClient(args);
    const body: Record<string, unknown> = {};
    if (args.flags.get("sign") === true) {
      const deployment = await client.request<Record<string, unknown>>(
        `/v1/${deploymentName(name)}`,
      );
      const manifestStrategy = deployment.manifestStrategy as {
        manifests?: { manifestType?: string; raw?: string }[];
      };
      const placement = deployment.placementStrategy as {
        type?: string;
        targetIds?: string[];
        targetSelector?: { matchLabels?: Record<string, string> };
      };
      const manifest = manifestStrategy.manifests?.[0];
      if (!manifest?.raw || !manifest.manifestType) {
        throw new Error("deployment has no inline manifest to sign");
      }
      const validUntil = new Date(Date.now() + 24 * 60 * 60 * 1000);
      const envelope = buildDeploymentEnvelope({
        deploymentID: name,
        manifestType: manifest.manifestType,
        manifest: JSON.parse(Buffer.from(manifest.raw, "base64").toString()),
        placement: {
          type: (placement.type ?? "TYPE_ALL")
            .replace("TYPE_", "")
            .toLowerCase(),
          ...(placement.targetIds ? { targets: placement.targetIds } : {}),
          ...(placement.targetSelector?.matchLabels
            ? { match_labels: placement.targetSelector.matchLabels }
            : {}),
        },
        validUntil,
        expectedGeneration: Number(deployment.generation ?? 0) + 1,
      });
      body.userSignature = await signDeploymentEnvelope(
        configDirectory(flagString(args, "config-dir") || undefined),
        envelope,
      );
      body.validUntil = validUntil.toISOString();
      body.etag = deployment.etag;
      body.expectedGeneration = Number(deployment.generation ?? 0) + 1;
    }
    const response = await client.request(
      `/v1/${deploymentName(name)}:resume`,
      {
        method: "POST",
        body: JSON.stringify(body),
      },
    );
    return <JsonOutput value={response} />;
  },
};
