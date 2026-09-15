import {
  deploymentServiceGetDeployment,
  deploymentServiceResumeDeployment,
} from "@fleetshift/common/dynamic/client/generated/sdk.gen";
import type { V1Deployment } from "@fleetshift/common/dynamic/client/generated/types.gen";

import { flagString } from "../../argv";
import { configDirectory } from "../../config";
import {
  buildDeploymentEnvelope,
  signDeploymentEnvelope,
} from "../../crypto/signing";
import { JsonOutput } from "../../ui";
import { clientForArgs, unwrap } from "../context";
import type { CommandSpec } from "../types";
import { deploymentName } from "./helpers";

export const resumeCommand: CommandSpec = {
  path: "deployment resume",
  description: "Resume deployment paused for authentication",
  implemented: true,
  run: async ({ args }) => {
    const name = args.positionals[0];
    if (!name) throw new Error("deployment name is required");
    await clientForArgs(args);
    const body: {
      userSignature?: string;
      validUntil?: string;
      etag?: string;
      expectedGeneration?: string;
    } = {};
    if (args.flags.get("sign") === true) {
      const deployment = await unwrap(
        deploymentServiceGetDeployment({
          path: { name_1: deploymentName(name) },
        }),
      );
      const typedDeployment = deployment as V1Deployment;
      const manifestStrategy = typedDeployment.manifestStrategy;
      const placement = typedDeployment.placementStrategy;
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
          type: placement.type.replace("TYPE_", "").toLowerCase(),
          ...(placement.targetIds ? { targets: placement.targetIds } : {}),
          ...(placement.targetSelector?.matchLabels
            ? { match_labels: placement.targetSelector.matchLabels }
            : {}),
        },
        validUntil,
        expectedGeneration: Number(typedDeployment.generation ?? 0) + 1,
      });
      body.userSignature = await signDeploymentEnvelope(
        configDirectory(flagString(args, "config-dir") || undefined),
        envelope,
      );
      body.validUntil = validUntil.toISOString();
      body.etag = typedDeployment.etag;
      body.expectedGeneration = String(
        Number(typedDeployment.generation ?? 0) + 1,
      );
    }
    const response = await unwrap(
      deploymentServiceResumeDeployment({
        path: { name: deploymentName(name) },
        body,
      }),
    );
    return <JsonOutput value={response} />;
  },
};
