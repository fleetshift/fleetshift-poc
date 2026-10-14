import { flagString } from "../../argv";
import { configDirectory } from "../../config";
import {
  buildDeploymentEnvelope,
  signDeploymentEnvelope,
} from "../../crypto/signing";
import { JsonOutput } from "../../ui";
import type { CommandSpec } from "../types";
import {
  deploymentBody,
  deploymentClient,
  parsePlacement,
  readManifest,
} from "./helpers";

export const createCommand: CommandSpec = {
  path: "deployment create",
  description: "Create a deployment",
  implemented: true,
  run: async ({ args }) => {
    const id = flagString(args, "id");
    const manifestFile = flagString(args, "manifest-file");
    const resourceType = flagString(args, "resource-type");
    if (!id || !manifestFile || !resourceType) {
      throw new Error(
        "--id, --manifest-file, and --resource-type are required",
      );
    }
    const manifest = await readManifest(manifestFile);
    const body = deploymentBody(args, manifest.raw);
    if (args.flags.get("sign") === true) {
      const validUntil = new Date(Date.now() + 24 * 60 * 60 * 1000);
      const envelope = buildDeploymentEnvelope({
        deploymentID: id,
        manifestType: resourceType,
        manifest: manifest.content,
        placement: parsePlacement(args),
        validUntil,
      });
      body.userSignature = await signDeploymentEnvelope(
        configDirectory(flagString(args, "config-dir") || undefined),
        envelope,
      );
      body.validUntil = validUntil.toISOString();
    }
    const response = await (
      await deploymentClient(args)
    ).request(`/v1/deployments?deploymentId=${encodeURIComponent(id)}`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    return <JsonOutput value={response} />;
  },
};
