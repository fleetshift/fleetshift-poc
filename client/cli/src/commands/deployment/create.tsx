import { deploymentServiceCreateDeployment } from "@fleetshift/common/dynamic/client/generated/sdk.gen";
import type { DeploymentServiceCreateDeploymentData } from "@fleetshift/common/dynamic/client/generated/types.gen";

import { flagString } from "../../argv";
import { configDirectory } from "../../config";
import {
  buildDeploymentEnvelope,
  signDeploymentEnvelope,
} from "../../crypto/signing";
import { JsonOutput } from "../../ui";
import { clientForArgs, unwrap } from "../context";
import type { CommandSpec } from "../types";
import { deploymentBody, parsePlacement, readManifest } from "./helpers";

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
    let userSignature: string | undefined;
    let validUntil: string | undefined;
    if (args.flags.get("sign") === true) {
      const validUntilDate = new Date(Date.now() + 24 * 60 * 60 * 1000);
      const envelope = buildDeploymentEnvelope({
        deploymentID: id,
        manifestType: resourceType,
        manifest: manifest.content,
        placement: parsePlacement(args),
        validUntil: validUntilDate,
      });
      userSignature = await signDeploymentEnvelope(
        configDirectory(flagString(args, "config-dir") || undefined),
        envelope,
      );
      validUntil = validUntilDate.toISOString();
    }
    const query: DeploymentServiceCreateDeploymentData["query"] = {
      deploymentId: id,
      ...(userSignature ? { userSignature, validUntil } : {}),
    };
    await clientForArgs(args);
    const response = await unwrap(
      deploymentServiceCreateDeployment({
        body,
        query,
      }),
    );
    return <JsonOutput value={response} />;
  },
};
