import { deploymentServiceGetDeployment } from "@fleetshift/common/dynamic/client/generated/sdk.gen";

import { JsonOutput } from "../../ui";
import { clientForArgs, unwrap } from "../context";
import type { CommandSpec } from "../types";
import { deploymentName } from "./helpers";

export const getCommand: CommandSpec = {
  path: "deployment get",
  description: "Get deployment by name",
  implemented: true,
  run: async ({ args }) => {
    const name = args.positionals[0];
    if (!name) throw new Error("deployment name is required");
    await clientForArgs(args);
    const response = await unwrap(
      deploymentServiceGetDeployment({
        path: { name_1: deploymentName(name) },
      }),
    );
    return <JsonOutput value={response} />;
  },
};
