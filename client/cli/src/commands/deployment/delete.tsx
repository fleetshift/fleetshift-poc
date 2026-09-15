import { deploymentServiceDeleteDeployment } from "@fleetshift/common/dynamic/client/generated/sdk.gen";

import { Output } from "../../ui";
import { clientForArgs, unwrap } from "../context";
import type { CommandSpec } from "../types";
import { deploymentName } from "./helpers";

export const deleteCommand: CommandSpec = {
  path: "deployment delete",
  description: "Delete a deployment",
  implemented: true,
  run: async ({ args }) => {
    const name = args.positionals[0];
    if (!name) throw new Error("deployment name is required");
    await clientForArgs(args);
    await unwrap(
      deploymentServiceDeleteDeployment({
        path: { name: deploymentName(name) },
      }),
    );
    return <Output value="Deleted." />;
  },
};
