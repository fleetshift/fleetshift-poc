import { Output } from "../../ui";
import type { CommandSpec } from "../types";
import { deploymentClient, deploymentName } from "./helpers";

export const deleteCommand: CommandSpec = {
  path: "deployment delete",
  description: "Delete a deployment",
  implemented: true,
  run: async ({ args }) => {
    const name = args.positionals[0];
    if (!name) throw new Error("deployment name is required");
    await (
      await deploymentClient(args)
    ).request(`/v1/${deploymentName(name)}`, { method: "DELETE" });
    return <Output value="Deleted." />;
  },
};
