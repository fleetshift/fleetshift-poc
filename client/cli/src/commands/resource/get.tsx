import { JsonOutput } from "../../ui";
import { clientForArgs } from "../context";
import type { CommandSpec } from "../types";
import { resourceRoute } from "./helpers";

export const getCommand: CommandSpec = {
  path: "resource get",
  description: "Get a managed resource by id",
  implemented: true,
  run: async ({ args }) => {
    const type = args.positionals[0];
    const id = args.positionals[1];
    if (!type || !id) throw new Error("resource type and id are required");
    const route = resourceRoute(type);
    const response = await (
      await clientForArgs(args)
    ).request(`${route.path}/${encodeURIComponent(id)}`);
    return <JsonOutput value={response} />;
  },
};
