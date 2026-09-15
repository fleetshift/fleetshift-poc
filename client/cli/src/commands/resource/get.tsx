import { JsonOutput } from "../../ui";
import { clientForArgs, unwrap } from "../context";
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
    const client = await clientForArgs(args);
    const response = await unwrap(
      client.get({ url: `${route.path}/${encodeURIComponent(id)}` }),
    );
    return <JsonOutput value={response} />;
  },
};
