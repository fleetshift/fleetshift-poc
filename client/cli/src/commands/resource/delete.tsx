import { Output } from "../../ui";
import { clientForArgs, unwrap } from "../context";
import type { CommandSpec } from "../types";
import { resourceRoute } from "./helpers";

export const deleteCommand: CommandSpec = {
  path: "resource delete",
  description: "Delete a managed resource",
  implemented: true,
  run: async ({ args }) => {
    const type = args.positionals[0];
    const id = args.positionals[1];
    if (!type || !id) throw new Error("resource type and id are required");
    const route = resourceRoute(type);
    const client = await clientForArgs(args);
    await unwrap(
      client.delete({ url: `${route.path}/${encodeURIComponent(id)}` }),
    );
    return <Output value="Deleted." />;
  },
};
