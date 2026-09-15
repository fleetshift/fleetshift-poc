import { flagString } from "../../argv";
import { JsonOutput } from "../../ui";
import { clientForArgs, unwrap } from "../context";
import type { CommandSpec } from "../types";
import { resourceID, resourceRoute, resourceSpec } from "./helpers";

export const createCommand: CommandSpec = {
  path: "resource create",
  description: "Create a managed resource",
  implemented: true,
  run: async ({ args }) => {
    const type = args.positionals[0];
    const specFile = flagString(args, "spec-file");
    if (!type || !specFile)
      throw new Error("resource type and --spec-file are required");
    const route = resourceRoute(type);
    const client = await clientForArgs(args);
    const response = await unwrap(
      client.post({
        url: route.path,
        query: { [`${route.singular}_id`]: resourceID(args) },
        body: { spec: await resourceSpec(specFile) },
      }),
    );
    return <JsonOutput value={response} />;
  },
};
