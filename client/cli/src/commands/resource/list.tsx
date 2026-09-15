import { flagString } from "../../argv";
import { JsonOutput } from "../../ui";
import { clientForArgs, unwrap } from "../context";
import type { CommandSpec } from "../types";
import { resourceRoute } from "./helpers";

export const listCommand: CommandSpec = {
  path: "resource list",
  description: "List managed resources of a type",
  implemented: true,
  run: async ({ args }) => {
    const type = args.positionals[0];
    if (!type) throw new Error("resource type is required");
    const route = resourceRoute(type);
    const pageSize = flagString(args, "page-size");
    const pageToken = flagString(args, "page-token");
    const client = await clientForArgs(args);
    const response = await unwrap(
      client.get({
        url: route.path,
        query: {
          ...(pageSize ? { page_size: Number(pageSize) } : {}),
          ...(pageToken ? { page_token: pageToken } : {}),
        },
      }),
    );
    const resources = listResources(response, route.collection);
    return <JsonOutput value={resources} />;
  },
};

function listResources(response: unknown, collection: string): unknown {
  if (
    Array.isArray(response) ||
    response === null ||
    typeof response !== "object"
  ) {
    return response;
  }
  const objectResponse = response as Record<string, unknown>;
  if (Array.isArray(objectResponse[collection])) {
    return objectResponse[collection];
  }
  return Object.values(objectResponse).find(Array.isArray) ?? [];
}
