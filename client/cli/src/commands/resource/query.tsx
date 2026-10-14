import { flagString } from "../../argv";
import { JsonOutput } from "../../ui";
import { clientForArgs } from "../context";
import type { CommandSpec } from "../types";

export const queryCommand: CommandSpec = {
  path: "resource query",
  aliases: ["search"],
  description: "Query managed resources with CEL filter",
  implemented: true,
  run: async ({ args }) => {
    const scope = flagString(args, "scope", "-");
    const params = new URLSearchParams({ filter: flagString(args, "filter") });
    const pageSize = flagString(args, "page-size");
    const pageToken = flagString(args, "page-token");
    const orderBy = flagString(args, "order-by");
    if (pageSize) params.set("page_size", pageSize);
    if (pageToken) params.set("page_token", pageToken);
    if (orderBy) params.set("order_by", orderBy);
    const response = await (
      await clientForArgs(args)
    ).request(
      `/apis/fleetshift.io/v1/${encodeURIComponent(scope)}:queryResources?${params}`,
    );
    return <JsonOutput value={response} />;
  },
};
