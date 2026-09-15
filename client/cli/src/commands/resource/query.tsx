import { resourceQueryServiceQueryResources } from "@fleetshift/common/dynamic/client/generated/sdk.gen";

import { flagNumber, flagString } from "../../argv";
import { JsonOutput } from "../../ui";
import { clientForArgs, unwrap } from "../context";
import type { CommandSpec } from "../types";

export const queryCommand: CommandSpec = {
  path: "resource query",
  aliases: ["search"],
  description: "Query managed resources with CEL filter",
  implemented: true,
  run: async ({ args }) => {
    const scope = flagString(args, "scope", "-") ?? "-";
    await clientForArgs(args);
    const response = await unwrap(
      resourceQueryServiceQueryResources({
        path: { scope },
        query: {
          filter: flagString(args, "filter") ?? "",
          pageSize: flagNumber(args, "page-size"),
          pageToken: flagString(args, "page-token"),
          orderBy: flagString(args, "order-by"),
        },
      }),
    );
    return <JsonOutput value={response} />;
  },
};
