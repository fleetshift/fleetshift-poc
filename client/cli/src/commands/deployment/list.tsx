import { Box, Text } from "ink";
import Table from "ink-table";
import React from "react";

import { flagString } from "../../argv";
import { JsonOutput, useOutputFormat } from "../../ui";
import { clientForArgs } from "../context";
import type { CommandSpec } from "../types";
import type { DeploymentListResponse } from "./types";

export const listCommand: CommandSpec = {
  path: "deployment list",
  description: "List deployments",
  implemented: true,
  run: async ({ args }) => {
    const query = new URLSearchParams();
    const pageSize = flagString(args, "page-size");
    if (pageSize) query.set("pageSize", pageSize);
    const response = await (
      await clientForArgs(args)
    ).request<DeploymentListResponse>(
      `/v1/deployments${query.size ? `?${query}` : ""}`,
      {
        method: "GET",
      },
    );
    return <DeploymentListOutput deployments={response.deployments ?? []} />;
  },
};

function DeploymentListOutput({
  deployments,
}: {
  deployments: NonNullable<DeploymentListResponse["deployments"]>;
}): React.ReactElement {
  if (useOutputFormat() === "json") {
    return <JsonOutput value={deployments} />;
  }
  const tableData = deployments.map((deployment) => ({
    name: deployment.name,
    state: deployment.state ?? "UNKNOWN",
    reconciling: Boolean(deployment.reconciling),
    createTime: deployment.createTime ?? "UNKNOWN",
    updated: deployment.updateTime ?? "UNKNOWN",
    targets: (deployment.resolvedTargetIds ?? ["NONE"]).join(", "),
    pauseReason: deployment.pauseReason ?? "NONE",
  }));
  return (
    <Box flexDirection="column">
      <Table data={tableData} />
      <Text dimColor>Use --output json for full details.</Text>
    </Box>
  );
}
