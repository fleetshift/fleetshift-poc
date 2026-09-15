import { deploymentServiceListDeployments } from "@fleetshift/common/dynamic/client/generated/sdk.gen";
import type { V1Deployment } from "@fleetshift/common/dynamic/client/generated/types.gen";
import { Box, Text } from "ink";
import Table from "ink-table";
import React from "react";

import { flagNumber, flagString } from "../../argv";
import { JsonOutput, useOutputFormat } from "../../ui";
import { clientForArgs, unwrap } from "../context";
import type { CommandSpec } from "../types";

export const listCommand: CommandSpec = {
  path: "deployment list",
  description: "List deployments",
  implemented: true,
  run: async ({ args }) => {
    await clientForArgs(args);
    const response = await unwrap(
      deploymentServiceListDeployments({
        query: {
          pageSize: flagNumber(args, "page-size"),
          pageToken: flagString(args, "page-token"),
        },
      }),
    );
    return <DeploymentListOutput deployments={response.deployments ?? []} />;
  },
};

function DeploymentListOutput({
  deployments,
}: {
  deployments: V1Deployment[];
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
