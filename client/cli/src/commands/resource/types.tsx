import { listResourceTypes } from "../../grpc/reflection";
import { Output } from "../../ui";
import type { CommandSpec } from "../types";

export const typesCommand: CommandSpec = {
  path: "resource types",
  description: "List available extension resource types",
  implemented: true,
  run: async ({ args }) => {
    const types = await listResourceTypes(args);
    if (types.length === 0) {
      return <Output value="No extension resource types available." />;
    }
    return (
      <Output
        value={[
          "TYPE\tSINGULAR\tSERVICE",
          ...types.map(
            (type) =>
              `${type.qualifiedName}\t${type.singular}\t${type.serviceName}`,
          ),
        ].join("\n")}
      />
    );
  },
};
