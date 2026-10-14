import { describeResourceType, listResourceTypes } from "../../grpc/reflection";
import { Output } from "../../ui";
import type { CommandSpec } from "../types";

export const describeCommand: CommandSpec = {
  path: "resource describe",
  description: "Show managed resource schema",
  implemented: true,
  run: async ({ args }) => {
    const requested = args.positionals[0];
    if (!requested) throw new Error("resource type is required");
    const type = (await listResourceTypes(args)).find(
      (candidate) =>
        candidate.qualifiedName === requested ||
        candidate.collection === requested,
    );
    if (!type) throw new Error(`unknown resource type ${requested}`);
    const description = await describeResourceType(type, args);
    return (
      <Output
        value={[
          `Type:     ${type.qualifiedName}`,
          `Singular: ${type.singular}`,
          `Service:  ${type.serviceName}`,
          "",
          "Methods:",
          ...description.methods.map((method) => `  ${method}`),
          "",
          `Spec (${description.specType}):`,
          ...description.fields.map((field) => `  ${field}`),
        ].join("\n")}
      />
    );
  },
};
