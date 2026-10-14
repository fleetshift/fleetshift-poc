import { runAuthSetup } from "../../auth/setup";
import { Output } from "../../ui";
import type { CommandSpec } from "../types";
export const setupCommand: CommandSpec = {
  path: "auth setup",
  description: "Configure OIDC client settings",
  implemented: true,
  run: async ({ args }) => <Output value={await runAuthSetup(args)} />,
};
