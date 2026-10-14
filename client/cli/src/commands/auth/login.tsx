import { runAuthLogin } from "../../auth/login";
import { Output } from "../../ui";
import type { CommandSpec } from "../types";

export const loginCommand: CommandSpec = {
  path: "auth login",
  description: "Authenticate with configured OIDC provider",
  implemented: true,
  run: async ({ args }) => <Output value={await runAuthLogin(args)} />,
};
