import { flagString } from "../../argv";
import { clearStoredTokens } from "../../config";
import { Output } from "../../ui";
import type { CommandSpec } from "../types";

export const logoutCommand: CommandSpec = {
  path: "auth logout",
  description: "Clear stored authentication tokens",
  implemented: true,
  run: async ({ args }) => {
    await clearStoredTokens(flagString(args, "config-dir") || undefined);
    return <Output value="Logged out." />;
  },
};
