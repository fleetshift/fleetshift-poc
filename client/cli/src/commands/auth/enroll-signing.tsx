import { runAuthEnrollSigning } from "../../auth/enroll-signing";
import { Output } from "../../ui";
import type { CommandSpec } from "../types";

export const enrollSigningCommand: CommandSpec = {
  path: "auth enroll-signing",
  description: "Generate and enroll signing key pair",
  implemented: true,
  run: async ({ args }) => <Output value={await runAuthEnrollSigning(args)} />,
};
