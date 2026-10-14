import type { CommandSpec } from "../types";
import { enrollSigningCommand } from "./enroll-signing";
import { inspectTokenCommand } from "./inspect-token";
import { loginCommand } from "./login";
import { logoutCommand } from "./logout";
import { setupCommand } from "./setup";

export const authCommand: CommandSpec = {
  path: "auth",
  description: "Manage authentication",
};

export const authCommands: CommandSpec[] = [
  setupCommand,
  loginCommand,
  logoutCommand,
  inspectTokenCommand,
  enrollSigningCommand,
];
