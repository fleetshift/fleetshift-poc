import type { CommandSpec } from "../types";
import { createCommand } from "./create";
import { deleteCommand } from "./delete";
import { describeCommand } from "./describe";
import { getCommand } from "./get";
import { listCommand } from "./list";
import { queryCommand } from "./query";
import { typesCommand } from "./types";

export const resourceCommand: CommandSpec = {
  path: "resource",
  aliases: ["res"],
  description: "Manage addon-provided managed resources",
};

export const resourceCommands: CommandSpec[] = [
  typesCommand,
  describeCommand,
  createCommand,
  getCommand,
  listCommand,
  queryCommand,
  deleteCommand,
];
