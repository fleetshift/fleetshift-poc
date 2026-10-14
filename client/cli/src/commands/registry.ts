import { authCommand, authCommands } from "./auth";
import { deploymentCommand, deploymentCommands } from "./deployment";
import { resourceCommand, resourceCommands } from "./resource";
import type { CommandSpec } from "./types";

export const commandSpecs: CommandSpec[] = [
  authCommand,
  ...authCommands,
  deploymentCommand,
  ...deploymentCommands,
  resourceCommand,
  ...resourceCommands,
];

export function findCommand(tokens: string[]): CommandSpec | undefined {
  const input = tokens.join(" ");
  const direct = commandSpecs.find((command) => command.path === input);
  if (direct) return direct;
  const group = tokens[0];
  const canonicalGroup = commandSpecs.find(
    (command) =>
      !command.path.includes(" ") &&
      (command.path === group || command.aliases?.includes(group)),
  )?.path;
  if (!canonicalGroup) return undefined;
  if (tokens.length === 1) {
    return commandSpecs.find((command) => command.path === canonicalGroup);
  }
  const canonicalLeaf = commandSpecs.find(
    (command) =>
      command.path.startsWith(`${canonicalGroup} `) &&
      command.aliases?.includes(tokens[1]),
  );
  return (
    canonicalLeaf ??
    commandSpecs.find(
      (command) =>
        command.path === `${canonicalGroup} ${tokens.slice(1).join(" ")}`,
    )
  );
}
