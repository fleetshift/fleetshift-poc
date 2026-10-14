import { commandSpecs } from "./registry";

export function helpText(command?: string): string {
  const entries = command
    ? commandSpecs.filter((item) => item.path.startsWith(`${command} `))
    : commandSpecs.filter((item) => !item.path.includes(" "));
  const lines = [
    command
      ? `Usage: fleetctl ${command} <command>`
      : "Usage: fleetctl <command>",
    "",
    "Commands:",
  ];
  for (const item of entries) {
    const child = command ? item.path.slice(command.length + 1) : item.path;
    lines.push(`  ${child.padEnd(22)} ${item.description}`);
  }
  return lines.join("\n");
}
