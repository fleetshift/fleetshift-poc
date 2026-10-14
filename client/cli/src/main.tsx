import { performance } from "node:perf_hooks";

import { print } from "./app";
import { hasFlag, outputFormat, parseArgs } from "./argv";
import { helpText } from "./commands/help";
import { commandSpecs, findCommand } from "./commands/registry";
import { Output } from "./ui";

const startedAt = performance.now();
const debugEnabled =
  process.env.DEBUG === "true" ||
  process.env.DEBUG === "1" ||
  process.argv.includes("--debug");

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const command = args.command.join(" ");
  if (
    hasFlag(args, "help") ||
    !command ||
    commandSpecs.some(
      (item) =>
        item.path === command && !item.implemented && !item.path.includes(" "),
    )
  ) {
    await print(<Output value={helpText(command || undefined)} />);
    return;
  }

  const spec = findCommand(args.command);
  if (!spec) throw new Error(`Unknown command.\n\n${helpText()}`);
  if (!spec.implemented)
    throw new Error(`${spec.path} is not implemented yet.`);
  if (!spec.run)
    throw new Error(
      `${spec.path} command wiring pending API contract migration.`,
    );

  const output = outputFormat(args);
  await print(await spec.run({ args, output }), output);
}

main()
  .catch(async (error: unknown) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  })
  .finally(() => {
    if (!debugEnabled) return;
    const elapsedMilliseconds = performance.now() - startedAt;
    process.stderr.write(
      `[debug] fleetctl completed in ${elapsedMilliseconds.toFixed(1)} ms\n`,
    );
  });
