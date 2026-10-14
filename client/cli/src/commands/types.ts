import { ReactNode } from "react";

import type { OutputFormat, ParsedArgs } from "../argv";

export interface CommandContext {
  args: ParsedArgs;
  output: OutputFormat;
}

export type CommandRunner = (context: CommandContext) => Promise<ReactNode>;

export interface CommandSpec {
  path: string;
  aliases?: string[];
  description: string;
  implemented?: boolean;
  run?: CommandRunner;
}
