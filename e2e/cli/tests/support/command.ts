import { execFile } from "node:child_process";

const TIMEOUT_MS = 30_000;
const MAX_OUTPUT_BYTES = 1024 * 1024;

export type CommandResult = {
  exitCode: number;
  stdout: string;
  stderr: string;
  timedOut: boolean;
};

/** Run a child process without a shell, with a fixed timeout and output cap. */
export function runCommand(
  command: string,
  args: readonly string[] = [],
  timeoutMs = TIMEOUT_MS,
): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = execFile(
      command,
      [...args],
      {
        encoding: "utf8",
        killSignal: "SIGKILL",
        maxBuffer: MAX_OUTPUT_BYTES,
        timeout: timeoutMs,
      },
      (error, stdout, stderr) => {
        if (!error) {
          resolve({
            exitCode: 0,
            stdout: stdout ?? "",
            stderr: stderr ?? "",
            timedOut: false,
          });
          return;
        }
        if (typeof error.code === "string") {
          reject(error);
          return;
        }
        resolve({
          exitCode: typeof error.code === "number" ? error.code : -1,
          stdout: stdout ?? "",
          stderr: stderr ?? "",
          timedOut: Boolean(error.killed && error.signal === "SIGKILL"),
        });
      },
    );
    child.stdin?.end();
  });
}

export function commandFailure(label: string, result: CommandResult): Error {
  const details = [result.stderr.trim(), result.stdout.trim()]
    .filter(Boolean)
    .join("\n");
  const reason = result.timedOut
    ? "timed out"
    : `exited with code ${result.exitCode}`;
  return new Error(`${label} ${reason}${details ? `\n${details}` : ""}`);
}

export function isNotFound(stderr: string): boolean {
  const detail = stderr.toLowerCase();
  return detail.includes("notfound") || detail.includes("not found");
}

export function isUnauthenticated(output: string): boolean {
  const detail = output.toLowerCase();
  return (
    detail.includes("unauthenticated") ||
    detail.includes("code = unauthenticated")
  );
}

export function requireCommandSuccess(
  label: string,
  result: CommandResult,
): CommandResult {
  if (result.exitCode !== 0 || result.timedOut) {
    throw commandFailure(label, result);
  }
  return result;
}
