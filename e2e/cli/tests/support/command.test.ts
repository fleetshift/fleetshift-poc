/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { execPath } from "node:process";

import { describe, expect, it } from "vitest";

import { isNotFound, isUnauthenticated, runCommand } from "./command";

describe("isNotFound", () => {
  it("recognizes gRPC and kubectl not-found phrasing", () => {
    expect(isNotFound("rpc error: code = NotFound desc = missing")).toBe(true);
    expect(isNotFound("Error from server (NotFound): namespaces")).toBe(true);
    expect(isNotFound("rpc error: code = PermissionDenied")).toBe(false);
  });
});

describe("isUnauthenticated", () => {
  it("recognizes gRPC unauthenticated phrasing", () => {
    expect(
      isUnauthenticated(
        "rpc error: code = Unauthenticated desc = unauthenticated",
      ),
    ).toBe(true);
    expect(isUnauthenticated("Error: unauthenticated")).toBe(true);
    expect(isUnauthenticated("rpc error: code = PermissionDenied")).toBe(false);
  });
});

describe("runCommand", () => {
  it("returns stdout from a successful command", async () => {
    const result = await runCommand(execPath, [
      "-e",
      "process.stdout.write('ok')",
    ]);

    expect(result).toEqual({
      exitCode: 0,
      stderr: "",
      stdout: "ok",
      timedOut: false,
    });
  });

  it("resolves non-zero exits so callers can inspect the result", async () => {
    const result = await runCommand(execPath, [
      "-e",
      "process.stdout.write('out'); process.stderr.write('err'); process.exitCode = 7",
    ]);

    expect(result).toEqual({
      exitCode: 7,
      stderr: "err",
      stdout: "out",
      timedOut: false,
    });
  });

  it("closes stdin so a process waiting for EOF can exit", async () => {
    const result = await runCommand(execPath, [
      "-e",
      "require('fs').readFileSync(0)",
    ]);

    expect(result.exitCode).toBe(0);
    expect(result.timedOut).toBe(false);
  });

  it("resolves timeouts instead of rejecting", async () => {
    const result = await runCommand(
      execPath,
      ["-e", "setTimeout(() => {}, 60_000)"],
      200,
    );

    expect(result.timedOut).toBe(true);
    expect(result.exitCode).not.toBe(0);
  });

  it("rejects when output exceeds the internal buffer", async () => {
    await expect(
      runCommand(execPath, [
        "-e",
        "process.stdout.write('x'.repeat(2 * 1024 * 1024))",
      ]),
    ).rejects.toMatchObject({ code: "ERR_CHILD_PROCESS_STDIO_MAXBUFFER" });
  });

  it("rejects when the binary is missing", async () => {
    await expect(
      runCommand("fleetshift-e2e-missing-binary"),
    ).rejects.toMatchObject({ code: "ENOENT" });
  });
});
