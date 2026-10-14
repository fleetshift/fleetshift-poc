/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import { CleanupStack } from "./cleanup";

describe("CleanupStack", () => {
  it("runs cleanup in LIFO order, continues after errors, and throws AggregateError", async () => {
    const calls: number[] = [];
    const cleanup = new CleanupStack();
    cleanup.add(async () => {
      calls.push(1);
    });
    cleanup.add(async () => {
      calls.push(2);
      throw new Error("already gone");
    });
    cleanup.add(async () => {
      calls.push(3);
      throw new Error("command failed");
    });

    const error = await cleanup.run().then(
      () => undefined,
      (caught: unknown) => caught,
    );

    expect(calls).toEqual([3, 2, 1]);
    expect(error).toBeInstanceOf(AggregateError);
    expect((error as AggregateError).errors.map(String)).toEqual([
      "Error: command failed",
      "Error: already gone",
    ]);
  });

  it("resolves when every action succeeds", async () => {
    const cleanup = new CleanupStack();
    cleanup.add(() => undefined);
    await expect(cleanup.run()).resolves.toBeUndefined();
  });
});
