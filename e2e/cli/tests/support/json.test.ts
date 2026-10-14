/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import { parseJSON } from "./json";

describe("parseJSON", () => {
  it("returns the parsed value", () => {
    expect(parseJSON<{ n: number }>('{"n":1}')).toEqual({ n: 1 });
  });

  it("reports invalid JSON without schema-checking the value", () => {
    expect(() => parseJSON("{")).toThrow(/invalid JSON/);
    expect(parseJSON<{ name: string }>("{}")).toEqual({});
    expect(parseJSON<{ name: string }>('{"name":1,"extra":true}')).toEqual({
      extra: true,
      name: 1,
    });
  });
});
