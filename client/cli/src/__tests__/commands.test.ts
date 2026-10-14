/* eslint-disable playwright/no-standalone-expect */
import { describe, expect, it } from "vitest";

import { helpText } from "../commands/help";
import { findCommand } from "../commands/registry";

describe("command registry", () => {
  it("keeps resource aliases", () => {
    expect(findCommand(["res", "search"])?.path).toBe("resource query");
  });

  it("lists root commands", () => {
    expect(helpText()).toContain("deployment");
    expect(helpText()).toContain("resource");
  });
});
