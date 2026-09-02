/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  deploymentTerminalFailure,
  parseDeployment,
  parseDeploymentList,
} from "./deployments";

describe("deploymentTerminalFailure", () => {
  it("classifies failed and unexpectedly paused deployments", () => {
    expect(
      deploymentTerminalFailure({
        name: "deployments/a",
        state: "STATE_FAILED",
        pauseReason: "boom",
      }),
    ).toContain("STATE_FAILED");
    expect(
      deploymentTerminalFailure({
        name: "deployments/a",
        state: "STATE_ACTIVE",
        pauseReason: "delivery auth failed",
      }),
    ).toContain("paused");
    expect(
      deploymentTerminalFailure({
        name: "deployments/a",
        state: "STATE_ACTIVE",
        pauseReason: "",
      }),
    ).toBeNull();
  });
});

describe("deployment JSON parsing", () => {
  it("reads name, state, and pauseReason", () => {
    expect(
      parseDeployment(
        '{"name":"deployments/a","state":"STATE_CREATING","pauseReason":"auth_failed","uid":"ignored"}',
      ),
    ).toEqual({
      name: "deployments/a",
      pauseReason: "auth_failed",
      state: "STATE_CREATING",
    });
  });

  it("defaults missing pauseReason", () => {
    expect(
      parseDeployment('{"name":"deployments/a","state":"STATE_ACTIVE"}'),
    ).toEqual({
      name: "deployments/a",
      pauseReason: "",
      state: "STATE_ACTIVE",
    });
  });

  it("parses a deployment list", () => {
    expect(
      parseDeploymentList('[{"name":"deployments/a","state":"STATE_ACTIVE"}]'),
    ).toEqual([
      { name: "deployments/a", pauseReason: "", state: "STATE_ACTIVE" },
    ]);
  });

  it("reports invalid JSON", () => {
    expect(() => parseDeployment("[")).toThrow(/invalid JSON/);
    expect(() => parseDeploymentList("{")).toThrow(/invalid JSON/);
    expect(() => parseDeploymentList("{}")).toThrow(/invalid JSON/);
  });
});
