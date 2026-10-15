/* eslint-disable playwright/no-standalone-expect */
import { PassThrough } from "node:stream";

import { render as renderInk } from "ink";
import { render } from "ink-testing-library";
import { describe, expect, it } from "vitest";

import { Output, PlainOutput } from "../ui";

describe("Ink output", () => {
  it("renders command output in terminal frame", () => {
    const instance = render(<Output value="fleetctl ready" />);
    expect(instance.lastFrame()).toContain("fleetctl ready");
  });

  it("preserves complete schema identifiers in redirected output", async () => {
    const stdout = new PassThrough();
    let captured = "";
    stdout.on("data", (chunk) => {
      captured += chunk.toString();
    });
    const value = [
      "TYPE\tSINGULAR\tSERVICE",
      "ocm.resource_manager.kind.v1alpha/clusters\tCluster\tocm.resource_manager.kind.v1alpha.ClusterService",
    ].join("\n");
    const instance = renderInk(<PlainOutput value={value} />, {
      stdout,
      patchConsole: false,
    });
    instance.unmount();
    await instance.waitUntilExit();
    expect(captured).toBe(`${value}\n`);
  });
});
