/* eslint-disable playwright/no-standalone-expect */
import { render } from "ink-testing-library";
import { describe, expect, it } from "vitest";

import { Output } from "../ui";

describe("Ink output", () => {
  it("renders command output in terminal frame", () => {
    const instance = render(<Output value="fleetctl ready" />);
    expect(instance.lastFrame()).toContain("fleetctl ready");
  });
});
