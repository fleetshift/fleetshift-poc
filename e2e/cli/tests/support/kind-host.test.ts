/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import { isAlreadyUnpaused, parsePodmanPort } from "./kind-host";

describe("parsePodmanPort", () => {
  it("prefers IPv4 and normalizes unspecified binds", () => {
    expect(
      parsePodmanPort("6443/tcp -> [::]:42000\n6443/tcp -> 0.0.0.0:41000\n"),
    ).toBe("https://127.0.0.1:41000");
  });

  it("supports unadorned podman port output", () => {
    expect(parsePodmanPort("127.0.0.1:41000")).toBe("https://127.0.0.1:41000");
  });

  it("builds an IPv6 URL and normalizes unspecified IPv6 and wildcard binds", () => {
    expect(parsePodmanPort("6443/tcp -> [::1]:42000")).toBe(
      "https://[::1]:42000",
    );
    expect(parsePodmanPort("[::]:42000")).toBe("https://127.0.0.1:42000");
    expect(parsePodmanPort("*:42000")).toBe("https://127.0.0.1:42000");
  });

  it("rejects output without a port", () => {
    expect(() => parsePodmanPort("not a port")).toThrow("podman port");
  });
});

describe("isAlreadyUnpaused", () => {
  it("recognizes podman unpause when the container is already running", () => {
    expect(isAlreadyUnpaused("Error: this container is not paused")).toBe(true);
    expect(isAlreadyUnpaused("cannot pause: already paused")).toBe(false);
  });
});
