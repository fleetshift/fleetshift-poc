/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  buildFleetctlArgs,
  parseAccessToken,
  parseAuthURLLine,
  sanitizeSecretText,
} from "./fleetctl";

describe("fleetctl support", () => {
  it("constructs the fixed safe argument prefix", () => {
    expect(
      buildFleetctlArgs("/private/config", "127.0.0.1:50051", [
        "deployment",
        "list",
      ]),
    ).toEqual([
      "--config-dir",
      "/private/config",
      "--insecure-storage",
      "--server",
      "127.0.0.1:50051",
      "--output",
      "json",
      "deployment",
      "list",
    ]);
  });

  it("parses only AUTH_URL records", () => {
    expect(
      parseAuthURLLine("AUTH_URL https://issuer.example/auth?secret=yes"),
    ).toBe("https://issuer.example/auth?secret=yes");
    expect(parseAuthURLLine("waiting")).toBeNull();
  });

  it("guards malformed credentials", () => {
    expect(parseAccessToken('{"access_token":"token"}')).toBe("token");
    expect(() => parseAccessToken("{}")).toThrow("access_token");
  });

  it("redacts secrets from command failures", () => {
    const bearer = sanitizeSecretText("Authorization: Bearer tokensecret");
    expect(bearer).toContain("Bearer [REDACTED]");
    expect(bearer).not.toContain("tokensecret");

    const login = sanitizeSecretText(
      "AUTH_URL https://issuer.example/auth?code=oauthcode",
    );
    expect(login).toContain("[LOGIN URL REDACTED]");
    expect(login).not.toContain("oauthcode");
    expect(login).not.toContain("issuer.example");

    const query = sanitizeSecretText(
      "retry ?state=abc&code_challenge=xyz leftover",
    );
    expect(query).toContain("state=[REDACTED]");
    expect(query).toContain("code_challenge=[REDACTED]");
    expect(query).not.toContain("abc");
    expect(query).not.toContain("xyz");
  });
});
