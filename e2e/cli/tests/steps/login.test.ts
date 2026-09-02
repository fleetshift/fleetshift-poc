/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import { tokenEmail } from "./login";

describe("tokenEmail", () => {
  it("reads email from access_token claims", () => {
    expect(
      tokenEmail(
        JSON.stringify({
          access_token: { claims: { email: "ops@fleetshift.local" } },
          id_token: { claims: { email: "other@fleetshift.local" } },
        }),
      ),
    ).toBe("ops@fleetshift.local");
  });

  it("falls back to id_token when access_token has no email", () => {
    expect(
      tokenEmail(
        JSON.stringify({
          access_token: { claims: { email: "   " } },
          id_token: { claims: { email: "dev@fleetshift.local" } },
        }),
      ),
    ).toBe("dev@fleetshift.local");
  });

  it.each([
    { name: "null JSON", raw: "null", message: /malformed/ },
    {
      name: "a JSON string",
      raw: '"ops@fleetshift.local"',
      message: /malformed/,
    },
    { name: "missing claims", raw: "{}", message: /no email claim/ },
    {
      name: "empty email",
      raw: JSON.stringify({ access_token: { claims: { email: "" } } }),
      message: /no email claim/,
    },
  ])("rejects $name", ({ raw, message }) => {
    expect(() => tokenEmail(raw)).toThrow(message);
  });
});
