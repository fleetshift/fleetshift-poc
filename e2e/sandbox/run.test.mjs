/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  buildTestEnvironment,
  kindNodeBelongsToRun,
  normalizeSocketPath,
  parseCommand,
  sanitize,
  usesPrebuiltImage,
  usesPulledImage,
} from "./run.mjs";

describe("parseCommand", () => {
  it("requires -- followed by a command", () => {
    expect(parseCommand(["--", "playwright", "test"])).toEqual({
      args: ["test"],
      command: "playwright",
    });
    expect(() => parseCommand([])).toThrow(/usage:/i);
    expect(() => parseCommand(["playwright", "test"])).toThrow(/usage:/i);
    expect(() => parseCommand(["--"])).toThrow(/usage:/i);
  });
});

describe("buildTestEnvironment", () => {
  it("publishes only the sandbox connection contract", () => {
    const env = buildTestEnvironment(
      { EXISTING: "kept" },
      {
        caFile: "/tmp/run/ca.crt",
        kindPrefix: "kind-e2e-abcd-",
        runId: "abcd",
        workDir: "/tmp/run",
      },
    );

    expect(env.EXISTING).toBe("kept");
    expect(env.BASE_URL).toBe("https://fleetshift-sandbox.localhost:8085");
    expect(env.FLEETSHIFT_GRPC_TARGET).toBe("127.0.0.1:50051");
    expect(env.FLEETSHIFT_CA_FILE).toBe("/tmp/run/ca.crt");
    expect(env.FLEETSHIFT_E2E_RUN_ID).toBe("abcd");
    expect(env.FLEETSHIFT_E2E_WORK_DIR).toBe("/tmp/run");
    expect(env.FLEETSHIFT_KIND_PREFIX).toBe("kind-e2e-abcd-");
  });
});

describe("kindNodeBelongsToRun", () => {
  it("is exact to the current run prefix", () => {
    expect(kindNodeBelongsToRun("fs--kind-e2e-abcd-a", "kind-e2e-abcd-")).toBe(
      true,
    );
    expect(kindNodeBelongsToRun("fs--kind-e2e-other-a", "kind-e2e-abcd-")).toBe(
      false,
    );
    expect(kindNodeBelongsToRun("kind-e2e-abcd-a", "kind-e2e-abcd-")).toBe(
      false,
    );
  });
});

describe("usesPrebuiltImage", () => {
  it("only an explicit prebuilt flag skips the local image build", () => {
    expect(usesPrebuiltImage({ FLEETSHIFT_E2E_AIO_PREBUILT: "1" })).toBe(true);
    expect(usesPrebuiltImage({})).toBe(false);
    expect(usesPrebuiltImage({ FLEETSHIFT_E2E_AIO_PREBUILT: "0" })).toBe(false);
    expect(usesPrebuiltImage({ FLEETSHIFT_E2E_AIO_PULL: "1" })).toBe(false);
  });
});

describe("usesPulledImage", () => {
  it("only an explicit pull flag pulls instead of building", () => {
    expect(usesPulledImage({ FLEETSHIFT_E2E_AIO_PULL: "1" })).toBe(true);
    expect(usesPulledImage({})).toBe(false);
    expect(usesPulledImage({ FLEETSHIFT_E2E_AIO_PULL: "0" })).toBe(false);
    expect(usesPulledImage({ FLEETSHIFT_E2E_AIO_PREBUILT: "1" })).toBe(false);
  });
});

describe("sanitize", () => {
  it("redacts tokens, login URLs, and access_token values", () => {
    const bearer = sanitize("Authorization: Bearer tokensecret");
    expect(bearer).toContain("Bearer [REDACTED]");
    expect(bearer).not.toContain("tokensecret");

    const auth = sanitize(
      "open https://issuer.example/auth?client_id=cli&secret=yes",
    );
    expect(auth).toContain("[AUTH URL REDACTED]");
    expect(auth).not.toContain("secret=yes");

    const callback = sanitize("GET http://127.0.0.1:0/callback?code=oauthcode");
    expect(callback).toContain("[CALLBACK URL REDACTED]");
    expect(callback).not.toContain("oauthcode");

    const query = sanitize("retry ?state=abc&code_challenge=xyz leftover");
    expect(query).toContain("state=[REDACTED]");
    expect(query).toContain("code_challenge=[REDACTED]");
    expect(query).not.toContain("abc");
    expect(query).not.toContain("xyz");

    const token = sanitize('{"access_token":"jwt-secret"}');
    expect(token).toContain('"access_token":"[REDACTED]"');
    expect(token).not.toContain("jwt-secret");
  });
});

describe("normalizeSocketPath", () => {
  it("normalizes the socket path reported by Podman on macOS", () => {
    expect(
      normalizeSocketPath(
        "unix:///Users/me/.local/share/containers/podman.sock\n",
      ),
    ).toBe("/Users/me/.local/share/containers/podman.sock");
    expect(normalizeSocketPath("tcp://127.0.0.1:1234")).toBe("");
  });
});
