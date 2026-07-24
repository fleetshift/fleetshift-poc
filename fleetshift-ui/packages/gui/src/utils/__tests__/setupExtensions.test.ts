import { describe, expect, it, vi } from "vitest";

import { type ResolvedSetup, resolveSetupExtensions } from "../setupExtensions";

function makeExt(
  id: string,
  label: string,
  requires: string[] = [],
  priority?: number,
): ResolvedSetup {
  return {
    properties: {
      id,
      label,
      path: id,
      component:
        (() => {}) as unknown as ResolvedSetup["properties"]["component"],
      requires,
      requiresAuth: true,
      ...(priority !== undefined ? { priority } : {}),
    },
    type: "fleetshift.setup",
    pluginName: "test-plugin",
    uid: `test[${id}]`,
  } as ResolvedSetup;
}

describe("resolveSetupExtensions", () => {
  it("returns a single root extension unchanged", () => {
    const root = makeExt("auth", "Authentication");
    const result = resolveSetupExtensions([root], [root]);
    expect(result.map((e) => e.properties.id)).toEqual(["auth"]);
  });

  it("sorts a linear dependency chain", () => {
    const auth = makeExt("auth", "Authentication");
    const enroll = makeExt("enroll", "Enrollment", ["auth"]);
    const deploy = makeExt("deploy", "Deploy", ["enroll"]);
    const all = [auth, enroll, deploy];

    const result = resolveSetupExtensions([deploy, auth, enroll], all);
    expect(result.map((e) => e.properties.id)).toEqual([
      "auth",
      "enroll",
      "deploy",
    ]);
  });

  it("sorts alphabetically when extensions share the same requirements", () => {
    const auth = makeExt("auth", "Authentication");
    const beta = makeExt("beta", "Beta Step", ["auth"]);
    const alpha = makeExt("alpha", "Alpha Step", ["auth"]);
    const charlie = makeExt("charlie", "Charlie Step", ["auth"]);
    const all = [auth, beta, alpha, charlie];

    const result = resolveSetupExtensions([charlie, beta, auth, alpha], all);
    expect(result.map((e) => e.properties.id)).toEqual([
      "auth",
      "alpha",
      "beta",
      "charlie",
    ]);
  });

  it("respects priority over alphabetical order", () => {
    const auth = makeExt("auth", "Authentication");
    const beta = makeExt("beta", "Beta Step", ["auth"], 1);
    const alpha = makeExt("alpha", "Alpha Step", ["auth"], 10);
    const all = [auth, beta, alpha];

    const result = resolveSetupExtensions([alpha, auth, beta], all);
    expect(result.map((e) => e.properties.id)).toEqual([
      "auth",
      "beta",
      "alpha",
    ]);
  });

  it("filters out extensions with missing dependencies", () => {
    const enroll = makeExt("enroll", "Enrollment", ["auth"]);
    const result = resolveSetupExtensions([enroll], [enroll]);
    expect(result).toEqual([]);
  });

  it("handles a subset referencing dependencies in the full set", () => {
    const auth = makeExt("auth", "Authentication");
    const enroll = makeExt("enroll", "Enrollment", ["auth"]);
    const deploy = makeExt("deploy", "Deploy", ["enroll"]);
    const all = [auth, enroll, deploy];

    const authSubset = [auth];
    const restSubset = [enroll, deploy];

    const authResult = resolveSetupExtensions(authSubset, all);
    expect(authResult.map((e) => e.properties.id)).toEqual(["auth"]);

    const restResult = resolveSetupExtensions(restSubset, all);
    expect(restResult.map((e) => e.properties.id)).toEqual([
      "enroll",
      "deploy",
    ]);
  });

  it("handles diamond dependencies", () => {
    const root = makeExt("root", "Root");
    const left = makeExt("left", "Left", ["root"]);
    const right = makeExt("right", "Right", ["root"]);
    const bottom = makeExt("bottom", "Bottom", ["left", "right"]);
    const all = [root, left, right, bottom];

    const result = resolveSetupExtensions([bottom, right, root, left], all);
    expect(result.map((e) => e.properties.id)).toEqual([
      "root",
      "left",
      "right",
      "bottom",
    ]);
  });

  it("returns empty array for empty input", () => {
    expect(resolveSetupExtensions([], [])).toEqual([]);
  });

  it("places a step after all of its multiple required dependencies", () => {
    const auth = makeExt("auth", "Authentication");
    const networking = makeExt("networking", "Networking", ["auth"]);
    const signing = makeExt("signing", "Signing", ["auth"]);
    const deploy = makeExt("deploy", "Deploy", ["networking", "signing"]);
    const all = [auth, networking, signing, deploy];

    const result = resolveSetupExtensions(
      [deploy, signing, auth, networking],
      all,
    );
    const ids = result.map((e) => e.properties.id);
    expect(ids).toEqual(["auth", "networking", "signing", "deploy"]);
  });

  it("handles multiple roots sorted alphabetically", () => {
    const zeta = makeExt("zeta", "Zeta");
    const alpha = makeExt("alpha", "Alpha");
    const mid = makeExt("mid", "Mid");
    const all = [zeta, alpha, mid];

    const result = resolveSetupExtensions([zeta, alpha, mid], all);
    expect(result.map((e) => e.properties.id)).toEqual([
      "alpha",
      "mid",
      "zeta",
    ]);
  });

  it("warns and excludes an extension when a dependency is missing", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const enroll = makeExt("enroll", "Enrollment", ["auth"]);
    const result = resolveSetupExtensions([enroll], [enroll]);

    expect(result).toEqual([]);
    expect(warnSpy).toHaveBeenCalledWith(
      'Setup extension "enroll" requires "auth" which was not found',
    );

    warnSpy.mockRestore();
  });

  it("warns and excludes a transitive chain when the root dependency is missing", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const enroll = makeExt("enroll", "Enrollment", ["auth"]);
    const deploy = makeExt("deploy", "Deploy", ["enroll"]);
    const all = [enroll, deploy];

    const result = resolveSetupExtensions(all, all);

    expect(result).toEqual([]);
    expect(warnSpy).toHaveBeenCalledWith(
      'Setup extension "enroll" requires "auth" which was not found',
    );
    expect(warnSpy).toHaveBeenCalledWith(
      'Setup extension "deploy" excluded: dependency "enroll" is invalid',
    );

    warnSpy.mockRestore();
  });

  it("warns but keeps valid siblings when only one has a missing dependency", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const auth = makeExt("auth", "Authentication");
    const enroll = makeExt("enroll", "Enrollment", ["auth"]);
    const broken = makeExt("broken", "Broken Step", ["nonexistent"]);
    const all = [auth, enroll, broken];

    const result = resolveSetupExtensions([enroll, broken], all);

    expect(result.map((e) => e.properties.id)).toEqual(["enroll"]);
    expect(warnSpy).toHaveBeenCalledWith(
      'Setup extension "broken" requires "nonexistent" which was not found',
    );

    warnSpy.mockRestore();
  });

  it("excludes cyclic dependencies", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const a = makeExt("a", "A", ["b"]);
    const b = makeExt("b", "B", ["a"]);

    const result = resolveSetupExtensions([a, b], [a, b]);
    expect(result).toEqual([]);
    expect(warnSpy).toHaveBeenCalled();

    warnSpy.mockRestore();
  });

  it("warns on duplicate extension ids and keeps the first", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const first = makeExt("auth", "Auth First");
    const duplicate = makeExt("auth", "Auth Duplicate");

    const result = resolveSetupExtensions([first], [first, duplicate]);
    expect(result.map((e) => e.properties.id)).toEqual(["auth"]);
    expect(result[0].properties.label).toBe("Auth First");
    expect(warnSpy).toHaveBeenCalledWith(
      'Duplicate setup extension id "auth" detected; ignoring later definition',
    );

    warnSpy.mockRestore();
  });
});
