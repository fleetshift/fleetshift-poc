/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { describe, expect, it } from "vitest";

import {
  ALLOW_LOW_KEYRING_ENV,
  KEYRING_DOCS_PATH,
  REQUIRED_MAXBYTES,
  REQUIRED_MAXKEYS,
  SYSCTL_D_BASENAME,
  SYSCTL_D_FILE,
  classifyPodmanHost,
  ensureCiKeyringLimits,
  formatCompetingKeyringDefinitions,
  formatKeyringDiagnostics,
  formatKeyringGuidance,
  hasSufficientKeyringLimits,
  honorsLowKeyringOverride,
  inspectKeyring,
  isKeyringQuotaError,
  keyUsersLine,
  keyringSysctlDefinitions,
  laterConflictingKeyringDefinitions,
  parseSysctlValue,
  preflightKeyring,
  sysctlDBasenameSortsLater,
  sysctlWritesNeeded,
  winningKeyringDefinition,
} from "./keyring-host.mjs";

const DEFAULT_LIMITS = { maxbytes: 20_000, maxkeys: 200 };
const SUFFICIENT = { maxbytes: REQUIRED_MAXBYTES, maxkeys: REQUIRED_MAXKEYS };
const HIGH = { maxbytes: 500_000, maxkeys: 5_000 };

const NATIVE_LINUX = {
  kind: "native-linux",
  label: "native Linux rootless Podman",
};

function nativeLinuxHost() {
  return classifyPodmanHost({
    connections: [
      {
        isDefault: true,
        name: "local",
        uri: "unix:///run/user/1000/podman/podman.sock",
      },
    ],
    engine: "podman",
    machines: [],
    platform: "linux",
    rootless: true,
    uid: 1000,
  });
}

function testIO(overrides = {}) {
  const commands = [];
  const files = {
    "/proc/sys/kernel/keys/maxkeys": "200\n",
    "/proc/sys/kernel/keys/maxbytes": "20000\n",
    "/proc/key-users":
      "    0:     1 0/0 0/200 0/20000\n  1000:     4 3/3 2/200 400/20000\n",
    ...overrides.files,
  };
  const io = {
    commands,
    env: { ...overrides.env },
    files,
    error(message) {
      io.errors.push(String(message));
    },
    errors: [],
    log(message) {
      io.logs.push(String(message));
    },
    logs: [],
    platform: overrides.platform ?? "linux",
    readFile(path) {
      if (!(path in files)) throw new Error(`ENOENT: ${path}`);
      return files[path];
    },
    run(command, args) {
      commands.push([command, ...args]);
      const key = [command, ...args].join(" ");
      const responses = overrides.responses ?? {};
      if (typeof responses[key] === "function") return responses[key]();
      if (responses[key]) return responses[key];
      return { status: 0, stderr: "", stdout: "" };
    },
    uid: overrides.uid ?? 1000,
    ...overrides.io,
  };
  return io;
}

function podmanInfo(rootless = true) {
  return JSON.stringify({ host: { security: { rootless } } });
}

function nativeResponses(limits = DEFAULT_LIMITS) {
  return {
    files: {
      "/proc/sys/kernel/keys/maxkeys": `${limits.maxkeys}\n`,
      "/proc/sys/kernel/keys/maxbytes": `${limits.maxbytes}\n`,
      "/proc/key-users":
        "    0:     1 0/0 0/200 0/20000\n  1000:     4 3/3 2/200 400/20000\n",
    },
    responses: {
      "podman info --format json": {
        status: 0,
        stderr: "",
        stdout: podmanInfo(true),
      },
      "podman machine list --format json": {
        status: 0,
        stderr: "",
        stdout: "[]",
      },
      "podman system connection list --format json": {
        status: 0,
        stderr: "",
        stdout: JSON.stringify([
          {
            Default: true,
            Name: "local",
            URI: "unix:///run/user/1000/podman/podman.sock",
          },
        ]),
      },
    },
  };
}

describe("parseSysctlValue", () => {
  it("parses /proc and sysctl output", () => {
    expect(parseSysctlValue("200\n")).toBe(200);
    expect(parseSysctlValue("  200000  ")).toBe(200_000);
    expect(parseSysctlValue("kernel.keys.maxkeys = 200\n")).toBe(200);
    expect(parseSysctlValue("kernel.keys.maxbytes = 20000")).toBe(20_000);
  });

  it("rejects non-integers", () => {
    expect(() => parseSysctlValue("")).toThrow(/sysctl/i);
    expect(() => parseSysctlValue("abc")).toThrow(/sysctl/i);
    expect(() => parseSysctlValue("-1")).toThrow(/sysctl/i);
    expect(() => parseSysctlValue("200.5")).toThrow(/sysctl/i);
    expect(() => parseSysctlValue("200 extra")).toThrow(/sysctl/i);
  });
});

describe("hasSufficientKeyringLimits / sysctlWritesNeeded", () => {
  it("treats values at or above the required minimum as already sufficient", () => {
    expect(hasSufficientKeyringLimits(SUFFICIENT)).toBe(true);
    expect(hasSufficientKeyringLimits(HIGH)).toBe(true);
    expect(hasSufficientKeyringLimits(DEFAULT_LIMITS)).toBe(false);
    expect(
      hasSufficientKeyringLimits({ maxbytes: REQUIRED_MAXBYTES, maxkeys: 199 }),
    ).toBe(false);
    expect(
      hasSufficientKeyringLimits({
        maxbytes: 199_999,
        maxkeys: REQUIRED_MAXKEYS,
      }),
    ).toBe(false);
    expect(sysctlWritesNeeded(SUFFICIENT)).toEqual([]);
    expect(sysctlWritesNeeded(HIGH)).toEqual([]);
  });

  it("never lowers a value that is already higher", () => {
    expect(sysctlWritesNeeded({ maxbytes: 20_000, maxkeys: 5_000 })).toEqual([
      { key: "kernel.keys.maxbytes", value: REQUIRED_MAXBYTES },
    ]);
    expect(sysctlWritesNeeded({ maxbytes: 500_000, maxkeys: 200 })).toEqual([
      { key: "kernel.keys.maxkeys", value: REQUIRED_MAXKEYS },
    ]);
    expect(sysctlWritesNeeded(DEFAULT_LIMITS)).toEqual([
      { key: "kernel.keys.maxkeys", value: REQUIRED_MAXKEYS },
      { key: "kernel.keys.maxbytes", value: REQUIRED_MAXBYTES },
    ]);
  });
});

describe("classifyPodmanHost", () => {
  it("detects native Linux rootless Podman", () => {
    const host = nativeLinuxHost();
    expect(host.kind).toBe("native-linux");
    expect(host.label).toMatch(/native Linux rootless Podman/i);
  });

  it("detects a running Podman machine from the default connection", () => {
    const host = classifyPodmanHost({
      connections: [
        {
          isDefault: true,
          name: "podman-machine-default",
          uri: "ssh://core@127.0.0.1:1234/run/user/1000/podman/podman.sock",
        },
      ],
      engine: "podman",
      machines: [
        { isDefault: true, name: "podman-machine-default", running: true },
      ],
      platform: "darwin",
      rootless: true,
      uid: 501,
    });
    expect(host.kind).toBe("podman-machine");
    expect(host.machineName).toBe("podman-machine-default");
    expect(host.label).toMatch(/podman-machine-default/);
  });

  it("detects a Linux Podman machine even when a stopped extra machine exists", () => {
    const host = classifyPodmanHost({
      connections: [
        {
          isDefault: true,
          name: "dev",
          uri: "ssh://core@127.0.0.1:2222/run/user/1000/podman/podman.sock",
        },
      ],
      engine: "podman",
      machines: [
        { isDefault: false, name: "idle", running: false },
        { isDefault: true, name: "dev", running: true },
      ],
      platform: "linux",
      rootless: true,
      uid: 1000,
    });
    expect(host.kind).toBe("podman-machine");
    expect(host.machineName).toBe("dev");
  });

  it("detects an unmanaged remote Podman host", () => {
    const host = classifyPodmanHost({
      connections: [
        {
          isDefault: true,
          name: "lab",
          uri: "ssh://root@lab.example:22/run/podman/podman.sock",
        },
      ],
      engine: "podman",
      machines: [],
      platform: "linux",
      rootless: true,
      uid: 1000,
    });
    expect(host.kind).toBe("unmanaged-remote");
    expect(host.uri).toContain("lab.example");
    expect(host.label).toMatch(/unmanaged remote/i);
  });

  it("skips rootful Podman and unrelated engines", () => {
    expect(
      classifyPodmanHost({
        connections: [],
        engine: "podman",
        machines: [],
        platform: "linux",
        rootless: false,
        uid: 1000,
      }).kind,
    ).toBe("skipped");
    expect(
      classifyPodmanHost({
        connections: [],
        engine: "podman",
        machines: [],
        platform: "linux",
        rootless: true,
        uid: 0,
      }).kind,
    ).toBe("skipped");
    expect(
      classifyPodmanHost({
        connections: [],
        engine: "docker",
        machines: [],
        platform: "linux",
        rootless: true,
        uid: 1000,
      }).kind,
    ).toBe("skipped");
  });
});

describe("formatKeyringGuidance", () => {
  it("explains kernel keyring quotas and native Linux commands", () => {
    const text = formatKeyringGuidance(NATIVE_LINUX, DEFAULT_LIMITS);
    expect(text).toMatch(/native Linux rootless Podman/);
    expect(text).toMatch(/maxkeys: +200 \(required >= 2000\)/);
    expect(text).toMatch(/maxbytes: +20000 \(required >= 200000\)/);
    expect(text).toMatch(/Disk quota exceeded/);
    expect(text).toMatch(/kernel keyring/i);
    expect(text).not.toMatch(/filesystem disk/i);
    expect(text).toMatch(/simultaneous rootless Kind node containers/);
    expect(text).toMatch(/one container consumed per Kind cluster node/);
    expect(text).toContain("sudo sysctl -w kernel.keys.maxkeys=2000");
    expect(text).toContain("sudo sysctl -w kernel.keys.maxbytes=200000");
    expect(text).toContain(SYSCTL_D_FILE);
    expect(text).toContain(`sudo tee ${SYSCTL_D_FILE}`);
    expect(text).toContain(`sudo sysctl -p ${SYSCTL_D_FILE}`);
    expect(text).toContain("sysctl -n kernel.keys.maxkeys");
    expect(text).toContain("sysctl -n kernel.keys.maxbytes");
    expect(text).toMatch(/99-keys\.conf/);
    expect(text).toMatch(/lexicographic/i);
    expect(text).toMatch(/ask an administrator/i);
    expect(text).toContain(
      `${ALLOW_LOW_KEYRING_ENV}=1 npx nx test:e2e e2e-cli`,
    );
    expect(text).toContain(KEYRING_DOCS_PATH);
  });

  it("prints Podman machine commands including custom machine-name syntax", () => {
    const text = formatKeyringGuidance(
      {
        kind: "podman-machine",
        label: "Podman machine dev",
        machineName: "dev",
      },
      DEFAULT_LIMITS,
    );
    expect(text).toContain("Podman machine dev");
    expect(text).toContain(
      "podman machine ssh dev -- sudo sysctl -w kernel.keys.maxkeys=2000",
    );
    expect(text).toContain(
      "podman machine ssh <machine-name> -- sudo sysctl -w kernel.keys.maxkeys=2000",
    );
    expect(text).toContain(
      `podman machine ssh dev -- sudo sysctl -p ${SYSCTL_D_FILE}`,
    );
    expect(text).toContain(
      "podman machine ssh dev -- sysctl -n kernel.keys.maxkeys",
    );
    expect(text).toMatch(/survives machine restart, not deletion or reset/i);
  });

  it("tells unmanaged remotes to change quotas on the remote host", () => {
    const text = formatKeyringGuidance(
      {
        kind: "unmanaged-remote",
        label:
          "unmanaged remote Podman (ssh://root@lab.example:22/run/podman/podman.sock)",
        uri: "ssh://root@lab.example:22/run/podman/podman.sock",
      },
      undefined,
    );
    expect(text).toMatch(/lab\.example/);
    expect(text).toMatch(/on that host/i);
    expect(text).toContain("sudo sysctl -w kernel.keys.maxkeys=2000");
    expect(text).toContain(SYSCTL_D_FILE);
  });

  it("includes competing sysctl.d definitions when verification would lose", () => {
    const text = formatKeyringGuidance(NATIVE_LINUX, DEFAULT_LIMITS, {
      competing: [
        {
          basename: "zzz-keys.conf",
          key: "kernel.keys.maxkeys",
          path: "/etc/sysctl.d/zzz-keys.conf",
          value: "200",
        },
      ],
    });
    expect(text).toContain("/etc/sysctl.d/zzz-keys.conf");
    expect(text).toContain("kernel.keys.maxkeys = 200");
    expect(text).toMatch(/competing/i);
  });
});

describe("sysctl.d lexicographic ordering", () => {
  it("treats 99-keys.conf as later than 99-fleetshift-e2e-keys.conf", () => {
    expect(
      sysctlDBasenameSortsLater("99-keys.conf", "99-fleetshift-e2e-keys.conf"),
    ).toBe(true);
    expect(
      sysctlDBasenameSortsLater("99-fleetshift-e2e-keys.conf", "99-keys.conf"),
    ).toBe(false);
  });

  it("treats the zz- filename as later than 99-keys.conf", () => {
    expect(sysctlDBasenameSortsLater(SYSCTL_D_BASENAME, "99-keys.conf")).toBe(
      true,
    );
    expect(sysctlDBasenameSortsLater("99-keys.conf", SYSCTL_D_BASENAME)).toBe(
      false,
    );
  });

  it("selects the lexicographically later file as the winner", () => {
    const files = {
      "/etc/sysctl.d/99-fleetshift-e2e-keys.conf":
        "kernel.keys.maxkeys = 2000\nkernel.keys.maxbytes = 200000\n",
      "/etc/sysctl.d/99-keys.conf":
        "kernel.keys.maxkeys = 200\nkernel.keys.maxbytes = 20000\n",
    };
    const definitions = keyringSysctlDefinitions(files);
    expect(
      winningKeyringDefinition(definitions, "kernel.keys.maxkeys"),
    ).toMatchObject({
      path: "/etc/sysctl.d/99-keys.conf",
      value: "200",
    });
    expect(
      winningKeyringDefinition(
        keyringSysctlDefinitions({
          "/etc/sysctl.d/99-keys.conf": "kernel.keys.maxkeys = 200\n",
          [SYSCTL_D_FILE]: "kernel.keys.maxkeys = 2000\n",
        }),
        "kernel.keys.maxkeys",
      ),
    ).toMatchObject({ path: SYSCTL_D_FILE, value: "2000" });
  });

  it("detects a later conflicting definition that would override zz-", () => {
    const definitions = keyringSysctlDefinitions({
      [SYSCTL_D_FILE]: "kernel.keys.maxkeys = 2000\n",
      "/etc/sysctl.d/zzz-keys.conf": "kernel.keys.maxkeys = 200\n",
      "/run/sysctl.d/99-keys.conf": "kernel.keys.maxkeys = 200\n",
    });
    expect(laterConflictingKeyringDefinitions(definitions)).toEqual([
      {
        basename: "zzz-keys.conf",
        key: "kernel.keys.maxkeys",
        path: "/etc/sysctl.d/zzz-keys.conf",
        value: "200",
      },
    ]);
  });

  it("formats competing definitions instead of claiming persistence worked", () => {
    const text = formatCompetingKeyringDefinitions([
      {
        basename: "99-keys.conf",
        key: "kernel.keys.maxkeys",
        path: "/etc/sysctl.d/99-keys.conf",
        value: "200",
      },
      {
        basename: "99-keys.conf",
        key: "kernel.keys.maxbytes",
        path: "/usr/lib/sysctl.d/99-keys.conf",
        value: "20000",
      },
    ]);
    expect(text).toContain(
      "/etc/sysctl.d/99-keys.conf: kernel.keys.maxkeys = 200",
    );
    expect(text).toContain(
      "/usr/lib/sysctl.d/99-keys.conf: kernel.keys.maxbytes = 20000",
    );
    expect(text).toMatch(/competing/i);
    expect(text).not.toMatch(/persistence (is|was) configured/i);
  });
});

describe("honorsLowKeyringOverride / isKeyringQuotaError / keyUsersLine", () => {
  it("honors the local override only outside CI", () => {
    expect(honorsLowKeyringOverride({})).toBe(false);
    expect(honorsLowKeyringOverride({ [ALLOW_LOW_KEYRING_ENV]: "1" })).toBe(
      true,
    );
    expect(
      honorsLowKeyringOverride({
        CI: "true",
        [ALLOW_LOW_KEYRING_ENV]: "1",
      }),
    ).toBe(false);
    expect(
      honorsLowKeyringOverride({
        GITHUB_ACTIONS: "true",
        [ALLOW_LOW_KEYRING_ENV]: "1",
      }),
    ).toBe(false);
  });

  it("recognizes crun keyctl disk-quota errors", () => {
    expect(isKeyringQuotaError("crun: join keyctl: Disk quota exceeded")).toBe(
      true,
    );
    expect(isKeyringQuotaError("Disk quota exceeded")).toBe(false);
    expect(isKeyringQuotaError("keyctl: permission denied")).toBe(false);
  });

  it("selects the uid line from /proc/key-users", () => {
    const raw =
      "    0:     1 0/0 0/200 0/20000\n  1000:     4 3/3 2/200 400/20000\n";
    expect(keyUsersLine(raw, 1000)).toMatch(/1000:/);
    expect(keyUsersLine(raw, 1000)).not.toMatch(/^\s*0:/);
    expect(keyUsersLine(raw, 42)).toBe("");
  });
});

describe("inspectKeyring", () => {
  it("reads native Linux sysctls from /proc and does not ssh a machine", () => {
    const io = testIO(nativeResponses());
    const result = inspectKeyring(io);
    expect(result.host.kind).toBe("native-linux");
    expect(result.limits).toEqual(DEFAULT_LIMITS);
    expect(io.commands.some((parts) => parts.includes("ssh"))).toBe(false);
  });

  it("inspects a Podman machine through podman machine ssh", () => {
    const io = testIO({
      platform: "darwin",
      uid: 501,
      responses: {
        "podman info --format json": {
          status: 0,
          stderr: "",
          stdout: podmanInfo(true),
        },
        "podman machine list --format json": {
          status: 0,
          stderr: "",
          stdout: JSON.stringify([
            {
              Default: true,
              Name: "podman-machine-default",
              Running: true,
            },
          ]),
        },
        "podman system connection list --format json": {
          status: 0,
          stderr: "",
          stdout: JSON.stringify([
            {
              Default: true,
              Name: "podman-machine-default",
              URI: "ssh://core@127.0.0.1:1234/run/user/1000/podman/podman.sock",
            },
          ]),
        },
        "podman machine ssh podman-machine-default -- sh -c cat /proc/sys/kernel/keys/maxkeys; echo ###FLEETSHIFT-E2E-KEYRING###; cat /proc/sys/kernel/keys/maxbytes; echo ###FLEETSHIFT-E2E-KEYRING###; cat /proc/key-users":
          {
            status: 0,
            stderr: "",
            stdout:
              "200\n###FLEETSHIFT-E2E-KEYRING###\n20000\n###FLEETSHIFT-E2E-KEYRING###\n  1000:     4 3/3 2/200 400/20000\n",
          },
      },
    });
    const result = inspectKeyring(io);
    expect(result.host.kind).toBe("podman-machine");
    expect(result.limits).toEqual(DEFAULT_LIMITS);
    expect(io.commands.filter((parts) => parts.includes("ssh"))).toHaveLength(
      1,
    );
  });

  it("fails when podman machine list is not JSON", () => {
    const io = testIO({
      responses: {
        "podman info --format json": {
          status: 0,
          stderr: "",
          stdout: podmanInfo(true),
        },
        "podman machine list --format json": {
          status: 0,
          stderr: "",
          stdout: "Error: cannot list machines",
        },
        "podman system connection list --format json": {
          status: 0,
          stderr: "",
          stdout: "[]",
        },
      },
    });
    expect(() => inspectKeyring(io)).toThrow(/invalid JSON/);
  });
});

describe("preflightKeyring", () => {
  it("blocks insufficient local values before any mutation", () => {
    const io = testIO(nativeResponses());
    expect(() => preflightKeyring(io)).toThrow(/kernel keyring/i);
    expect(() => preflightKeyring(io)).toThrow(/native Linux rootless Podman/);
    expect(() => preflightKeyring(io)).toThrow(/Disk quota exceeded/);
    expect(io.commands.every((parts) => parts[0] !== "sudo")).toBe(true);
    expect(
      io.commands.every((parts) => !parts.some((part) => part.includes("-w"))),
    ).toBe(true);
  });

  it("warns and continues when the local override is set", () => {
    const io = testIO({
      ...nativeResponses(),
      env: { [ALLOW_LOW_KEYRING_ENV]: "1" },
    });
    expect(preflightKeyring(io)).toEqual({ action: "warned" });
    expect(io.errors.join("\n")).toMatch(/WARNING/i);
    expect(io.errors.join("\n")).toMatch(/Disk quota exceeded/);
    expect(io.errors.join("\n")).toContain(
      "sudo sysctl -w kernel.keys.maxkeys=2000",
    );
    expect(io.commands.every((parts) => parts[0] !== "sudo")).toBe(true);
  });

  it("ignores the local override in CI", () => {
    const io = testIO({
      ...nativeResponses(),
      env: { CI: "true", [ALLOW_LOW_KEYRING_ENV]: "1" },
    });
    expect(() => preflightKeyring(io)).toThrow(/kernel keyring/i);
  });

  it("stays quiet and does not mutate when limits are already sufficient", () => {
    const io = testIO(nativeResponses(SUFFICIENT));
    expect(preflightKeyring(io)).toEqual({ action: "ok" });
    expect(io.errors).toEqual([]);
    expect(io.logs).toEqual([]);
    expect(io.commands.every((parts) => parts[0] !== "sudo")).toBe(true);
  });

  it("skips rootful Podman without reading or changing sysctls", () => {
    const io = testIO({
      responses: {
        "podman info --format json": {
          status: 0,
          stderr: "",
          stdout: podmanInfo(false),
        },
        "podman machine list --format json": {
          status: 0,
          stderr: "",
          stdout: "[]",
        },
        "podman system connection list --format json": {
          status: 0,
          stderr: "",
          stdout: "[]",
        },
      },
    });
    expect(preflightKeyring(io)).toEqual({ action: "skipped" });
    expect(io.commands.some((parts) => parts[0] === "sudo")).toBe(false);
  });

  it("does not invoke sudo when reporting an unmanaged remote host", () => {
    const io = testIO({
      responses: {
        "podman info --format json": {
          status: 0,
          stderr: "",
          stdout: podmanInfo(true),
        },
        "podman machine list --format json": {
          status: 0,
          stderr: "",
          stdout: "[]",
        },
        "podman system connection list --format json": {
          status: 0,
          stderr: "",
          stdout: JSON.stringify([
            {
              Default: true,
              Name: "lab",
              URI: "ssh://root@lab.example:22/run/podman/podman.sock",
            },
          ]),
        },
      },
    });
    expect(preflightKeyring(io)).toEqual({ action: "reported-remote" });
    expect(io.errors.join("\n")).toMatch(/lab\.example/);
    expect(io.errors.join("\n")).toMatch(/on that host/i);
    expect(io.commands.every((parts) => parts[0] !== "sudo")).toBe(true);
  });
});

describe("ensureCiKeyringLimits", () => {
  it("raises insufficient values, never lowers higher ones, and verifies", () => {
    const io = testIO({
      env: { [ALLOW_LOW_KEYRING_ENV]: "1" },
      files: {
        "/proc/sys/kernel/keys/maxkeys": "5000\n",
        "/proc/sys/kernel/keys/maxbytes": "20000\n",
        "/proc/key-users": "  1001:     4 3/3 2/5000 400/20000\n",
      },
      uid: 1001,
      responses: {
        "sudo sysctl -w kernel.keys.maxbytes=200000": () => {
          io.files["/proc/sys/kernel/keys/maxbytes"] = "200000\n";
          return {
            status: 0,
            stderr: "",
            stdout: "kernel.keys.maxbytes = 200000",
          };
        },
      },
    });
    ensureCiKeyringLimits(io);
    expect(io.commands).toEqual([
      ["sudo", "sysctl", "-w", "kernel.keys.maxbytes=200000"],
    ]);
    expect(io.logs.join("\n")).toMatch(/kernel\.keys\.maxkeys=5000/);
    expect(io.logs.join("\n")).toMatch(/kernel\.keys\.maxbytes=200000/);
    expect(io.logs.join("\n")).toMatch(/1001:/);
  });

  it("does not write when both values are already sufficient", () => {
    const io = testIO({
      files: {
        "/proc/sys/kernel/keys/maxkeys": "2000\n",
        "/proc/sys/kernel/keys/maxbytes": "200000\n",
        "/proc/key-users": "  1000:     4 3/3 2/2000 400/200000\n",
      },
    });
    ensureCiKeyringLimits(io);
    expect(io.commands).toEqual([]);
    expect(io.logs.join("\n")).toMatch(/kernel\.keys\.maxkeys=2000/);
  });

  it("fails when sysctl cannot raise a low value", () => {
    const io = testIO({
      files: {
        "/proc/sys/kernel/keys/maxkeys": "200\n",
        "/proc/sys/kernel/keys/maxbytes": "20000\n",
        "/proc/key-users": "",
      },
      responses: {
        "sudo sysctl -w kernel.keys.maxkeys=2000": {
          status: 1,
          stderr: "sysctl: permission denied",
          stdout: "",
        },
      },
    });
    expect(() => ensureCiKeyringLimits(io)).toThrow(/sysctl/i);
  });

  it("fails when verification still sees a low value", () => {
    const io = testIO({
      files: {
        "/proc/sys/kernel/keys/maxkeys": "200\n",
        "/proc/sys/kernel/keys/maxbytes": "20000\n",
        "/proc/key-users": "",
      },
      responses: {
        "sudo sysctl -w kernel.keys.maxkeys=2000": {
          status: 0,
          stderr: "",
          stdout: "kernel.keys.maxkeys = 2000",
        },
        "sudo sysctl -w kernel.keys.maxbytes=200000": {
          status: 0,
          stderr: "",
          stdout: "kernel.keys.maxbytes = 200000",
        },
      },
    });
    expect(() => ensureCiKeyringLimits(io)).toThrow(/required >= 2000/);
  });

  it("does not honor the local override", () => {
    const io = testIO({
      env: { CI: "true", [ALLOW_LOW_KEYRING_ENV]: "1" },
      files: {
        "/proc/sys/kernel/keys/maxkeys": "200\n",
        "/proc/sys/kernel/keys/maxbytes": "20000\n",
        "/proc/key-users": "",
      },
      responses: {
        "sudo sysctl -w kernel.keys.maxkeys=2000": {
          status: 1,
          stderr: "denied",
          stdout: "",
        },
      },
    });
    expect(() => ensureCiKeyringLimits(io)).toThrow();
  });
});

describe("formatKeyringDiagnostics", () => {
  it("prints current limits and key-users without remediation", () => {
    const text = formatKeyringDiagnostics({
      captured: "unrelated failure",
      host: NATIVE_LINUX,
      keyUsers:
        "    0:     1 0/0 0/200 0/20000\n  1000:     4 3/3 2/200 400/20000\n",
      limits: DEFAULT_LIMITS,
      uid: 1000,
    });
    expect(text).toMatch(/maxkeys=200/);
    expect(text).toMatch(/maxbytes=20000/);
    expect(text).toMatch(/1000:/);
    expect(text).not.toContain("sudo sysctl -w");
  });

  it("repeats remediation when captured output has a keyctl quota error", () => {
    const text = formatKeyringDiagnostics({
      captured: 'msg="crun: join keyctl: Disk quota exceeded"',
      host: NATIVE_LINUX,
      keyUsers: "",
      limits: DEFAULT_LIMITS,
      uid: 1000,
    });
    expect(text).toContain("sudo sysctl -w kernel.keys.maxkeys=2000");
    expect(text).toMatch(/Disk quota exceeded/);
  });
});
