import { $, sleep } from "zx";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

$.verbose = true;

export const scriptDir = dirname(fileURLToPath(import.meta.url));
export const composeDir = resolve(scriptDir, "..");
export const deployDir = resolve(composeDir, "..");
export const rootDir = resolve(deployDir, "..");

// Nx forwards KEY=value arguments after the target command; expose them to scripts.
export function importKeyValueArgs(args) {
  const positional = [];
  for (const arg of args) {
    if (arg.includes("=")) {
      const separator = arg.indexOf("=");
      process.env[arg.slice(0, separator)] = arg.slice(separator + 1);
    } else {
      positional.push(arg);
    }
  }
  return positional;
}

export async function ensurePodmanReady() {
  // Compose uses Podman's API socket rather than a Docker daemon.
  if (process.platform === "linux") {
    try {
      await $`systemctl --user is-active podman.socket`;
    } catch {
      throw new Error(
        "Podman API socket is not running. Start it with: systemctl --user enable --now podman.socket",
      );
    }
  }

  if (!process.env.PODMAN_SOCKET) {
    try {
      process.env.PODMAN_SOCKET = (
        await $`podman info --format {{.Host.RemoteSocket.Path}}`
      ).stdout
        .trim()
        .replace(/^unix:\/\//, "");
    } catch {
      if (process.platform === "linux") {
        process.env.PODMAN_SOCKET = `/run/user/${process.getuid()}/podman/podman.sock`;
      }
    }
  }

  if (!process.env.PODMAN_SOCKET) {
    throw new Error(
      "Could not determine Podman socket path. Is Podman running?",
    );
  }

  if (process.platform === "linux") {
    const socket = await $`test -S ${process.env.PODMAN_SOCKET}`.nothrow();
    if (!socket.ok)
      throw new Error(
        `Podman API socket not found at ${process.env.PODMAN_SOCKET}. Start it with: systemctl --user enable --now podman.socket`,
      );
    if (!process.env.DOCKER_HOST)
      process.env.DOCKER_HOST = `unix://${process.env.PODMAN_SOCKET}`;
  }
}

export function composeFiles() {
  // Match compose overlays to the flags accepted by the Nx targets.
  const files = ["-f", resolve(composeDir, "compose.yaml")];
  if (process.env.DEV === "true")
    files.push("-f", resolve(composeDir, "overrides/dev.yaml"));
  if (process.env.LOCAL_WEB === "true")
    files.push("-f", resolve(composeDir, "overrides/local-web.yaml"));
  if (process.env.NX_CACHE === "true")
    files.push("-f", resolve(composeDir, "overrides/nx-cache.yaml"));
  return files;
}

export function compose(...args) {
  if (!process.env.COMPOSE_PROVIDER_CHECKED) {
    if (
      spawnSync("command", ["-v", "docker-compose"], {
        shell: true,
        stdio: "ignore",
      }).status !== 0
    ) {
      throw new Error(
        "docker-compose is not installed. Install: brew install docker-compose (podman-compose is not supported)",
      );
    }
    process.env.COMPOSE_PROVIDER_CHECKED = "true";
  }
  return $`podman compose ${composeFiles()} --env-file ${resolve(rootDir, ".env")} ${args}`;
}

export async function copySandboxCA() {
  // Dex creates its CA during startup, so copy it with a bounded retry loop.
  const destination = resolve(composeDir, ".certs/ca.crt");
  await $`mkdir -p ${dirname(destination)}`;
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      await compose(
        "cp",
        "fleetshift-server:/data/sandbox/pki/ca.crt",
        destination,
      );
      return;
    } catch {
      await sleep(1000);
    }
  }
  throw new Error("sandbox CA was not ready before timeout");
}
