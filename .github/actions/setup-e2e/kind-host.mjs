#!/usr/bin/env node
// User podman.socket + kind network for E2E jobs (same recipe as local Linux).
import { appendFile } from "node:fs/promises";
import { $, sleep } from "zx";

import { ensureCiKeyringLimits } from "../../../e2e/sandbox/keyring-host.mjs";

$.verbose = true;

const uid = (await $`id -u`).stdout.trim();
const user = (await $`id -un`).stdout.trim();
const runtimeDir = process.env.XDG_RUNTIME_DIR || `/run/user/${uid}`;
process.env.XDG_RUNTIME_DIR = runtimeDir;

if (!(await $`test -d ${runtimeDir}`.nothrow().quiet()).ok) {
  await $`sudo mkdir -p ${runtimeDir}`;
  await $`sudo chown ${uid}:${uid} ${runtimeDir}`;
  await $`sudo chmod 700 ${runtimeDir}`;
}

if ((await $`command -v loginctl`.nothrow().quiet()).ok) {
  if (!(await $`sudo loginctl enable-linger ${user}`.nothrow()).ok) {
    console.error(
      `warning: loginctl enable-linger ${user} failed (continuing)`,
    );
  }
}

const userUnit = `user@${uid}.service`;
if (!(await $`sudo systemctl start ${userUnit}`.nothrow()).ok) {
  console.error(`${userUnit} failed to start`);
  await $`sudo systemctl status ${userUnit}`.nothrow();
  process.exit(1);
}

process.env.DBUS_SESSION_BUS_ADDRESS =
  process.env.DBUS_SESSION_BUS_ADDRESS || `unix:path=${runtimeDir}/bus`;

await $`systemctl --user enable --now podman.socket`;

const sock = `${runtimeDir}/podman/podman.sock`;
process.env.CONTAINER_HOST = `unix://${sock}`;

let ready = false;
for (let i = 0; i < 20; i++) {
  const hasSocket = (await $`test -S ${sock}`.nothrow().quiet()).ok;
  const podmanOk = (await $`podman info`.nothrow().quiet()).ok;
  if (hasSocket && podmanOk) {
    ready = true;
    break;
  }
  await sleep(1000);
}

if (!ready) {
  console.error(`user podman.socket is not ready at ${sock}`);
  await $`systemctl --user status podman.socket`.nothrow();
  await $`sudo systemctl status ${userUnit}`.nothrow();
  await $`podman info`.nothrow();
  process.exit(1);
}

if (!(await $`podman network exists kind`.nothrow()).ok) {
  await $`podman network create kind`;
}

try {
  ensureCiKeyringLimits({
    error: (message) => console.error(message),
    log: (message) => console.log(message),
  });
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}

if (process.env.GITHUB_ENV) {
  // Host job env: the user socket path the tests mount. CONTAINER_HOST is the
  // podman-remote URL for the same socket.
  await appendFile(
    process.env.GITHUB_ENV,
    [
      `XDG_RUNTIME_DIR=${process.env.XDG_RUNTIME_DIR}`,
      `PODMAN_SOCKET=${sock}`,
      `CONTAINER_HOST=${process.env.CONTAINER_HOST}`,
      "",
    ].join("\n"),
  );
}

console.log(`PODMAN_SOCKET=${sock}`);
