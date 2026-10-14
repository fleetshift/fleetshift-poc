#!/usr/bin/env node
// Print the podman API socket path for compose.
import { $ } from "zx";

$.verbose = false;

if (process.env.PODMAN_SOCKET) {
  console.log(process.env.PODMAN_SOCKET);
  process.exit(0);
}

const result =
  await $`podman info --format {{.Host.RemoteSocket.Path}}`.nothrow();
const path = result.stdout.trim().replace(/^unix:\/\//, "");
if (path) console.log(path);
else if (process.platform === "linux")
  console.log(`/run/user/${process.getuid()}/podman/podman.sock`);
else {
  console.error(
    "ERROR: Could not detect podman socket. Is podman machine running?",
  );
  process.exit(1);
}
