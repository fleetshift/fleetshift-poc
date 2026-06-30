#!/usr/bin/env bash
# Print the podman API socket path. Used by Taskfile vars.
set -euo pipefail

if [ -n "${PODMAN_SOCKET:-}" ]; then
  echo "$PODMAN_SOCKET"
  exit 0
fi

path=$(podman info --format '{{.Host.RemoteSocket.Path}}' 2>/dev/null | sed 's|^unix://||') || true
if [ -n "$path" ]; then
  echo "$path"
elif [ "$(uname -s)" = "Linux" ]; then
  echo "/run/user/$(id -u)/podman/podman.sock"
else
  echo "ERROR: Could not detect podman socket. Is podman machine running?" >&2
  exit 1
fi
