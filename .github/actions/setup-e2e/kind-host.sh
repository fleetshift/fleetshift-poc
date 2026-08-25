#!/usr/bin/env bash
# User podman.socket + kind network for E2E jobs (same recipe as local Linux).
set -euo pipefail

uid="$(id -u)"
user="$(id -un)"
runtime_dir="${XDG_RUNTIME_DIR:-/run/user/${uid}}"
export XDG_RUNTIME_DIR="${runtime_dir}"

if [ ! -d "${XDG_RUNTIME_DIR}" ]; then
  sudo mkdir -p "${XDG_RUNTIME_DIR}"
  sudo chown "${uid}:${uid}" "${XDG_RUNTIME_DIR}"
  sudo chmod 700 "${XDG_RUNTIME_DIR}"
fi

if command -v loginctl >/dev/null 2>&1; then
  if ! sudo loginctl enable-linger "${user}"; then
    echo "warning: loginctl enable-linger ${user} failed (continuing)" >&2
  fi
fi
if ! sudo systemctl start "user@${uid}.service"; then
  echo "user@${uid}.service failed to start" >&2
  sudo systemctl status "user@${uid}.service" >&2 || true
  exit 1
fi

export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=${XDG_RUNTIME_DIR}/bus}"

systemctl --user enable --now podman.socket

sock="${XDG_RUNTIME_DIR}/podman/podman.sock"
export CONTAINER_HOST="unix://${sock}"

ready=0
for _ in $(seq 1 20); do
  if [ -S "${sock}" ] && podman info >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [ "${ready}" -ne 1 ]; then
  echo "user podman.socket is not ready at ${sock}" >&2
  systemctl --user status podman.socket >&2 || true
  sudo systemctl status "user@${uid}.service" >&2 || true
  podman info >&2 || true
  exit 1
fi

podman network exists kind || podman network create kind

if [ -n "${GITHUB_ENV:-}" ]; then
  # Host job env: the user socket path the tests mount. CONTAINER_HOST is the
  # podman-remote URL for the same socket.
  {
    echo "XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR}"
    echo "PODMAN_SOCKET=${sock}"
    echo "CONTAINER_HOST=${CONTAINER_HOST}"
  } >> "${GITHUB_ENV}"
fi

echo "PODMAN_SOCKET=${sock}"
