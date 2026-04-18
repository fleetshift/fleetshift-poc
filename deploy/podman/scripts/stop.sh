#!/usr/bin/env bash
set -euo pipefail

CONTAINERS=(
  fleetshift-gui
  fleetshift-mock-servers
  fleetshift-mock-ui-plugins
  fleetshift-server
  keycloak
)

VOLUMES=(
  fleetshift-data
  fleetshift-mock-servers-db
  keycloak-certs
  ui-plugins-dist
)

NETWORK=fleetshift

echo "==> Stopping containers"
for c in "${CONTAINERS[@]}"; do
  if podman container exists "$c" 2>/dev/null; then
    podman stop "$c" 2>/dev/null || true
    podman rm "$c" 2>/dev/null || true
    echo "    Stopped $c"
  fi
done

if [ "${1:-}" = "--clean" ]; then
  echo "==> Removing volumes"
  for v in "${VOLUMES[@]}"; do
    podman volume rm "$v" 2>/dev/null || true
    echo "    Removed $v"
  done

  echo "==> Removing network"
  podman network rm "$NETWORK" 2>/dev/null || true
  echo "    Removed $NETWORK"
fi

echo "==> Done."
