#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# Stop the AIO compose stack. Called by task podman:down / podman:clean.
#   ./stop.sh          — stop containers, preserve volumes
#   ./stop.sh --clean  — also delete volumes and .certs

ensure_podman_ready

# Include every overlay that can define a service so compose can find them
# regardless of how the stack was started.
# shellcheck disable=SC2034 # used by compose() in common.sh
COMPOSE_FILES="-f $COMPOSE_DIR/compose.yaml -f $COMPOSE_DIR/overrides/dev.yaml -f $COMPOSE_DIR/overrides/local-web.yaml -f $COMPOSE_DIR/overrides/nx-cache.yaml"

if [ "${1:-}" = "--clean" ]; then
  echo "==> Stopping stack and removing volumes and .certs"
  compose down -v
  rm -rf "$COMPOSE_DIR/.certs"
else
  echo "==> Stopping stack (preserving data)"
  compose down
fi

echo "==> Done."
