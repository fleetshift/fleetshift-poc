#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# Stop the FleetShift stack. Called by 'task podman:down' and 'task podman:clean'.
#
#   ./stop.sh          — stop containers, preserve volumes
#   ./stop.sh --clean  — stop containers, delete volumes, remove kind cluster

# compose down never executes commands — it stops containers by name.
ensure_podman_ready

# Include every override that can define a service (dev, nx-cache) so compose
# can find them all regardless of how the stack was started.
# shellcheck disable=SC2034 # used by compose() in common.sh
COMPOSE_FILES="-f $COMPOSE_DIR/compose.yaml -f $COMPOSE_DIR/overrides/dev.yaml -f $COMPOSE_DIR/overrides/local-web.yaml -f $COMPOSE_DIR/overrides/nx-cache.yaml"

if [ "${1:-}" = "--clean" ]; then
  echo "==> Stopping stack and removing all data"
  if command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -q "^my-oidc-cluster$"; then
    echo "==> Deleting kind cluster: my-oidc-cluster"
    kind delete cluster --name my-oidc-cluster
  fi
  compose down -v
  rm -rf "$COMPOSE_DIR/.certs"
else
  echo "==> Stopping stack (preserving data)"
  compose down
fi

echo "==> Done."
