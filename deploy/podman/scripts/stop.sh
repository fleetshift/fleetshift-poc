#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# Stop the FleetShift stack. Called by 'task podman:down' and 'task podman:clean'.
#
#   ./stop.sh          — stop containers, preserve volumes
#   ./stop.sh --clean  — stop containers, delete volumes, remove kind cluster

# compose down never executes commands — it stops containers by name.
# Export placeholders so compose doesn't warn about unset variables during YAML parsing.
export DB_FLAG="unused"
export OIDC_ISSUER_URL="${OIDC_ISSUER_URL:-unused}"
export GCPHCP_CONFIG="${GCPHCP_CONFIG:-/dev/null}"
export FLEETSHIFT_SERVER_ADDONS="${FLEETSHIFT_SERVER_ADDONS:-kind,ocp,kubernetes}"

# Always include all override files so compose can find every possible service,
# regardless of which mode was used to start the stack.
# shellcheck disable=SC2034 # used by compose() in common.sh
COMPOSE_FILES="-f $COMPOSE_DIR/compose.yaml -f $COMPOSE_DIR/overrides/sqlite.yaml -f $COMPOSE_DIR/overrides/postgres.yaml -f $COMPOSE_DIR/overrides/local-keycloak.yaml -f $COMPOSE_DIR/overrides/external-oidc.yaml -f $COMPOSE_DIR/overrides/gcphcp.yaml -f $COMPOSE_DIR/overrides/dev.yaml"

if [ "${1:-}" = "--clean" ]; then
  echo "==> Stopping stack and removing all data"
  if command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -q "^my-oidc-cluster$"; then
    echo "==> Deleting kind cluster: my-oidc-cluster"
    kind delete cluster --name my-oidc-cluster
  fi
  compose down -v
  rm -f "$COMPOSE_DIR/.realm.json"
else
  echo "==> Stopping stack (preserving data)"
  compose down
fi

echo "==> Done."
