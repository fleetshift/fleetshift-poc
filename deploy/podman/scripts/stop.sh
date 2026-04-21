#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

load_env

if [ "${1:-}" = "--clean" ]; then
  echo "==> Stopping stack and removing all data"
  compose down -v
  rm -f "$COMPOSE_DIR/.realm.json"
  if command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -q "^my-oidc-cluster$"; then
    echo "==> Deleting kind cluster: my-oidc-cluster"
    kind delete cluster --name my-oidc-cluster
  fi
else
  echo "==> Stopping stack (preserving data)"
  compose down
fi

echo "==> Done."
