#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

load_env

if [ "${1:-}" = "--clean" ]; then
  echo "==> Stopping stack and removing all data"
  compose down -v
  rm -f "$COMPOSE_DIR/.realm.json"
else
  echo "==> Stopping stack (preserving data)"
  compose down
fi

echo "==> Done."
