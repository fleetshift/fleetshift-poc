#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$(cd "$COMPOSE_DIR/.." && pwd)"

# Load .env to know if demo profile is active
if [ -f "$DEPLOY_DIR/.env" ]; then
  set -a; source "$DEPLOY_DIR/.env"; set +a
elif [ -f "$DEPLOY_DIR/.env.template" ]; then
  set -a; source "$DEPLOY_DIR/.env.template"; set +a
fi

PROFILES=""
if [ "${DEMO_MODE:-true}" = "true" ]; then
  PROFILES="--profile demo"
fi

if [ "${1:-}" = "--clean" ]; then
  echo "==> Stopping stack and removing all data"
  podman compose -f "$COMPOSE_DIR/docker-compose.yml" $PROFILES down -v
  rm -f "$COMPOSE_DIR/.realm.json"
else
  echo "==> Stopping stack (preserving data)"
  podman compose -f "$COMPOSE_DIR/docker-compose.yml" $PROFILES down
fi

echo "==> Done."
