#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$(cd "$COMPOSE_DIR/.." && pwd)"

# Ensure .env exists
if [ ! -f "$DEPLOY_DIR/.env" ]; then
  if [ -f "$DEPLOY_DIR/.env.template" ]; then
    echo "No .env found, creating from .env.template"
    cp "$DEPLOY_DIR/.env.template" "$DEPLOY_DIR/.env"
  else
    echo "ERROR: No .env or .env.template found in $DEPLOY_DIR" >&2
    exit 1
  fi
fi
set -a; source "$DEPLOY_DIR/.env"; set +a

DEMO_MODE="${DEMO_MODE:-true}"
REALM_TEMPLATE="${DEPLOY_DIR}/keycloak/fleetshift-realm.json"
REALM_JSON="${COMPOSE_DIR}/.realm.json"
COMPOSE_PROFILES=""

if [ "$DEMO_MODE" = "true" ]; then
  COMPOSE_PROFILES="demo"

  # Generate all passwords
  echo "==> Generating passwords"
  KC_BOOTSTRAP_ADMIN_PASSWORD=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16)
  export KC_BOOTSTRAP_ADMIN_PASSWORD
  OPS_PASSWORD=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16)
  DEV_PASSWORD=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16)
  ADMIN_USER_PASSWORD=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16)

  jq \
    --arg ops "$OPS_PASSWORD" \
    --arg dev "$DEV_PASSWORD" \
    --arg adm "$ADMIN_USER_PASSWORD" \
    '.users |= map(
        if .username == "ops" then .credentials[0].value = $ops
        elif .username == "dev" then .credentials[0].value = $dev
        elif .username == "admin" then .credentials[0].value = $adm
        else .
        end
    )' "$REALM_TEMPLATE" > "$REALM_JSON"
fi

# Detect podman socket path
if [ -z "${PODMAN_SOCKET:-}" ]; then
  PODMAN_SOCKET=$(podman info --format '{{.Host.RemoteSocket.Path}}' 2>/dev/null | sed 's|^unix://||' || echo "/run/user/$(id -u)/podman/podman.sock")
fi

echo "==> Starting FleetShift stack (demo_mode=$DEMO_MODE)"
COMPOSE_PROFILES="$COMPOSE_PROFILES" \
PODMAN_SOCKET="$PODMAN_SOCKET" \
  podman compose -f "$COMPOSE_DIR/docker-compose.yml" --env-file "$DEPLOY_DIR/.env" up -d

echo ""
echo "==> FleetShift stack is starting!"
echo "    GUI:             http://localhost:3000"
echo "    Mock API:        http://localhost:4000"
echo "    FleetShift API:  http://localhost:${FLEETSHIFT_SERVER_HTTP_PORT:-8085}"
echo "    Mock Plugins:    http://localhost:8001"
if [ "$DEMO_MODE" = "true" ]; then
  echo "    Keycloak Admin:  https://keycloak:${KC_HTTPS_PORT:-8443}"
  echo "    Keycloak (HTTP): http://localhost:${KC_HTTP_PORT:-8180}"
  echo ""
  echo "  Keycloak Admin Console:"
  echo "    ${KC_BOOTSTRAP_ADMIN_USERNAME:-admin} / ${KC_BOOTSTRAP_ADMIN_PASSWORD}"
  echo ""
  echo "  FleetShift Realm Credentials:"
  echo "    ops / ${OPS_PASSWORD}"
  echo "    dev / ${DEV_PASSWORD}"
  echo "    admin / ${ADMIN_USER_PASSWORD}"
fi
echo ""
echo "    Run 'make logs' to tail container output."
echo "    Run 'make status' to check container health."
