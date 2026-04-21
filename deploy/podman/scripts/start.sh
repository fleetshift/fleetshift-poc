#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

load_env
detect_podman_socket

REALM_TEMPLATE="${DEPLOY_DIR}/keycloak/fleetshift-realm.json"
REALM_JSON="${COMPOSE_DIR}/.realm.json"

if [ "${DEMO_MODE:-true}" = "true" ]; then
  echo "==> Generating passwords"
  KC_BOOTSTRAP_ADMIN_PASSWORD=$(generate_password)
  export KC_BOOTSTRAP_ADMIN_PASSWORD
  OPS_PASSWORD=$(generate_password)
  DEV_PASSWORD=$(generate_password)

  jq \
    --arg ops "$OPS_PASSWORD" \
    --arg dev "$DEV_PASSWORD" \
    '.users |= map(
        if .username == "ops" then .credentials[0].value = $ops
        elif .username == "dev" then .credentials[0].value = $dev
        else .
        end
    )' "$REALM_TEMPLATE" > "$REALM_JSON"
fi

echo "==> Starting FleetShift stack (demo_mode=${DEMO_MODE:-true})"
PODMAN_SOCKET="$PODMAN_SOCKET" compose up -d

echo ""
echo "==> FleetShift stack is starting!"
echo "    GUI:             http://localhost:3000"
echo "    Mock API:        http://localhost:4000"
echo "    FleetShift API:  http://localhost:${FLEETSHIFT_SERVER_HTTP_PORT:-8085}"
echo "    Mock Plugins:    http://localhost:8001"
if [ "${DEMO_MODE:-true}" = "true" ]; then
  echo "    Keycloak Admin:  https://localhost:${KC_HTTPS_PORT:-8443}"
  echo "    Keycloak (HTTP): http://localhost:${KC_HTTP_PORT:-8180}"
  echo ""
  echo "  Keycloak Admin Console:"
  echo "    admin / ${KC_BOOTSTRAP_ADMIN_PASSWORD}"
  echo ""
  echo "  FleetShift Realm Credentials:"
  echo "    ops / ${OPS_PASSWORD}"
  echo "    dev / ${DEV_PASSWORD}"
fi
echo ""
echo "    Run 'make logs' to tail container output."
echo "    Run 'make status' to check container health."
