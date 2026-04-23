#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

load_env
detect_podman_socket

: "${KIND_TEMP_DIR:=${HOME}/.fleetshift/tmp}"
mkdir -p "$KIND_TEMP_DIR"
export KIND_TEMP_DIR

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

# Register github_username attribute in Keycloak user profile schema.
# Keycloak 26 requires attributes to be registered before the admin API accepts them.
# This can't be done via realm import — requires the admin API after startup.
if [ "${DEMO_MODE:-true}" = "true" ]; then
  KC_URL="http://${KC_HOSTNAME:-localhost}:${KC_HTTP_PORT:-8180}/auth"

  echo "==> Waiting for Keycloak API..."
  until curl -sf "$KC_URL/realms/master" >/dev/null 2>&1; do
    sleep 2
  done

  ADMIN_TOKEN=$(curl -sf "$KC_URL/realms/master/protocol/openid-connect/token" \
    -d "grant_type=password&client_id=admin-cli&username=admin&password=${KC_BOOTSTRAP_ADMIN_PASSWORD}" \
    | jq -r .access_token)

  PROFILE_JSON=$(curl -sf "$KC_URL/admin/realms/fleetshift/users/profile" \
    -H "Authorization: Bearer $ADMIN_TOKEN")

  if echo "$PROFILE_JSON" | jq -e '.attributes[] | select(.name == "github_username")' >/dev/null 2>&1; then
    echo "    github_username attribute already registered."
  else
    echo "==> Registering github_username in user profile schema"
    UPDATED_PROFILE=$(echo "$PROFILE_JSON" | jq '.attributes += [{
      "name": "github_username",
      "displayName": "GitHub Username",
      "validations": {},
      "annotations": {},
      "permissions": {"view": ["admin", "user"], "edit": ["admin"]},
      "multivalued": false
    }]')
    curl -sf -o /dev/null -X PUT \
      "$KC_URL/admin/realms/fleetshift/users/profile" \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "Content-Type: application/json" \
      -d "$UPDATED_PROFILE"
    echo "    github_username attribute registered."
  fi
fi

# Create optional dev user if configured
if [ -n "${DEV_USER_USERNAME:-}" ] && [ "${DEMO_MODE:-true}" = "true" ]; then
  echo "==> Creating dev user: ${DEV_USER_USERNAME}"
  "$SCRIPT_DIR/add-user.sh" \
    --admin-password "$KC_BOOTSTRAP_ADMIN_PASSWORD" \
    --username "$DEV_USER_USERNAME" \
    --password "${DEV_USER_PASSWORD:-changeme}" \
    --github "${DEV_USER_GITHUB:-}" \
    ${DEV_USER_ROLES:+--roles "$DEV_USER_ROLES"}
fi

echo ""
echo "==> FleetShift stack is running!"
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
