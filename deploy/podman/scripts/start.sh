#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# Start the FleetShift stack. Called by 'task podman:up'.
#
# In demo mode (AUTH=local): generates Keycloak passwords, templates the
# realm JSON, starts the stack, then registers the github_username user
# profile attribute and optionally creates a dev user.
#
# In prod mode (AUTH=external): validates OIDC_ISSUER_URL is set, then
# starts the stack. No local Keycloak — auth-setup points at the
# external OIDC provider.

# Env vars (DEPLOY_MODE, DB, AUTH, DB_FLAG, COMPOSE_FILES) are set by Taskfile.
# AUTH_MODE is derived from AUTH for backwards compatibility within this script.
AUTH_MODE="$AUTH"
DB_BACKEND="$DB"
detect_podman_socket

: "${KIND_TEMP_DIR:=${HOME}/.fleetshift/tmp}"
mkdir -p "$KIND_TEMP_DIR"
export KIND_TEMP_DIR
podman network exists kind 2>/dev/null || podman network create kind

REALM_TEMPLATE="${DEPLOY_DIR}/keycloak/fleetshift-realm.json"
REALM_JSON="${COMPOSE_DIR}/.realm.json"

if [ "$AUTH_MODE" = "external" ]; then
  if [ -z "${OIDC_ISSUER_URL:-}" ]; then
    echo "ERROR: OIDC_ISSUER_URL is required when AUTH=external (DEPLOY_MODE=prod)." >&2
    echo "Set it in .env (at the project root) or pass it as an environment variable." >&2
    exit 1
  fi
fi

if [ "$AUTH_MODE" = "local" ]; then
  echo "==> Generating passwords"
  KC_BOOTSTRAP_ADMIN_PASSWORD=$(generate_password)
  export KC_BOOTSTRAP_ADMIN_PASSWORD
  OPS_PASSWORD=$(generate_password)
  DEV_PASSWORD=$(generate_password)

  jq \
    --arg ops "$OPS_PASSWORD" \
    --arg dev "$DEV_PASSWORD" \
    '.users |= map(
        if .username == "ops-user" then .credentials[0].value = $ops
        elif .username == "dev-user" then .credentials[0].value = $dev
        else .
        end
    )' "$REALM_TEMPLATE" > "$REALM_JSON"
fi

echo "==> Starting FleetShift stack (db=$DB_BACKEND, auth=$AUTH_MODE)"
UP_ARGS=(-d)
if [ "${DEV:-}" = "true" ] || [ "${BUILD:-}" = "true" ]; then
  echo "==> Building base fleetshift-server image"
  podman build -t fleetshift-server "$ROOT_DIR"
  UP_ARGS+=(--build)
  podman volume rm -f web-assets podman_web-assets 2>/dev/null || true
fi
PODMAN_SOCKET="$PODMAN_SOCKET" compose up "${UP_ARGS[@]}"

if [ "$AUTH_MODE" = "local" ]; then
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

if [ -n "${DEV_USER_USERNAME:-}" ] && [ "$AUTH_MODE" = "local" ]; then
  echo "==> Creating dev user: ${DEV_USER_USERNAME}"
  "$DEPLOY_DIR/keycloak/scripts/add-user.sh" \
    --admin-password "$KC_BOOTSTRAP_ADMIN_PASSWORD" \
    --username "$DEV_USER_USERNAME" \
    --password "${DEV_USER_PASSWORD:-changeme}" \
    --github "${DEV_USER_GITHUB:-}" \
    ${DEV_USER_ROLES:+--roles "$DEV_USER_ROLES"}
fi

echo ""
echo "==> FleetShift stack is running!"
echo "    FleetShift:      http://localhost:${FLEETSHIFT_SERVER_HTTP_PORT:-8085}"
if [ "$AUTH_MODE" = "local" ]; then
  echo "    Keycloak Admin:  https://localhost:${KC_HTTPS_PORT:-8443}"
  echo "    Keycloak (HTTP): http://localhost:${KC_HTTP_PORT:-8180}"
  echo ""
  echo "  Keycloak Admin Console:"
  echo "    admin / ${KC_BOOTSTRAP_ADMIN_PASSWORD}"
  echo ""
  echo "  FleetShift Realm Credentials:"
  echo "    ops-user / ${OPS_PASSWORD}"
  echo "    dev-user / ${DEV_PASSWORD}"
fi
echo ""
if [ "$AUTH_MODE" = "local" ]; then
  echo "    Run 'task podman:cli-setup' to configure fleetctl."
fi
echo "    Run 'task podman:logs' to tail container output."
echo "    Run 'task podman:status' to check container health."
echo "    Run 'task --list' to see all available commands."
