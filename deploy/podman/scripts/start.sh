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
# starts the stack. No local Keycloak — serve OIDC bootstrap flags point at
# the external issuer (including AuthMethod policy fields).

# Env vars (DEPLOY_MODE, DB, AUTH, DB_FLAG, COMPOSE_FILES) are set by Taskfile.
# AUTH_MODE is derived from AUTH for backwards compatibility within this script.
AUTH_MODE="$AUTH"
DB_BACKEND="$DB"
ensure_podman_ready

podman network exists kind 2>/dev/null || podman network create kind

# ---------------------------------------------------------------------------
# Warn when host /etc/hosts will leak into kind-node containers.
#
# Podman seeds each container's /etc/hosts from the host's /etc/hosts by
# default (base_hosts_file = "").  When the host maps the local Keycloak
# hostname (typically "keycloak") to 127.0.0.1 — which is needed for
# browser and CLI access — that entry is copied into every container,
# including kind cluster nodes.  Inside a kind node the kube-apiserver
# resolves the OIDC issuer to loopback instead of the container-network
# address, so OIDC token validation fails and deployments pause with an
# authentication error.
# ---------------------------------------------------------------------------
warn_base_hosts_file() {
  local kc_host="${KC_HOSTNAME:-keycloak}"

  # Only relevant when the host has a loopback entry for the issuer hostname.
  # Use awk for exact whitespace-delimited field matching — portable and
  # avoids false positives on partial matches (e.g. "keycloak-old").
  if ! awk -v host="${kc_host}" \
    '$1 == "127.0.0.1" { for(i=2;i<=NF;i++) if($i == host) {f=1; exit} } END { exit !f }' \
    /etc/hosts 2>/dev/null; then
    return 0
  fi

  local conf="$HOME/.config/containers/containers.conf"

  # Already configured to a non-default value — nothing to do.
  # The default ("" or "/etc/hosts") still copies host entries, so only
  # values like "/dev/null" actually fix the problem.
  if [ -f "$conf" ]; then
    local val
    val=$(sed -n 's/^[[:space:]]*base_hosts_file[[:space:]]*=[[:space:]]*"\{0,1\}\([^"]*\)"\{0,1\}/\1/p' "$conf" | tail -1)
    case "$val" in
      ""|/etc/hosts) ;;   # default or explicit host file — still vulnerable
      *) return 0 ;;      # set to /dev/null or similar — safe
    esac
  fi

  echo ""
  echo "⚠️ WARNING ⚠️ : host /etc/hosts maps '${kc_host}' to 127.0.0.1."
  echo "  For Podman running on Linux systems, Podman copies this into every"
  echo "  container it creates, including kind cluster nodes. The kube-apiserver"
  echo "  inside those nodes will resolve the OIDC issuer to loopback instead of "
  echo "  the Keycloak container, causing deployments to provisioned clusters "
  echo "  to pause with an auth error."
  echo ""
  echo "  To fix, add this to ${conf}:"
  echo ""
  echo "    [containers]"
  echo "    base_hosts_file = \"/dev/null\""
  echo ""
  echo "  This tells podman to start containers with a minimal /etc/hosts"
  echo "  (localhost + container hostname). Container-to-container DNS via"
  echo "  the podman network is unaffected — service names still resolve."
  echo ""
  echo "  ⚠️  This may affect the behavior of other containers running in podman"
  echo "     that could be relying on the /etc/hosts to be copied from the host"
  echo "     to the container"
  echo ""  
  echo "  After changing the file, restart the podman API service:"
  echo ""
  echo "    systemctl --user restart podman.service"
  echo ""
  echo "  Then recreate any existing kind clusters for the fix to take effect."
  echo ""
}

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
compose up "${UP_ARGS[@]}"

if [ "$AUTH_MODE" = "local" ]; then
  kc_host="${KC_HOSTNAME:-keycloak}"
  KC_URL="https://${kc_host}:${KC_HTTPS_PORT:-8443}/auth"

  if ! { command -v getent >/dev/null 2>&1 && getent hosts "$kc_host" >/dev/null 2>&1; } \
    && ! grep -Eq "^[^#]*[[:space:]]${kc_host}([[:space:]]|$)" /etc/hosts 2>/dev/null; then
    echo "ERROR: hostname '${kc_host}' does not resolve on this host." >&2
    echo "Map it to loopback before continuing (needed for Keycloak checks and fleetctl):" >&2
    echo "  echo \"127.0.0.1 ${kc_host}\" | sudo tee -a /etc/hosts" >&2
    if [ "$(uname -s)" = "Darwin" ]; then
      echo "On macOS, also add IPv6 (Podman may only forward IPv6 loopback):" >&2
      echo "  echo \"::1 ${kc_host}\" | sudo tee -a /etc/hosts" >&2
    fi
    exit 1
  fi

  echo "==> Waiting for Keycloak API..."
  api_deadline=$((SECONDS + 90))
  while true; do
    if curl -sf --connect-timeout 2 --max-time 3 \
      --cacert "${COMPOSE_DIR}/.certs/ca.crt" \
      "$KC_URL/realms/master" >/dev/null 2>&1; then
      break
    fi
    if (( SECONDS >= api_deadline )); then
      echo "ERROR: Keycloak API not reachable after 90 seconds." >&2
      echo "  Check local mkcert trust or rerun ./deploy/podman/scripts/generate-certs.sh." >&2
      exit 1
    fi
    sleep 1
  done

  ADMIN_TOKEN=$(curl -sf --cacert "${COMPOSE_DIR}/.certs/ca.crt" \
    "$KC_URL/realms/master/protocol/openid-connect/token" \
    -d "grant_type=password&client_id=admin-cli&username=admin&password=${KC_BOOTSTRAP_ADMIN_PASSWORD}" \
    | jq -r .access_token)

  PROFILE_JSON=$(curl -sf --cacert "${COMPOSE_DIR}/.certs/ca.crt" \
    "$KC_URL/admin/realms/fleetshift/users/profile" \
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
    curl -sf -o /dev/null --cacert "${COMPOSE_DIR}/.certs/ca.crt" -X PUT \
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
  echo "    Keycloak Admin:  https://${kc_host}:${KC_HTTPS_PORT:-8443}"
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
  echo "    Configure fleetctl:"
  echo "      bin/fleetctl auth setup \\"
  echo "        --issuer-url ${OIDC_URL} \\"
  echo "        --client-id fleetshift-cli \\"
  echo "        --key-enrollment-client-id fleetshift-signing \\"
  echo "        --oidc-ca-file deploy/podman/.certs/ca.crt"
  echo "      bin/fleetctl auth login"
fi
if [ "$AUTH_MODE" = "local" ] && [ "$(uname -s)" = "Linux" ]; then
  warn_base_hosts_file
fi
echo "    Run 'task podman:logs' to tail container output."
echo "    Run 'task podman:status' to check container health."
echo "    Run 'task --list' to see all available commands."
