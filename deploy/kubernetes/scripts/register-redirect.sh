#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# Register FleetShift UI redirect URI in Keycloak
#
# Detects the web route URL from the cluster and registers it as a
# valid redirect URI in the Keycloak 'fleetshift-ui' client.
#
# Run while logged into the FleetShift cluster (not the Keycloak cluster) —
# the route URL and Keycloak URL are auto-detected from the cluster.
#
# Usage:
#   ./register-redirect.sh <admin-user> <admin-password>
# ------------------------------------------------------------------

NAMESPACE="fleetshift"
REALM="fleetshift"
CLIENT_ID="fleetshift-ui"

log()  { echo "==> $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }

# --- Detect route URL ---
log "Detecting FleetShift web route..."
HTTP_HOST=$(oc get route web -n "${NAMESPACE}" -o jsonpath='{.spec.host}' 2>/dev/null) \
  || die "Route 'web' not found in namespace '${NAMESPACE}'. Deploy FleetShift first."
REDIRECT_URI="https://${HTTP_HOST}/*"
WEB_ORIGIN="https://${HTTP_HOST}"
log "Route: ${WEB_ORIGIN}"

# --- Derive Keycloak URL from OIDC issuer ---
ISSUER_URL=$(oc get configmap fleetshift-server-config -n "${NAMESPACE}" \
  -o jsonpath='{.data.OIDC_ISSUER_URL}' 2>/dev/null) \
  || die "ConfigMap 'fleetshift-server-config' not found."
KC_URL="${ISSUER_URL%%/realms/*}"
log "Keycloak: ${KC_URL}"

# --- Get admin credentials ---
ADMIN_USER="${1:-}"
ADMIN_PASS="${2:-}"
[ -n "$ADMIN_USER" ] || die "Usage: $0 <admin-user> <admin-password>"
[ -n "$ADMIN_PASS" ] || die "Usage: $0 <admin-user> <admin-password>"

# --- Get admin token ---
log "Obtaining admin token..."
ADMIN_TOKEN=$(curl -sf -X POST \
  "${KC_URL}/realms/master/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=admin-cli&username=${ADMIN_USER}&password=${ADMIN_PASS}" \
  | jq -r .access_token)
[ "$ADMIN_TOKEN" != "null" ] && [ -n "$ADMIN_TOKEN" ] \
  || die "Failed to obtain admin token. Check credentials and Keycloak URL."

# --- Get client ---
log "Looking up client '${CLIENT_ID}'..."
CLIENT_UUID=$(curl -sf \
  "${KC_URL}/admin/realms/${REALM}/clients?clientId=${CLIENT_ID}" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.[0].id')
[ "$CLIENT_UUID" != "null" ] && [ -n "$CLIENT_UUID" ] \
  || die "Client '${CLIENT_ID}' not found in realm '${REALM}'."

CLIENT_JSON=$(curl -sf \
  "${KC_URL}/admin/realms/${REALM}/clients/${CLIENT_UUID}" \
  -H "Authorization: Bearer $ADMIN_TOKEN")

CURRENT_URIS=$(echo "$CLIENT_JSON" | jq -r '.redirectUris')
CURRENT_ORIGINS=$(echo "$CLIENT_JSON" | jq -r '.webOrigins')

# --- Check if already registered ---
if echo "$CURRENT_URIS" | jq -e --arg uri "$REDIRECT_URI" 'index($uri) != null' > /dev/null 2>&1; then
  log "Redirect URI already registered (skipping)."
  exit 0
fi

# --- Update client ---
UPDATED_URIS=$(echo "$CURRENT_URIS" | jq --arg uri "$REDIRECT_URI" '. + [$uri]')
UPDATED_ORIGINS=$(echo "$CURRENT_ORIGINS" | jq --arg origin "$WEB_ORIGIN" '. + [$origin] | unique')

log "Adding redirect URI: ${REDIRECT_URI}"
HTTP_CODE=$(curl -sf -o /dev/null -w '%{http_code}' -X PUT \
  "${KC_URL}/admin/realms/${REALM}/clients/${CLIENT_UUID}" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "$(echo "$CLIENT_JSON" | jq \
    --argjson uris "$UPDATED_URIS" \
    --argjson origins "$UPDATED_ORIGINS" \
    '.redirectUris = $uris | .webOrigins = $origins')")

case "$HTTP_CODE" in
  2*) log "Done. Redirect URI registered for ${WEB_ORIGIN}." ;;
  *)  die "Failed to update client (HTTP ${HTTP_CODE})." ;;
esac
