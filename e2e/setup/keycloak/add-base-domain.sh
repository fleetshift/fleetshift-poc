#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# Add a cluster's console redirect URI to the ocp-console Keycloak client
#
# Keycloak does NOT support wildcard subdomain patterns like
# "apps.*.example.com" — the * only works at the end of a URI path.
# So each cluster needs its own explicit redirect URI added.
#
# Idempotent — safe to run multiple times with the same arguments.
#
# Usage:
#   ./add-base-domain.sh --base-domain example.com --cluster-name my-cluster
# ------------------------------------------------------------------

NAMESPACE="keycloak-prod"
KEYCLOAK_CR_NAME="keycloak"
REALM="fleetshift"
CLIENT_ID="ocp-console"
BASE_DOMAIN=""
CLUSTER_NAME=""

log()  { printf '\033[1;34m>>> %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-domain) BASE_DOMAIN="$2"; shift 2 ;;
    --cluster-name) CLUSTER_NAME="$2"; shift 2 ;;
    *) die "Unknown argument: $1" ;;
  esac
done

[ -n "$BASE_DOMAIN" ] || die "Usage: $0 --base-domain <domain> --cluster-name <name>"
[ -n "$CLUSTER_NAME" ] || die "Usage: $0 --base-domain <domain> --cluster-name <name>"

# Get Keycloak URL
KC_HOST=$(oc get route -n "${NAMESPACE}" -o jsonpath='{.items[0].spec.host}' 2>/dev/null) \
  || die "Could not get Keycloak route. Are you logged into the correct OCP cluster?"
KC_URL="https://${KC_HOST}"

# Get admin credentials
ADMIN_USER=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
  -o jsonpath='{.data.username}' | base64 -d)
ADMIN_PASS=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
  -o jsonpath='{.data.password}' | base64 -d)

log "Obtaining admin token..."
ADMIN_TOKEN=$(curl -s -X POST \
  "${KC_URL}/realms/master/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=admin-cli&username=${ADMIN_USER}&password=${ADMIN_PASS}" \
  | jq -r .access_token)
[ "$ADMIN_TOKEN" != "null" ] && [ -n "$ADMIN_TOKEN" ] \
  || die "Failed to obtain admin token"

# Get client UUID
CLIENT_UUID=$(curl -s \
  "${KC_URL}/admin/realms/${REALM}/clients?clientId=${CLIENT_ID}" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.[0].id')
[ "$CLIENT_UUID" != "null" ] && [ -n "$CLIENT_UUID" ] \
  || die "Client '${CLIENT_ID}' not found in realm '${REALM}'. Run deploy.sh first."

# Get current redirect URIs
CURRENT_URIS=$(curl -s \
  "${KC_URL}/admin/realms/${REALM}/clients/${CLIENT_UUID}" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.redirectUris')

# Build the redirect URI for this specific cluster
NEW_URI="https://console-openshift-console.apps.${CLUSTER_NAME}.${BASE_DOMAIN}/*"

# Check if already present
if echo "$CURRENT_URIS" | jq -e --arg uri "$NEW_URI" 'index($uri) != null' > /dev/null 2>&1; then
  log "Redirect URI already configured for ${CLUSTER_NAME} (skipping)"
  exit 0
fi

# Add the new URI
UPDATED_URIS=$(echo "$CURRENT_URIS" | jq --arg uri "$NEW_URI" '. + [$uri]')

log "Adding redirect URI: ${NEW_URI}"
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
  "${KC_URL}/admin/realms/${REALM}/clients/${CLIENT_UUID}" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"clientId\": \"${CLIENT_ID}\", \"redirectUris\": ${UPDATED_URIS}}")

case "$HTTP_CODE" in
  2*) log "Done. Redirect URI for cluster '${CLUSTER_NAME}' added to ${CLIENT_ID}." ;;
  *)  die "Failed to update client (HTTP ${HTTP_CODE})" ;;
esac
