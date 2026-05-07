#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# Reset the FleetShift Keycloak realm to match the realm config file
#
# Deletes and re-imports the KeycloakRealmImport CR with freshly
# generated user passwords. Keycloak itself (pods, database, TLS,
# operators) is left untouched — only the realm configuration is
# replaced.
#
# Use this after editing fleetshift-realm.json, or to reset the
# realm back to its default state (e.g. clear manual UI changes).
#
# Note: any users created via 'task kc:add-user' will be lost.
# Re-run it after the reset to recreate personal accounts.
#
# Usage:
#   ./reset-realm.sh
# ------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEYCLOAK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="keycloak-prod"
KEYCLOAK_CR_NAME="keycloak"

info()  { echo "==> $*"; }
warn()  { echo "WARNING: $*"; }
error() { echo "ERROR: $*" >&2; exit 1; }

# --- Preflight checks ---
command -v oc &>/dev/null || error "'oc' CLI not found in PATH."
command -v jq &>/dev/null || error "'jq' not found in PATH."
command -v openssl &>/dev/null || error "'openssl' not found in PATH."
timeout 5 oc whoami &>/dev/null || error "Not logged in to OpenShift. Run 'oc login' first."

oc get keycloak/"${KEYCLOAK_CR_NAME}" -n "${NAMESPACE}" &>/dev/null \
    || error "Keycloak CR '${KEYCLOAK_CR_NAME}' not found in namespace '${NAMESPACE}'. Is Keycloak deployed?"

echo ""
echo "This will delete and re-import the FleetShift realm."
echo "All realm state (manually created users, role changes, client"
echo "config tweaks) will be reset to the realm config file defaults."
echo ""
read -rp "Continue? (y/N): " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
echo ""

# --- Step 1: Get admin credentials and delete realm ---
APPS_DOMAIN=$(oc get ingresses.config/cluster -o jsonpath='{.spec.domain}')
KEYCLOAK_HOST="${KEYCLOAK_CR_NAME}-${NAMESPACE}.${APPS_DOMAIN}"
KC_URL="https://${KEYCLOAK_HOST}"

ADMIN_USER=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
    -o jsonpath='{.data.username}' | base64 -d)
ADMIN_PASS=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
    -o jsonpath='{.data.password}' | base64 -d)

info "Obtaining admin token..."
ADMIN_TOKEN=$(curl -sk --connect-timeout 10 --max-time 30 -X POST \
    "${KC_URL}/realms/master/protocol/openid-connect/token" \
    -d "grant_type=password&client_id=admin-cli&username=${ADMIN_USER}&password=${ADMIN_PASS}" \
    | jq -r .access_token)
[[ "$ADMIN_TOKEN" != "null" && -n "$ADMIN_TOKEN" ]] \
    || error "Failed to obtain admin token"

info "Deleting existing KeycloakRealmImport CR..."
oc delete keycloakrealmimport fleetshift-realm -n "${NAMESPACE}" --ignore-not-found
oc wait --for=delete keycloakrealmimport/fleetshift-realm \
    -n "${NAMESPACE}" --timeout=60s 2>/dev/null || true

info "Deleting fleetshift realm from Keycloak..."
HTTP_CODE=$(curl -sk -o /dev/null -w '%{http_code}' -X DELETE \
    "${KC_URL}/admin/realms/fleetshift" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}")
case "$HTTP_CODE" in
    2*|404) info "Realm deleted." ;;
    *)      error "Failed to delete realm (HTTP ${HTTP_CODE})" ;;
esac

# --- Step 2: Wait for Keycloak to stabilise ---
info "Waiting for Keycloak to be ready..."
oc wait --for=condition=Ready keycloak/"${KEYCLOAK_CR_NAME}" \
    -n "${NAMESPACE}" --timeout=300s

# --- Step 3: Re-import realm with fresh passwords ---
OPS_PASSWORD=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16)
DEV_PASSWORD=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16)

info "Importing FleetShift realm..."
REALM_JSON=$(jq \
    --arg ops "$OPS_PASSWORD" \
    --arg dev "$DEV_PASSWORD" \
    '.users |= map(
        if .username == "ops-user" then .credentials[0].value = $ops
        elif .username == "dev-user" then .credentials[0].value = $dev
        else .
        end
    )' "${KEYCLOAK_DIR}/fleetshift-realm.json")

cat <<EOF | oc apply -n "${NAMESPACE}" -f -
{
  "apiVersion": "k8s.keycloak.org/v2alpha1",
  "kind": "KeycloakRealmImport",
  "metadata": {
    "name": "fleetshift-realm"
  },
  "spec": {
    "keycloakCRName": "${KEYCLOAK_CR_NAME}",
    "realm": ${REALM_JSON}
  }
}
EOF

info "Waiting for realm import to complete..."
oc wait --for=condition=Done keycloakrealmimport/fleetshift-realm \
    -n "${NAMESPACE}" --timeout=120s 2>/dev/null \
    || warn "Realm import may still be in progress. Check: oc get keycloakrealmimport -n ${NAMESPACE}"

# --- Step 4: Post-import configuration ---
info "Waiting for Keycloak to be ready after realm import..."
oc wait --for=condition=Ready keycloak/"${KEYCLOAK_CR_NAME}" \
    -n "${NAMESPACE}" --timeout=300s

info "Waiting for Keycloak API to be reachable..."
api_timeout=120
api_elapsed=0
while [[ $api_elapsed -lt $api_timeout ]]; do
    if curl -sk --connect-timeout 5 --max-time 10 \
        "${KC_URL}/realms/master" >/dev/null 2>&1; then
        break
    fi
    sleep 5
    api_elapsed=$((api_elapsed + 5))
done
[[ $api_elapsed -lt $api_timeout ]] || error "Keycloak API not reachable after ${api_timeout}s"

# Re-acquire admin token (original may have expired during realm import restart)
ADMIN_TOKEN=$(curl -sk --connect-timeout 10 --max-time 30 -X POST \
    "${KC_URL}/realms/master/protocol/openid-connect/token" \
    -d "grant_type=password&client_id=admin-cli&username=${ADMIN_USER}&password=${ADMIN_PASS}" \
    | jq -r .access_token)
[[ "$ADMIN_TOKEN" != "null" && -n "$ADMIN_TOKEN" ]] \
    || error "Failed to obtain admin token"

# Register github_username user profile attribute (not supported via realm import)
PROFILE_JSON=$(curl -sk --connect-timeout 10 --max-time 30 \
    "${KC_URL}/admin/realms/fleetshift/users/profile" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}")

if echo "$PROFILE_JSON" | jq -e '.attributes[] | select(.name == "github_username")' >/dev/null 2>&1; then
    info "github_username attribute already registered, skipping."
else
    info "Registering github_username in user profile..."
    UPDATED_PROFILE=$(echo "$PROFILE_JSON" | jq '.attributes += [{
        "name": "github_username",
        "displayName": "GitHub Username",
        "validations": {},
        "annotations": {},
        "permissions": {"view": ["admin", "user"], "edit": ["admin"]},
        "multivalued": false
    }]')
    HTTP_CODE=$(curl -sk --connect-timeout 10 --max-time 30 -o /dev/null -w '%{http_code}' -X PUT \
        "${KC_URL}/admin/realms/fleetshift/users/profile" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "$UPDATED_PROFILE")
    [[ "$HTTP_CODE" =~ ^2 ]] || error "Failed to update user profile (HTTP ${HTTP_CODE})"
    info "github_username attribute registered."
fi

# Regenerate ocp-console client secret
OCP_CONSOLE_UUID=$(curl -sk \
    "${KC_URL}/admin/realms/fleetshift/clients?clientId=ocp-console" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq -r '.[0].id')

if [[ "$OCP_CONSOLE_UUID" != "null" && -n "$OCP_CONSOLE_UUID" ]]; then
    curl -sk -o /dev/null -w '' -X POST \
        "${KC_URL}/admin/realms/fleetshift/clients/${OCP_CONSOLE_UUID}/client-secret" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}"

    CONSOLE_SECRET=$(curl -sk \
        "${KC_URL}/admin/realms/fleetshift/clients/${OCP_CONSOLE_UUID}/client-secret" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq -r .value)

    if oc get secret ocp-console-client-secret -n "${NAMESPACE}" >/dev/null 2>&1; then
        oc delete secret ocp-console-client-secret -n "${NAMESPACE}"
    fi
    oc create secret generic ocp-console-client-secret \
        -n "${NAMESPACE}" \
        --from-literal=clientSecret="${CONSOLE_SECRET}"
    info "ocp-console client secret regenerated."
else
    warn "ocp-console client not found in realm. Console OIDC will not be configured."
fi

# --- Summary ---
echo ""
echo "=========================================="
echo "  Realm Reset Complete"
echo "=========================================="
echo ""
echo "  URL:  https://${KEYCLOAK_HOST}"
echo ""
echo "  FleetShift Realm Users:"
echo "    ops-user / ${OPS_PASSWORD}"
echo "    dev-user / ${DEV_PASSWORD}"
echo ""
echo "  Personal dev accounts were removed by the reset."
echo "  Re-run 'task kc:add-user' to recreate them."
echo ""
echo "  Redirect URIs were removed by the reset."
echo "  Re-run 'task k8s:register-redirect' for OME."
echo ""
echo "=========================================="
