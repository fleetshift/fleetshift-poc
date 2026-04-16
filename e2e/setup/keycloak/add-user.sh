#!/usr/bin/env bash
set -euo pipefail

# Add a user to the fleetshift realm with a specific GitHub username.
#
# Usage:
#   ./add-user.sh --username mshort@redhat.com --password mypass --github mshort55
#   ./add-user.sh --username mshort@redhat.com --password mypass --github mshort55 --roles ops,dev
#
# All flags can also be set via environment variables:
#   KC_NEW_USERNAME, KC_NEW_PASSWORD, KC_NEW_GITHUB, KC_NEW_ROLES

NAMESPACE="keycloak-prod"
KEYCLOAK_CR_NAME="keycloak"
REALM="fleetshift"

USERNAME="${KC_NEW_USERNAME:-}"
PASSWORD="${KC_NEW_PASSWORD:-}"
GITHUB="${KC_NEW_GITHUB:-}"
ROLES="${KC_NEW_ROLES:-}"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --username) USERNAME="$2"; shift 2 ;;
        --password) PASSWORD="$2"; shift 2 ;;
        --github)   GITHUB="$2"; shift 2 ;;
        --roles)    ROLES="$2"; shift 2 ;;
        *) error "Unknown flag: $1" ;;
    esac
done

[[ -n "$USERNAME" ]] || error "Username required. Usage: ./add-user.sh --username you@example.com --password pass --github ghuser"
[[ -n "$PASSWORD" ]] || error "Password required."
[[ -n "$GITHUB" ]]   || error "GitHub username required."

command -v oc &>/dev/null || error "'oc' CLI not found in PATH."
command -v jq &>/dev/null || error "'jq' not found in PATH."
timeout 5 oc whoami &>/dev/null || error "Not logged in to OpenShift. Run 'oc login' first."

# Determine Keycloak URL
APPS_DOMAIN=$(oc get ingresses.config/cluster -o jsonpath='{.spec.domain}')
KEYCLOAK_HOST="${KEYCLOAK_CR_NAME}-${NAMESPACE}.${APPS_DOMAIN}"
KC_URL="https://${KEYCLOAK_HOST}"

# Get admin credentials and token
ADMIN_USER=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
    -o jsonpath='{.data.username}' | base64 -d)
ADMIN_PASS=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
    -o jsonpath='{.data.password}' | base64 -d)

info "Obtaining admin token..."
ADMIN_TOKEN=$(curl -sk -X POST \
    "${KC_URL}/realms/master/protocol/openid-connect/token" \
    -d "grant_type=password&client_id=admin-cli&username=${ADMIN_USER}&password=${ADMIN_PASS}" \
    | jq -r .access_token)
[[ "$ADMIN_TOKEN" != "null" && -n "$ADMIN_TOKEN" ]] \
    || error "Failed to obtain admin token"

# Build role list
ROLE_ARRAY="[]"
if [[ -n "$ROLES" ]]; then
    ROLE_ARRAY=$(echo "$ROLES" | tr ',' '\n' | jq -R . | jq -s .)
fi

# Create user
info "Creating user '${USERNAME}' (github: ${GITHUB})..."
USER_JSON=$(jq -n \
    --arg user "$USERNAME" \
    --arg pass "$PASSWORD" \
    --arg github "$GITHUB" \
    --argjson roles "$ROLE_ARRAY" \
    '{
        username: $user,
        email: $user,
        enabled: true,
        emailVerified: true,
        credentials: [{type: "password", value: $pass, temporary: false}],
        attributes: {github_username: [$github]},
        realmRoles: $roles
    }')

HTTP_CODE=$(curl -sk -o /dev/null -w '%{http_code}' -X POST \
    "${KC_URL}/admin/realms/${REALM}/users" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$USER_JSON")

case "$HTTP_CODE" in
    2*) info "User created successfully." ;;
    409) info "User '${USERNAME}' already exists. Updating password and attributes..."
         # Get user ID
         USER_ID=$(curl -sk \
             "${KC_URL}/admin/realms/${REALM}/users?username=${USERNAME}&exact=true" \
             -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq -r '.[0].id')
         [[ "$USER_ID" != "null" && -n "$USER_ID" ]] || error "Could not find user ID"

         # Update attributes
         curl -sk -o /dev/null -w '' -X PUT \
             "${KC_URL}/admin/realms/${REALM}/users/${USER_ID}" \
             -H "Authorization: Bearer ${ADMIN_TOKEN}" \
             -H "Content-Type: application/json" \
             -d "$(jq -n --arg github "$GITHUB" '{attributes: {github_username: [$github]}}')"

         # Reset password
         curl -sk -o /dev/null -w '' -X PUT \
             "${KC_URL}/admin/realms/${REALM}/users/${USER_ID}/reset-password" \
             -H "Authorization: Bearer ${ADMIN_TOKEN}" \
             -H "Content-Type: application/json" \
             -d "$(jq -n --arg pass "$PASSWORD" '{type: "password", value: $pass, temporary: false}')"

         info "User updated."
         ;;
    *)  error "Failed to create user (HTTP ${HTTP_CODE})" ;;
esac

# Assign realm roles if specified
if [[ -n "$ROLES" ]]; then
    USER_ID="${USER_ID:-$(curl -sk \
        "${KC_URL}/admin/realms/${REALM}/users?username=${USERNAME}&exact=true" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq -r '.[0].id')}"

    IFS=',' read -ra ROLE_LIST <<< "$ROLES"
    for role in "${ROLE_LIST[@]}"; do
        ROLE_JSON=$(curl -sk \
            "${KC_URL}/admin/realms/${REALM}/roles/${role}" \
            -H "Authorization: Bearer ${ADMIN_TOKEN}" 2>/dev/null)
        if [[ $(echo "$ROLE_JSON" | jq -r '.name // empty') == "$role" ]]; then
            curl -sk -o /dev/null -X POST \
                "${KC_URL}/admin/realms/${REALM}/users/${USER_ID}/role-mappings/realm" \
                -H "Authorization: Bearer ${ADMIN_TOKEN}" \
                -H "Content-Type: application/json" \
                -d "[${ROLE_JSON}]"
            info "Assigned role '${role}'."
        else
            echo -e "${RED}[WARN]${NC} Role '${role}' not found, skipping."
        fi
    done
fi

echo ""
echo "  User:     ${USERNAME}"
echo "  Password: ${PASSWORD}"
echo "  GitHub:   ${GITHUB}"
echo "  Roles:    ${ROLES:-none}"
echo "  Login:    ${KC_URL}/realms/fleetshift/account"
echo ""
