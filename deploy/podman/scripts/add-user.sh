#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# Add a user to the fleetshift realm with a specific GitHub username.
#
# Local usage (podman deployment):
#   ./add-user.sh --admin-password <from make up output> \
#     --username mshort@redhat.com --password mypass --github mshort55
#
# External usage (OpenShift Keycloak):
#   ./add-user.sh --username mshort@redhat.com --password mypass --github mshort55
#
# All flags can also be set via environment variables:
#   KC_ADMIN_URL, KC_ADMIN_USER, KC_ADMIN_PASSWORD,
#   KC_NEW_USERNAME, KC_NEW_PASSWORD, KC_NEW_GITHUB, KC_NEW_ROLES

REALM="fleetshift"

# Admin connection (auto-detected if not provided)
ADMIN_URL="${KC_ADMIN_URL:-}"
ADMIN_USER="${KC_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${KC_ADMIN_PASSWORD:-}"

# New user details
USERNAME="${KC_NEW_USERNAME:-}"
PASSWORD="${KC_NEW_PASSWORD:-}"
GITHUB="${KC_NEW_GITHUB:-}"
ROLES="${KC_NEW_ROLES:-}"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keycloak-url)     ADMIN_URL="$2"; shift 2 ;;
        --admin-user)       ADMIN_USER="$2"; shift 2 ;;
        --admin-password)   ADMIN_PASSWORD="$2"; shift 2 ;;
        --username)         USERNAME="$2"; shift 2 ;;
        --password)         PASSWORD="$2"; shift 2 ;;
        --github)           GITHUB="$2"; shift 2 ;;
        --roles)            ROLES="$2"; shift 2 ;;
        *) error "Unknown flag: $1" ;;
    esac
done

[[ -n "$USERNAME" ]] || error "Username required. Usage: ./add-user.sh --admin-password <pw> --username you@example.com --password pass --github ghuser"
[[ -n "$PASSWORD" ]] || error "Password required."
[[ -n "$GITHUB" ]]   || error "GitHub username required."

# ── Resolve Keycloak URL and admin credentials ───────────────────

if [[ -n "$ADMIN_URL" && -n "$ADMIN_PASSWORD" ]]; then
    # Explicit credentials provided — use directly
    KC_URL="$ADMIN_URL"
elif [[ -n "$ADMIN_PASSWORD" ]]; then
    # Password provided but no URL — use local deployment
    load_env
    KC_URL="http://${KC_HOSTNAME:-localhost}:${KC_HTTP_PORT:-8180}/auth"
else
    # No credentials — discover from OpenShift
    command -v oc &>/dev/null || error "'oc' CLI not found. For local usage, pass --admin-password."
    timeout 5 oc whoami &>/dev/null || error "Not logged in to OpenShift. Run 'oc login' first."

    OC_NAMESPACE="${OC_NAMESPACE:-keycloak-prod}"
    OC_CR_NAME="${OC_CR_NAME:-keycloak}"

    APPS_DOMAIN=$(oc get ingresses.config/cluster -o jsonpath='{.spec.domain}')
    KC_URL="https://${OC_CR_NAME}-${OC_NAMESPACE}.${APPS_DOMAIN}"

    ADMIN_USER=$(oc get secret "${OC_CR_NAME}-initial-admin" -n "${OC_NAMESPACE}" \
        -o jsonpath='{.data.username}' | base64 -d)
    ADMIN_PASSWORD=$(oc get secret "${OC_CR_NAME}-initial-admin" -n "${OC_NAMESPACE}" \
        -o jsonpath='{.data.password}' | base64 -d)
fi

command -v jq &>/dev/null || error "'jq' not found in PATH."

# ── Get admin token ──────────────────────────────────────────────

info "Obtaining admin token from ${KC_URL}..."
ADMIN_TOKEN=$(curl -sk -X POST \
    "${KC_URL}/realms/master/protocol/openid-connect/token" \
    -d "grant_type=password&client_id=admin-cli&username=${ADMIN_USER}&password=${ADMIN_PASSWORD}" \
    | jq -r .access_token)
[[ "$ADMIN_TOKEN" != "null" && -n "$ADMIN_TOKEN" ]] \
    || error "Failed to obtain admin token. Check Keycloak URL and admin credentials."

# ── Create user ──────────────────────────────────────────────────

ROLE_ARRAY="[]"
if [[ -n "$ROLES" ]]; then
    ROLE_ARRAY=$(echo "$ROLES" | tr ',' '\n' | jq -R . | jq -s .)
fi

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
         USER_ID=$(curl -sk \
             "${KC_URL}/admin/realms/${REALM}/users?username=${USERNAME}&exact=true" \
             -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq -r '.[0].id')
         [[ "$USER_ID" != "null" && -n "$USER_ID" ]] || error "Could not find user ID"

         curl -sk -o /dev/null -w '' -X PUT \
             "${KC_URL}/admin/realms/${REALM}/users/${USER_ID}" \
             -H "Authorization: Bearer ${ADMIN_TOKEN}" \
             -H "Content-Type: application/json" \
             -d "$(jq -n --arg github "$GITHUB" '{attributes: {github_username: [$github]}}')"

         curl -sk -o /dev/null -w '' -X PUT \
             "${KC_URL}/admin/realms/${REALM}/users/${USER_ID}/reset-password" \
             -H "Authorization: Bearer ${ADMIN_TOKEN}" \
             -H "Content-Type: application/json" \
             -d "$(jq -n --arg pass "$PASSWORD" '{type: "password", value: $pass, temporary: false}')"

         info "User updated."
         ;;
    *)  error "Failed to create user (HTTP ${HTTP_CODE})" ;;
esac

# ── Assign realm roles ───────────────────────────────────────────

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
echo "  Keycloak: ${KC_URL}/realms/fleetshift/account"
echo ""
