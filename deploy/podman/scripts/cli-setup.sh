#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

load_env

ISSUER_URL="http://${KC_HOSTNAME:-localhost}:${KC_HTTP_PORT:-8180}/auth/realms/fleetshift"
DISCOVERY_URL="${ISSUER_URL}/.well-known/openid-configuration"

echo "==> Fetching OIDC discovery from ${DISCOVERY_URL}"
DISCOVERY=$(curl -sf "$DISCOVERY_URL") || {
  echo "ERROR: Could not reach Keycloak at ${DISCOVERY_URL}" >&2
  echo "Is the stack running? Try 'make up' first." >&2
  exit 1
}

AUTH_ENDPOINT=$(echo "$DISCOVERY" | jq -r '.authorization_endpoint')
TOKEN_ENDPOINT=$(echo "$DISCOVERY" | jq -r '.token_endpoint')

CONFIG_DIR="${HOME}/.config/fleetshift"
CONFIG_FILE="${CONFIG_DIR}/auth.json"

mkdir -p "$CONFIG_DIR"

jq -n \
  --arg issuer "$ISSUER_URL" \
  --arg client "fleetshift-cli" \
  --arg auth "$AUTH_ENDPOINT" \
  --arg token "$TOKEN_ENDPOINT" \
  '{
    issuer_url: $issuer,
    client_id: $client,
    scopes: ["openid", "profile", "email"],
    authorization_endpoint: $auth,
    token_endpoint: $token,
    key_enrollment_client_id: "fleetshift-signing"
  }' > "$CONFIG_FILE"

echo "CLI config written to ${CONFIG_FILE}"
echo "Run 'fleetctl auth login' to authenticate."
