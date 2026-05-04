#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# Configure fleetctl CLI for the current deployment. Called by 'task podman:cli-setup'.
#
# Fetches OIDC discovery and writes auth config to ~/.config/fleetshift/auth.json.
# Env vars (OIDC_URL, OIDC_CLI_CLIENT_ID, etc.) are set by Taskfile.

ISSUER_URL="$OIDC_URL"
CLI_CLIENT_ID="${OIDC_CLI_CLIENT_ID:-fleetshift-cli}"
KEY_ENROLLMENT_CLIENT="${KEY_ENROLLMENT_CLIENT_ID:-fleetshift-signing}"

DISCOVERY_URL="${ISSUER_URL}/.well-known/openid-configuration"

echo "==> Fetching OIDC discovery from ${DISCOVERY_URL}"
DISCOVERY=$(curl -sf "$DISCOVERY_URL") || {
  echo "ERROR: Could not reach OIDC provider at ${DISCOVERY_URL}" >&2
  echo "Is the stack running? Try 'task podman:up' first." >&2
  exit 1
}

AUTH_ENDPOINT=$(echo "$DISCOVERY" | jq -r '.authorization_endpoint')
TOKEN_ENDPOINT=$(echo "$DISCOVERY" | jq -r '.token_endpoint')

CONFIG_DIR="${HOME}/.config/fleetshift"
CONFIG_FILE="${CONFIG_DIR}/auth.json"

mkdir -p "$CONFIG_DIR"

jq -n \
  --arg issuer "$ISSUER_URL" \
  --arg client "$CLI_CLIENT_ID" \
  --arg auth "$AUTH_ENDPOINT" \
  --arg token "$TOKEN_ENDPOINT" \
  --arg enrollment_client "$KEY_ENROLLMENT_CLIENT" \
  '{
    issuer_url: $issuer,
    client_id: $client,
    scopes: ["openid", "profile", "email"],
    authorization_endpoint: $auth,
    token_endpoint: $token,
    key_enrollment_client_id: $enrollment_client
  }' > "$CONFIG_FILE"

echo "CLI config written to ${CONFIG_FILE}"
echo "Run 'fleetctl auth login' to authenticate."
