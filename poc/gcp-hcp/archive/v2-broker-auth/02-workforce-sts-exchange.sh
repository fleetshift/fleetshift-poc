#!/usr/bin/env bash
# Stage 2: Exchange Keycloak JWT for a Google access token via Workforce Identity Federation.
#
# The resulting token is an opaque Google access token (ya29...), NOT a JWT.
# This is expected — we use it in stage 3 to mint a broker ID token.
#
# Reads:  tmp/keycloak_token.jwt  (from stage 1)
# Writes: tmp/workforce_access_token.txt
#         tmp/user_email.txt

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config WORKFORCE_POOL WORKFORCE_PROVIDER

log_header "Stage 2: Workforce Identity Federation STS Exchange"

# --- Load Keycloak token ---
KC_TOKEN_FILE="${TMP_DIR}/keycloak_token.jwt"
if [[ ! -f "${KC_TOKEN_FILE}" ]]; then
    log_fail "Keycloak token not found at ${KC_TOKEN_FILE}"
    log_fail "Run 01-get-keycloak-token.sh first"
    exit 1
fi
KC_TOKEN=$(cat "${KC_TOKEN_FILE}")
log_ok "Loaded Keycloak token from tmp/keycloak_token.jwt"

# --- Extract user email from Keycloak JWT ---
if ! is_jwt "${KC_TOKEN}"; then
    log_fail "Keycloak token is not a JWT — cannot extract email"
    exit 1
fi

KC_EMAIL=$(decode_jwt "${KC_TOKEN}" | jq -r '.email // empty')
if [[ -z "${KC_EMAIL}" ]]; then
    log_fail "No email claim in Keycloak token."
    log_fail "Add an email mapper to the Keycloak client (see prerequisites.md section 4)."
    exit 1
fi
echo -n "${KC_EMAIL}" > "${TMP_DIR}/user_email.txt"
log_ok "User email: ${KC_EMAIL} (saved to tmp/user_email.txt)"

# --- Build STS audience ---
STS_AUDIENCE="//iam.googleapis.com/locations/global/workforcePools/${WORKFORCE_POOL}/providers/${WORKFORCE_PROVIDER}"
log_step "STS audience: ${STS_AUDIENCE}"

# --- Call STS token exchange ---
log_step "Calling GCP STS token exchange endpoint..."

STS_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X POST "https://sts.googleapis.com/v1/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=urn:ietf:params:oauth:grant-type:token-exchange" \
    -d "audience=${STS_AUDIENCE}" \
    -d "requested_token_type=urn:ietf:params:oauth:token-type:access_token" \
    -d "scope=https://www.googleapis.com/auth/cloud-platform" \
    -d "subject_token_type=urn:ietf:params:oauth:token-type:jwt" \
    -d "subject_token=${KC_TOKEN}")

STS_HTTP_CODE=$(echo "${STS_RESPONSE}" | tail -1)
STS_BODY=$(echo "${STS_RESPONSE}" | sed '$d')

save_response "workforce_sts_exchange" "${STS_HTTP_CODE}" "${STS_BODY}"

# --- Check for HTTP error ---
if [[ "${STS_HTTP_CODE}" -ge 400 ]]; then
    log_fail "STS returned HTTP ${STS_HTTP_CODE}"
    echo "${STS_BODY}" | jq . 2>/dev/null || echo "${STS_BODY}"

    echo ""
    log_warn "Troubleshooting checklist:"
    echo "  1. Does the Keycloak issuer URI match the provider config?"
    echo "     Check: gcloud iam workforce-pools providers describe ${WORKFORCE_PROVIDER} \\"
    echo "       --location=global --workforce-pool=${WORKFORCE_POOL}"
    echo "  2. Can GCP reach the Keycloak JWKS endpoint?"
    KC_ISS=$(decode_jwt "${KC_TOKEN}" 2>/dev/null | jq -r '.iss // "unknown"')
    echo "     JWKS URL: ${KC_ISS}/protocol/openid-connect/certs"
    echo "  3. Is the Keycloak token expired?"
    KC_EXP=$(decode_jwt "${KC_TOKEN}" 2>/dev/null | jq -r '.exp // "unknown"')
    echo "     Token exp: ${KC_EXP} (now: $(date +%s))"
    exit 1
fi

# --- Extract and inspect the STS token ---
STS_TOKEN=$(echo "${STS_BODY}" | jq -r '.access_token')
STS_TOKEN_TYPE=$(echo "${STS_BODY}" | jq -r '.token_type')
STS_EXPIRES_IN=$(echo "${STS_BODY}" | jq -r '.expires_in // "not specified"')

log_ok "STS exchange succeeded (HTTP ${STS_HTTP_CODE})"

echo ""
log_step "Response metadata:"
echo "  token_type:  ${STS_TOKEN_TYPE}"
echo "  expires_in:  ${STS_EXPIRES_IN}"
echo "  user_email:  ${KC_EMAIL}"

echo ""
log_step "Token format analysis:"

if [[ "${STS_TOKEN}" == ya29.* ]]; then
    log_ok "Token is an opaque Google access token (ya29...) — this is expected"
    echo "  Token prefix: $(echo "${STS_TOKEN}" | head -c 20)..."
    echo "  Token length: ${#STS_TOKEN} chars"
elif is_jwt "${STS_TOKEN}"; then
    log_warn "Token is a JWT — unexpected but potentially usable"
    decode_jwt "${STS_TOKEN}"
else
    log_warn "Token format is unrecognized"
    echo "  Token prefix: $(echo "${STS_TOKEN}" | head -c 50)..."
fi

# Save token for stage 3
echo -n "${STS_TOKEN}" > "${TMP_DIR}/workforce_access_token.txt"
log_ok "Token saved to tmp/workforce_access_token.txt"

echo ""
log_ok "Stage 2 complete"
log_step "Next: run 03-generate-broker-idtoken.sh"
