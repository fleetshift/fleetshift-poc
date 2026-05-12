#!/usr/bin/env bash
# Stage 2: Exchange the Keycloak JWT for a Google token via GCP STS.
#
# THIS IS THE MAKE-OR-BREAK EXPERIMENT.
#
# Key question: does the STS return a JWT (which the gateway can validate)
# or an opaque access token (which it can't)?
#
# Reads:  tmp/keycloak_token.jwt  (from stage 1)
# Writes: tmp/sts_token.txt       (for stage 3)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config WORKFORCE_POOL WORKFORCE_PROVIDER GATEWAY_AUDIENCE

log_header "Stage 2: GCP STS Token Exchange"

# --- Load Keycloak token ---
KC_TOKEN_FILE="${TMP_DIR}/keycloak_token.jwt"
if [[ ! -f "${KC_TOKEN_FILE}" ]]; then
    log_fail "Keycloak token not found at ${KC_TOKEN_FILE}"
    log_fail "Run 01-get-keycloak-token.sh first"
    exit 1
fi
KC_TOKEN=$(cat "${KC_TOKEN_FILE}")
log_ok "Loaded Keycloak token from tmp/keycloak_token.jwt"

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
    -d "scope=openid email" \
    -d "subject_token_type=urn:ietf:params:oauth:token-type:jwt" \
    -d "subject_token=${KC_TOKEN}")

STS_HTTP_CODE=$(echo "${STS_RESPONSE}" | tail -1)
STS_BODY=$(echo "${STS_RESPONSE}" | sed '$d')

save_response "sts_exchange" "${STS_HTTP_CODE}" "${STS_BODY}"

# --- Check for HTTP error ---
if [[ "${STS_HTTP_CODE}" -ge 400 ]]; then
    log_fail "STS returned HTTP ${STS_HTTP_CODE}"
    echo "${STS_BODY}" | jq . 2>/dev/null || echo "${STS_BODY}"

    echo ""
    log_warn "Troubleshooting checklist:"
    echo "  1. Does the Keycloak issuer URI match the provider config?"
    echo "     Check: gcloud iam workforce-pools providers describe ${WORKFORCE_PROVIDER} ..."
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

echo ""
log_step "Token format analysis:"

if is_jwt "${STS_TOKEN}"; then
    log_ok "STS token IS a JWT (3 dot-separated segments)"
    echo ""
    log_step "JWT claims:"
    decode_jwt "${STS_TOKEN}"

    # Quick summary of key fields
    STS_ISS=$(decode_jwt "${STS_TOKEN}" | jq -r '.iss // "missing"')
    STS_AUD=$(decode_jwt "${STS_TOKEN}" | jq -r '.aud // "missing"')
    STS_SUB=$(decode_jwt "${STS_TOKEN}" | jq -r '.sub // "missing"')
    STS_EMAIL=$(decode_jwt "${STS_TOKEN}" | jq -r '.email // "missing"')
    STS_EXP=$(decode_jwt "${STS_TOKEN}" | jq -r '.exp // "missing"')

    echo ""
    log_step "Quick summary:"
    echo "  iss:   ${STS_ISS}"
    echo "  aud:   ${STS_AUD}"
    echo "  sub:   ${STS_SUB}"
    echo "  email: ${STS_EMAIL}"
    echo "  exp:   ${STS_EXP}"

    echo ""
    log_step "Gateway compatibility check:"
    echo "  Gateway expects JWKS from: https://www.googleapis.com/oauth2/v3/certs"
    echo "  Gateway expects audience:  ${GATEWAY_AUDIENCE}"
    echo "  Token issuer:              ${STS_ISS}"
    echo "  Token audience:            ${STS_AUD}"

    if [[ "${STS_AUD}" == "${GATEWAY_AUDIENCE}" ]]; then
        log_ok "Audience matches gateway config"
    else
        log_warn "Audience MISMATCH — gateway expects '${GATEWAY_AUDIENCE}', token has '${STS_AUD}'"
        log_warn "Stage 3 will likely fail with 401. You may need to adjust GATEWAY_AUDIENCE in config.env."
    fi
else
    log_warn "STS token is NOT a JWT (opaque access token)"
    echo ""
    echo "  Token prefix: $(echo "${STS_TOKEN}" | head -c 50)..."
    echo "  Token length: ${#STS_TOKEN} chars"
    echo ""
    log_warn "IMPLICATION: The gateway validates JWTs via JWKS. An opaque access token"
    log_warn "will NOT pass JWT validation. Workforce IdF may not work with this gateway."
    log_warn ""
    log_warn "Options:"
    log_warn "  1. Try stage 3 anyway — maybe the gateway accepts access tokens too"
    log_warn "  2. Run the Workload IdF fallback (02b/02c) to test SA-based tokens"
    log_warn "  3. Take this finding to the CLS team"
fi

# Save token for stage 3
echo -n "${STS_TOKEN}" > "${TMP_DIR}/sts_token.txt"
log_ok "Token saved to tmp/sts_token.txt"

echo ""
log_ok "Stage 2 complete"
log_step "Next: run 03-test-gateway.sh"
