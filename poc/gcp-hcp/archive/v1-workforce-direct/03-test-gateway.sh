#!/usr/bin/env bash
# Stage 3: Test the STS token against the live GCP HCP API Gateway.
#
# Hits /health (no auth) to verify connectivity, then /api/v1/clusters (auth required)
# with the token from stage 2.
#
# Reads:  tmp/sts_token.txt  (from stage 2)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GATEWAY_URL

log_header "Stage 3: API Gateway Token Test"

# --- Load STS token ---
STS_TOKEN_FILE="${TMP_DIR}/sts_token.txt"
if [[ ! -f "${STS_TOKEN_FILE}" ]]; then
    log_fail "STS token not found at ${STS_TOKEN_FILE}"
    log_fail "Run 02-sts-exchange.sh first"
    exit 1
fi
STS_TOKEN=$(cat "${STS_TOKEN_FILE}")
log_ok "Loaded STS token from tmp/sts_token.txt"

# --- Test 1: Connectivity (no auth) ---
log_step "Test 1: Connectivity check — GET /health (no auth)"

HEALTH_RESPONSE=$(curl -s -w "\n%{http_code}" "${GATEWAY_URL}/health")
HEALTH_CODE=$(echo "${HEALTH_RESPONSE}" | tail -1)
HEALTH_BODY=$(echo "${HEALTH_RESPONSE}" | sed '$d')

save_response "gateway_health" "${HEALTH_CODE}" "${HEALTH_BODY}"

if [[ "${HEALTH_CODE}" -ge 200 && "${HEALTH_CODE}" -lt 300 ]]; then
    log_ok "Gateway reachable (HTTP ${HEALTH_CODE})"
    echo "${HEALTH_BODY}" | jq . 2>/dev/null || echo "${HEALTH_BODY}"
else
    log_fail "Gateway health check failed (HTTP ${HEALTH_CODE})"
    echo "${HEALTH_BODY}" | jq . 2>/dev/null || echo "${HEALTH_BODY}"
    log_warn "The gateway may be down or the URL may be wrong."
    log_warn "Configured URL: ${GATEWAY_URL}"
    log_warn "Continuing with auth test anyway..."
fi

# --- Test 2: Authenticated request ---
echo ""
log_step "Test 2: Auth test — GET /api/v1/clusters with Bearer token"

AUTH_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -H "Authorization: Bearer ${STS_TOKEN}" \
    "${GATEWAY_URL}/api/v1/clusters")
AUTH_CODE=$(echo "${AUTH_RESPONSE}" | tail -1)
AUTH_BODY=$(echo "${AUTH_RESPONSE}" | sed '$d')

save_response "gateway_auth_test" "${AUTH_CODE}" "${AUTH_BODY}"

echo ""
log_step "HTTP Status: ${AUTH_CODE}"
log_step "Response body:"
echo "${AUTH_BODY}" | jq . 2>/dev/null || echo "${AUTH_BODY}"

echo ""
if [[ "${AUTH_CODE}" -ge 200 && "${AUTH_CODE}" -lt 300 ]]; then
    log_ok "AUTH SUCCESS — Gateway accepted the STS token!"
    log_ok "Workforce Identity Federation works with this gateway."
    echo ""
    log_step "Next: run 04-crud-lifecycle.sh for full lifecycle test"

elif [[ "${AUTH_CODE}" -eq 401 ]]; then
    log_fail "AUTH FAILED — 401 Unauthorized"
    echo ""
    log_step "Diagnosis:"
    # Try to extract error details
    ERROR_MSG=$(echo "${AUTH_BODY}" | jq -r '.message // .error_description // .error // "no details"' 2>/dev/null)
    echo "  Error: ${ERROR_MSG}"
    echo ""
    log_step "Possible causes:"
    echo "  1. Token format: gateway expects JWT, STS may have returned opaque token"
    echo "  2. Audience mismatch: token aud ≠ gateway's configured oauth2.clientId"
    echo "  3. Issuer not recognized: token issuer not in gateway's JWKS config"
    echo "  4. Signature validation: token not signed by keys at googleapis.com/oauth2/v3/certs"
    echo ""
    log_warn "If Workforce IdF token was rejected, consider running the Workload IdF fallback:"
    log_warn "  ./02b-workload-idf-setup.sh && ./02c-workload-sts-exchange.sh && ./03b-test-gateway-workload.sh"

elif [[ "${AUTH_CODE}" -eq 403 ]]; then
    log_warn "AUTH PARTIAL — 403 Forbidden"
    log_warn "Token was accepted (authenticated) but the user lacks permission."
    log_warn "This might mean the federated identity needs IAM roles, or the"
    log_warn "gateway has additional authorization rules."
    echo ""
    log_step "Next: investigate what permissions the federated identity needs."

else
    log_fail "Unexpected HTTP ${AUTH_CODE}"
    echo ""
    log_step "Full response saved to tmp/gateway_auth_test.body.json"
fi
