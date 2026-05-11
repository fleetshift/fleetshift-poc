#!/usr/bin/env bash
# Fallback Stage 3b: Test the Workload IdF (SA) token against the gateway.
#
# Same test as 03-test-gateway.sh but with the SA-derived token.
#
# Reads: tmp/workload_sts_token.txt (from 02c)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GATEWAY_URL

log_header "Fallback Stage 3b: Gateway Test (Workload IdF / SA Token)"

log_warn "Testing with SA-derived token — if this works, auth is proven but"
log_warn "zero-trust is violated (created_by = SA email, not user email)."
echo ""

# --- Load token ---
WL_TOKEN_FILE="${TMP_DIR}/workload_sts_token.txt"
if [[ ! -f "${WL_TOKEN_FILE}" ]]; then
    log_fail "Workload token not found. Run 02c-workload-sts-exchange.sh first."
    exit 1
fi
WL_TOKEN=$(cat "${WL_TOKEN_FILE}")
log_ok "Loaded workload SA token"

# --- Auth test ---
log_step "Auth test — GET /api/v1/clusters with SA-derived Bearer token"

AUTH_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -H "Authorization: Bearer ${WL_TOKEN}" \
    "${GATEWAY_URL}/api/v1/clusters")
AUTH_CODE=$(echo "${AUTH_RESPONSE}" | tail -1)
AUTH_BODY=$(echo "${AUTH_RESPONSE}" | sed '$d')

save_response "gateway_workload_test" "${AUTH_CODE}" "${AUTH_BODY}"

echo ""
log_step "HTTP Status: ${AUTH_CODE}"
log_step "Response body:"
echo "${AUTH_BODY}" | jq . 2>/dev/null || echo "${AUTH_BODY}"

echo ""
if [[ "${AUTH_CODE}" -ge 200 && "${AUTH_CODE}" -lt 300 ]]; then
    log_ok "AUTH SUCCESS — Gateway accepted the SA-derived token"
    echo ""
    log_warn "╔══════════════════════════════════════════════════════════════╗"
    log_warn "║  FINDING: SA token works, but Workforce IdF token did not.  ║"
    log_warn "║                                                            ║"
    log_warn "║  This means:                                               ║"
    log_warn "║  • The gateway accepts Google-signed ID tokens (JWTs)      ║"
    log_warn "║  • But NOT the token format that Workforce IdF produces    ║"
    log_warn "║  • Using SA tokens violates zero-trust (no per-user ID)    ║"
    log_warn "║                                                            ║"
    log_warn "║  ACTION: Take to CLS team — ask if they can:               ║"
    log_warn "║  1. Accept workforce federation tokens at the gateway, or  ║"
    log_warn "║  2. Expose an auth mechanism that preserves user identity   ║"
    log_warn "╚══════════════════════════════════════════════════════════════╝"

elif [[ "${AUTH_CODE}" -eq 401 ]]; then
    log_fail "AUTH FAILED — SA-derived token also rejected (HTTP 401)"
    echo ""
    log_fail "Neither Workforce IdF NOR Workload IdF tokens work with this gateway."
    log_fail "This is a fundamental compatibility issue."
    log_fail "ACTION: Escalate to CLS team with diagnostic data from tmp/ directory."

else
    log_fail "Unexpected HTTP ${AUTH_CODE}"
fi

echo ""
log_ok "Fallback stage 3b complete"
