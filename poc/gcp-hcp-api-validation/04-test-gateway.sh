#!/usr/bin/env bash
# Stage 4: Test the broker ID token against the live GCP HCP API Gateway.
#
# Runs three tests:
#   1. Connectivity check (no auth) — GET /health
#   2. JWT only, no X-User-Email (expect AUTH_REQUIRED)
#   3. JWT + X-User-Email (expect 200)
#
# Reads: tmp/broker_idtoken.jwt  (from stage 3)
#        tmp/user_email.txt      (from stage 2)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GATEWAY_URL

log_header "Stage 4: API Gateway Auth Test"

# --- Load broker token and user email ---
BROKER_TOKEN_FILE="${TMP_DIR}/broker_idtoken.jwt"
EMAIL_FILE="${TMP_DIR}/user_email.txt"

if [[ ! -f "${BROKER_TOKEN_FILE}" ]]; then
    log_fail "Broker ID token not found. Run scripts 01-03 first."
    exit 1
fi
if [[ ! -f "${EMAIL_FILE}" ]]; then
    log_fail "User email not found. Run scripts 01-02 first."
    exit 1
fi

BROKER_TOKEN=$(cat "${BROKER_TOKEN_FILE}")
USER_EMAIL=$(cat "${EMAIL_FILE}")
log_ok "Loaded broker token and user email (${USER_EMAIL})"

PASS_COUNT=0
FAIL_COUNT=0

# --- Test 1: Connectivity (no auth) ---
echo ""
log_step "Test 1: Connectivity — GET /health (no auth)"

HEALTH_RESPONSE=$(curl -s -w "\n%{http_code}" "${GATEWAY_URL}/health")
HEALTH_CODE=$(echo "${HEALTH_RESPONSE}" | tail -1)
HEALTH_BODY=$(echo "${HEALTH_RESPONSE}" | sed '$d')
save_response "gateway_health" "${HEALTH_CODE}" "${HEALTH_BODY}"

if [[ "${HEALTH_CODE}" -ge 200 && "${HEALTH_CODE}" -lt 300 ]]; then
    log_ok "PASS — Gateway reachable (HTTP ${HEALTH_CODE})"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    log_fail "FAIL — Gateway health check failed (HTTP ${HEALTH_CODE})"
    echo "${HEALTH_BODY}" | jq . 2>/dev/null || echo "${HEALTH_BODY}"
    log_warn "Gateway may be down. Continuing with auth tests anyway..."
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# --- Test 2: JWT only, no X-User-Email (expect AUTH_REQUIRED) ---
echo ""
log_step "Test 2: JWT only (no X-User-Email) — expect AUTH_REQUIRED"

JWT_ONLY_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -H "Authorization: Bearer ${BROKER_TOKEN}" \
    "${GATEWAY_URL}/api/v1/clusters")
JWT_ONLY_CODE=$(echo "${JWT_ONLY_RESPONSE}" | tail -1)
JWT_ONLY_BODY=$(echo "${JWT_ONLY_RESPONSE}" | sed '$d')
save_response "gateway_jwt_only" "${JWT_ONLY_CODE}" "${JWT_ONLY_BODY}"

JWT_ONLY_ERROR=$(echo "${JWT_ONLY_BODY}" | jq -r '.code // .message // "unknown"' 2>/dev/null)

if [[ "${JWT_ONLY_CODE}" -eq 401 ]]; then
    log_ok "PASS — Backend returned 401 without X-User-Email (expected: ${JWT_ONLY_ERROR})"
    PASS_COUNT=$((PASS_COUNT + 1))
elif [[ "${JWT_ONLY_CODE}" -ge 200 && "${JWT_ONLY_CODE}" -lt 300 ]]; then
    log_warn "UNEXPECTED — Backend accepted request without X-User-Email (HTTP ${JWT_ONLY_CODE})"
    log_warn "This means X-User-Email may not be required — investigate."
    FAIL_COUNT=$((FAIL_COUNT + 1))
else
    log_warn "UNEXPECTED — Got HTTP ${JWT_ONLY_CODE} instead of 401"
    echo "${JWT_ONLY_BODY}" | jq . 2>/dev/null || echo "${JWT_ONLY_BODY}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# --- Test 3: JWT + X-User-Email (expect 200) ---
echo ""
log_step "Test 3: JWT + X-User-Email — expect 200"

FULL_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -H "Authorization: Bearer ${BROKER_TOKEN}" \
    -H "X-User-Email: ${USER_EMAIL}" \
    "${GATEWAY_URL}/api/v1/clusters")
FULL_CODE=$(echo "${FULL_RESPONSE}" | tail -1)
FULL_BODY=$(echo "${FULL_RESPONSE}" | sed '$d')
save_response "gateway_full_test" "${FULL_CODE}" "${FULL_BODY}"

if [[ "${FULL_CODE}" -ge 200 && "${FULL_CODE}" -lt 300 ]]; then
    log_ok "PASS — Broker JWT + X-User-Email accepted (HTTP ${FULL_CODE})"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    log_fail "FAIL — Full auth rejected (HTTP ${FULL_CODE})"
    echo "${FULL_BODY}" | jq . 2>/dev/null || echo "${FULL_BODY}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# --- Summary ---
echo ""
log_header "Test Results"
echo "  Test 1 (connectivity):    $([ "${HEALTH_CODE}" -ge 200 ] && [ "${HEALTH_CODE}" -lt 300 ] && echo "PASS" || echo "FAIL") (HTTP ${HEALTH_CODE})"
echo "  Test 2 (JWT only):        $([ "${JWT_ONLY_CODE}" -eq 401 ] && echo "PASS" || echo "FAIL") (HTTP ${JWT_ONLY_CODE})"
echo "  Test 3 (JWT + email):     $([ "${FULL_CODE}" -ge 200 ] && [ "${FULL_CODE}" -lt 300 ] && echo "PASS" || echo "FAIL") (HTTP ${FULL_CODE})"
echo ""
echo "  Passed: ${PASS_COUNT}/3"
echo "  Failed: ${FAIL_COUNT}/3"

echo ""
if [[ "${PASS_COUNT}" -eq 3 ]]; then
    log_ok "All tests passed — broker auth flow is proven end-to-end"
    log_step "Next: run 05-crud-lifecycle.sh"
else
    log_warn "Some tests failed — review the output above."
fi
