#!/usr/bin/env bash
# Fallback Stage 2c: Exchange Keycloak JWT via Workload IdF + SA impersonation.
#
# 1. Exchange Keycloak JWT → STS federated access token
# 2. Use access token to impersonate SA → generate ID token
# 3. The ID token is a Google-signed JWT with the SA's identity
#
# Reads:  tmp/keycloak_token.jwt, tmp/workload_config.env
# Writes: tmp/workload_sts_token.txt

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GCP_PROJECT GATEWAY_AUDIENCE

# Load workload config from 02b
if [[ -f "${TMP_DIR}/workload_config.env" ]]; then
    source "${TMP_DIR}/workload_config.env"
else
    load_config WORKLOAD_POOL WORKLOAD_PROVIDER WORKLOAD_SA_EMAIL
fi

log_header "Fallback Stage 2c: Workload IdF Token Exchange"

log_warn "This path uses SA impersonation — identity will be the SA, not the user."
echo ""

# --- Load Keycloak token ---
KC_TOKEN_FILE="${TMP_DIR}/keycloak_token.jwt"
if [[ ! -f "${KC_TOKEN_FILE}" ]]; then
    log_fail "Keycloak token not found. Run 01-get-keycloak-token.sh first."
    exit 1
fi
KC_TOKEN=$(cat "${KC_TOKEN_FILE}")
log_ok "Loaded Keycloak token"

# --- Step 1: STS exchange for federated access token ---
log_step "Step 1: Exchange Keycloak JWT for federated access token via STS"

PROJECT_NUMBER=$(gcloud projects describe "${GCP_PROJECT}" --format='value(projectNumber)')
WL_STS_AUDIENCE="//iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${WORKLOAD_POOL}/providers/${WORKLOAD_PROVIDER}"

STS_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X POST "https://sts.googleapis.com/v1/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=urn:ietf:params:oauth:token-type:token-exchange" \
    -d "audience=${WL_STS_AUDIENCE}" \
    -d "requested_token_type=urn:ietf:params:oauth:token-type:access_token" \
    -d "scope=https://www.googleapis.com/auth/cloud-platform" \
    -d "subject_token_type=urn:ietf:params:oauth:token-type:jwt" \
    -d "subject_token=${KC_TOKEN}")

STS_HTTP_CODE=$(echo "${STS_RESPONSE}" | tail -1)
STS_BODY=$(echo "${STS_RESPONSE}" | sed '$d')

save_response "workload_sts_exchange" "${STS_HTTP_CODE}" "${STS_BODY}"

if [[ "${STS_HTTP_CODE}" -ge 400 ]]; then
    log_fail "Workload STS exchange failed (HTTP ${STS_HTTP_CODE})"
    echo "${STS_BODY}" | jq . 2>/dev/null || echo "${STS_BODY}"
    exit 1
fi

FED_ACCESS_TOKEN=$(echo "${STS_BODY}" | jq -r '.access_token')
log_ok "Got federated access token"

# --- Step 2: Impersonate SA to get an ID token ---
log_step "Step 2: Impersonate SA to generate Google ID token"
log_step "SA: ${WORKLOAD_SA_EMAIL}"
log_step "Target audience: ${GATEWAY_AUDIENCE}"

ID_TOKEN_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X POST "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${WORKLOAD_SA_EMAIL}:generateIdToken" \
    -H "Authorization: Bearer ${FED_ACCESS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"audience\": \"${GATEWAY_AUDIENCE}\", \"includeEmail\": true}")

ID_HTTP_CODE=$(echo "${ID_TOKEN_RESPONSE}" | tail -1)
ID_BODY=$(echo "${ID_TOKEN_RESPONSE}" | sed '$d')

save_response "workload_id_token" "${ID_HTTP_CODE}" "${ID_BODY}"

if [[ "${ID_HTTP_CODE}" -ge 400 ]]; then
    log_fail "SA ID token generation failed (HTTP ${ID_HTTP_CODE})"
    echo "${ID_BODY}" | jq . 2>/dev/null || echo "${ID_BODY}"
    exit 1
fi

ID_TOKEN=$(echo "${ID_BODY}" | jq -r '.token')
log_ok "Got Google ID token"

# --- Inspect the ID token ---
echo ""
log_step "ID token claims:"
if is_jwt "${ID_TOKEN}"; then
    decode_jwt "${ID_TOKEN}"

    SA_ISS=$(decode_jwt "${ID_TOKEN}" | jq -r '.iss // "missing"')
    SA_AUD=$(decode_jwt "${ID_TOKEN}" | jq -r '.aud // "missing"')
    SA_SUB=$(decode_jwt "${ID_TOKEN}" | jq -r '.sub // "missing"')
    SA_EMAIL_CLAIM=$(decode_jwt "${ID_TOKEN}" | jq -r '.email // "missing"')

    echo ""
    log_step "Quick summary:"
    echo "  iss:   ${SA_ISS}"
    echo "  aud:   ${SA_AUD}"
    echo "  sub:   ${SA_SUB}"
    echo "  email: ${SA_EMAIL_CLAIM}"
    echo ""
    log_warn "Note: email is the SERVICE ACCOUNT (${SA_EMAIL_CLAIM}), not the original user."
else
    log_warn "ID token is not a JWT. Raw:"
    echo "${ID_TOKEN}" | head -c 200
fi

# Save for 03b
echo -n "${ID_TOKEN}" > "${TMP_DIR}/workload_sts_token.txt"
log_ok "Token saved to tmp/workload_sts_token.txt"

echo ""
log_ok "Fallback stage 2c complete"
log_step "Next: run 03b-test-gateway-workload.sh"
