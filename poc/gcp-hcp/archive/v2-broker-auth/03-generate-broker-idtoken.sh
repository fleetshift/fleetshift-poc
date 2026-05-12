#!/usr/bin/env bash
# Stage 3: Mint a Google-signed ID token for the broker service account.
#
# Uses the Workforce access token from stage 2 to call IAM Credentials
# generateIdToken. The result is a Google-signed JWT that the API Gateway
# will accept.
#
# Reads:  tmp/workforce_access_token.txt  (from stage 2)
# Writes: tmp/broker_idtoken.jwt

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GCP_PROJECT BROKER_SA_EMAIL GATEWAY_AUDIENCE

log_header "Stage 3: Generate Broker ID Token"

# --- Load Workforce access token ---
WF_TOKEN_FILE="${TMP_DIR}/workforce_access_token.txt"
if [[ ! -f "${WF_TOKEN_FILE}" ]]; then
    log_fail "Workforce access token not found at ${WF_TOKEN_FILE}"
    log_fail "Run 02-workforce-sts-exchange.sh first"
    exit 1
fi
WF_TOKEN=$(cat "${WF_TOKEN_FILE}")
log_ok "Loaded Workforce access token"

# --- Call generateIdToken ---
log_step "Minting ID token for broker SA: ${BROKER_SA_EMAIL}"
log_step "Target audience: ${GATEWAY_AUDIENCE}"

ID_TOKEN_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X POST "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${BROKER_SA_EMAIL}:generateIdToken" \
    -H "Authorization: Bearer ${WF_TOKEN}" \
    -H "Content-Type: application/json" \
    -H "x-goog-user-project: ${GCP_PROJECT}" \
    -d "{\"audience\": \"${GATEWAY_AUDIENCE}\", \"includeEmail\": true}")

ID_HTTP_CODE=$(echo "${ID_TOKEN_RESPONSE}" | tail -1)
ID_BODY=$(echo "${ID_TOKEN_RESPONSE}" | sed '$d')

save_response "broker_idtoken" "${ID_HTTP_CODE}" "${ID_BODY}"

# --- Handle errors ---
if [[ "${ID_HTTP_CODE}" -ge 400 ]]; then
    log_fail "generateIdToken failed (HTTP ${ID_HTTP_CODE})"
    echo "${ID_BODY}" | jq . 2>/dev/null || echo "${ID_BODY}"

    echo ""
    if [[ "${ID_HTTP_CODE}" -eq 403 ]]; then
        ERROR_MSG=$(echo "${ID_BODY}" | jq -r '.error.message // ""' 2>/dev/null)
        if echo "${ERROR_MSG}" | grep -qi "unregistered callers"; then
            log_warn "403 — Workforce token lacks project context for this API."
            log_warn "Fix: grant serviceUsageConsumer on the project to the workforce principal:"
            echo ""
            echo "  gcloud projects add-iam-policy-binding ${GCP_PROJECT} \\"
            echo "    --member=\"principalSet://iam.googleapis.com/locations/global/workforcePools/<pool>/attribute.email/<your-email>\" \\"
            echo "    --role=\"roles/serviceusage.serviceUsageConsumer\""
        else
            log_warn "403 Forbidden — the Workforce identity lacks permission to mint ID tokens."
            log_warn "Fix: grant roles/iam.serviceAccountOpenIdTokenCreator on the broker SA:"
            echo ""
            echo "  gcloud iam service-accounts add-iam-policy-binding ${BROKER_SA_EMAIL} \\"
            echo "    --role=\"roles/iam.serviceAccountOpenIdTokenCreator\" \\"
            echo "    --member=\"principalSet://iam.googleapis.com/locations/global/workforcePools/<pool>/attribute.email/<your-email>\""
        fi
    elif [[ "${ID_HTTP_CODE}" -eq 404 ]]; then
        log_warn "404 Not Found — the broker service account may not exist."
        log_warn "Fix: create it:"
        echo ""
        echo "  gcloud iam service-accounts create $(echo "${BROKER_SA_EMAIL}" | cut -d@ -f1) \\"
        echo "    --project=$(echo "${BROKER_SA_EMAIL}" | cut -d@ -f2 | cut -d. -f1) \\"
        echo "    --display-name=\"ID Token Broker\""
    fi
    exit 1
fi

# --- Extract and verify the ID token ---
ID_TOKEN=$(echo "${ID_BODY}" | jq -r '.token')

if [[ -z "${ID_TOKEN}" || "${ID_TOKEN}" == "null" ]]; then
    log_fail "No token in generateIdToken response"
    echo "${ID_BODY}" | jq .
    exit 1
fi

if ! is_jwt "${ID_TOKEN}"; then
    log_fail "Generated token is not a JWT — unexpected"
    echo "  Token prefix: $(echo "${ID_TOKEN}" | head -c 50)..."
    exit 1
fi

log_ok "Got Google-signed ID token (JWT)"

# --- Decode and display ---
echo ""
log_step "ID token claims:"
decode_jwt "${ID_TOKEN}"

TOKEN_ISS=$(decode_jwt "${ID_TOKEN}" | jq -r '.iss // "missing"')
TOKEN_AUD=$(decode_jwt "${ID_TOKEN}" | jq -r '.aud // "missing"')
TOKEN_SUB=$(decode_jwt "${ID_TOKEN}" | jq -r '.sub // "missing"')
TOKEN_EMAIL=$(decode_jwt "${ID_TOKEN}" | jq -r '.email // "missing"')
TOKEN_EXP=$(decode_jwt "${ID_TOKEN}" | jq -r '.exp // "missing"')
TOKEN_EXP_HUMAN=""
if [[ "${TOKEN_EXP}" != "missing" ]]; then
    TOKEN_EXP_HUMAN=" ($(date -d "@${TOKEN_EXP}" 2>/dev/null || date -r "${TOKEN_EXP}" 2>/dev/null || echo "?"))"
fi

echo ""
log_step "Quick summary:"
echo "  iss:   ${TOKEN_ISS}"
echo "  aud:   ${TOKEN_AUD}"
echo "  sub:   ${TOKEN_SUB}"
echo "  email: ${TOKEN_EMAIL}"
echo "  exp:   ${TOKEN_EXP}${TOKEN_EXP_HUMAN}"

echo ""
log_step "Verification:"

if [[ "${TOKEN_ISS}" == "https://accounts.google.com" ]]; then
    log_ok "Issuer is accounts.google.com (Google-signed)"
else
    log_warn "Unexpected issuer: ${TOKEN_ISS}"
fi

if [[ "${TOKEN_AUD}" == "${GATEWAY_AUDIENCE}" ]]; then
    log_ok "Audience matches gateway config (${GATEWAY_AUDIENCE})"
else
    log_warn "Audience MISMATCH — gateway expects '${GATEWAY_AUDIENCE}', token has '${TOKEN_AUD}'"
fi

if echo "${TOKEN_EMAIL}" | grep -q "iam.gserviceaccount.com"; then
    log_ok "Token email is the broker SA (${TOKEN_EMAIL}) — this is expected"
else
    log_warn "Token email is not the broker SA: ${TOKEN_EMAIL}"
fi

# Save for stage 4
echo -n "${ID_TOKEN}" > "${TMP_DIR}/broker_idtoken.jwt"
log_ok "Token saved to tmp/broker_idtoken.jwt"

echo ""
log_ok "Stage 3 complete"
log_step "Next: run 04-test-gateway.sh"
