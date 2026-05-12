#!/usr/bin/env bash
# Stage 5: Inspect the created_by field to verify identity propagation.
#
# Creates a cluster (or reuses stage 4 data), reads it back, and checks
# whether created_by is an email, a principal URI, or a service account.
#
# Reads: tmp/sts_token.txt, optionally tmp/cluster_id.txt (from stage 4)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GATEWAY_URL

log_header "Stage 5: Identity Verification"

# --- Load token ---
STS_TOKEN_FILE="${TMP_DIR}/sts_token.txt"
if [[ ! -f "${STS_TOKEN_FILE}" ]]; then
    log_fail "STS token not found. Run stages 1-3 first."
    exit 1
fi
STS_TOKEN=$(cat "${STS_TOKEN_FILE}")
AUTH_HEADER="Authorization: Bearer ${STS_TOKEN}"

# --- Get or create a cluster ---
CLUSTER_ID=""

if [[ -f "${TMP_DIR}/cluster_id.txt" ]]; then
    CLUSTER_ID=$(cat "${TMP_DIR}/cluster_id.txt")
    log_step "Using existing cluster from stage 4: ${CLUSTER_ID}"

    # Verify it still exists
    CHECK_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "${AUTH_HEADER}" \
        "${GATEWAY_URL}/api/v1/clusters/${CLUSTER_ID}")
    if [[ "${CHECK_CODE}" -eq 404 ]]; then
        log_warn "Cluster ${CLUSTER_ID} no longer exists (deleted in stage 4). Creating a new one."
        CLUSTER_ID=""
    fi
fi

if [[ -z "${CLUSTER_ID}" ]]; then
    log_step "Creating a cluster for identity check..."

    CLUSTER_NAME="poc-identity-$(date +%s)"
    CREATE_BODY=$(jq -n --arg name "${CLUSTER_NAME}" '{name: $name, spec: {}}')

    CREATE_RESPONSE=$(curl -s -w "\n%{http_code}" \
        -X POST \
        -H "${AUTH_HEADER}" \
        -H "Content-Type: application/json" \
        -d "${CREATE_BODY}" \
        "${GATEWAY_URL}/api/v1/clusters")
    CREATE_CODE=$(echo "${CREATE_RESPONSE}" | tail -1)
    CREATE_RESP=$(echo "${CREATE_RESPONSE}" | sed '$d')

    if [[ "${CREATE_CODE}" -ge 200 && "${CREATE_CODE}" -lt 300 ]]; then
        CLUSTER_ID=$(echo "${CREATE_RESP}" | jq -r '.id // empty')
        log_ok "Created cluster: ${CLUSTER_ID}"
    else
        log_fail "Cannot create cluster for identity check (HTTP ${CREATE_CODE})"
        echo "${CREATE_RESP}" | jq . 2>/dev/null || echo "${CREATE_RESP}"
        exit 1
    fi
fi

# --- Read the cluster back ---
log_step "Reading cluster ${CLUSTER_ID} to inspect created_by..."

GET_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -H "${AUTH_HEADER}" \
    "${GATEWAY_URL}/api/v1/clusters/${CLUSTER_ID}")
GET_CODE=$(echo "${GET_RESPONSE}" | tail -1)
GET_BODY=$(echo "${GET_RESPONSE}" | sed '$d')

save_response "identity_check" "${GET_CODE}" "${GET_BODY}"

if [[ "${GET_CODE}" -lt 200 || "${GET_CODE}" -ge 300 ]]; then
    log_fail "Cannot read cluster (HTTP ${GET_CODE})"
    echo "${GET_BODY}" | jq . 2>/dev/null || echo "${GET_BODY}"
    exit 1
fi

# --- Extract and analyze created_by ---
CREATED_BY=$(echo "${GET_BODY}" | jq -r '.created_by // "FIELD_NOT_PRESENT"')

echo ""
log_step "Full cluster response:"
echo "${GET_BODY}" | jq .

echo ""
log_header "IDENTITY CHECK RESULT"

echo "  created_by value: ${CREATED_BY}"
echo ""

if [[ "${CREATED_BY}" == "FIELD_NOT_PRESENT" ]]; then
    log_warn "The created_by field is not present in the response."
    log_warn "The API spec defines it as format: email, but the backend may not populate it."
    log_warn "Check the full response above for any user identity fields."

elif echo "${CREATED_BY}" | grep -qE '^[^@]+@[^@]+\.[^@]+$'; then
    log_ok "created_by is an EMAIL: ${CREATED_BY}"
    echo ""
    log_ok "RESULT: Identity propagation works correctly."
    log_ok "The federated user's email appears as the cluster owner."
    log_ok "This is the ideal outcome for zero-trust integration."

elif echo "${CREATED_BY}" | grep -q "^principal://"; then
    log_warn "created_by is a PRINCIPAL URI: ${CREATED_BY}"
    echo ""
    log_warn "RESULT: Authentication works, but identity is in federated principal format."
    log_warn "The backend may not handle this correctly for ownership checks."
    log_warn "ACTION: Discuss with CLS team whether they can map this to an email,"
    log_warn "or whether OME needs to send the email in a different way."

elif echo "${CREATED_BY}" | grep -qE '\.iam\.gserviceaccount\.com$'; then
    log_fail "created_by is a SERVICE ACCOUNT: ${CREATED_BY}"
    echo ""
    log_fail "RESULT: User identity was lost. The API sees a service account, not the user."
    log_fail "This means Workforce IdF is falling back to SA impersonation somewhere."
    log_fail "This violates zero-trust — investigate the token exchange chain."

else
    log_warn "created_by has an UNEXPECTED FORMAT: ${CREATED_BY}"
    echo ""
    log_warn "This needs manual investigation. The value doesn't match any expected pattern."
fi

# --- Cleanup: delete the test cluster if we created it ---
if [[ ! -f "${TMP_DIR}/cluster_id.txt" ]] || [[ "$(cat "${TMP_DIR}/cluster_id.txt")" != "${CLUSTER_ID}" ]]; then
    echo ""
    log_step "Cleaning up: deleting test cluster ${CLUSTER_ID}"
    DELETE_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        -X DELETE \
        -H "${AUTH_HEADER}" \
        "${GATEWAY_URL}/api/v1/clusters/${CLUSTER_ID}")
    if [[ "${DELETE_CODE}" -eq 202 || ("${DELETE_CODE}" -ge 200 && "${DELETE_CODE}" -lt 300) ]]; then
        log_ok "Cleanup: deletion initiated (HTTP ${DELETE_CODE})"
    else
        log_warn "Cleanup: delete returned HTTP ${DELETE_CODE} — may need manual cleanup"
    fi
fi

echo ""
log_ok "Stage 5 complete"
