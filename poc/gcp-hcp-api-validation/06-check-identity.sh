#!/usr/bin/env bash
# Stage 6: Verify that created_by matches the X-User-Email value.
#
# Creates a test cluster (or reuses one from stage 5), reads it back,
# and checks whether created_by is the Keycloak user's email.
#
# Reads: tmp/broker_idtoken.jwt  (from stage 3)
#        tmp/user_email.txt      (from stage 2)
#        tmp/cluster_id.txt      (optional, from stage 5)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GATEWAY_URL
load_broker_auth

log_header "Stage 6: Identity Verification"

CREATED_HERE=false

# --- Get or create a cluster ---
CLUSTER_ID=""

if [[ -f "${TMP_DIR}/cluster_id.txt" ]]; then
    CLUSTER_ID=$(cat "${TMP_DIR}/cluster_id.txt")
    log_step "Using existing cluster from stage 5: ${CLUSTER_ID}"

    CHECK_CODE=$(api_call GET "/api/v1/clusters/${CLUSTER_ID}" "identity_check_exists")
    if [[ "${CHECK_CODE}" -eq 404 ]]; then
        log_warn "Cluster ${CLUSTER_ID} no longer exists (deleted in stage 5). Creating a new one."
        CLUSTER_ID=""
    fi
fi

if [[ -z "${CLUSTER_ID}" ]]; then
    log_step "Creating a cluster for identity check..."

    CLUSTER_NAME="poc-identity-$(date +%s)"
    CREATE_BODY=$(jq -n --arg name "${CLUSTER_NAME}" '{name: $name, spec: {}}')

    CREATE_CODE=$(api_call POST "/api/v1/clusters" "identity_create" "${CREATE_BODY}")
    if [[ "${CREATE_CODE}" -ge 200 && "${CREATE_CODE}" -lt 300 ]]; then
        CREATE_RESP=$(cat "${TMP_DIR}/identity_create.body.json")
        CLUSTER_ID=$(echo "${CREATE_RESP}" | jq -r '.id // empty')
        log_ok "Created cluster: ${CLUSTER_ID}"
        CREATED_HERE=true
    else
        log_fail "Cannot create cluster for identity check (HTTP ${CREATE_CODE})"
        cat "${TMP_DIR}/identity_create.body.json" | jq . 2>/dev/null
        exit 1
    fi
fi

# --- Read the cluster back ---
log_step "Reading cluster ${CLUSTER_ID} to inspect created_by..."

GET_CODE=$(api_call GET "/api/v1/clusters/${CLUSTER_ID}" "identity_check")

if [[ "${GET_CODE}" -lt 200 || "${GET_CODE}" -ge 300 ]]; then
    log_fail "Cannot read cluster (HTTP ${GET_CODE})"
    cat "${TMP_DIR}/identity_check.body.json" | jq . 2>/dev/null
    exit 1
fi

GET_BODY=$(cat "${TMP_DIR}/identity_check.body.json")

# --- Extract and analyze created_by ---
CREATED_BY=$(echo "${GET_BODY}" | jq -r '.created_by // "FIELD_NOT_PRESENT"')

echo ""
log_step "Full cluster response:"
echo "${GET_BODY}" | jq .

echo ""
log_header "IDENTITY CHECK RESULT"

echo "  created_by:   ${CREATED_BY}"
echo "  X-User-Email: ${USER_EMAIL}"
echo ""

if [[ "${CREATED_BY}" == "FIELD_NOT_PRESENT" ]]; then
    log_warn "The created_by field is not present in the response."
    log_warn "The API spec defines it as format: email, but the backend may not populate it."

elif [[ "${CREATED_BY}" == "${USER_EMAIL}" ]]; then
    log_ok "MATCH — created_by equals the Keycloak user's email"
    echo ""
    log_ok "The broker auth flow preserves user identity end-to-end."
    log_ok "X-User-Email is correctly propagated to created_by."

elif echo "${CREATED_BY}" | grep -qE '\.iam\.gserviceaccount\.com$'; then
    log_fail "created_by is the BROKER SERVICE ACCOUNT: ${CREATED_BY}"
    echo ""
    log_fail "The backend recorded the broker SA email, not the user's email."
    log_fail "This means X-User-Email was ignored for created_by."
    log_fail "The identity propagation does NOT work as expected."

elif echo "${CREATED_BY}" | grep -q "^principal://"; then
    log_warn "created_by is a PRINCIPAL URI: ${CREATED_BY}"
    echo ""
    log_warn "The backend stored a federated principal format instead of an email."
    log_warn "This needs discussion with the CLS team."

elif echo "${CREATED_BY}" | grep -qE '^[^@]+@[^@]+\.[^@]+$'; then
    log_warn "created_by is an EMAIL but does NOT match the Keycloak user"
    echo ""
    echo "  created_by:   ${CREATED_BY}"
    echo "  expected:     ${USER_EMAIL}"
    log_warn "Investigate where this email came from."

else
    log_warn "created_by has an UNEXPECTED FORMAT: ${CREATED_BY}"
    log_warn "This needs manual investigation."
fi

# --- Cleanup ---
if [[ "${CREATED_HERE}" == "true" ]]; then
    echo ""
    log_step "Cleaning up: deleting test cluster ${CLUSTER_ID}"
    DELETE_CODE=$(api_call DELETE "/api/v1/clusters/${CLUSTER_ID}?force=true" "identity_cleanup")
    if [[ "${DELETE_CODE}" -eq 202 || ("${DELETE_CODE}" -ge 200 && "${DELETE_CODE}" -lt 300) ]]; then
        log_ok "Cleanup: deletion initiated (HTTP ${DELETE_CODE})"
    else
        log_warn "Cleanup: delete returned HTTP ${DELETE_CODE} — may need manual cleanup"
    fi
fi

echo ""
log_ok "Stage 6 complete"
