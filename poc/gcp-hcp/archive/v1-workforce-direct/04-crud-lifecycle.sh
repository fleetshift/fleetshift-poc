#!/usr/bin/env bash
# Stage 4: Full cluster CRUD lifecycle against the GCP HCP API.
#
# Only run this if stage 3 succeeded (gateway accepted the token).
# Tests: create → list → get → status → delete → poll until 404.
#
# Reads: tmp/sts_token.txt (from stage 2)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GATEWAY_URL

log_header "Stage 4: Cluster CRUD Lifecycle"

# --- Load token ---
STS_TOKEN_FILE="${TMP_DIR}/sts_token.txt"
if [[ ! -f "${STS_TOKEN_FILE}" ]]; then
    log_fail "STS token not found. Run stages 1-3 first."
    exit 1
fi
STS_TOKEN=$(cat "${STS_TOKEN_FILE}")
log_ok "Loaded STS token"

AUTH_HEADER="Authorization: Bearer ${STS_TOKEN}"

api_call() {
    local method="$1"
    local path="$2"
    local name="$3"
    local body="${4:-}"

    local curl_args=(-s -w "\n%{http_code}" -H "${AUTH_HEADER}" -H "Content-Type: application/json")
    if [[ -n "${body}" ]]; then
        curl_args+=(-d "${body}")
    fi

    local response
    response=$(curl -X "${method}" "${curl_args[@]}" "${GATEWAY_URL}${path}")
    local http_code=$(echo "${response}" | tail -1)
    local resp_body=$(echo "${response}" | sed '$d')

    save_response "${name}" "${http_code}" "${resp_body}"
    echo "${http_code}"
}

CLUSTER_NAME="poc-test-$(date +%s)"
CLUSTER_ID=""

# --- 1. Create cluster ---
log_step "1. Create cluster: ${CLUSTER_NAME}"

CREATE_BODY=$(jq -n --arg name "${CLUSTER_NAME}" '{name: $name, spec: {}}')
log_step "Request body:"
echo "${CREATE_BODY}" | jq .

CREATE_CODE=$(api_call POST "/api/v1/clusters" "crud_create" "${CREATE_BODY}")

if [[ "${CREATE_CODE}" -ge 200 && "${CREATE_CODE}" -lt 300 ]]; then
    log_ok "Cluster create returned HTTP ${CREATE_CODE}"
    CREATE_RESP=$(cat "${TMP_DIR}/crud_create.body.json")
    echo "${CREATE_RESP}" | jq .
    CLUSTER_ID=$(echo "${CREATE_RESP}" | jq -r '.id // empty')
    if [[ -n "${CLUSTER_ID}" ]]; then
        log_ok "Cluster ID: ${CLUSTER_ID}"
        echo "${CLUSTER_ID}" > "${TMP_DIR}/cluster_id.txt"
    else
        log_warn "No cluster ID in response — check the response format"
    fi
else
    log_fail "Cluster create failed (HTTP ${CREATE_CODE})"
    cat "${TMP_DIR}/crud_create.body.json" | jq . 2>/dev/null || cat "${TMP_DIR}/crud_create.body.json"
    if [[ "${CREATE_CODE}" -eq 400 ]]; then
        log_warn "400 likely means the spec format is wrong. The error should indicate required fields."
    fi
    log_warn "Skipping remaining CRUD steps."
    exit 1
fi

echo ""

# --- 2. List clusters ---
log_step "2. List clusters"
LIST_CODE=$(api_call GET "/api/v1/clusters" "crud_list")
if [[ "${LIST_CODE}" -ge 200 && "${LIST_CODE}" -lt 300 ]]; then
    log_ok "List clusters returned HTTP ${LIST_CODE}"
    TOTAL=$(cat "${TMP_DIR}/crud_list.body.json" | jq -r '.total // "?"')
    echo "  Total clusters: ${TOTAL}"
else
    log_fail "List clusters failed (HTTP ${LIST_CODE})"
    cat "${TMP_DIR}/crud_list.body.json" | jq . 2>/dev/null
fi

echo ""

# --- 3. Get cluster ---
if [[ -n "${CLUSTER_ID}" ]]; then
    log_step "3. Get cluster: ${CLUSTER_ID}"
    GET_CODE=$(api_call GET "/api/v1/clusters/${CLUSTER_ID}" "crud_get")
    if [[ "${GET_CODE}" -ge 200 && "${GET_CODE}" -lt 300 ]]; then
        log_ok "Get cluster returned HTTP ${GET_CODE}"
        cat "${TMP_DIR}/crud_get.body.json" | jq .
    else
        log_fail "Get cluster failed (HTTP ${GET_CODE})"
        cat "${TMP_DIR}/crud_get.body.json" | jq . 2>/dev/null
    fi

    echo ""

    # --- 4. Get status ---
    log_step "4. Get cluster status"
    STATUS_CODE=$(api_call GET "/api/v1/clusters/${CLUSTER_ID}/status" "crud_status")
    if [[ "${STATUS_CODE}" -ge 200 && "${STATUS_CODE}" -lt 300 ]]; then
        log_ok "Get status returned HTTP ${STATUS_CODE}"
        cat "${TMP_DIR}/crud_status.body.json" | jq .
    else
        log_fail "Get status failed (HTTP ${STATUS_CODE})"
        cat "${TMP_DIR}/crud_status.body.json" | jq . 2>/dev/null
    fi

    echo ""

    # --- 5. Delete cluster ---
    log_step "5. Delete cluster: ${CLUSTER_ID}"
    DELETE_CODE=$(api_call DELETE "/api/v1/clusters/${CLUSTER_ID}" "crud_delete")
    if [[ "${DELETE_CODE}" -eq 202 ]]; then
        log_ok "Delete returned 202 (async deletion initiated)"
    elif [[ "${DELETE_CODE}" -ge 200 && "${DELETE_CODE}" -lt 300 ]]; then
        log_ok "Delete returned HTTP ${DELETE_CODE}"
    else
        log_fail "Delete failed (HTTP ${DELETE_CODE})"
        cat "${TMP_DIR}/crud_delete.body.json" | jq . 2>/dev/null
    fi

    echo ""

    # --- 6. Poll for deletion ---
    log_step "6. Polling for cluster deletion (waiting for 404)..."
    MAX_POLLS=30
    POLL_INTERVAL=10
    for i in $(seq 1 ${MAX_POLLS}); do
        POLL_CODE=$(api_call GET "/api/v1/clusters/${CLUSTER_ID}" "crud_poll_${i}")
        if [[ "${POLL_CODE}" -eq 404 ]]; then
            log_ok "Cluster deleted (404 after ${i} polls)"
            break
        elif [[ "${POLL_CODE}" -ge 200 && "${POLL_CODE}" -lt 300 ]]; then
            PHASE=$(cat "${TMP_DIR}/crud_poll_${i}.body.json" | jq -r '.status.phase // "unknown"')
            echo "  Poll ${i}/${MAX_POLLS}: HTTP ${POLL_CODE}, phase=${PHASE} — waiting ${POLL_INTERVAL}s..."
            sleep "${POLL_INTERVAL}"
        else
            log_warn "Poll ${i}: unexpected HTTP ${POLL_CODE}"
            sleep "${POLL_INTERVAL}"
        fi

        if [[ "${i}" -eq "${MAX_POLLS}" ]]; then
            log_warn "Timed out waiting for deletion after ${MAX_POLLS} polls"
        fi
    done
else
    log_warn "No cluster ID — skipping get/status/delete steps"
fi

echo ""
log_ok "Stage 4 complete"
log_step "Next: run 05-check-identity.sh"
