#!/usr/bin/env bash
# Stage 10: Full teardown — cluster, nodepools, and GCP infrastructure.
#
# Order: nodepools → cluster → wait for 404 → hypershift destroy infra → hypershift destroy iam
#
# Reads: tmp/broker_idtoken.jwt  (from stage 3)
#        tmp/user_email.txt      (from stage 2)
#        tmp/cluster_id.txt      (from stage 7)
#        config.env              (INFRA_ID, TARGET_GCP_PROJECT, REGION)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GATEWAY_URL INFRA_ID TARGET_GCP_PROJECT REGION
load_broker_auth

log_header "Stage 10: Full Teardown"

CLUSTER_ID=""
if [[ -f "${TMP_DIR}/cluster_id.txt" ]]; then
    CLUSTER_ID=$(cat "${TMP_DIR}/cluster_id.txt")
fi

# ── 1. Delete nodepools ────────────────────────────────────────────────────

if [[ -n "${CLUSTER_ID}" ]]; then
    log_step "1. Deleting nodepools for cluster ${CLUSTER_ID}..."
    LIST_CODE=$(api_call GET "/api/v1/nodepools?clusterId=${CLUSTER_ID}" "teardown_list_np")

    if [[ "${LIST_CODE}" -ge 200 && "${LIST_CODE}" -lt 300 ]]; then
        NODEPOOL_IDS=$(jq -r '.nodepools[]?.id // empty' "${TMP_DIR}/teardown_list_np.body.json" 2>/dev/null)
        if [[ -n "${NODEPOOL_IDS}" ]]; then
            while IFS= read -r np_id; do
                log_step "  Deleting nodepool: ${np_id}"
                DEL_CODE=$(api_call DELETE "/api/v1/nodepools/${np_id}" "teardown_del_np_${np_id}")
                if [[ "${DEL_CODE}" -ge 200 && "${DEL_CODE}" -lt 300 ]]; then
                    log_ok "  Nodepool ${np_id} deleted (HTTP ${DEL_CODE})"
                else
                    log_warn "  Nodepool delete returned HTTP ${DEL_CODE}"
                fi
            done <<< "${NODEPOOL_IDS}"
        else
            log_ok "No nodepools found"
        fi
    else
        log_warn "Could not list nodepools (HTTP ${LIST_CODE})"
    fi

    echo ""

    # ── 2. Delete cluster ──────────────────────────────────────────────────

    log_step "2. Deleting cluster ${CLUSTER_ID}..."
    DEL_CODE=$(api_call DELETE "/api/v1/clusters/${CLUSTER_ID}?force=true" "teardown_del_cluster")
    if [[ "${DEL_CODE}" -eq 202 || "${DEL_CODE}" -eq 200 ]]; then
        log_ok "Cluster deletion initiated (HTTP ${DEL_CODE})"
    elif [[ "${DEL_CODE}" -eq 404 ]]; then
        log_ok "Cluster already deleted"
    else
        log_warn "Cluster delete returned HTTP ${DEL_CODE}"
    fi

    # Poll for 404
    log_step "Waiting for cluster deletion..."
    for i in $(seq 1 30); do
        POLL_CODE=$(api_call GET "/api/v1/clusters/${CLUSTER_ID}" "teardown_poll_${i}")
        if [[ "${POLL_CODE}" -eq 404 ]]; then
            log_ok "Cluster deleted (404 after ${i} polls)"
            break
        fi
        if [[ "${i}" -eq 30 ]]; then
            log_warn "Timed out waiting for cluster deletion"
        fi
        sleep 10
    done

    echo ""
else
    log_warn "No cluster ID found in tmp/cluster_id.txt — skipping API cleanup"
    echo ""
fi

# ── 3. Destroy network infrastructure ──────────────────────────────────────

log_step "3. Destroying network infrastructure..."

if command -v hypershift &>/dev/null; then
    hypershift destroy infra gcp \
        --infra-id "${INFRA_ID}" \
        --project-id "${TARGET_GCP_PROJECT}" \
        --region "${REGION}" \
        && log_ok "Network infrastructure destroyed" \
        || log_warn "Network infrastructure destroy failed (may already be cleaned up)"
else
    log_warn "hypershift binary not found — skipping infra destroy"
fi

echo ""

# ── 4. Destroy IAM infrastructure ─────────────────────────────────────────

log_step "4. Destroying IAM infrastructure..."

if command -v hypershift &>/dev/null; then
    hypershift destroy iam gcp \
        --infra-id "${INFRA_ID}" \
        --project-id "${TARGET_GCP_PROJECT}" \
        && log_ok "IAM infrastructure destroyed" \
        || log_warn "IAM infrastructure destroy failed (may already be cleaned up)"
else
    log_warn "hypershift binary not found — skipping IAM destroy"
fi

echo ""

# ── 5. Optionally delete target project ────────────────────────────────────

log_step "5. Delete target project ${TARGET_GCP_PROJECT}?"
read -r -p "   Type 'yes' to delete the project, anything else to skip: " CONFIRM
if [[ "${CONFIRM}" == "yes" ]]; then
    gcloud projects delete "${TARGET_GCP_PROJECT}" --quiet
    log_ok "Project ${TARGET_GCP_PROJECT} deleted"
else
    log_step "Skipping project deletion"
fi

echo ""

# Clean up local state
rm -f "${TMP_DIR}/cluster_id.txt" "${TMP_DIR}/cluster_name.txt"
rm -f "${TMP_DIR}/iam-config.json" "${TMP_DIR}/infra-config.json"
rm -f "${TMP_DIR}/signing-key.pem" "${TMP_DIR}/signing-key-base64.txt" "${TMP_DIR}/jwks.json"

log_ok "Stage 10 complete — all resources torn down"
