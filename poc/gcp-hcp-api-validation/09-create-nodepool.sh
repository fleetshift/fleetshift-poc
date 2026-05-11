#!/usr/bin/env bash
# Stage 9: Create a worker nodepool for the cluster.
#
# Reads: tmp/broker_idtoken.jwt  (from stage 3)
#        tmp/user_email.txt      (from stage 2)
#        tmp/cluster_id.txt      (from stage 7)
#        tmp/cluster_name.txt    (from stage 7)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GATEWAY_URL
load_broker_auth

CLUSTER_ID_FILE="${TMP_DIR}/cluster_id.txt"
CLUSTER_NAME_FILE="${TMP_DIR}/cluster_name.txt"

if [[ ! -f "${CLUSTER_ID_FILE}" ]]; then
    log_fail "No cluster ID found. Run 07-create-and-monitor.sh first."
    exit 1
fi

CLUSTER_ID=$(cat "${CLUSTER_ID_FILE}")
CLUSTER_NAME=$(cat "${CLUSTER_NAME_FILE}" 2>/dev/null || echo "cluster")
NODEPOOL_NAME="${1:-${CLUSTER_NAME}-nodepool-1}"
REPLICAS="${2:-2}"

log_header "Stage 9: Create Nodepool"

log_step "Cluster ID: ${CLUSTER_ID}"
log_step "Nodepool name: ${NODEPOOL_NAME}"
log_step "Replicas: ${REPLICAS}"

NODEPOOL_BODY=$(jq -n \
    --arg name "${NODEPOOL_NAME}" \
    --arg cluster_id "${CLUSTER_ID}" \
    --argjson replicas "${REPLICAS}" \
    '{
        name: $name,
        cluster_id: $cluster_id,
        spec: {
            replicas: $replicas,
            platform: {
                type: "GCP",
                gcp: {
                    instanceType: "n1-standard-4",
                    rootVolume: {
                        size: 128,
                        type: "pd-standard"
                    }
                }
            },
            management: {
                autoRepair: true,
                upgradeType: "Replace"
            }
        }
    }')

log_step "Creating nodepool..."
CREATE_CODE=$(api_call POST "/api/v1/nodepools" "nodepool_create" "${NODEPOOL_BODY}")

if [[ "${CREATE_CODE}" -ge 200 && "${CREATE_CODE}" -lt 300 ]]; then
    log_ok "Nodepool created (HTTP ${CREATE_CODE})"
    cat "${TMP_DIR}/nodepool_create.body.json" | jq '{id, name, cluster_id, replicas: .spec.replicas}'
else
    log_fail "Nodepool creation failed (HTTP ${CREATE_CODE})"
    cat "${TMP_DIR}/nodepool_create.body.json" | jq . 2>/dev/null || cat "${TMP_DIR}/nodepool_create.body.json"
    exit 1
fi

echo ""
log_ok "Stage 9 complete"
