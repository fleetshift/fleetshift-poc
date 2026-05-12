#!/usr/bin/env bash
# Create a cluster and poll until Ready or Failed.
#
# Usage:
#   ./07-create-and-monitor.sh [cluster-name]
#
# If no name is given, generates one from the timestamp.
# Polls GET /api/v1/clusters/{id} and watches status.phase.
#
# Reads: tmp/broker_idtoken.jwt  (from stage 3)
#        tmp/user_email.txt      (from stage 2)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GATEWAY_URL INFRA_ID TARGET_GCP_PROJECT
load_broker_auth
load_infra_configs

CLUSTER_NAME="${1:-poc-$(date +%s)}"
POLL_INTERVAL=15
MAX_POLLS=80  # 80 * 15s = 20 minutes

log_header "Create and Monitor Cluster"

# --- Create ---
log_step "Creating cluster: ${CLUSTER_NAME}"

CREATE_BODY=$(jq -n \
    --arg name "${CLUSTER_NAME}" \
    --arg target_project "${TARGET_GCP_PROJECT}" \
    --arg infra_id "${INFRA_ID}" \
    --arg signing_key "${SIGNING_KEY_BASE64}" \
    --arg issuer_url "https://hypershift-${INFRA_ID}-oidc" \
    --arg region "$(echo "${INFRA_CONFIG}" | jq -r '.region')" \
    --arg network "$(echo "${INFRA_CONFIG}" | jq -r '.networkName')" \
    --arg subnet "$(echo "${INFRA_CONFIG}" | jq -r '.subnetName')" \
    --arg project_number "$(echo "${IAM_CONFIG}" | jq -r '.projectNumber')" \
    --arg pool_id "$(echo "${IAM_CONFIG}" | jq -r '.workloadIdentityPool.poolId')" \
    --arg provider_id "$(echo "${IAM_CONFIG}" | jq -r '.workloadIdentityPool.providerId')" \
    --arg sa_ctrlplane "$(echo "${IAM_CONFIG}" | jq -r '.serviceAccounts["ctrlplane-op"]')" \
    --arg sa_nodepool "$(echo "${IAM_CONFIG}" | jq -r '.serviceAccounts["nodepool-mgmt"]')" \
    --arg sa_cloud_ctrl "$(echo "${IAM_CONFIG}" | jq -r '.serviceAccounts["cloud-controller"]')" \
    --arg sa_csi "$(echo "${IAM_CONFIG}" | jq -r '.serviceAccounts["gcp-pd-csi"]')" \
    --arg sa_registry "$(echo "${IAM_CONFIG}" | jq -r '.serviceAccounts["image-registry"]')" \
    --arg sa_network "$(echo "${IAM_CONFIG}" | jq -r '.serviceAccounts["cloud-network"]')" \
    '{
        name: $name,
        target_project_id: $target_project,
        spec: {
            infraID: $infra_id,
            issuerURL: $issuer_url,
            serviceAccountSigningKey: $signing_key,
            platform: {
                type: "GCP",
                gcp: {
                    projectID: $target_project,
                    region: $region,
                    network: $network,
                    subnet: $subnet,
                    endpointAccess: "PublicAndPrivate",
                    workloadIdentity: {
                        projectNumber: $project_number,
                        poolID: $pool_id,
                        providerID: $provider_id,
                        serviceAccountsRef: {
                            controlPlaneEmail: $sa_ctrlplane,
                            nodePoolEmail: $sa_nodepool,
                            cloudControllerEmail: $sa_cloud_ctrl,
                            storageEmail: $sa_csi,
                            imageRegistryEmail: $sa_registry,
                            networkEmail: $sa_network
                        }
                    }
                }
            }
        }
    }')
CREATE_CODE=$(api_call POST "/api/v1/clusters" "monitor_create" "${CREATE_BODY}")

if [[ "${CREATE_CODE}" -lt 200 || "${CREATE_CODE}" -ge 300 ]]; then
    log_fail "Cluster create failed (HTTP ${CREATE_CODE})"
    cat "${TMP_DIR}/monitor_create.body.json" | jq . 2>/dev/null
    exit 1
fi

CREATE_RESP=$(cat "${TMP_DIR}/monitor_create.body.json")
CLUSTER_ID=$(echo "${CREATE_RESP}" | jq -r '.id // empty')

if [[ -z "${CLUSTER_ID}" ]]; then
    log_fail "No cluster ID in response"
    echo "${CREATE_RESP}" | jq .
    exit 1
fi

CREATED_BY=$(echo "${CREATE_RESP}" | jq -r '.created_by // "unknown"')
log_ok "Created cluster ${CLUSTER_ID} (created_by: ${CREATED_BY})"
echo "${CLUSTER_ID}" > "${TMP_DIR}/cluster_id.txt"
echo "${CLUSTER_NAME}" > "${TMP_DIR}/cluster_name.txt"

# --- Poll until Ready or Failed ---
echo ""
log_step "Polling every ${POLL_INTERVAL}s (timeout: $((MAX_POLLS * POLL_INTERVAL))s)..."
echo ""

PREV_PHASE=""
for i in $(seq 1 ${MAX_POLLS}); do
    POLL_CODE=$(api_call GET "/api/v1/clusters/${CLUSTER_ID}" "monitor_poll_${i}")

    if [[ "${POLL_CODE}" -eq 404 ]]; then
        log_fail "Cluster disappeared (404)"
        exit 1
    fi

    if [[ "${POLL_CODE}" -lt 200 || "${POLL_CODE}" -ge 300 ]]; then
        log_warn "Poll ${i}: unexpected HTTP ${POLL_CODE}"
        sleep "${POLL_INTERVAL}"
        continue
    fi

    POLL_BODY=$(cat "${TMP_DIR}/monitor_poll_${i}.body.json")
    PHASE=$(echo "${POLL_BODY}" | jq -r '.status.phase // "Pending"')
    MESSAGE=$(echo "${POLL_BODY}" | jq -r '.status.message // ""')
    REASON=$(echo "${POLL_BODY}" | jq -r '.status.reason // ""')

    if [[ "${PHASE}" != "${PREV_PHASE}" ]]; then
        echo ""
        log_step "Phase: ${PHASE}"
        [[ -n "${REASON}" ]] && echo "  Reason:  ${REASON}"
        [[ -n "${MESSAGE}" ]] && echo "  Message: ${MESSAGE}"
        PREV_PHASE="${PHASE}"
    else
        printf "  ."
    fi

    case "${PHASE}" in
        Ready)
            echo ""
            log_ok "Cluster is Ready"
            echo ""
            echo "${POLL_BODY}" | jq '{id, name, created_by, phase: .status.phase, conditions: .status.conditions}'
            exit 0
            ;;
        Failed)
            echo ""
            log_fail "Cluster failed"
            echo ""
            echo "${POLL_BODY}" | jq '{id, name, phase: .status.phase, reason: .status.reason, message: .status.message, conditions: .status.conditions}'
            exit 1
            ;;
    esac

    sleep "${POLL_INTERVAL}"
done

echo ""
log_warn "Timed out after $((MAX_POLLS * POLL_INTERVAL))s — cluster still in phase: ${PHASE}"
log_step "Cluster ID: ${CLUSTER_ID}"
log_step "Check manually: curl ... /api/v1/clusters/${CLUSTER_ID} | jq .status"
exit 1
