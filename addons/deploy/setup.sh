#!/usr/bin/env bash
#
# Deploy fleetlet + monitoring addon into a FleetShift-provisioned Kind cluster.
#
# The Kind cluster must already exist (provisioned via docker compose + FleetShift API).
# The cluster containers share the "fleetshift" Docker network, so the fleetlet
# reaches the platform at fleetshift:50051 (compose service name).
#
# Usage:
#   ./setup.sh <cluster-name>

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <cluster-name>"
    echo "  cluster-name: name of the Kind cluster provisioned by FleetShift"
    exit 1
fi

CLUSTER_NAME="$1"
FLEETLET_TARGET_ID="fleetlet-${CLUSTER_NAME}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

echo "=== Exporting kubeconfig for Kind cluster ${CLUSTER_NAME} ==="
KUBECONFIG_FILE="/tmp/kind-${CLUSTER_NAME}.kubeconfig"
kind get kubeconfig --name "${CLUSTER_NAME}" > "${KUBECONFIG_FILE}"
export KUBECONFIG="${KUBECONFIG_FILE}"

echo "=== Building images ==="
cd "${REPO_ROOT}"
docker build -f addons/Dockerfile.fleetlet -t fleetlet:dev .
docker build -f addons/Dockerfile.monitoring -t monitoring-agent:dev .

echo "=== Loading images into Kind cluster ${CLUSTER_NAME} ==="
kind load docker-image fleetlet:dev monitoring-agent:dev --name "${CLUSTER_NAME}"

echo "=== Deploying MonitoringConfig CRD ==="
kubectl apply -f addons/deploy/kind/monitoring-crd.yaml

echo "=== Deploying monitoring addon ==="
kubectl apply -f addons/deploy/kind/monitoring.yaml

echo "=== Waiting for monitoring addon ==="
kubectl rollout status deployment/monitoring-addon --timeout=60s

echo "=== Deploying fleetlet (target: ${FLEETLET_TARGET_ID}) ==="
sed \
    -e "s/--target-id=fleetlet-kind-1/--target-id=${FLEETLET_TARGET_ID}/" \
    -e "s/--target-name=Kind Addon Spike/--target-name=${CLUSTER_NAME} fleetlet/" \
    addons/deploy/kind/fleetlet.yaml | kubectl apply -f -

echo "=== Waiting for fleetlet ==="
kubectl rollout status deployment/fleetlet --timeout=60s

echo ""
echo "=== Done ==="
echo "Fleetlet registered as target '${FLEETLET_TARGET_ID}'."
echo ""
echo "export KUBECONFIG=${KUBECONFIG_FILE}"
echo "  kubectl logs -f deploy/fleetlet"
echo "  kubectl logs -f deploy/monitoring-addon"
echo "  kubectl get monitoringconfigs"
