#!/usr/bin/env bash
#
# Deploy fleetlet + monitoring addon (sidecar) into a FleetShift-provisioned Kind cluster.
#
# The Kind cluster must already exist (provisioned via docker compose + FleetShift API).
# The cluster containers share the "fleetshift" Docker network, so the fleetlet
# reaches the platform at fleetshift:50051 (compose service name).
#
# The monitoring addon runs as a sidecar container in the same pod as the fleetlet,
# communicating via a Unix domain socket on a shared emptyDir volume.
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

echo "=== Installing metrics-server (for real CPU/memory usage) ==="
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl -n kube-system patch deployment metrics-server --type=json \
    -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

echo "=== Deploying MonitoringConfig CRD ==="
kubectl apply -f addons/deploy/kind/monitoring-crd.yaml

echo "=== Deploying fleetlet + monitoring sidecar (target: ${FLEETLET_TARGET_ID}) ==="
sed \
    -e "s/--target-id=fleetlet-kind-1/--target-id=${FLEETLET_TARGET_ID}/" \
    -e "s/--target-name=Kind Addon Spike/--target-name=${CLUSTER_NAME} fleetlet/" \
    addons/deploy/kind/fleetlet.yaml | kubectl apply -f -

echo "=== Waiting for fleetlet pod ==="
kubectl rollout status deployment/fleetlet --timeout=60s

echo ""
echo "=== Done ==="
echo "Fleetlet registered as target '${FLEETLET_TARGET_ID}'."
echo "Monitoring addon runs as sidecar, communicating via Unix socket."
echo ""
echo "export KUBECONFIG=${KUBECONFIG_FILE}"
echo "  kubectl logs -f deploy/fleetlet -c fleetlet"
echo "  kubectl logs -f deploy/fleetlet -c monitoring"
echo "  kubectl get monitoringconfigs"
