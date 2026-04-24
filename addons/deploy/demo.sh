#!/usr/bin/env bash
#
# Demo script: deploy and scale workloads on a Kind cluster to show
# live metrics updating in the FleetShift Addon Metrics dashboard.
#
# Usage:
#   ./demo.sh <cluster-name>
#
# The script deploys an nginx workload, scales it up, waits between
# steps so you can watch the pod count change in the UI, then cleans up.

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <cluster-name>"
    exit 1
fi

CLUSTER_NAME="$1"
KUBECONFIG_FILE="/tmp/kind-${CLUSTER_NAME}.kubeconfig"

if [ ! -f "${KUBECONFIG_FILE}" ]; then
    echo "Exporting kubeconfig for ${CLUSTER_NAME}..."
    kind get kubeconfig --name "${CLUSTER_NAME}" > "${KUBECONFIG_FILE}"
fi
export KUBECONFIG="${KUBECONFIG_FILE}"

WAIT=35  # monitoring addon collects every ~30s

echo ""
echo "=== Current pods ==="
kubectl get pods -A --no-headers | wc -l | xargs -I{} echo "{} pods running"
echo ""

echo "=== Step 1: Deploy nginx (3 replicas) ==="
kubectl create deployment nginx-demo --image=nginx:alpine --replicas=3 2>/dev/null \
    || kubectl scale deployment nginx-demo --replicas=3
kubectl rollout status deployment/nginx-demo --timeout=60s
echo "Waiting ${WAIT}s for metrics to update..."
sleep ${WAIT}
echo "Pods now: $(kubectl get pods -A --no-headers | wc -l | xargs)"
echo ""

echo "=== Step 2: Scale up to 8 replicas ==="
kubectl scale deployment nginx-demo --replicas=8
kubectl rollout status deployment/nginx-demo --timeout=60s
echo "Waiting ${WAIT}s for metrics to update..."
sleep ${WAIT}
echo "Pods now: $(kubectl get pods -A --no-headers | wc -l | xargs)"
echo ""

echo "=== Step 3: Scale down to 1 replica ==="
kubectl scale deployment nginx-demo --replicas=1
echo "Waiting ${WAIT}s for metrics to update..."
sleep ${WAIT}
echo "Pods now: $(kubectl get pods -A --no-headers | wc -l | xargs)"
echo ""

read -p "Press Enter to clean up (delete nginx-demo)..."
kubectl delete deployment nginx-demo
echo "Waiting ${WAIT}s for final metrics update..."
sleep ${WAIT}
echo "Pods now: $(kubectl get pods -A --no-headers | wc -l | xargs)"
echo ""
echo "=== Demo complete ==="
