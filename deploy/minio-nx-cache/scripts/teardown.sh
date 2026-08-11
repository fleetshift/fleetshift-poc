#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# Tear down the MinIO Nx cache deployment from OpenShift
#
# Removes the MinIO deployment, service, routes, secrets, PVC,
# and the minio-nx-cache namespace.
#
# Usage:
#   ./teardown.sh
# ------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../scripts/common.sh
source "$(cd "$SCRIPT_DIR/../../scripts" && pwd)/common.sh"

NAMESPACE="minio-nx-cache"

info()  { echo "==> $*"; }
error() { echo "ERROR: $*" >&2; exit 1; }

require_oc_login

echo ""
echo "This will remove the MinIO Nx cache deployment from namespace '${NAMESPACE}'."
echo "All cached data will be PERMANENTLY DELETED."
echo ""
read -rp "Are you sure? (y/N): " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
echo ""

# Step 1: Delete nx-cache-proxy
info "Deleting nx-cache-proxy..."
oc delete deployment nx-cache-proxy -n "${NAMESPACE}" --ignore-not-found
oc delete service nx-cache-proxy -n "${NAMESPACE}" --ignore-not-found
oc delete route nx-cache-proxy -n "${NAMESPACE}" --ignore-not-found

# Step 2: Delete bucket creation job
info "Deleting bucket creation job..."
oc delete job minio-create-bucket -n "${NAMESPACE}" --ignore-not-found

# Step 3: Delete MinIO routes
info "Deleting MinIO routes..."
oc delete route minio-api -n "${NAMESPACE}" --ignore-not-found
oc delete route minio-console -n "${NAMESPACE}" --ignore-not-found

# Step 4: Delete MinIO deployment and service
info "Deleting MinIO deployment..."
oc delete deployment minio -n "${NAMESPACE}" --ignore-not-found
oc delete service minio -n "${NAMESPACE}" --ignore-not-found

# Step 5: Wait for pods to terminate
info "Waiting for pods to terminate..."
oc wait --for=delete pod -l app=minio -n "${NAMESPACE}" --timeout=60s 2>/dev/null || true
oc wait --for=delete pod -l app=nx-cache-proxy -n "${NAMESPACE}" --timeout=60s 2>/dev/null || true

# Step 6: Delete secrets and PVC
info "Deleting secrets..."
oc delete secret minio-credentials -n "${NAMESPACE}" --ignore-not-found
oc delete secret nx-cache-tokens -n "${NAMESPACE}" --ignore-not-found
info "Deleting PVC (this destroys all cached data)..."
oc delete pvc minio-data -n "${NAMESPACE}" --ignore-not-found

# Step 6: Delete namespace
info "Deleting namespace ${NAMESPACE}..."
oc delete namespace "${NAMESPACE}" --ignore-not-found
info "Waiting for namespace deletion..."
oc wait --for=delete namespace/"${NAMESPACE}" --timeout=120s 2>/dev/null || true

echo ""
info "Teardown complete."
