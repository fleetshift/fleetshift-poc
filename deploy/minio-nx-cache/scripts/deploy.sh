#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# Deploy MinIO + nx-cache-proxy on OpenShift
#
# Creates the namespace, generates credentials and bearer tokens,
# deploys MinIO with persistent storage, deploys the nx-cache-proxy
# (which implements the Nx custom remote cache OpenAPI spec), creates
# the nx-cache bucket, and prints connection info.
#
# The proxy enforces:
#   - Bearer token auth (separate read-only and read-write tokens)
#   - Immutable cache entries (409 on existing keys — CREEP mitigation)
#
# Idempotent — safe to re-run if something fails partway through.
#
# Usage:
#   ./deploy.sh
# ------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=../../scripts/common.sh
source "$(cd "$SCRIPT_DIR/../../scripts" && pwd)/common.sh"

NAMESPACE="minio-nx-cache"

info()  { echo "==> $*"; }
warn()  { echo "WARNING: $*"; }
error() { echo "ERROR: $*" >&2; exit 1; }

# --- Step 1: Preflight checks ---
info "Checking prerequisites..."
command -v oc &>/dev/null || error "'oc' CLI not found in PATH."
command -v openssl &>/dev/null || error "'openssl' not found in PATH."
require_oc_login

# --- Step 2: Create namespace ---
info "Creating namespace ${NAMESPACE}..."
oc apply -f "${MINIO_DIR}/manifests/namespace.yaml"

# --- Step 3: Generate MinIO credentials ---
if oc get secret minio-credentials -n "${NAMESPACE}" &>/dev/null; then
    info "MinIO credentials already exist, skipping generation..."
else
    info "Generating MinIO credentials..."
    ROOT_USER="minio-admin"
    ROOT_PASSWORD=$(openssl rand -base64 48 | tr -dc 'a-zA-Z0-9' | head -c 32)

    oc create secret generic minio-credentials \
        --from-literal=MINIO_ROOT_USER="${ROOT_USER}" \
        --from-literal=MINIO_ROOT_PASSWORD="${ROOT_PASSWORD}" \
        -n "${NAMESPACE}" --dry-run=client -o yaml | oc apply -f -

    info "MinIO credentials created (user: ${ROOT_USER})."
fi

# --- Step 4: Generate cache proxy bearer tokens ---
if oc get secret nx-cache-tokens -n "${NAMESPACE}" &>/dev/null; then
    info "Cache proxy tokens already exist, skipping generation..."
else
    info "Generating cache proxy bearer tokens..."
    READ_TOKEN=$(openssl rand -hex 32)
    WRITE_TOKEN=$(openssl rand -hex 32)

    oc create secret generic nx-cache-tokens \
        --from-literal=read-token="${READ_TOKEN}" \
        --from-literal=write-token="${WRITE_TOKEN}" \
        -n "${NAMESPACE}" --dry-run=client -o yaml | oc apply -f -

    info "Cache proxy tokens created."
fi

# --- Step 5: Deploy MinIO ---
info "Deploying MinIO..."
oc apply -f "${MINIO_DIR}/manifests/deployment.yaml" -n "${NAMESPACE}"
oc apply -f "${MINIO_DIR}/manifests/service.yaml" -n "${NAMESPACE}"

info "Waiting for MinIO to be ready..."
oc rollout status deployment/minio -n "${NAMESPACE}" --timeout=180s

# --- Step 6: Create Routes (MinIO direct access — admin only) ---
info "Creating MinIO Routes..."
oc apply -f "${MINIO_DIR}/manifests/route-api.yaml" -n "${NAMESPACE}"
oc apply -f "${MINIO_DIR}/manifests/route-console.yaml" -n "${NAMESPACE}"

# --- Step 7: Create nx-cache bucket ---
info "Creating nx-cache bucket..."
oc delete job minio-create-bucket -n "${NAMESPACE}" --ignore-not-found
oc apply -f "${MINIO_DIR}/manifests/bucket-job.yaml" -n "${NAMESPACE}"

info "Waiting for bucket creation to complete..."
oc wait -n "${NAMESPACE}" job/minio-create-bucket \
    --for=condition=Complete --timeout=120s

# --- Step 8: Deploy nx-cache-proxy ---
info "Deploying nx-cache-proxy..."
oc apply -f "${MINIO_DIR}/manifests/proxy-deployment.yaml" -n "${NAMESPACE}"

info "Waiting for nx-cache-proxy to be ready..."
oc rollout status deployment/nx-cache-proxy -n "${NAMESPACE}" --timeout=120s

# --- Step 9: Print summary ---
PROXY_ROUTE=$(oc get route nx-cache-proxy -n "${NAMESPACE}" -o jsonpath='{.spec.host}')
CONSOLE_ROUTE=$(oc get route minio-console -n "${NAMESPACE}" -o jsonpath='{.spec.host}')
READ_TOKEN=$(oc get secret nx-cache-tokens -n "${NAMESPACE}" \
    -o jsonpath='{.data.read-token}' | base64 -d)
WRITE_TOKEN=$(oc get secret nx-cache-tokens -n "${NAMESPACE}" \
    -o jsonpath='{.data.write-token}' | base64 -d)

echo ""
echo "=========================================="
echo "  Nx Remote Cache Deployment Complete"
echo "=========================================="
echo ""
echo "  Cache Proxy: https://${PROXY_ROUTE}"
echo "  Console:     https://${CONSOLE_ROUTE}"
echo "  Bucket:      nx-cache"
echo ""
echo "  Bearer Tokens:"
echo "    Read-only:  ${READ_TOKEN}"
echo "    Read-write: ${WRITE_TOKEN}"
echo ""
echo "  Nx configuration (nx.json):"
echo "    {"
echo "      \"remoteCache\": {"
echo "        \"server\": \"https://${PROXY_ROUTE}\""
echo "      }"
echo "    }"
echo ""
echo "  Environment variables:"
echo "    NX_SELF_HOSTED_REMOTE_CACHE_SERVER=https://${PROXY_ROUTE}"
echo ""
echo "  CI (read-only):"
echo "    NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN=${READ_TOKEN}"
echo ""
echo "  Local dev (read-write):"
echo "    NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN=${WRITE_TOKEN}"
echo ""
echo "=========================================="
