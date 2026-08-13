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
# ClusterIssuer used to mint a publicly-trusted cert for the proxy Route.
CERT_ISSUER="${CERT_ISSUER:-zerossl-prod}"

info()  { echo "==> $*"; }
warn()  { echo "WARNING: $*"; }
error() { echo "ERROR: $*" >&2; exit 1; }

# --- Step 1: Preflight checks ---
info "Checking prerequisites..."
command -v oc &>/dev/null || error "'oc' CLI not found in PATH."
command -v openssl &>/dev/null || error "'openssl' not found in PATH."
command -v python3 &>/dev/null || error "'python3' not found in PATH (used to build the Route TLS patch)."
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

# --- Step 8b: Issue a trusted TLS cert for the proxy Route ---
# The Nx CLI's HTTP client rejects the cluster's default self-signed ingress
# cert. Mint a publicly-trusted cert via cert-manager and copy it into the
# Route's edge TLS config so the https route works without any client-side
# CA trust. Idempotent: re-running re-applies the Certificate and re-patches.
PROXY_HOST=$(oc get route nx-cache-proxy -n "${NAMESPACE}" -o jsonpath='{.spec.host}')
info "Issuing TLS cert for ${PROXY_HOST} via ClusterIssuer ${CERT_ISSUER}..."
sed -e "s|__PROXY_HOST__|${PROXY_HOST}|" -e "s|__CERT_ISSUER__|${CERT_ISSUER}|" \
    "${MINIO_DIR}/manifests/certificate.yaml" | oc apply -n "${NAMESPACE}" -f -

info "Waiting for certificate to be issued..."
oc wait -n "${NAMESPACE}" certificate/nx-cache-proxy-tls \
    --for=condition=Ready --timeout=180s

info "Patching Route with issued certificate..."
PATCH=$(oc get secret nx-cache-proxy-tls -n "${NAMESPACE}" -o json | python3 -c '
import sys, json, base64
d = json.load(sys.stdin)["data"]
tls = {
    "termination": "edge",
    "insecureEdgeTerminationPolicy": "Redirect",
    "certificate": base64.b64decode(d["tls.crt"]).decode(),
    "key": base64.b64decode(d["tls.key"]).decode(),
}
ca = d.get("ca.crt")
if ca:
    tls["caCertificate"] = base64.b64decode(ca).decode()
print(json.dumps({"spec": {"tls": tls}}))
')
oc patch route nx-cache-proxy -n "${NAMESPACE}" --type=merge -p "${PATCH}"

# --- Step 9: Print summary ---
PROXY_ROUTE=$(oc get route nx-cache-proxy -n "${NAMESPACE}" -o jsonpath='{.spec.host}')
CONSOLE_ROUTE=$(oc get route minio-console -n "${NAMESPACE}" -o jsonpath='{.spec.host}')

echo ""
echo "=========================================="
echo "  Nx Remote Cache Deployment Complete"
echo "=========================================="
echo ""
echo "  Cache Proxy: https://${PROXY_ROUTE}"
echo "  Console:     https://${CONSOLE_ROUTE}"
echo "  Bucket:      nx-cache"
echo ""
echo "  Run 'task minio:credentials' to retrieve bearer tokens."
echo ""
echo "=========================================="
