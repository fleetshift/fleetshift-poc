#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# FleetShift Kubernetes Deployment
#
# Deploys FleetShift to an OpenShift cluster using Kustomize manifests.
# Called by 'task kubernetes:deploy'.
#
# Steps:
#   1. Applies Kustomize manifests (namespace, postgres, server, routes)
#   2. Waits for PostgreSQL to be ready
#   3. Imports container images from quay.io via ImageStreams
#   4. Configures image change triggers on the server deployment
#   5. Waits for the fleetshift-server deployment to be available
#   6. Runs the auth-setup job (creates OIDC client config)
#
# Prerequisites:
#   - 'oc' CLI installed
#   - Logged into an OpenShift cluster (oc login)
#   - Container images pushed to quay.io (task image:build && task image:push)
#
# Usage:
#   ./deploy.sh
# ------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_DIR="$(cd "${K8S_DIR}/../.." && pwd)"
NAMESPACE="fleetshift"

echo "=== FleetShift Kubernetes Deployment ==="
echo "Manifests: ${K8S_DIR}"
echo ""

# --- Preconditions ---
command -v oc >/dev/null 2>&1 || { echo "ERROR: 'oc' CLI not found."; exit 1; }
timeout 5 oc whoami &>/dev/null || { echo "ERROR: Not logged in to OpenShift. Run 'oc login' first."; exit 1; }
[ -f "${ROOT_DIR}/.env" ] || { echo "ERROR: ${ROOT_DIR}/.env not found. Copy from .env.template."; exit 1; }

# --- Generate config/secrets from .env ---
echo "Generating config.env and secrets.env from .env..."
set -a
source "${ROOT_DIR}/.env"
set +a

cat > "${K8S_DIR}/config.env" <<EOF
OIDC_ISSUER_URL=${OIDC_ISSUER_URL}
OIDC_UI_CLIENT_ID=${OIDC_UI_CLIENT_ID:-fleetshift-ui}
OIDC_CLI_CLIENT_ID=${OIDC_CLI_CLIENT_ID}
OIDC_AUDIENCE=${OIDC_AUDIENCE}
KEY_ENROLLMENT_CLIENT_ID=${KEY_ENROLLMENT_CLIENT_ID}
KEY_REGISTRY_ID=${KEY_REGISTRY_ID}
KEY_REGISTRY_SUBJECT_EXPR=${KEY_REGISTRY_SUBJECT_EXPR}
EOF

cat > "${K8S_DIR}/secrets.env" <<EOF
POSTGRES_USER=${POSTGRES_USER}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
POSTGRES_DB=${POSTGRES_DB}
DATABASE_URL=postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable
EOF

# --- Apply manifests ---
echo "Applying Kustomize manifests..."
oc apply -k "${K8S_DIR}"

# --- Wait for PostgreSQL ---
echo "Waiting for PostgreSQL to be ready..."
oc wait -n "${NAMESPACE}" \
  statefulset/postgres \
  --for=jsonpath='{.status.readyReplicas}'=1 \
  --timeout=120s

# --- Force image import ---
echo "Importing images from quay.io..."
oc import-image fleetshift-server:latest -n "${NAMESPACE}" --confirm 2>/dev/null || true
oc import-image fleetshift-web:latest -n "${NAMESPACE}" --confirm 2>/dev/null || true

# --- Ensure triggers ---
echo "Setting image triggers..."
oc set triggers deployment/fleetshift-server -n "${NAMESPACE}" \
  --from-image=fleetshift-server:latest -c fleetshift-server 2>/dev/null || true
oc set triggers deployment/fleetshift-server -n "${NAMESPACE}" \
  --from-image=fleetshift-web:latest -c web-builder --containers=web-builder 2>/dev/null || true

# --- Wait for server ---
echo "Waiting for fleetshift-server to be ready..."
oc wait -n "${NAMESPACE}" \
  deployment/fleetshift-server \
  --for=condition=Available \
  --timeout=300s

# --- Auth setup ---
echo "Checking auth-setup job..."
JOB_STATUS=$(oc get job auth-setup -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "")
if [ "${JOB_STATUS}" = "1" ]; then
  echo "Auth setup already completed."
else
  echo "Deleting previous auth-setup job (if any) and re-running..."
  oc delete job auth-setup -n "${NAMESPACE}" --ignore-not-found=true
  oc apply -k "${K8S_DIR}" -l "batch.kubernetes.io/job-name=auth-setup" 2>/dev/null || \
    oc apply -f "${K8S_DIR}/auth-setup/job.yaml" -n "${NAMESPACE}"
  echo "Waiting for auth-setup to complete..."
  oc wait -n "${NAMESPACE}" job/auth-setup --for=condition=Complete --timeout=120s || \
    echo "WARNING: Auth setup did not complete. Check logs: oc logs -n ${NAMESPACE} job/auth-setup"
fi

# --- Summary ---
echo ""
echo "=== Deployment Complete ==="
HTTP_ROUTE=$(oc get route web -n "${NAMESPACE}" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
GRPC_ROUTE=$(oc get route grpc -n "${NAMESPACE}" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
echo "  Frontend + API: https://${HTTP_ROUTE}"
echo "  gRPC:           ${GRPC_ROUTE}:443"
echo ""
echo "  OIDC redirect URI (must be registered in your IdP's 'fleetshift-ui' client):"
echo "    https://${HTTP_ROUTE}/*"
echo "    This only needs to be set once"
echo "    Use task k8s:register-redirect if this is the first deploy in this cluster"
echo ""
echo "  Status:            task kubernetes:status"
echo "  View logs:         task kubernetes:logs"
echo "  Image override:    task kubernetes:set-image TAG=<tag>"
echo "  Force reimport:    task kubernetes:import-images"
echo "  Register redirect: task kubernetes:register-redirect  (Keycloak only)"
echo "  Tear down:         task kubernetes:teardown"
