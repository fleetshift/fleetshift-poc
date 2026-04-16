#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="keycloak-prod"
KEYCLOAK_CR_NAME="keycloak"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

timeout 5 oc whoami &>/dev/null || error "Not logged in to OpenShift. Run 'oc login' first."

echo ""
echo "This will remove the Keycloak deployment from namespace '${NAMESPACE}'."
echo "All data (including the PostgreSQL database) will be PERMANENTLY DELETED."
echo ""
read -rp "Are you sure? (y/N): " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
echo ""

# Step 1: Delete realm import
info "Deleting KeycloakRealmImport..."
oc delete keycloakrealmimport fleetshift-realm -n "${NAMESPACE}" --ignore-not-found

# Step 2: Delete Keycloak CR
info "Deleting Keycloak CR..."
oc delete keycloak "${KEYCLOAK_CR_NAME}" -n "${NAMESPACE}" --ignore-not-found
# Wait for Keycloak pods to terminate
info "Waiting for Keycloak pods to terminate..."
oc wait --for=delete pod -l app=keycloak -n "${NAMESPACE}" --timeout=120s 2>/dev/null || true

# Step 3: Delete PostgreSQL
info "Deleting PostgreSQL StatefulSet..."
oc delete statefulset postgres -n "${NAMESPACE}" --ignore-not-found
oc delete service postgres -n "${NAMESPACE}" --ignore-not-found
info "Deleting PostgreSQL PVC..."
oc delete pvc postgres-data-postgres-0 -n "${NAMESPACE}" --ignore-not-found

# Step 4: Preserve TLS cert (avoid Let's Encrypt rate limits on re-deploy)
if oc get secret keycloak-tls -n "${NAMESPACE}" &>/dev/null; then
    info "Backing up TLS certificate to keycloak-tls-backup secret in cert-manager-operator namespace..."
    oc get secret keycloak-tls -n "${NAMESPACE}" -o json \
        | jq 'del(.metadata.namespace, .metadata.resourceVersion, .metadata.uid, .metadata.creationTimestamp, .metadata.ownerReferences)' \
        | jq '.metadata.name = "keycloak-tls-backup"' \
        | oc apply -n cert-manager-operator -f -
    info "TLS certificate backed up."
fi

# Step 5: Delete secrets
info "Deleting secrets..."
oc delete secret keycloak-db-credentials -n "${NAMESPACE}" --ignore-not-found
oc delete secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" --ignore-not-found
oc delete secret keycloak-tls -n "${NAMESPACE}" --ignore-not-found

# Step 6: Delete TLS resources
info "Deleting Certificate..."
oc delete certificate keycloak-tls -n "${NAMESPACE}" --ignore-not-found
info "Deleting ClusterIssuer..."
oc delete clusterissuer letsencrypt-prod --ignore-not-found

# Step 7: Delete namespace
info "Deleting namespace ${NAMESPACE}..."
oc delete namespace "${NAMESPACE}" --ignore-not-found
info "Waiting for namespace deletion..."
oc wait --for=delete namespace/"${NAMESPACE}" --timeout=120s 2>/dev/null || true

# Step 8: Optionally uninstall operators
echo ""
read -rp "Uninstall cert-manager operator? (y/N): " remove_cm
if [[ "$remove_cm" =~ ^[Yy]$ ]]; then
    info "Removing cert-manager operator..."
    oc delete subscription openshift-cert-manager-operator -n cert-manager-operator --ignore-not-found
    cm_csv=$(oc get csv -n cert-manager-operator \
        -l operators.coreos.com/openshift-cert-manager-operator.cert-manager-operator \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [[ -n "$cm_csv" ]]; then
        oc delete csv "$cm_csv" -n cert-manager-operator --ignore-not-found
    fi
    oc delete namespace cert-manager-operator --ignore-not-found
    info "cert-manager operator removed."
fi

read -rp "Uninstall RHBK operator? (y/N): " remove_rhbk
if [[ "$remove_rhbk" =~ ^[Yy]$ ]]; then
    info "Removing RHBK operator..."
    oc delete subscription rhbk-operator -n rhbk-operator --ignore-not-found
    rhbk_csv=$(oc get csv -n rhbk-operator \
        -l operators.coreos.com/rhbk-operator.rhbk-operator \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [[ -n "$rhbk_csv" ]]; then
        oc delete csv "$rhbk_csv" -n rhbk-operator --ignore-not-found
    fi
    oc delete namespace rhbk-operator --ignore-not-found
    info "RHBK operator removed."
fi

echo ""
info "Teardown complete."
