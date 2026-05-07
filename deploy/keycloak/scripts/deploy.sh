#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# Deploy a production-like Keycloak instance on OpenShift
#
# Installs cert-manager and RHBK operators, provisions TLS (Let's
# Encrypt or self-signed), deploys PostgreSQL + Keycloak, imports the
# FleetShift realm with generated user passwords, and configures the
# ocp-console client secret.
#
# Idempotent — safe to re-run if something fails partway through.
#
# Usage:
#   ./deploy.sh --acme-email you@example.com
#   ./deploy.sh --acme-email you@example.com --base-domain example.com
#   ./deploy.sh --fresh-cert --acme-email you@example.com
# ------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEYCLOAK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="keycloak-prod"
KEYCLOAK_CR_NAME="keycloak"
ACME_EMAIL=""
FRESH_CERT="${FRESH_CERT:-false}"
BASE_DOMAINS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --acme-email) ACME_EMAIL="$2"; shift 2 ;;
    --base-domain) BASE_DOMAINS+=("$2"); shift 2 ;;
    --fresh-cert) FRESH_CERT=true; shift ;;
    *) break ;;
  esac
done

info()  { echo "==> $*"; }
warn()  { echo "WARNING: $*"; }
error() { echo "ERROR: $*" >&2; exit 1; }

wait_for_csv() {
    local namespace="$1"
    local label="$2"
    local timeout="${3:-300}"
    local elapsed=0
    local phase
    while [[ $elapsed -lt $timeout ]]; do
        phase=$(oc get csv -n "$namespace" -l "$label" \
            -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "")
        if [[ "$phase" == "Succeeded" ]]; then
            return 0
        fi
        sleep 10
        elapsed=$((elapsed + 10))
    done
    return 1
}

render_template() {
    local src="$1"
    local out="$2"

    sed \
        -e "s|ACME_EMAIL|${ACME_EMAIL}|g" \
        -e "s|KEYCLOAK_HOST|${KEYCLOAK_HOST}|g" \
        "${src}" > "${out}"
}

apply_template() {
    local src="$1"
    local namespace="${2:-}"
    local tmp

    tmp="$(mktemp)"
    render_template "${src}" "${tmp}"

    if [[ -n "${namespace}" ]]; then
        oc apply -n "${namespace}" -f "${tmp}"
    else
        oc apply -f "${tmp}"
    fi

    rm -f "${tmp}"
}

wait_for_cert_manager_webhook_trust() {
    local validation_manifest="$1"
    local timeout="${2:-180}"
    local elapsed=0
    local ca_bundle=""
    local tmp

    tmp="$(mktemp)"
    render_template "${validation_manifest}" "${tmp}"

    while [[ $elapsed -lt $timeout ]]; do
        ca_bundle="$(oc get validatingwebhookconfiguration cert-manager-webhook \
            -o jsonpath='{.webhooks[0].clientConfig.caBundle}' 2>/dev/null || true)"

        if [[ -n "${ca_bundle}" ]] && oc apply --dry-run=server -f "${tmp}" >/dev/null 2>&1; then
            rm -f "${tmp}"
            return 0
        fi

        sleep 5
        elapsed=$((elapsed + 5))
    done

    rm -f "${tmp}"
    return 1
}

# --- Step 1: Preflight checks ---
info "Checking prerequisites..."
command -v oc &>/dev/null || error "'oc' CLI not found in PATH."
command -v jq &>/dev/null || error "'jq' not found in PATH."
command -v openssl &>/dev/null || error "'openssl' not found in PATH."
timeout 5 oc whoami &>/dev/null || error "Not logged in to OpenShift. Run 'oc login' first."

APPS_DOMAIN=$(oc get ingresses.config/cluster -o jsonpath='{.spec.domain}')
[[ -n "$APPS_DOMAIN" ]] || error "Could not determine cluster apps domain."
KEYCLOAK_HOST="${KEYCLOAK_CR_NAME}-${NAMESPACE}.${APPS_DOMAIN}"
info "Keycloak will be available at: https://${KEYCLOAK_HOST}"

# --- Step 2: Create namespace ---
info "Creating namespace ${NAMESPACE}..."
oc apply -f "${KEYCLOAK_DIR}/manifests/namespace.yaml"

# --- Step 3: Install cert-manager operator ---
info "Installing cert-manager operator..."
if oc get subscription openshift-cert-manager-operator -n cert-manager-operator &>/dev/null; then
    info "cert-manager operator subscription already exists, skipping..."
else
    oc apply -f "${KEYCLOAK_DIR}/manifests/cert-manager-sub.yaml"
fi

info "Waiting for cert-manager operator to be ready..."
if ! wait_for_csv "cert-manager-operator" \
    "operators.coreos.com/openshift-cert-manager-operator.cert-manager-operator" 300; then
    error "Timed out waiting for cert-manager operator."
fi
info "cert-manager operator is ready."

info "Waiting for cert-manager deployments to appear..."
webhook_timeout=120
webhook_elapsed=0
while [[ $webhook_elapsed -lt $webhook_timeout ]]; do
    if oc get deployment cert-manager-webhook -n cert-manager &>/dev/null; then
        break
    fi
    sleep 5
    webhook_elapsed=$((webhook_elapsed + 5))
done

if [[ $webhook_elapsed -ge $webhook_timeout ]]; then
    warn "cert-manager-webhook deployment not found after ${webhook_timeout}s — certificate issuance may fail."
else
    info "Waiting for cert-manager webhook to be ready..."
    oc wait --for=condition=Available deployment/cert-manager -n cert-manager --timeout=120s
    oc wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=120s
    oc wait --for=condition=Available deployment/cert-manager-cainjector -n cert-manager --timeout=120s
    info "cert-manager is fully ready."
fi

# --- Step 4: TLS certificate ---
# Priority: 1) restore from backup  2) Let's Encrypt (first deploy only)  3) self-signed
# Use --fresh-cert to skip backup and force a new Let's Encrypt request.
CERT_READY=false

if [[ "$FRESH_CERT" != "true" ]] && oc get secret keycloak-tls-backup -n cert-manager-operator &>/dev/null; then
    info "Restoring TLS certificate from backup..."
    oc get secret keycloak-tls-backup -n cert-manager-operator -o json \
        | jq 'del(.metadata.namespace, .metadata.resourceVersion, .metadata.uid, .metadata.creationTimestamp)' \
        | jq '.metadata.name = "keycloak-tls"' \
        | oc apply -n "${NAMESPACE}" -f -
    info "TLS certificate restored from backup."
    CERT_READY=true
fi

if [[ "$CERT_READY" != "true" && -n "$ACME_EMAIL" ]]; then
    info "No backup found. Requesting certificate from Let's Encrypt..."
    info "Waiting for cert-manager webhook CA injection and API trust..."
    wait_for_cert_manager_webhook_trust "${KEYCLOAK_DIR}/manifests/cluster-issuer.yaml" 180 || \
        error "Timed out waiting for cert-manager webhook trust to become ready."

    apply_template "${KEYCLOAK_DIR}/manifests/cluster-issuer.yaml"
    apply_template "${KEYCLOAK_DIR}/manifests/certificate.yaml" "${NAMESPACE}"

    info "Waiting for TLS certificate to be issued (up to 3 minutes)..."
    cert_timeout=180
    cert_elapsed=0
    while [[ $cert_elapsed -lt $cert_timeout ]]; do
        cert_ready=$(oc get certificate keycloak-tls -n "${NAMESPACE}" \
            -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
        if [[ "$cert_ready" == "True" ]]; then
            info "TLS certificate issued successfully."
            CERT_READY=true
            break
        fi
        sleep 10
        cert_elapsed=$((cert_elapsed + 10))
    done
    if [[ "$CERT_READY" != "true" ]]; then
        warn "Let's Encrypt certificate not issued within timeout."
        oc delete certificate keycloak-tls -n "${NAMESPACE}" --ignore-not-found
    fi
fi

# Last resort: self-signed certificate
if [[ "$CERT_READY" != "true" ]]; then
    if oc get secret keycloak-tls -n "${NAMESPACE}" &>/dev/null; then
        info "TLS secret already exists, skipping..."
    else
        warn "Generating self-signed certificate (browser will show warning)..."
        TMPDIR=$(mktemp -d)
        openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
            -keyout "${TMPDIR}/tls.key" -out "${TMPDIR}/tls.crt" \
            -days 365 -nodes \
            -subj "/CN=keycloak" \
            -addext "subjectAltName=DNS:${KEYCLOAK_HOST}" 2>/dev/null
        oc create secret tls keycloak-tls \
            --cert="${TMPDIR}/tls.crt" --key="${TMPDIR}/tls.key" \
            -n "${NAMESPACE}"
        rm -rf "${TMPDIR}"
        warn "Self-signed TLS certificate created."
    fi
fi

# --- Step 5: Install RHBK operator ---
info "Installing RHBK operator..."
if oc get subscription rhbk-operator -n rhbk-operator &>/dev/null; then
    info "RHBK operator subscription already exists, skipping..."
else
    oc apply -f "${KEYCLOAK_DIR}/manifests/rhbk-sub.yaml"
fi

info "Waiting for RHBK operator to be ready..."
if ! wait_for_csv "rhbk-operator" \
    "operators.coreos.com/rhbk-operator.rhbk-operator" 300; then
    error "Timed out waiting for RHBK operator."
fi
info "RHBK operator is ready."

# --- Step 6: Generate database credentials ---
if oc get secret keycloak-db-credentials -n "${NAMESPACE}" &>/dev/null; then
    info "Database credentials already exist, skipping generation..."
    DB_PASSWORD="(existing — check secret keycloak-db-credentials)"
else
    info "Generating database credentials..."
    DB_USER="keycloak"
    DB_PASSWORD=$(openssl rand -base64 48 | tr -dc 'a-zA-Z0-9' | head -c 24)
    DB_NAME="keycloak"

    oc create secret generic keycloak-db-credentials \
        --from-literal=username="${DB_USER}" \
        --from-literal=password="${DB_PASSWORD}" \
        --from-literal=database="${DB_NAME}" \
        -n "${NAMESPACE}" --dry-run=client -o yaml | oc apply -f -

    info "Database credentials created."
fi

# Generate realm user passwords (used during realm import)
OPS_PASSWORD=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16)
DEV_PASSWORD=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16)

# --- Step 7: Deploy PostgreSQL ---
info "Deploying PostgreSQL..."
oc apply -f "${KEYCLOAK_DIR}/manifests/postgres-statefulset.yaml" -n "${NAMESPACE}"

info "Waiting for PostgreSQL to be ready..."
oc wait --for=condition=Ready pod/postgres-0 -n "${NAMESPACE}" --timeout=180s

# --- Step 8: Deploy Keycloak ---
info "Deploying Keycloak..."
sed "s|KEYCLOAK_HOST|${KEYCLOAK_HOST}|g" "${KEYCLOAK_DIR}/manifests/keycloak.yaml" \
    | oc apply -n "${NAMESPACE}" -f -

info "Waiting for Keycloak to be ready (this may take a few minutes)..."
oc wait --for=condition=Ready keycloak/"${KEYCLOAK_CR_NAME}" \
    -n "${NAMESPACE}" --timeout=300s

# --- Step 9: Import realm ---
info "Importing FleetShift realm..."

REALM_JSON=$(jq \
    --arg ops "$OPS_PASSWORD" \
    --arg dev "$DEV_PASSWORD" \
    '.users |= map(
        if .username == "ops-user" then .credentials[0].value = $ops
        elif .username == "dev-user" then .credentials[0].value = $dev
        else .
        end
    )' "${KEYCLOAK_DIR}/fleetshift-realm.json")

cat <<EOF | oc apply -n "${NAMESPACE}" -f -
{
  "apiVersion": "k8s.keycloak.org/v2alpha1",
  "kind": "KeycloakRealmImport",
  "metadata": {
    "name": "fleetshift-realm"
  },
  "spec": {
    "keycloakCRName": "${KEYCLOAK_CR_NAME}",
    "realm": ${REALM_JSON}
  }
}
EOF

info "Waiting for realm import to complete..."
oc wait --for=condition=Done keycloakrealmimport/fleetshift-realm \
    -n "${NAMESPACE}" --timeout=120s 2>/dev/null \
    || warn "Realm import may still be in progress. Check: oc get keycloakrealmimport -n ${NAMESPACE}"

# --- Step 10: Configure user profile attributes ---
# Register github_username in the realm's user profile schema.
# This cannot be done via KeycloakRealmImport — it requires the admin API.
# Realm import restarts Keycloak, so wait for it to be ready again.
info "Waiting for Keycloak to be ready after realm import..."
oc wait --for=condition=Ready keycloak/"${KEYCLOAK_CR_NAME}" \
    -n "${NAMESPACE}" --timeout=300s

info "Configuring user profile attributes..."
KC_URL="https://${KEYCLOAK_HOST}"
ADMIN_USER=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
    -o jsonpath='{.data.username}' | base64 -d)
ADMIN_PASS=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
    -o jsonpath='{.data.password}' | base64 -d)

# Wait for Keycloak to respond to API requests
info "Waiting for Keycloak API to be reachable..."
api_timeout=120
api_elapsed=0
while [[ $api_elapsed -lt $api_timeout ]]; do
    if curl -sk --connect-timeout 5 --max-time 10 \
        "${KC_URL}/realms/master" >/dev/null 2>&1; then
        break
    fi
    sleep 5
    api_elapsed=$((api_elapsed + 5))
done
[[ $api_elapsed -lt $api_timeout ]] || error "Keycloak API not reachable after ${api_timeout}s"

ADMIN_TOKEN=$(curl -sk --connect-timeout 10 --max-time 30 -X POST \
    "${KC_URL}/realms/master/protocol/openid-connect/token" \
    -d "grant_type=password&client_id=admin-cli&username=${ADMIN_USER}&password=${ADMIN_PASS}" \
    | jq -r .access_token)
[[ "$ADMIN_TOKEN" != "null" && -n "$ADMIN_TOKEN" ]] \
    || error "Failed to obtain admin token"

PROFILE_JSON=$(curl -sk --connect-timeout 10 --max-time 30 \
    "${KC_URL}/admin/realms/fleetshift/users/profile" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}")

if echo "$PROFILE_JSON" | jq -e '.attributes[] | select(.name == "github_username")' >/dev/null 2>&1; then
    info "github_username attribute already registered, skipping."
else
    info "Registering github_username in user profile..."
    UPDATED_PROFILE=$(echo "$PROFILE_JSON" | jq '.attributes += [{
        "name": "github_username",
        "displayName": "GitHub Username",
        "validations": {},
        "annotations": {},
        "permissions": {"view": ["admin", "user"], "edit": ["admin"]},
        "multivalued": false
    }]')
    HTTP_CODE=$(curl -sk --connect-timeout 10 --max-time 30 -o /dev/null -w '%{http_code}' -X PUT \
        "${KC_URL}/admin/realms/fleetshift/users/profile" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "$UPDATED_PROFILE")
    [[ "$HTTP_CODE" =~ ^2 ]] || error "Failed to update user profile (HTTP ${HTTP_CODE})"
    info "github_username attribute registered."
fi

# --- Step 11: Configure ocp-console client secret and base domains ---
# The ocp-console client is created by realm import with a placeholder secret.
# Generate a real secret and store it in a Kubernetes secret for retrieval.

OCP_CONSOLE_UUID=$(curl -sk \
    "${KC_URL}/admin/realms/fleetshift/clients?clientId=ocp-console" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq -r '.[0].id')

if [[ "$OCP_CONSOLE_UUID" != "null" && -n "$OCP_CONSOLE_UUID" ]]; then
    # Generate and set a random client secret
    CONSOLE_SECRET=$(openssl rand -hex 32)

    curl -sk -o /dev/null -w '' -X POST \
        "${KC_URL}/admin/realms/fleetshift/clients/${OCP_CONSOLE_UUID}/client-secret" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}"

    # Read back the generated secret
    CONSOLE_SECRET=$(curl -sk \
        "${KC_URL}/admin/realms/fleetshift/clients/${OCP_CONSOLE_UUID}/client-secret" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq -r .value)

    # Store in a Kubernetes secret for retrieval by the OCP agent
    if oc get secret ocp-console-client-secret -n "${NAMESPACE}" >/dev/null 2>&1; then
        info "ocp-console-client-secret already exists, updating..."
        oc delete secret ocp-console-client-secret -n "${NAMESPACE}"
    fi
    oc create secret generic ocp-console-client-secret \
        -n "${NAMESPACE}" \
        --from-literal=clientSecret="${CONSOLE_SECRET}"
    info "ocp-console client secret stored in secret/${NAMESPACE}/ocp-console-client-secret"

    # Add base domain redirect URIs if provided
    for domain in "${BASE_DOMAINS[@]}"; do
        info "Adding base domain: ${domain}"
        "${SCRIPT_DIR}/add-base-domain.sh" --base-domain "$domain"
    done
else
    warn "ocp-console client not found in realm. Console OIDC will not be configured."
fi

# --- Step 12: Print summary ---
ADMIN_PASSWORD=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
    -o jsonpath='{.data.password}' | base64 -d)
ADMIN_USERNAME=$(oc get secret "${KEYCLOAK_CR_NAME}-initial-admin" -n "${NAMESPACE}" \
    -o jsonpath='{.data.username}' | base64 -d)
echo ""
echo "=========================================="
echo "  Keycloak Deployment Complete"
echo "=========================================="
echo ""
echo "  URL:     https://${KEYCLOAK_HOST}"
echo "  Admin:   https://${KEYCLOAK_HOST}/admin"
echo ""
echo "  Admin Console Credentials:"
echo "    Username: ${ADMIN_USERNAME}"
echo "    Password: ${ADMIN_PASSWORD}"
echo ""
echo "  FleetShift Realm Users:"
echo "    ops-user / ${OPS_PASSWORD}"
echo "    dev-user / ${DEV_PASSWORD}"
echo ""
echo "  Run 'task kc:add-user' to create personal dev accounts."
echo ""
echo "  Redirect URIs were removed by the reset."
echo "  Re-run 'task k8s:register-redirect' for OME."
echo ""
if [[ "$CERT_READY" == "true" ]]; then
    echo "  TLS: Trusted certificate (restored from backup or issued by CA)"
else
    echo "  TLS: Self-signed certificate (browser will show warning)"
fi
echo ""
echo "=========================================="
