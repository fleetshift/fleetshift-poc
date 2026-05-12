#!/usr/bin/env bash
# Fallback Stage 2b: Set up Workload Identity Federation with Service Account.
#
# WARNING: This path violates zero-trust — the gateway sees a service account,
# not the actual user. Only use this as a diagnostic fallback if Workforce IdF fails.
#
# Creates: Workload Identity Pool, Keycloak provider, Service Account, IAM bindings.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GCP_PROJECT KEYCLOAK_URL KEYCLOAK_REALM KEYCLOAK_CLIENT_ID

ISSUER_URI="${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}"
WORKLOAD_POOL="fallback-workload-pool"
WORKLOAD_PROVIDER="keycloak-workload"
SA_NAME="fallback-gateway-sa"
SA_EMAIL="${SA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com"

log_header "Fallback Stage 2b: Workload Identity Federation Setup"

log_warn "╔══════════════════════════════════════════════════════════════╗"
log_warn "║  SECURITY TRADEOFF: Workload IdF uses SA impersonation.    ║"
log_warn "║  The API will see the SA, not the user. This violates      ║"
log_warn "║  OME's zero-trust model. For diagnostic comparison only.   ║"
log_warn "╚══════════════════════════════════════════════════════════════╝"
echo ""

# --- Step 1: Create Workload Identity Pool (project-level) ---
log_step "Creating Workload Identity Pool: ${WORKLOAD_POOL}"

if gcloud iam workload-identity-pools describe "${WORKLOAD_POOL}" \
    --location="global" \
    --project="${GCP_PROJECT}" &>/dev/null; then
    log_warn "Pool already exists, skipping"
else
    gcloud iam workload-identity-pools create "${WORKLOAD_POOL}" \
        --location="global" \
        --project="${GCP_PROJECT}" \
        --display-name="Workload Pool (fallback)"
    log_ok "Pool created"
fi

# --- Step 2: Add Keycloak OIDC provider ---
log_step "Adding Keycloak OIDC provider: ${WORKLOAD_PROVIDER}"

if gcloud iam workload-identity-pools providers describe "${WORKLOAD_PROVIDER}" \
    --location="global" \
    --workload-identity-pool="${WORKLOAD_POOL}" \
    --project="${GCP_PROJECT}" &>/dev/null; then
    log_warn "Provider already exists, skipping"
else
    gcloud iam workload-identity-pools providers create-oidc "${WORKLOAD_PROVIDER}" \
        --location="global" \
        --workload-identity-pool="${WORKLOAD_POOL}" \
        --project="${GCP_PROJECT}" \
        --issuer-uri="${ISSUER_URI}" \
        --attribute-mapping="google.subject=assertion.sub,attribute.email=assertion.email" \
        --allowed-audiences="${KEYCLOAK_CLIENT_ID}"
    log_ok "Provider created"
fi

# --- Step 3: Create service account ---
log_step "Creating service account: ${SA_NAME}"

if gcloud iam service-accounts describe "${SA_EMAIL}" \
    --project="${GCP_PROJECT}" &>/dev/null; then
    log_warn "Service account already exists, skipping"
else
    gcloud iam service-accounts create "${SA_NAME}" \
        --project="${GCP_PROJECT}" \
        --display-name="Gateway SA (fallback)"
    log_ok "Service account created: ${SA_EMAIL}"
fi

# --- Step 4: Allow the workload pool to impersonate the SA ---
log_step "Granting workload identity user role to pool principal"

PROJECT_NUMBER=$(gcloud projects describe "${GCP_PROJECT}" --format='value(projectNumber)')
POOL_PRINCIPAL="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${WORKLOAD_POOL}/*"

gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
    --project="${GCP_PROJECT}" \
    --role="roles/iam.workloadIdentityUser" \
    --member="${POOL_PRINCIPAL}" \
    --condition=None \
    2>/dev/null

log_ok "IAM binding created"

# --- Save config for next scripts ---
echo ""
log_step "Workload IdF config (add to config.env if running 02c manually):"
echo "  WORKLOAD_POOL=\"${WORKLOAD_POOL}\""
echo "  WORKLOAD_PROVIDER=\"${WORKLOAD_PROVIDER}\""
echo "  WORKLOAD_SA_EMAIL=\"${SA_EMAIL}\""

# Write to tmp for auto-pickup by 02c
cat > "${TMP_DIR}/workload_config.env" <<WEOF
WORKLOAD_POOL="${WORKLOAD_POOL}"
WORKLOAD_PROVIDER="${WORKLOAD_PROVIDER}"
WORKLOAD_SA_EMAIL="${SA_EMAIL}"
WEOF

echo ""
log_ok "Fallback stage 2b complete"
log_step "Next: run 02c-workload-sts-exchange.sh"
