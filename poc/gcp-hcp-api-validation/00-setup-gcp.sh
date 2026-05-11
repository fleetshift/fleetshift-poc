#!/usr/bin/env bash
# Stage 0: Create GCP Workforce Identity Pool and add Keycloak as OIDC provider.
# Run once. Requires gcloud CLI authenticated with org-level IAM admin.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GCP_PROJECT GCP_ORG_ID WORKFORCE_POOL WORKFORCE_PROVIDER KEYCLOAK_URL KEYCLOAK_REALM KEYCLOAK_CLIENT_ID

ISSUER_URI="${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}"

log_header "Stage 0: GCP Workforce Identity Federation Setup"

# --- Step 1: Create Workforce Identity Pool ---
log_step "Creating Workforce Identity Pool: ${WORKFORCE_POOL}"

if gcloud iam workforce-pools describe "${WORKFORCE_POOL}" \
    --location="global" &>/dev/null; then
    log_warn "Pool '${WORKFORCE_POOL}' already exists, skipping creation"
else
    gcloud iam workforce-pools create "${WORKFORCE_POOL}" \
        --location="global" \
        --organization="${GCP_ORG_ID}" \
        --display-name="OME POC Workforce Pool" \
        --description="POC: test Keycloak-to-GCP token exchange for OME"
    log_ok "Pool created"
fi

# --- Step 2: Add Keycloak as OIDC provider ---
log_step "Adding Keycloak OIDC provider: ${WORKFORCE_PROVIDER}"
log_step "Issuer URI: ${ISSUER_URI}"
log_step "Client ID: ${KEYCLOAK_CLIENT_ID}"

if gcloud iam workforce-pools providers describe "${WORKFORCE_PROVIDER}" \
    --location="global" \
    --workforce-pool="${WORKFORCE_POOL}" &>/dev/null; then
    log_warn "Provider '${WORKFORCE_PROVIDER}' already exists, skipping creation"
else
    gcloud iam workforce-pools providers create-oidc "${WORKFORCE_PROVIDER}" \
        --location="global" \
        --workforce-pool="${WORKFORCE_POOL}" \
        --issuer-uri="${ISSUER_URI}" \
        --client-id="${KEYCLOAK_CLIENT_ID}" \
        --attribute-mapping="google.subject=assertion.sub,attribute.email=assertion.email" \
        --web-sso-response-type="id-token" \
        --web-sso-assertion-claims-behavior="only-id-token-claims"
    log_ok "Provider created"
fi

# --- Step 3: Verify ---
log_step "Verifying setup..."

echo ""
log_step "Pool details:"
gcloud iam workforce-pools describe "${WORKFORCE_POOL}" \
    --location="global" \
    --format="yaml(name,displayName,state)"

echo ""
log_step "Provider details:"
gcloud iam workforce-pools providers describe "${WORKFORCE_PROVIDER}" \
    --location="global" \
    --workforce-pool="${WORKFORCE_POOL}" \
    --format="yaml(name,issuerUri,oidcConfig,attributeMapping,state)"

echo ""
log_ok "GCP Workforce IdF setup complete"
log_step "STS audience for token exchange:"
echo "  //iam.googleapis.com/locations/global/workforcePools/${WORKFORCE_POOL}/providers/${WORKFORCE_PROVIDER}"

echo ""
log_step "Next: run 01-get-keycloak-token.sh"
