#!/usr/bin/env bash
# Stage 8: One-time infrastructure setup for the target cluster.
#
# Creates:
#   - Target GCP project (if it doesn't exist)
#   - RSA 4096-bit signing keypair + JWKS
#   - WIF pool, OIDC provider, service accounts (via hypershift)
#   - VPC, subnet, Cloud NAT, firewall rules (via hypershift)
#
# Requires: gcloud, openssl, jq, hypershift binary
#
# Reads: config.env (INFRA_ID, TARGET_GCP_PROJECT, REGION, BILLING_ACCOUNT,
#                     GCP_ORG_ID, WORKFORCE_POOL)
#        tmp/keycloak_token.jwt (for Keycloak subject ID)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config INFRA_ID TARGET_GCP_PROJECT REGION BILLING_ACCOUNT GCP_ORG_ID WORKFORCE_POOL GCP_PROJECT

# Pre-flight checks
if ! command -v hypershift &>/dev/null; then
    log_fail "hypershift binary not found in PATH"
    log_step "Build from upstream: https://github.com/openshift/hypershift"
    exit 1
fi

if ! gcloud auth application-default print-access-token &>/dev/null; then
    log_fail "Application Default Credentials (ADC) not configured."
    log_fail "The hypershift binary uses ADC (separate from 'gcloud auth login')."
    log_step "Run: gcloud auth application-default login"
    exit 1
fi

log_header "Stage 8: Infrastructure Setup"

# ── Step 0: Create target GCP project ──────────────────────────────────────

log_step "Step 0: Create and configure target GCP project"

if gcloud projects describe "${TARGET_GCP_PROJECT}" &>/dev/null; then
    log_ok "Project ${TARGET_GCP_PROJECT} already exists"
else
    log_step "Creating project ${TARGET_GCP_PROJECT}..."
    gcloud projects create "${TARGET_GCP_PROJECT}" --organization="${GCP_ORG_ID}"
    log_ok "Project created"
fi

log_step "Linking billing account..."
gcloud billing projects link "${TARGET_GCP_PROJECT}" \
    --billing-account="${BILLING_ACCOUNT}" 2>/dev/null || true

log_step "Enabling required APIs on target project..."
gcloud services enable \
    compute.googleapis.com \
    iam.googleapis.com \
    cloudresourcemanager.googleapis.com \
    dns.googleapis.com \
    --project="${TARGET_GCP_PROJECT}"
log_ok "Target project APIs enabled"

log_step "Enabling required APIs on broker project (ADC quota project)..."
gcloud services enable \
    cloudresourcemanager.googleapis.com \
    --project="${GCP_PROJECT}"
log_ok "Broker project APIs enabled"

# Grant Workforce principal permissions on the target project
KEYCLOAK_SUB=""
if [[ -f "${TMP_DIR}/keycloak_token.jwt" ]]; then
    KEYCLOAK_SUB=$(cat "${TMP_DIR}/keycloak_token.jwt" | cut -d'.' -f2 | \
        (base64 -d 2>/dev/null || true) | jq -r '.sub // empty')
fi
if [[ -z "${KEYCLOAK_SUB}" ]]; then
    log_warn "Could not extract Keycloak subject ID from tmp/keycloak_token.jwt"
    log_step "Enter your Keycloak subject ID (from JWT 'sub' claim):"
    read -r KEYCLOAK_SUB
fi

WORKFORCE_PRINCIPAL="principal://iam.googleapis.com/locations/global/workforcePools/${WORKFORCE_POOL}/subject/${KEYCLOAK_SUB}"
log_step "Granting Workforce principal permissions on ${TARGET_GCP_PROJECT}..."
log_step "Principal: ${WORKFORCE_PRINCIPAL}"

gcloud projects add-iam-policy-binding "${TARGET_GCP_PROJECT}" \
    --member="${WORKFORCE_PRINCIPAL}" \
    --role="roles/editor" \
    --condition=None --quiet 2>/dev/null
gcloud projects add-iam-policy-binding "${TARGET_GCP_PROJECT}" \
    --member="${WORKFORCE_PRINCIPAL}" \
    --role="roles/iam.securityAdmin" \
    --condition=None --quiet 2>/dev/null
log_ok "Workforce principal granted editor + iam.securityAdmin on ${TARGET_GCP_PROJECT}"

echo ""

# ── Step 1: Generate RSA signing keypair + JWKS ────────────────────────────

log_step "Step 1: Generate RSA 4096-bit signing keypair"

if [[ -f "${TMP_DIR}/signing-key.pem" ]]; then
    log_warn "Signing key already exists at tmp/signing-key.pem — reusing"
else
    openssl genrsa -out "${TMP_DIR}/signing-key.pem" 4096 2>/dev/null
    log_ok "RSA keypair generated"
fi

# Base64-encode the PEM for the cluster spec
base64 -w0 < "${TMP_DIR}/signing-key.pem" > "${TMP_DIR}/signing-key-base64.txt"
log_ok "Signing key base64-encoded"

# Generate JWKS from public key
log_step "Generating JWKS from public key..."

# Extract modulus (n) as base64url
MODULUS_HEX=$(openssl rsa -in "${TMP_DIR}/signing-key.pem" -modulus -noout 2>/dev/null | sed 's/Modulus=//')
N_BASE64URL=$(echo "${MODULUS_HEX}" | xxd -r -p | base64 -w0 | tr '+/' '-_' | tr -d '=')

# Exponent (e) — standard RSA public exponent 65537 = AQAB in base64url
E_BASE64URL="AQAB"

# Compute kid as SHA256 of DER-encoded public key, then base64url
KID=$(openssl rsa -in "${TMP_DIR}/signing-key.pem" -pubout -outform DER 2>/dev/null | \
    openssl dgst -sha256 -binary | base64 -w0 | tr '+/' '-_' | tr -d '=')

# Assemble JWKS
jq -n \
    --arg n "${N_BASE64URL}" \
    --arg e "${E_BASE64URL}" \
    --arg kid "${KID}" \
    '{keys: [{use: "sig", kty: "RSA", kid: $kid, alg: "RS256", n: $n, e: $e}]}' \
    > "${TMP_DIR}/jwks.json"

log_ok "JWKS written to tmp/jwks.json (kid: ${KID:0:16}...)"

echo ""

# ── Step 2: Create IAM infrastructure ──────────────────────────────────────

log_step "Step 2: Create IAM infrastructure (WIF pool, OIDC provider, service accounts)"

if [[ -f "${TMP_DIR}/iam-config.json" ]]; then
    log_warn "iam-config.json already exists — skipping (delete to re-create)"
else
    hypershift create iam gcp \
        --infra-id "${INFRA_ID}" \
        --project-id "${TARGET_GCP_PROJECT}" \
        --oidc-jwks-file "${TMP_DIR}/jwks.json" \
        --output-file "${TMP_DIR}/iam-config.json"
    log_ok "IAM infrastructure created"
fi

log_step "IAM config:"
jq '{projectId, projectNumber, infraId, poolId: .workloadIdentityPool.poolId, serviceAccounts: (.serviceAccounts | keys)}' \
    "${TMP_DIR}/iam-config.json"

echo ""

# ── Step 3: Create network infrastructure ──────────────────────────────────

log_step "Step 3: Create network infrastructure (VPC, subnet, NAT, firewall)"

if [[ -f "${TMP_DIR}/infra-config.json" ]]; then
    log_warn "infra-config.json already exists — skipping (delete to re-create)"
else
    hypershift create infra gcp \
        --infra-id "${INFRA_ID}" \
        --project-id "${TARGET_GCP_PROJECT}" \
        --region "${REGION}" \
        --output-file "${TMP_DIR}/infra-config.json"
    log_ok "Network infrastructure created"
fi

log_step "Infra config:"
jq '{region, projectId, networkName, subnetName}' "${TMP_DIR}/infra-config.json"

echo ""
log_ok "Stage 8 complete"
log_step "Outputs in tmp/: signing-key.pem, signing-key-base64.txt, jwks.json, iam-config.json, infra-config.json"
log_step "Next: run 01-get-keycloak-token.sh (if not already authenticated)"
