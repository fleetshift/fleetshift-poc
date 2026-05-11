#!/usr/bin/env bash
# Teardown: Delete GCP resources created by this POC.
# Safe to run multiple times — skips resources that don't exist.
#
# NOTE: The broker service account (BROKER_SA_EMAIL) is NOT deleted here.
# It is a persistent resource managed separately via prerequisites.md.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config GCP_PROJECT GCP_ORG_ID WORKFORCE_POOL WORKFORCE_PROVIDER

log_header "Teardown: Cleaning Up GCP Resources"

echo "This will delete:"
echo "  - Workforce Identity Pool: ${WORKFORCE_POOL}"
echo "  - Workforce Provider: ${WORKFORCE_PROVIDER}"
echo ""
echo "This will NOT delete:"
echo "  - Broker SA: ${BROKER_SA_EMAIL:-not configured}"
echo ""

read -p "Continue? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_warn "Aborted."
    exit 0
fi

# --- Workforce IdF ---
log_step "Deleting Workforce Identity Pool provider: ${WORKFORCE_PROVIDER}"
gcloud iam workforce-pools providers delete "${WORKFORCE_PROVIDER}" \
    --location="global" \
    --workforce-pool="${WORKFORCE_POOL}" \
    --organization="${GCP_ORG_ID}" \
    --quiet 2>/dev/null && log_ok "Deleted" || log_warn "Not found or already deleted"

log_step "Deleting Workforce Identity Pool: ${WORKFORCE_POOL}"
gcloud iam workforce-pools delete "${WORKFORCE_POOL}" \
    --location="global" \
    --organization="${GCP_ORG_ID}" \
    --quiet 2>/dev/null && log_ok "Deleted" || log_warn "Not found or already deleted"

# --- Local temp files ---
log_step "Cleaning up local tmp/ directory"
rm -rf "${TMP_DIR}"/*
log_ok "tmp/ cleaned"

echo ""
log_ok "Teardown complete"
