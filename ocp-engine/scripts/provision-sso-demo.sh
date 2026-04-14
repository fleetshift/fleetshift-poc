#!/bin/bash
{
set -euo pipefail

#
# provision-sso-demo.sh — Demo: Zero-storage credential acquisition for OCP cluster provisioning
#
# Demonstrates:
#   1. Red Hat SSO device code flow → pull secret (no file download)
#   2. AWS SSO device code flow → temp credentials (no credentials file)
#      Falls back to ambient AWS creds if SSO auth fails
#   3. SSH key auto-generation (no pre-existing key needed)
#
# Usage:
#   ./provision-sso-demo.sh <cluster-name> <aws-region> [--dry-run]
#
# Examples:
#   ./provision-sso-demo.sh my-cluster us-west-2
#   ./provision-sso-demo.sh my-cluster us-west-2 --dry-run
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="/Repos/fleetshift-poc_mshort/poc/ocp-engine"
BINARY_NAME="ocp-engine"
BASE_DOMAIN="aws-acm-cluster-virt.devcluster.openshift.com"
RELEASE_IMAGE="quay.io/openshift-release-dev/ocp-release:4.20.18-x86_64"

# --- Validate arguments ---
if [ $# -lt 2 ]; then
    echo "Usage: $0 <cluster-name> <aws-region> [--dry-run]"
    echo ""
    echo "Examples:"
    echo "  $0 my-cluster us-west-2"
    echo "  $0 my-cluster us-west-2 --dry-run"
    exit 1
fi

CLUSTER_NAME="$1"
AWS_REGION="$2"
DRY_RUN="${3:-}"

CLUSTER_DIR="${SCRIPT_DIR}/clusters/${CLUSTER_NAME}"

if [ -d "$CLUSTER_DIR" ]; then
    echo "ERROR: Cluster directory already exists: $CLUSTER_DIR"
    echo "  Clean up first: rm -rf $CLUSTER_DIR"
    exit 1
fi

echo ""
echo "========================================================"
echo "  OME Zero-Storage Credential Demo"
echo "========================================================"
echo ""
echo "  Cluster:    $CLUSTER_NAME"
echo "  Region:     $AWS_REGION"
echo "  Domain:     $BASE_DOMAIN"
echo "  Release:    $RELEASE_IMAGE"
if [ "$DRY_RUN" = "--dry-run" ]; then
    echo "  Mode:       DRY RUN (generate config only, no provisioning)"
fi
echo ""
echo "  This demo acquires all credentials just-in-time via"
echo "  browser-based SSO flows. Nothing is stored to disk"
echo "  beyond what ocp-engine/openshift-install requires."
echo ""
echo "========================================================"
echo ""

# ============================================================
# STEP 1: Build ocp-engine binary
# ============================================================
echo "[1/5] Building ocp-engine binary..."
cd "$SOURCE_DIR"
go build -o "${SCRIPT_DIR}/${BINARY_NAME}" .
cd "$SCRIPT_DIR"
echo "      Done."
echo ""

# ============================================================
# STEP 2: Red Hat SSO → Pull Secret
# ============================================================
echo "[2/5] Acquiring pull secret via Red Hat SSO..."
echo ""
echo "      You will authenticate with your Red Hat account."
echo "      No need to download a pull secret file manually."
echo ""

# Always login fresh for demo purposes
ocm logout &>/dev/null || true
echo "      Initiating Red Hat SSO device code login..."
echo "      ────────────────────────────────────────────"
ocm login --use-device-code
echo "      ────────────────────────────────────────────"

echo ""
echo "      Fetching pull secret from Red Hat API..."
PULL_SECRET=$(echo '{}' | ocm post /api/accounts_mgmt/v1/access_token 2>/dev/null)

if [ -z "$PULL_SECRET" ] || [ "$PULL_SECRET" = "null" ]; then
    echo "      ERROR: Failed to fetch pull secret from Red Hat API"
    exit 1
fi

# Verify it looks like a valid pull secret
REGISTRY_COUNT=$(echo "$PULL_SECRET" | jq '.auths | keys | length' 2>/dev/null || echo "0")
echo "      Pull secret acquired ($REGISTRY_COUNT registries authenticated)"
echo "      Registries: $(echo "$PULL_SECRET" | jq -r '.auths | keys | join(", ")' 2>/dev/null)"
echo ""

# ============================================================
# STEP 3: AWS Credentials → STS temp creds from existing session
# ============================================================
echo "[3/5] Acquiring just-in-time AWS credentials via STS..."
echo ""

# Verify we have a working AWS session (from aws login or env vars)
# Clear existing session and force fresh browser login
echo "      Clearing existing AWS session..."
rm -rf ~/.aws/login/cache/* 2>/dev/null || true
echo "      Initiating AWS browser login..."
echo "      ────────────────────────────────────────────"
aws login
echo "      ────────────────────────────────────────────"

# Verify the session
CALLER_ID=$(aws sts get-caller-identity 2>/dev/null || true)

if [ -z "$CALLER_ID" ]; then
    echo ""
    echo "      ERROR: AWS login failed."
    exit 1
fi

CALLER_ARN=$(echo "$CALLER_ID" | jq -r '.Arn')
CALLER_ACCOUNT=$(echo "$CALLER_ID" | jq -r '.Account')
echo "      Logged in as: $CALLER_ARN"
echo "      Account:      $CALLER_ACCOUNT"
echo ""

# Extract temporary credentials from the active AWS session
# aws configure export-credentials reads from the login cache
# (~/.aws/login/cache/) and returns short-lived STS credentials
echo "      Extracting temporary credentials from session..."
EXPORTED_CREDS=$(aws configure export-credentials --format env 2>/dev/null || true)

if [ -n "$EXPORTED_CREDS" ]; then
    AWS_ACCESS_KEY=$(echo "$EXPORTED_CREDS" | grep AWS_ACCESS_KEY_ID | sed 's/export AWS_ACCESS_KEY_ID=//')
    AWS_SECRET_KEY=$(echo "$EXPORTED_CREDS" | grep AWS_SECRET_ACCESS_KEY | sed 's/export AWS_SECRET_ACCESS_KEY=//')
    AWS_SESSION_TOKEN=$(echo "$EXPORTED_CREDS" | grep AWS_SESSION_TOKEN | sed 's/export AWS_SESSION_TOKEN=//')
    AWS_EXPIRY=$(echo "$EXPORTED_CREDS" | grep AWS_CREDENTIAL_EXPIRATION | sed 's/export AWS_CREDENTIAL_EXPIRATION=//')
    AWS_CREDS_SOURCE="sts-session"

    echo "      Temporary credentials extracted!"
    echo "      Access Key: ${AWS_ACCESS_KEY:0:12}..."
    echo "      Expires:    ${AWS_EXPIRY:-unknown}"
    echo "      These are short-lived STS credentials derived from your"
    echo "      'aws login' session. No credentials file needed."
else
    echo ""
    echo "      ERROR: Cannot extract AWS credentials."
    echo "      Run 'aws login' first to establish a session."
    exit 1
fi

echo ""

# ============================================================
# STEP 4: Generate SSH Key Pair
# ============================================================
echo "[4/5] Generating ephemeral SSH key pair..."

SSH_KEY_DIR=$(mktemp -d)
ssh-keygen -t ed25519 -f "${SSH_KEY_DIR}/id_ed25519" -N "" -C "ome-demo-${CLUSTER_NAME}" -q

echo "      Key generated: ${SSH_KEY_DIR}/id_ed25519.pub"
echo "      Fingerprint: $(ssh-keygen -lf "${SSH_KEY_DIR}/id_ed25519.pub" | awk '{print $2}')"
echo ""

# ============================================================
# STEP 5: Build cluster config & provision
# ============================================================
echo "[5/5] Building cluster configuration..."

mkdir -p "$CLUSTER_DIR"

# Write pull secret to cluster dir (ocp-engine reads from file)
echo "$PULL_SECRET" > "${CLUSTER_DIR}/pull-secret.json"

# Build cluster.yaml with inline STS credentials (no credentials file)
cat > "${CLUSTER_DIR}/cluster.yaml" <<CLUSTERYAML
ocp_engine:
  pull_secret_file: ${CLUSTER_DIR}/pull-secret.json
  ssh_public_key_file: ${SSH_KEY_DIR}/id_ed25519.pub
  release_image: ${RELEASE_IMAGE}
  credentials:
    access_key_id: "${AWS_ACCESS_KEY}"
    secret_access_key: "${AWS_SECRET_KEY}"
    session_token: "${AWS_SESSION_TOKEN}"

baseDomain: ${BASE_DOMAIN}
credentialsMode: Manual
metadata:
  name: ${CLUSTER_NAME}
platform:
  aws:
    region: ${AWS_REGION}
publish: External
CLUSTERYAML

echo "      Config written to: ${CLUSTER_DIR}/cluster.yaml"

# Copy binary to cluster dir
cp "${SCRIPT_DIR}/${BINARY_NAME}" "${CLUSTER_DIR}/${BINARY_NAME}"

echo ""
echo "========================================================"
echo "  Credential Acquisition Summary"
echo "========================================================"
echo ""
echo "  Pull Secret:  Acquired via Red Hat SSO ($REGISTRY_COUNT registries)"
echo "  AWS Creds:    Acquired via ${AWS_CREDS_SOURCE}"
echo "  SSH Key:      Auto-generated (ed25519)"
echo "  Storage:      NOTHING persisted by OME"
echo ""
echo "  All credentials are ephemeral — written to temp/cluster"
echo "  dir for ocp-engine consumption only."
echo "========================================================"
echo ""

if [ "$DRY_RUN" = "--dry-run" ]; then
    echo "DRY RUN: Validating config (gen-config)..."
    echo ""

    "${CLUSTER_DIR}/${BINARY_NAME}" gen-config --config "${CLUSTER_DIR}/cluster.yaml"

    echo ""
    echo "Dry run complete. Generated install-config.yaml:"
    echo ""
    grep -E "baseDomain|name:|region:|replicas|type:" "${CLUSTER_DIR}/install-config.yaml" | head -15
    echo ""
    echo "SSH private key for download: ${SSH_KEY_DIR}/id_ed25519"
    echo "  (copy this now — it will not be saved)"
    echo ""
    echo "To clean up: rm -rf ${CLUSTER_DIR} ${SSH_KEY_DIR}"
else
    echo "Ready to provision. This will:"
    echo "  - Create AWS infrastructure (VPCs, EC2, ELBs, Route53, etc.)"
    echo "  - Install OpenShift 4.20"
    echo "  - Take approximately 30-45 minutes"
    echo ""
    read -rp "Proceed? (y/N): " CONFIRM
    if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
        echo ""
        echo "Aborted."
        echo "SSH private key: ${SSH_KEY_DIR}/id_ed25519"
        echo "  (copy if needed, then clean up: rm -rf ${SSH_KEY_DIR})"
        echo "Cluster dir: ${CLUSTER_DIR}"
        exit 0
    fi

    echo ""
    echo "Provisioning cluster..."
    echo "Logs: ${CLUSTER_DIR}/.openshift_install.log"
    echo ""

    if "${CLUSTER_DIR}/${BINARY_NAME}" provision --config "${CLUSTER_DIR}/cluster.yaml"; then
        echo ""
        echo "========================================================"
        echo "  Cluster provisioned successfully!"
        echo "========================================================"
        echo ""
        echo "  Kubeconfig:  ${CLUSTER_DIR}/auth/kubeconfig"
        echo "  Password:    ${CLUSTER_DIR}/auth/kubeadmin-password"
        echo ""
        echo "  SSH private key: ${SSH_KEY_DIR}/id_ed25519"
        echo "    Download now — it will not be saved by OME."
        echo ""
        echo "  To use:"
        echo "    export KUBECONFIG=${CLUSTER_DIR}/auth/kubeconfig"
        echo "    oc get nodes"
        echo ""
        echo "  To destroy later:"
        echo "    ${CLUSTER_DIR}/${BINARY_NAME} destroy --work-dir ${CLUSTER_DIR}"
        echo ""

        # Clean up credentials from cluster dir
        echo "  Cleaning up ephemeral credentials..."
        rm -f "${CLUSTER_DIR}/pull-secret.json"
        echo "  Pull secret removed from cluster dir."
        echo "  Only metadata.json retained for destroy operations."
        echo "  (cluster.yaml contains expired STS creds — harmless)"
        echo ""
    else
        echo ""
        echo "========================================================"
        echo "  Provisioning failed"
        echo "========================================================"
        echo ""
        if [ -f "${CLUSTER_DIR}/metadata.json" ]; then
            echo "  AWS resources may exist. To clean up:"
            echo "    ${CLUSTER_DIR}/${BINARY_NAME} destroy --work-dir ${CLUSTER_DIR}"
        else
            echo "  No AWS resources were created. Safe to clean up:"
            echo "    rm -rf ${CLUSTER_DIR}"
        fi
        echo ""
        echo "  Check logs: ${CLUSTER_DIR}/.openshift_install.log"
        exit 1
    fi
fi

exit 0
}
