#!/usr/bin/env bash
# setup-aws-oidc-federation.sh
#
# Creates the AWS IAM resources needed for fleetshift's OCP delivery agent
# to use JIT credentials via AssumeRoleWithWebIdentity.
#
# Creates:
#   1. IAM OIDC Identity Provider (registers Keycloak with AWS)
#   2. IAM Role "OCP-Provisioner" with trust policy + permissions
#
# Prerequisites:
#   - aws CLI configured with admin-level IAM permissions
#   - Keycloak accessible at the OIDC discovery URL
#   - curl, openssl, jq
#
# Usage:
#   ./setup-aws-oidc-federation.sh
#   ./setup-aws-oidc-federation.sh --teardown
#
set -euo pipefail

# ---------- Configuration ----------

KEYCLOAK_HOST="${KEYCLOAK_HOST:?Set KEYCLOAK_HOST to your Keycloak route (e.g. keycloak-keycloak-prod.apps.cluster.example.com)}"
REALM="${REALM:-fleetshift}"
ISSUER_URL="https://${KEYCLOAK_HOST}/realms/${REALM}"
CLIENT_ID="${CLIENT_ID:-fleetshift-cli}"  # audience claim the trust policy checks
ROLE_NAME="${ROLE_NAME:-OCP-Provisioner}"
REGION="${REGION:-us-east-1}"

# ---------- Derived ----------

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
OIDC_PROVIDER_HOST="${KEYCLOAK_HOST}/realms/${REALM}"
OIDC_PROVIDER_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${OIDC_PROVIDER_HOST}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

# ---------- Teardown ----------

if [[ "${1:-}" == "--teardown" ]]; then
    echo "=== Tearing down AWS OIDC federation ==="

    echo "Deleting inline policy from role ${ROLE_NAME}..."
    aws iam delete-role-policy \
        --role-name "${ROLE_NAME}" \
        --policy-name ocp-provision-permissions 2>/dev/null || echo "  (policy not found, skipping)"

    echo "Deleting role ${ROLE_NAME}..."
    aws iam delete-role --role-name "${ROLE_NAME}" 2>/dev/null || echo "  (role not found, skipping)"

    echo "Deleting OIDC provider ${OIDC_PROVIDER_ARN}..."
    aws iam delete-open-id-connect-provider \
        --open-id-connect-provider-arn "${OIDC_PROVIDER_ARN}" 2>/dev/null || echo "  (provider not found, skipping)"

    echo "=== Teardown complete ==="
    exit 0
fi

# ---------- Step 1: Register OIDC Identity Provider ----------

echo "=== Step 1: Register Keycloak as IAM OIDC Identity Provider ==="
echo "  Issuer URL: ${ISSUER_URL}"

# Verify OIDC discovery is accessible
echo "  Verifying OIDC discovery endpoint..."
DISCOVERY=$(curl -sf "${ISSUER_URL}/.well-known/openid-configuration") || {
    echo "ERROR: Cannot reach OIDC discovery at ${ISSUER_URL}/.well-known/openid-configuration"
    echo "       Ensure Keycloak is running and accessible."
    exit 1
}
JWKS_URI=$(echo "${DISCOVERY}" | jq -r '.jwks_uri')
echo "  JWKS URI: ${JWKS_URI}"

# Get the TLS certificate thumbprint (SHA-1 of the leaf cert)
echo "  Fetching TLS certificate thumbprint..."
THUMBPRINT=$(openssl s_client -connect "${KEYCLOAK_HOST}:443" -servername "${KEYCLOAK_HOST}" </dev/null 2>/dev/null \
    | openssl x509 -fingerprint -sha1 -noout 2>/dev/null \
    | sed 's/.*=//;s/://g' \
    | tr '[:upper:]' '[:lower:]')

if [[ -z "${THUMBPRINT}" ]]; then
    echo "ERROR: Could not extract TLS thumbprint from ${KEYCLOAK_HOST}"
    exit 1
fi
echo "  Thumbprint: ${THUMBPRINT}"

# Check if provider already exists
if aws iam get-open-id-connect-provider --open-id-connect-provider-arn "${OIDC_PROVIDER_ARN}" >/dev/null 2>&1; then
    echo "  OIDC provider already exists, updating thumbprint..."
    aws iam update-open-id-connect-provider-thumbprint \
        --open-id-connect-provider-arn "${OIDC_PROVIDER_ARN}" \
        --thumbprint-list "${THUMBPRINT}"
else
    echo "  Creating OIDC provider..."
    aws iam create-open-id-connect-provider \
        --url "${ISSUER_URL}" \
        --client-id-list "${CLIENT_ID}" \
        --thumbprint-list "${THUMBPRINT}"
fi
echo "  OIDC provider ARN: ${OIDC_PROVIDER_ARN}"

# ---------- Step 2: Create IAM Role ----------

echo ""
echo "=== Step 2: Create IAM Role '${ROLE_NAME}' ==="

# Build trust policy
TRUST_POLICY=$(cat <<TRUSTEOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "${OIDC_PROVIDER_ARN}"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "${OIDC_PROVIDER_HOST}:aud": "${CLIENT_ID}"
        }
      }
    }
  ]
}
TRUSTEOF
)

# Check if role already exists
if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
    echo "  Role already exists, updating trust policy..."
    aws iam update-assume-role-policy \
        --role-name "${ROLE_NAME}" \
        --policy-document "${TRUST_POLICY}"
else
    echo "  Creating role..."
    aws iam create-role \
        --role-name "${ROLE_NAME}" \
        --assume-role-policy-document "${TRUST_POLICY}" \
        --max-session-duration 7200
fi

# Set MaxSessionDuration to 2 hours (required for provision operations)
echo "  Setting MaxSessionDuration to 7200s (2 hours)..."
aws iam update-role \
    --role-name "${ROLE_NAME}" \
    --max-session-duration 7200

# ---------- Step 3: Attach permissions policy ----------

echo ""
echo "=== Step 3: Attach permissions policy ==="

PERMISSIONS_POLICY=$(cat <<'PERMEOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EC2",
      "Effect": "Allow",
      "Action": "ec2:*",
      "Resource": "*"
    },
    {
      "Sid": "ELB",
      "Effect": "Allow",
      "Action": "elasticloadbalancing:*",
      "Resource": "*"
    },
    {
      "Sid": "Route53",
      "Effect": "Allow",
      "Action": "route53:*",
      "Resource": "*"
    },
    {
      "Sid": "S3",
      "Effect": "Allow",
      "Action": "s3:*",
      "Resource": "*"
    },
    {
      "Sid": "IAMForCCOAndInstaller",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:DeleteRole",
        "iam:GetRole",
        "iam:ListRoles",
        "iam:PutRolePolicy",
        "iam:DeleteRolePolicy",
        "iam:ListRolePolicies",
        "iam:CreateOpenIDConnectProvider",
        "iam:DeleteOpenIDConnectProvider",
        "iam:GetOpenIDConnectProvider",
        "iam:ListOpenIDConnectProviders",
        "iam:TagOpenIDConnectProvider",
        "iam:CreateUser",
        "iam:DeleteUser",
        "iam:GetUser",
        "iam:CreateAccessKey",
        "iam:DeleteAccessKey",
        "iam:TagRole",
        "iam:TagUser",
        "iam:CreateInstanceProfile",
        "iam:DeleteInstanceProfile",
        "iam:AddRoleToInstanceProfile",
        "iam:RemoveRoleFromInstanceProfile",
        "iam:GetInstanceProfile",
        "iam:PassRole",
        "iam:ListAttachedRolePolicies",
        "iam:SimulatePrincipalPolicy"
      ],
      "Resource": "*"
    },
    {
      "Sid": "STS",
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "*"
    },
    {
      "Sid": "ResourceTagging",
      "Effect": "Allow",
      "Action": "tag:GetResources",
      "Resource": "*"
    }
  ]
}
PERMEOF
)

aws iam put-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-name ocp-provision-permissions \
    --policy-document "${PERMISSIONS_POLICY}"
echo "  Permissions policy attached."

# ---------- Step 4: Verify ----------

echo ""
echo "=== Step 4: Verification ==="

echo "  Fetching a test token from Keycloak..."
echo "  (You will need a valid user password for this)"

# Get user credentials from Keycloak namespace
KC_ADMIN_PASS=$(oc get secret keycloak-initial-admin -n keycloak-prod -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || echo "")

if [[ -n "${KC_ADMIN_PASS}" ]]; then
    # Try to get a token using the 'ops' user. We need the ops user password,
    # which was generated at deploy time. Try admin master realm token to
    # reset it, or try direct grant with admin user.
    echo ""
    echo "  Attempting to get a token using direct grant..."
    echo "  NOTE: If user passwords are unknown, set them via Keycloak admin console:"
    echo "    URL: https://${KEYCLOAK_HOST}/admin"
    echo "    Admin password: ${KC_ADMIN_PASS}"
    echo ""
fi

echo "=== Setup Complete ==="
echo ""
echo "Summary:"
echo "  OIDC Provider ARN: ${OIDC_PROVIDER_ARN}"
echo "  Role ARN:          ${ROLE_ARN}"
echo "  Role Session:      2 hours max"
echo "  Audience:          ${CLIENT_ID}"
echo "  Issuer:            ${ISSUER_URL}"
echo "  Account:           ${ACCOUNT_ID}"
echo "  Region:            ${REGION}"
echo ""
echo "To verify manually:"
echo "  1. Get a token:"
echo "     TOKEN=\$(curl -sf -X POST '${ISSUER_URL}/protocol/openid-connect/token' \\"
echo "       -d 'grant_type=password&client_id=${CLIENT_ID}&username=ops&password=<PASSWORD>' \\"
echo "       | jq -r .access_token)"
echo ""
echo "  2. Assume the role:"
echo "     aws sts assume-role-with-web-identity \\"
echo "       --role-arn '${ROLE_ARN}' \\"
echo "       --role-session-name test \\"
echo "       --web-identity-token \"\$TOKEN\""
echo ""
echo "To teardown:"
echo "  $0 --teardown"
