# GCP HCP API Auth Validation POC — Setup Commands Log

All commands run during POC setup, in order. Placeholders used for sensitive values.

## Prerequisites

### Required Binaries

| Binary | Version | Purpose | Install |
|---|---|---|---|
| `gcloud` | Latest | GCP project creation, API enablement, IAM bindings | [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) |
| `curl` | Any | All HTTP API calls to the HCP API Gateway | Pre-installed on most systems |
| `jq` | 1.6+ | JSON assembly and parsing | `sudo dnf install jq` or `brew install jq` |
| `openssl` | 3.x | RSA signing keypair + JWKS generation | Pre-installed on most systems |
| `hypershift` | Latest | GCP infra provisioning (VPC, WIF, service accounts) | Build from source (see below) |

### Building the hypershift binary

The `hypershift` binary must be built from the **upstream** `openshift/hypershift` repo (not the `cristianoveiga` fork, which is older and missing features like cloud-network SA, rate limit handling, and optional JWKS).

```bash
# Clone (if not already)
git clone git@github.com:openshift/hypershift.git
cd hypershift

# Build (requires Go 1.25+)
go build -o ~/bin/hypershift .

# Verify
hypershift version
# Client Version: openshift/hypershift: <commit>. Latest supported OCP: 5.0.0
```

Ensure `~/bin` is in your PATH:
```bash
export PATH="$HOME/bin:$PATH"
```

### Application Default Credentials (ADC)

The `hypershift` binary (and any Go/Python SDK application) uses Application Default Credentials, which are **separate** from `gcloud auth login`. You must set up ADC once:

```bash
gcloud auth application-default login
```

This opens a browser — log in with the same account you use for `gcloud`. The credentials are saved to `~/.config/gcloud/application_default_credentials.json` and picked up automatically by the hypershift binary.

Without ADC, `hypershift` will fail with `403: The caller does not have permission`.

### Other Requirements

- Google Cloud Identity Free set up on a domain you own (gives you a GCP organization)
- A GCP billing account (for the target cluster project)

## 1. GCP Account Setup

```bash
# Log in as Cloud Identity admin
gcloud auth login  # <cloud-identity-admin-email>

# Verify org exists
gcloud organizations list
# DISPLAY_NAME   ID              DIRECTORY_CUSTOMER_ID
# <domain>       <org-id>        <customer-id>

# Set project (if project already exists)
gcloud config set project <project-id>

# Verify project is under org
gcloud projects get-ancestors <project-id>
# ID              TYPE
# <project-id>    project
# <org-id>        organization
```

## 2. IAM Roles (org level)

The Cloud Identity admin needs permissions to create workforce pools.

```bash
gcloud organizations add-iam-policy-binding <org-id> \
    --member="user:<cloud-identity-admin-email>" \
    --role="roles/iam.workforcePoolAdmin" --quiet

gcloud organizations add-iam-policy-binding <org-id> \
    --member="user:<cloud-identity-admin-email>" \
    --role="roles/owner" --quiet
```

Resulting roles on admin at org level:
- `roles/iam.workforcePoolAdmin`
- `roles/owner`
- `roles/resourcemanager.organizationAdmin` (auto-granted by Cloud Identity)

## 3. Enable Required GCP APIs

```bash
gcloud services enable iam.googleapis.com --project=<project-id>
gcloud services enable sts.googleapis.com --project=<project-id>
gcloud services enable iamcredentials.googleapis.com --project=<project-id>
```

## 4. Keycloak: Add Email Claim to Access Tokens

The `email` claim is not included in Keycloak access tokens by default. Add a protocol
mapper to each client that needs it.

**In Keycloak Admin Console** (for each client: `fleetshift-cli`, `fleetshift-ui`):
1. Clients → `<client>` → Client scopes tab → `<client>-dedicated` → Add mapper → By configuration → User Attribute
2. Name: `email`, User Attribute: `email`, Token Claim Name: `email`, Claim JSON Type: `String`
3. Set "Add to access token" to ON
4. Save

This is also configured in `deploy/keycloak/fleetshift-realm.json` for fresh deploys.

## 5. Workforce Pool Provider: Set Correct Audience

The OIDC provider's `client-id` must match the `aud` claim in Keycloak tokens.
Keycloak sets `aud` to the resource server (`fleetshift`), not the requesting
client (`fleetshift-cli`). After running `00-setup-gcp.sh`, update the audience:

```bash
gcloud iam workforce-pools providers update-oidc <provider-name> \
    --location=global \
    --workforce-pool=<pool-name> \
    --client-id="fleetshift"
```

## 6. Broker Service Account Setup

Create a dedicated service account that the workforce principal will use to mint
gateway-compatible ID tokens.

```bash
# Create the broker service account
gcloud iam service-accounts create ome-poc-idtoken-broker \
    --project=<project-id> \
    --display-name="OME POC ID token broker"

# Allow the workforce principal to mint only OIDC ID tokens for this SA.
#
# IMPORTANT: this binding is intentionally narrow for the POC and currently
# references a specific test user's email-mapped workforce principal. In the
# future, replace this with a more stable identifier such as google.subject
# (Keycloak sub) or a tightly managed group-based binding.
gcloud iam service-accounts add-iam-policy-binding \
    ome-poc-idtoken-broker@<project-id>.iam.gserviceaccount.com \
    --project=<project-id> \
    --member="principalSet://iam.googleapis.com/locations/global/workforcePools/<pool-name>/attribute.email/<user-email>" \
    --role="roles/iam.serviceAccountOpenIdTokenCreator" \
    --condition=None

# Allow the workforce principal to use the project for API quota/billing.
# Required because Workforce IdF tokens are org-level and don't carry project
# context. Without this, calls to iamcredentials.googleapis.com fail with
# "Method doesn't allow unregistered callers."
gcloud projects add-iam-policy-binding <project-id> \
    --member="principalSet://iam.googleapis.com/locations/global/workforcePools/<pool-name>/attribute.email/<user-email>" \
    --role="roles/serviceusage.serviceUsageConsumer"
```

## 7. Validation Commands

```bash
# Confirm active account
gcloud auth list

# Confirm org exists
gcloud organizations list

# Confirm project is under the org
gcloud projects get-ancestors <project-id>

# Confirm required APIs are enabled
gcloud services list --enabled --project=<project-id> \
    --filter="name:(iam.googleapis.com OR sts.googleapis.com OR iamcredentials.googleapis.com)" \
    --format="table(name)"

# Confirm workforce pool is active
gcloud iam workforce-pools describe <pool-name> --location=global \
    --format="yaml(name,displayName,state)"

# Confirm OIDC provider is active, pointing at Keycloak, and audience is correct
# oidc.clientId should be "fleetshift" (the resource server, not the requesting client)
gcloud iam workforce-pools providers describe <provider-name> \
    --location=global --workforce-pool=<pool-name>

# Confirm Keycloak JWKS endpoint is reachable (GCP STS needs this)
curl -s "<keycloak-url>/realms/<realm>/protocol/openid-connect/certs" | jq '.keys | length'
```
