# GCP HCP Lifecycle POC — Validated Runbook

This runbook captures the **validated GCP-side setup** for the current Python flow in `hcp_lifecycle.py`.

It assumes:
- the OIDC issuer already exists
- the CLS gateway already exists
- you will run every command yourself and use this file as the operator guide

## What The Current Python Flow Needs

The script uses this sequence:

`OIDC browser login -> access_token Workforce STS -> broker SA generateIdToken -> gateway -> HyperShift infra -> cluster create/delete`

Everything lives in a **single GCP project**:

- Workforce Identity Federation pool/provider (org-level, references this project for quota)
- Broker service account (mints Google-signed ID tokens for the gateway)
- HyperShift-created IAM resources
- HyperShift-created network resources
- Hosted cluster resources

## Important Inputs

This guide intentionally avoids committed environment-specific values.

- Put real values only in:
  - your current shell session
  - your local git-ignored `config.yaml`
  - your local git-ignored export file such as `export.txt`
- Do not commit real project IDs, account emails, org IDs, billing accounts, gateway audiences, or issuer URLs into this file.
- `gateway_url` and `gateway_audience` should be filled only in your local `config.yaml`.
- `gateway_audience` is the `aud` claim for the **final Google-signed broker ID token**. It is **not** the Workforce STS audience.

## External Requirements Before You Start

- Your OIDC issuer must expose a public `.well-known/openid-configuration` document and JWKS.
- The OIDC client used by the Python script must support **authorization code + PKCE**.
- The OIDC client must allow redirect URI `http://localhost:8888/callback`.
- The OIDC `id_token` used by the script must contain an `email` claim.
- The Keycloak `access_token` used for Workforce STS must contain `resource_access.fleetshift.roles`.
- The Google account running these commands must already have permission to:
  - create projects
  - link billing
  - create workforce pools/providers at the org level

## 0. Gather The Values You Will Need Later

Before you reach the export step later in this guide, collect or decide:

- `GCP_PROJECT`
  - the GCP project ID for all resources (workforce quota, broker SA, HyperShift infra)
- `GCP_ORG_ID`
  - discover this from `gcloud organizations list`
- `BILLING_ACCOUNT`
  - discover this from `gcloud billing accounts list`
- `WORKFORCE_POOL`
  - the Workforce Identity Federation pool name
- `WORKFORCE_PROVIDER`
  - the Workforce OIDC provider name
- `BROKER_SA_NAME`
  - the broker service account short name
- `REGION`
  - the GCP region for HyperShift network resources
- `OIDC_ISSUER_URL`
  - the OIDC issuer used for browser login and Workforce trust
- `OIDC_BROWSER_CLIENT_ID`
  - the OIDC client ID used by `hcp_lifecycle.py` for the browser login flow
- `WORKFORCE_OIDC_CLIENT_ID`
  - the Workforce provider client ID that must match the access token audience used for STS
- `GATEWAY_AUDIENCE`
  - the audience expected by the final Google-signed broker ID token
- `FLEETSHIFT_ROLE`
  - the Keycloak client role used for Workforce admission and GCP IAM binding

Why: later commands reference these values repeatedly, but the real values should live only in your local gitignored shell exports.

Success: you have a complete local export file or shell setup and nothing in this document needs to contain environment-specific secrets or identifiers.

## 1. Install And Verify Local Tooling

### 1.1 Build or verify the upstream `hypershift` binary

```bash
git clone git@github.com:openshift/hypershift.git
cd hypershift
go build -o ~/.local/bin/hypershift .
export PATH="$HOME/.local/bin:$PATH"
hypershift version
```

Why: the Python script shells out to upstream `hypershift` for IAM and network setup.

Success: `hypershift version` prints a client version instead of `command not found`.

### 1.2 Install Python dependencies for the POC

From `poc/gcp-hcp/`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -c "import authlib, cryptography, requests, yaml; print('python deps ok')"
```

Why: installs the runtime dependencies for `hcp_lifecycle.py`.

Success: the Python one-liner prints `python deps ok`.

### 1.3 Log in with `gcloud`

```bash
gcloud auth login
gcloud auth list
gcloud organizations list
gcloud billing accounts list
```

Why: authenticates the account that will create resources and shows the org ID and billing account you will need later.

Success: `gcloud auth list` shows the expected active account, `gcloud organizations list` shows your org, and `gcloud billing accounts list` shows the billing account you intend to use.

## 2. Validate The External OIDC Issuer

```bash
curl -s "${OIDC_ISSUER_URL}/.well-known/openid-configuration" | jq '{issuer, authorization_endpoint, token_endpoint, jwks_uri}'
```

Why: confirms the issuer you plan to use exposes the discovery data the Python browser-login step needs.

Success: you see non-empty `issuer`, `authorization_endpoint`, `token_endpoint`, and `jwks_uri`.

If your IdP is Keycloak and the Python script later says the `id_token` has no `email` claim, fix the client mapper before continuing.

## 3. Export The Values Used In Later Commands

Run this section only after you know the values from sections `0`, `1`, and `2`.

```bash
export GCP_PROJECT="your-gcp-project-id"
export GCP_ORG_ID="your-org-id"
export BILLING_ACCOUNT="your-billing-account-id"
export WORKFORCE_POOL="your-workforce-pool"
export WORKFORCE_PROVIDER="your-workforce-provider"
export BROKER_SA_NAME="your-broker-service-account-name"
export REGION="your-region"
export OIDC_ISSUER_URL="https://your-oidc-issuer.example.com/realms/your-realm"
export OIDC_BROWSER_CLIENT_ID="your-browser-login-client-id"
export WORKFORCE_OIDC_CLIENT_ID="your-workforce-oidc-client-id"
export GATEWAY_AUDIENCE="your-gateway-audience"
export BROKER_SA_EMAIL="${BROKER_SA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com"
export FLEETSHIFT_ROLE="your-fleetshift-provisioning-role"
export WORKFORCE_PROVISIONER_PRINCIPAL="principalSet://iam.googleapis.com/locations/global/workforcePools/${WORKFORCE_POOL}/group/${FLEETSHIFT_ROLE}"
```

Why: keeps all later `gcloud` commands copy/pasteable once the real values are known.

Success: `echo "$GCP_PROJECT $WORKFORCE_POOL $BROKER_SA_EMAIL $FLEETSHIFT_ROLE"` prints the values you expect and nothing is still a placeholder in your live shell setup.

## 4. Create The GCP Project

### 4.1 Create the project

```bash
gcloud projects create "${GCP_PROJECT}" --organization="${GCP_ORG_ID}"
```

Why: creates the project that holds all resources — workforce quota, broker service account, and HyperShift cluster infrastructure.

Success: the command returns the created project ID without an error.

### 4.2 Link billing

```bash
gcloud billing projects link "${GCP_PROJECT}" --billing-account="${BILLING_ACCOUNT}"
```

Why: ensures the project is fully usable for API-backed operations.

Success: `billingEnabled: true` appears in the response.

### 4.3 Enable the required APIs

```bash
gcloud services enable \
  compute.googleapis.com \
  iam.googleapis.com \
  sts.googleapis.com \
  iamcredentials.googleapis.com \
  cloudresourcemanager.googleapis.com \
  dns.googleapis.com \
  --project="${GCP_PROJECT}"
```

Why: these APIs are required for workforce STS exchange, broker `generateIdToken`, and the `hypershift create iam gcp` / `hypershift create infra gcp` steps.

Success: the command finishes without an API enablement error.

### 4.4 Verify the project

```bash
gcloud projects describe "${GCP_PROJECT}"
gcloud services list --enabled --project="${GCP_PROJECT}" \
  --filter="name:(compute.googleapis.com OR iam.googleapis.com OR sts.googleapis.com OR iamcredentials.googleapis.com OR cloudresourcemanager.googleapis.com OR dns.googleapis.com)" \
  --format="table(name)"
```

Why: confirms the project exists and the expected APIs are enabled.

Success: the project describes cleanly and the service list shows the six APIs.

## 5. Create Workforce Identity Federation Resources

### 5.1 Create the workforce pool

```bash
gcloud iam workforce-pools create "${WORKFORCE_POOL}" \
  --location="global" \
  --organization="${GCP_ORG_ID}" \
  --display-name="HCP lifecycle workforce pool" \
  --description="Workforce pool for the Python HCP lifecycle POC"
```

Why: creates the org-level workforce pool used for STS exchange.

Success: the command returns the pool resource name instead of a permission error.

### 5.2 Create the OIDC provider

```bash
gcloud iam workforce-pools providers create-oidc "${WORKFORCE_PROVIDER}" \
  --location="global" \
  --workforce-pool="${WORKFORCE_POOL}" \
  --issuer-uri="${OIDC_ISSUER_URL}" \
  --client-id="${WORKFORCE_OIDC_CLIENT_ID}" \
  --attribute-mapping="google.subject=assertion.sub,google.groups=assertion.resource_access.fleetshift.roles,attribute.email=assertion.email" \
  --attribute-condition="'${FLEETSHIFT_ROLE}' in google.groups" \
  --display-name="HCP lifecycle OIDC provider" \
  --web-sso-response-type="id-token" \
  --web-sso-assertion-claims-behavior="only-id-token-claims"
```

Why: a first-time setup can create the provider in its final validated shape with a single command.

Success: the provider is created without issuer or client-id validation errors.

### 5.3 Verify pool and provider

```bash
gcloud iam workforce-pools describe "${WORKFORCE_POOL}" \
  --location="global" \
  --format="yaml(name,displayName,state)"

gcloud iam workforce-pools providers describe "${WORKFORCE_PROVIDER}" \
  --location="global" \
  --workforce-pool="${WORKFORCE_POOL}" \
  --format="yaml(name,state,attributeMapping,attributeCondition,oidc)"
```

Why: confirms the workforce resources are active and pointing at the expected issuer/client.

Success: both resources show `state: ACTIVE` and the provider output includes:
- `attribute.email: assertion.email`
- `google.groups: assertion.resource_access.fleetshift.roles`
- `google.subject: assertion.sub`
- `attributeCondition` referencing your `${FLEETSHIFT_ROLE}` value
- `oidc.issuerUri` matching your chosen OIDC issuer
- `oidc.clientId` matching your chosen `${WORKFORCE_OIDC_CLIENT_ID}`

## 6. Create The Broker Service Account And Bindings

### 6.1 Create the broker service account

```bash
gcloud iam service-accounts create "${BROKER_SA_NAME}" \
  --project="${GCP_PROJECT}" \
  --display-name="HCP lifecycle ID token broker"
```

Why: creates the service account that will mint the gateway-compatible ID token.

Success: the command prints the created service account email.

### 6.2 Grant the workforce principal permission to mint broker ID tokens

```bash
gcloud iam service-accounts add-iam-policy-binding "${BROKER_SA_EMAIL}" \
  --project="${GCP_PROJECT}" \
  --member="${WORKFORCE_PROVISIONER_PRINCIPAL}" \
  --role="roles/iam.serviceAccountOpenIdTokenCreator" \
  --condition=None
```

Why: allows the federated user to call `generateIdToken` on the broker service account.

Success: the returned IAM policy includes the `roles/iam.serviceAccountOpenIdTokenCreator` binding.

### 6.3 Grant quota-project usage

```bash
gcloud projects add-iam-policy-binding "${GCP_PROJECT}" \
  --member="${WORKFORCE_PROVISIONER_PRINCIPAL}" \
  --role="roles/serviceusage.serviceUsageConsumer"
```

Why: allows the workforce token to use the project as `x-goog-user-project` when calling IAM Credentials.

Success: the returned policy includes `roles/serviceusage.serviceUsageConsumer`.

### 6.4 Verify the broker service account bindings

```bash
gcloud iam service-accounts get-iam-policy "${BROKER_SA_EMAIL}" \
  --project="${GCP_PROJECT}"
```

Why: confirms the broker service account policy includes the expected workforce principal binding.

Success: you can see the `roles/iam.serviceAccountOpenIdTokenCreator` binding for `${WORKFORCE_PROVISIONER_PRINCIPAL}`.

## 7. Grant The HyperShift Project Roles

```bash
for role in \
  roles/browser \
  roles/iam.workloadIdentityPoolAdmin \
  roles/iam.serviceAccountAdmin \
  roles/resourcemanager.projectIamAdmin \
  roles/compute.networkAdmin \
  roles/compute.securityAdmin
do
  gcloud projects add-iam-policy-binding "${GCP_PROJECT}" \
    --member="${WORKFORCE_PROVISIONER_PRINCIPAL}" \
    --role="${role}"
done
```

Why: these are the roles required to let the federated principal complete `hypershift create iam gcp` and `hypershift create infra gcp`.

Success: each command updates IAM successfully.

## 8. Populate `config.yaml`

From `poc/gcp-hcp/`:

```bash
cp config.yaml.example config.yaml
```

Why: creates the local runtime config file used by `hcp_lifecycle.py`.

Success: `config.yaml` exists in the working directory.

Then edit `config.yaml` so it looks like this:

```yaml
oidc_issuer_url: "https://your-oidc-issuer.example.com/realms/your-realm"
oidc_client_id: "your-browser-login-client-id"

gcp_project: "your-gcp-project-id"

workforce_pool: "your-workforce-pool"
workforce_provider: "your-workforce-provider"

broker_sa_email: "hcp-idtoken-broker@your-gcp-project-id.iam.gserviceaccount.com"

gateway_url: "https://your-gateway.example.com"
gateway_audience: "your-gateway-audience"

region: "us-central1"
endpoint_access: "PublicAndPrivate"
replicas: 2
```

Why: the Python script needs the project, workforce config, broker SA, and gateway details.

Success: every placeholder has been replaced with your real values.

Note: keep the real `gateway_url` only in your local git-ignored `config.yaml`.

## 9. Sanity-Check The Local Runtime

From `poc/gcp-hcp/`:

```bash
source .venv/bin/activate
python hcp_lifecycle.py --help
hypershift version
```

Why: catches local Python and `hypershift` issues before you start creating real resources.

Success: `python hcp_lifecycle.py --help` prints usage and `hypershift version` works.

## 10. Run The Python Flow

Use a short cluster name that already satisfies the script's infra-ID rules:
- max 15 characters
- starts with a lowercase letter
- only lowercase letters, digits, and hyphens

### 10.1 Prepare a clean validation shell

```bash
source .venv/bin/activate
unset GOOGLE_APPLICATION_CREDENTIALS
unset CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE
unset GOOGLE_OAUTH_ACCESS_TOKEN
unset GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES
export CLOUDSDK_CONFIG="$(mktemp -d)"
export XDG_CONFIG_HOME="$(mktemp -d)"
export HOME="$(mktemp -d)"
```

Why: prevents ambient local Google credentials from contaminating the validation run and matches the clean-shell flow used in the successful proof pass.

Success: the shell has the temporary config roots set and no inherited Google credential overrides remain.

### 10.2 Create the cluster resources

```bash
python hcp_lifecycle.py create <cluster-name>
```

Why: runs the full create flow: OIDC login, Workforce STS, broker token, HyperShift IAM/network setup, cluster create, nodepool create, and status polling.

Success: the script reaches `Ready`, or at minimum gets far enough to prove the auth flow, HyperShift setup, and cluster submission work.

Note: if automatic browser launch does not work in your terminal environment, the script now prints the full OIDC login URL. Open that URL manually in a browser and complete the login flow, then let the script continue waiting for the localhost callback.

### 10.3 Delete the cluster resources

```bash
python hcp_lifecycle.py delete <cluster-name>
```

Why: tears down the API-side cluster plus HyperShift-created IAM and network resources for the same cluster name.

Success: the script reports that the cluster is fully deleted.

If the cluster is incomplete or stuck and the CLS API will not delete it cleanly, use:

```bash
python hcp_lifecycle.py delete <cluster-name> --skip-api
```

Why: skips the CLS API delete step and runs GCP cleanup directly for partial validation clusters.

Success: the script proceeds to `Destroying Network Infrastructure` and `Destroying IAM Infrastructure` even if the cluster cannot be resolved or deleted through the API.
