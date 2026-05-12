# GCP HCP Lifecycle POC — Clean-Slate Runbook

This runbook rebuilds the **GCP side only** for the current Python flow in `hcp_lifecycle.py`.

It assumes:
- the OIDC issuer already exists
- the CLS gateway already exists
- you will run every command yourself and use this file as the operator guide

## What The Current Python Flow Needs

The script uses this sequence:

`OIDC browser login -> Workforce STS -> broker SA generateIdToken -> gateway -> HyperShift infra -> cluster create/delete`

There are **two separate GCP projects** in this flow:

1. **Broker/workforce project**
   - Workforce Identity Federation pool/provider
   - broker service account
   - quota context for `generateIdToken`

2. **Target HCP project**
   - HyperShift-created IAM resources
   - HyperShift-created network resources
   - hosted cluster resources

## Important Inputs

This guide intentionally avoids committed environment-specific values.

- Put real values only in:
  - your current shell session
  - your local git-ignored `config.yaml`
- Do not treat any placeholder in this file as a default.
- `gateway_url` and `gateway_audience` should be filled only in your local `config.yaml`.
- `gateway_audience` is the `aud` claim for the **final Google-signed broker ID token**. It is **not** the Workforce STS audience.

## External Requirements Before You Start

- Your OIDC issuer must expose a public `.well-known/openid-configuration` document and JWKS.
- The OIDC client used by the Python script must support **authorization code + PKCE**.
- The OIDC client must allow redirect URI `http://localhost:8888/callback`.
- The OIDC `id_token` used by the script must contain an `email` claim.
- The Google account running these commands must already have permission to:
  - create projects
  - link billing
  - create workforce pools/providers at the org level

## 0. Gather The Values You Will Need Later

Do **not** export anything yet if you do not know the values.

Before you reach the export step later in this guide, collect or decide:

- `BROKER_PROJECT`
  - choose the new broker/workforce project ID for this run
- `TARGET_PROJECT`
  - choose the new target HCP project ID for this run
- `GCP_ORG_ID`
  - discover this from `gcloud organizations list`
- `BILLING_ACCOUNT`
  - discover this from `gcloud billing accounts list`
- `OIDC_ISSUER_URL`
  - get this from the OIDC environment you are testing
- `OIDC_CLIENT_ID`
  - get this from the OIDC client you will use for browser login
- `USER_EMAIL`
  - this must match the `email` claim of the OIDC user you will log in as later
- `gateway_url`
  - keep this only in local `config.yaml`
- `gateway_audience`
  - keep this only in local `config.yaml`

You will also need to choose names for these resources later:
- `WORKFORCE_POOL`
- `WORKFORCE_PROVIDER`
- `BROKER_SA_NAME`
- `REGION`

Suggested defaults if you want simple names:
- `WORKFORCE_POOL="hcp-lifecycle-pool"`
- `WORKFORCE_PROVIDER="fleetshift-oidc"`
- `BROKER_SA_NAME="hcp-idtoken-broker"`
- `REGION="us-central1"`

Why: later commands reference these values repeatedly, but exporting placeholders too early creates confusion.

Success: you know which values are already known and which ones still need discovery.

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

From `poc/gcp-hcp-api-validation/`:

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

### 1.4 Set up Application Default Credentials

```bash
gcloud auth application-default login
gcloud auth application-default print-access-token >/dev/null && echo "adc ok"
```

Why: `hypershift` uses ADC, not the `gcloud auth login` session.

Success: the final command prints `adc ok`.

Note: if ADC reports or later shows a deleted or wrong `quota_project_id`, do not stop here. You can reset it after you create a valid project in section `7`.

## 2. Validate The External OIDC Issuer

```bash
OIDC_ISSUER_URL="https://your-idp.example.com/realms/your-realm"
curl -s "${OIDC_ISSUER_URL}/.well-known/openid-configuration" | jq '{issuer, authorization_endpoint, token_endpoint, jwks_uri}'
```

Why: confirms the issuer you plan to use exposes the discovery data the Python browser-login step needs.

Success: you see non-empty `issuer`, `authorization_endpoint`, `token_endpoint`, and `jwks_uri`.

If your IdP is Keycloak and the Python script later says the `id_token` has no `email` claim, fix the client mapper before continuing.

## 3. Export The Values Used In Later Commands

Run this section only after you know the values from sections `0`, `1`, and `2`.

```bash
export BROKER_PROJECT="your-broker-project-id"
export TARGET_PROJECT="your-target-project-id"
export GCP_ORG_ID="your-org-id"
export BILLING_ACCOUNT="your-billing-account-id"
export WORKFORCE_POOL="your-workforce-pool"
export WORKFORCE_PROVIDER="your-workforce-provider"
export BROKER_SA_NAME="your-broker-service-account-name"
export USER_EMAIL="your-email@example.com"
export OIDC_ISSUER_URL="https://your-idp.example.com/realms/your-realm"
export OIDC_CLIENT_ID="your-client-id"
export GATEWAY_AUDIENCE="your-gateway-audience"
export REGION="your-region"
export BROKER_SA_EMAIL="${BROKER_SA_NAME}@${BROKER_PROJECT}.iam.gserviceaccount.com"
export WORKFORCE_PRINCIPAL="principalSet://iam.googleapis.com/locations/global/workforcePools/${WORKFORCE_POOL}/attribute.email/${USER_EMAIL}"
```

Why: keeps all later `gcloud` commands copy/pasteable once the real values are known.

Success: `echo "$BROKER_PROJECT $TARGET_PROJECT $WORKFORCE_POOL $BROKER_SA_EMAIL"` prints the values you expect and nothing is still a placeholder.

## 4. Create The Broker/Workforce Project

### 4.1 Create the project

```bash
gcloud projects create "${BROKER_PROJECT}" --organization="${GCP_ORG_ID}"
```

Why: creates the project that holds the workforce-related resources and broker service account.

Success: the command returns the created project ID without an error.

### 4.2 Link billing

```bash
gcloud billing projects link "${BROKER_PROJECT}" --billing-account="${BILLING_ACCOUNT}"
```

Why: ensures the broker project is fully usable for API-backed operations.

Success: `billingEnabled: true` appears in the response.

### 4.3 Enable the required APIs

```bash
gcloud services enable \
  iam.googleapis.com \
  sts.googleapis.com \
  iamcredentials.googleapis.com \
  cloudresourcemanager.googleapis.com \
  --project="${BROKER_PROJECT}"
```

Why: these APIs are required for workforce setup and `generateIdToken`.

Success: the command finishes without an API enablement error.

### 4.4 Verify the project

```bash
gcloud projects describe "${BROKER_PROJECT}"
gcloud services list --enabled --project="${BROKER_PROJECT}" \
  --filter="name:(iam.googleapis.com OR sts.googleapis.com OR iamcredentials.googleapis.com OR cloudresourcemanager.googleapis.com)" \
  --format="table(name)"
```

Why: confirms the project exists and the expected APIs are enabled.

Success: the project describes cleanly and the service list shows the four APIs.

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
  --client-id="${OIDC_CLIENT_ID}" \
  --attribute-mapping="google.subject=assertion.sub,attribute.email=assertion.email" \
  --display-name="HCP lifecycle OIDC provider" \
  --web-sso-response-type="id-token" \
  --web-sso-assertion-claims-behavior="only-id-token-claims"
```

Why: configures GCP STS to trust the same OIDC client used by the Python browser login.

Success: the provider is created without issuer or client-id validation errors.

Note: the old archived shell flow used an extra `providers update-oidc --client-id=...` workaround for an access-token experiment. Do **not** carry that step into this Python flow. The Python code submits the OIDC `id_token`, so the provider should use the real OIDC client ID directly.

### 5.3 Verify pool and provider

```bash
gcloud iam workforce-pools describe "${WORKFORCE_POOL}" \
  --location="global" \
  --format="yaml(name,displayName,state)"

gcloud iam workforce-pools providers describe "${WORKFORCE_PROVIDER}" \
  --location="global" \
  --workforce-pool="${WORKFORCE_POOL}" \
  --format="yaml(name,state,attributeMapping,oidc)"
```

Why: confirms the workforce resources are active and pointing at the expected issuer/client.

Success: both resources show `state: ACTIVE` and the provider output includes:
- `attribute.email: assertion.email`
- `google.subject: assertion.sub`
- `oidc.issuerUri` matching your chosen OIDC issuer
- `oidc.clientId` matching your chosen OIDC client ID

## 6. Create The Broker Service Account And Bindings

### 6.1 Create the broker service account

```bash
gcloud iam service-accounts create "${BROKER_SA_NAME}" \
  --project="${BROKER_PROJECT}" \
  --display-name="HCP lifecycle ID token broker"
```

Why: creates the service account that will mint the gateway-compatible ID token.

Success: the command prints the created service account email.

### 6.2 Grant the workforce principal permission to mint broker ID tokens

```bash
gcloud iam service-accounts add-iam-policy-binding "${BROKER_SA_EMAIL}" \
  --project="${BROKER_PROJECT}" \
  --member="${WORKFORCE_PRINCIPAL}" \
  --role="roles/iam.serviceAccountOpenIdTokenCreator" \
  --condition=None
```

Why: allows the federated user to call `generateIdToken` on the broker service account.

Success: the returned IAM policy includes the `roles/iam.serviceAccountOpenIdTokenCreator` binding.

### 6.3 Grant quota-project usage on the broker project

```bash
gcloud projects add-iam-policy-binding "${BROKER_PROJECT}" \
  --member="${WORKFORCE_PRINCIPAL}" \
  --role="roles/serviceusage.serviceUsageConsumer"
```

Why: allows the workforce token to use the broker project as `x-goog-user-project` when calling IAM Credentials.

Success: the returned policy includes `roles/serviceusage.serviceUsageConsumer`.

### 6.4 Verify the broker service account bindings

```bash
gcloud iam service-accounts get-iam-policy "${BROKER_SA_EMAIL}" \
  --project="${BROKER_PROJECT}"
```

Why: confirms the broker service account policy includes the expected workforce principal binding.

Success: you can see the `roles/iam.serviceAccountOpenIdTokenCreator` binding for `${WORKFORCE_PRINCIPAL}`.

## 7. Create The Target HCP Project

### 7.1 Create the project

```bash
gcloud projects create "${TARGET_PROJECT}" --organization="${GCP_ORG_ID}"
```

Why: creates the separate project where HyperShift will create IAM and network resources for the hosted cluster.

Success: the command returns the created project ID without an error.

### 7.2 Link billing

```bash
gcloud billing projects link "${TARGET_PROJECT}" --billing-account="${BILLING_ACCOUNT}"
```

Why: the target project needs billing before compute and networking resources can be created.

Success: `billingEnabled: true` appears in the response.

### 7.3 Enable the target-project APIs

```bash
gcloud services enable \
  compute.googleapis.com \
  iam.googleapis.com \
  cloudresourcemanager.googleapis.com \
  dns.googleapis.com \
  --project="${TARGET_PROJECT}"
```

Why: these APIs are required by the current `hypershift create iam gcp` and `hypershift create infra gcp` steps.

Success: the command finishes without API enablement errors.

### 7.4 Verify the target project

```bash
gcloud projects describe "${TARGET_PROJECT}"
gcloud services list --enabled --project="${TARGET_PROJECT}" \
  --filter="name:(compute.googleapis.com OR iam.googleapis.com OR cloudresourcemanager.googleapis.com OR dns.googleapis.com)" \
  --format="table(name)"
```

Why: confirms the target project exists and the required APIs are enabled.

Success: the project describes cleanly and the service list shows the four APIs.

Important: the current Python flow uses your **local ADC identity** for `hypershift`. It does **not** make `hypershift` run as the workforce principal. Use the same Google account for ADC that has permission to create IAM and network resources in `${TARGET_PROJECT}`.

### 7.5 Optional: reset the ADC quota project if needed

If `gcloud auth application-default login` attached ADC to a deleted or unrelated project, reset it now to the new target project:

```bash
gcloud auth application-default set-quota-project "${TARGET_PROJECT}"
gcloud auth application-default print-access-token >/dev/null && echo "adc ok"
```

Why: fixes ADC quota/project-context issues before `hypershift` uses ADC against the target project.

Success: the command succeeds and the final check prints `adc ok`.

## 8. Populate `config.yaml`

From `poc/gcp-hcp-api-validation/`:

```bash
cp config.yaml.example config.yaml
```

Why: creates the local runtime config file used by `hcp_lifecycle.py`.

Success: `config.yaml` exists in the working directory.

Then edit `config.yaml` so it looks like this:

```yaml
oidc_issuer_url: "https://your-idp.example.com/realms/your-realm"
oidc_client_id: "your-client-id"

gcp_project: "your-broker-project-id"
workforce_pool: "your-workforce-pool"
workforce_provider: "your-workforce-provider"

broker_sa_email: "hcp-idtoken-broker@your-broker-project-id.iam.gserviceaccount.com"

gateway_url: "https://your-gateway.example.com"
gateway_audience: "your-gateway-audience"

project: "your-target-project-id"
region: "us-central1"
endpoint_access: "PublicAndPrivate"
replicas: 2
```

Why: the Python script needs both the broker/workforce project and the separate target HCP project.

Success: every placeholder has been replaced with your real values.

Note: keep the real `gateway_url` only in your local git-ignored `config.yaml`.

## 9. Sanity-Check The Local Runtime

From `poc/gcp-hcp-api-validation/`:

```bash
source .venv/bin/activate
python hcp_lifecycle.py --help
hypershift version
gcloud auth application-default print-access-token >/dev/null && echo "adc ok"
```

Why: catches local Python, `hypershift`, and ADC issues before you start creating real resources.

Success: `python hcp_lifecycle.py --help` prints usage, `hypershift version` works, and the final command prints `adc ok`.

## 10. Run The Python Flow

Use a short cluster name that already satisfies the script's infra-ID rules:
- max 15 characters
- starts with a lowercase letter
- only lowercase letters, digits, and hyphens

### 10.1 Create the cluster resources

```bash
source .venv/bin/activate
python hcp_lifecycle.py create <cluster-name>
```

Why: runs the full create flow: OIDC login, Workforce STS, broker token, HyperShift IAM/network setup, cluster create, nodepool create, and status polling.

Success: the script reaches `Ready`, or at minimum gets far enough to prove the auth flow, HyperShift setup, and cluster submission work.

Note: if automatic browser launch does not work in your terminal environment, the script now prints the full OIDC login URL. Open that URL manually in a browser and complete the login flow, then let the script continue waiting for the localhost callback.

### 10.2 Delete the cluster resources

```bash
source .venv/bin/activate
python hcp_lifecycle.py delete <cluster-name>
```

Why: tears down the API-side cluster plus HyperShift-created IAM and network resources for the same cluster name.

Success: the script reports that the cluster is fully deleted.
