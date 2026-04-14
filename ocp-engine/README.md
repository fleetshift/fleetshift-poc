# ocp-engine

A stateless CLI tool for provisioning and deprovisioning OpenShift 4.20 clusters on AWS. It wraps `openshift-install` with phased execution and structured JSON output, designed for integration with an external management platform.

No Kubernetes cluster required. No CRDs. No controllers. Just a binary on a management server.

## Prerequisites

The following must be available on your management server:

- **`oc` CLI** -- used to extract `openshift-install` from OCP release images
- **Podman or Docker** -- used to pull OCP release images
- **Red Hat pull secret** -- download from [console.redhat.com](https://console.redhat.com/openshift/install/pull-secret)
- **AWS credentials** with IAM permissions sufficient for IPI provisioning (EC2, VPC, ELB, Route53, IAM, S3, etc.)

## Installation

```bash
git clone <repo-url>
cd ocp-engine
go build -o ocp-engine .
```

Move the binary to a location in your PATH:

```bash
sudo mv ocp-engine /usr/local/bin/
```

## Quick Start

### 1. Create a cluster directory with config

Each cluster gets its own directory. The `cluster.yaml` config file lives inside it, and all artifacts (installer binary, manifests, kubeconfig) are written alongside it.

```bash
mkdir -p clusters/my-cluster
```

```yaml
# clusters/my-cluster/cluster.yaml
ocp_engine:
  pull_secret_file: /path/to/pull-secret.json
  ssh_public_key_file: ~/.ssh/id_rsa.pub
  credentials:
    access_key_id: "AKIA..."
    secret_access_key: "..."

baseDomain: example.com
metadata:
  name: my-cluster
platform:
  aws:
    region: us-east-1
```

The `ocp_engine` section holds engine-specific settings (credentials, file paths). Everything else is native OpenShift `install-config.yaml` and is passed through directly.

### 2. Validate configuration (dry run)

```bash
ocp-engine gen-config --config clusters/my-cluster/cluster.yaml
```

This generates `install-config.yaml` in the cluster directory without creating any AWS resources. Inspect it to verify your settings.

### 3. Provision the cluster

```bash
ocp-engine provision --config clusters/my-cluster/cluster.yaml
```

This runs through 5 phases and takes approximately 30-45 minutes:

| Phase | What happens | AWS resources created? |
|---|---|---|
| extract | Downloads `openshift-install` from release image | No |
| install-config | Generates `install-config.yaml` | No |
| manifests | Generates Kubernetes manifests | No |
| ignition | Generates ignition configs | No |
| cluster | Creates AWS infrastructure and installs OCP | **Yes** |

Each phase outputs a JSON line to stdout on completion:

```json
{"phase":"extract","status":"complete","elapsed_seconds":45}
{"phase":"install-config","status":"complete","elapsed_seconds":0}
{"phase":"manifests","status":"complete","elapsed_seconds":8}
{"phase":"ignition","status":"complete","elapsed_seconds":3}
{"phase":"cluster","status":"complete","elapsed_seconds":2100}
```

On success, your kubeconfig is at `clusters/my-cluster/auth/kubeconfig`:

```bash
export KUBECONFIG=clusters/my-cluster/auth/kubeconfig
oc get nodes
```

### 4. Check status

```bash
ocp-engine status --work-dir clusters/my-cluster
```

Returns structured JSON:

```json
{
  "state": "succeeded",
  "completed_phases": ["extract", "install-config", "manifests", "ignition", "cluster"],
  "infra_id": "my-cluster-a1b2c",
  "has_kubeconfig": true,
  "has_metadata": true
}
```

### 5. Destroy the cluster

```bash
ocp-engine destroy --work-dir clusters/my-cluster
```

This runs `openshift-install destroy cluster`, which finds all AWS resources tagged with `kubernetes.io/cluster/<infraID>: owned` and deletes them. Destroy is idempotent -- safe to run multiple times.

## Commands

### `ocp-engine provision`

Provision a new OCP cluster on AWS. The parent directory of the config file is used as the cluster directory for all artifacts.

```
ocp-engine provision --config <path>
```

| Flag | Required | Description |
|---|---|---|
| `--config` | Yes | Path to `cluster.yaml`. Parent directory is used as the cluster directory. |

### `ocp-engine status`

Check the status of a work directory.

```
ocp-engine status --work-dir <path>
```

| Flag | Required | Description |
|---|---|---|
| `--work-dir` | Yes | Path to work directory to inspect |

**Possible states:**

| State | Meaning |
|---|---|
| `empty` | Work directory exists but no phases have started |
| `running` | A provision or destroy operation is currently active |
| `succeeded` | All phases complete, kubeconfig available |
| `failed` | A phase failed, process exited |
| `partial` | Phases partially complete, process not running (e.g., server crashed) |

### `ocp-engine destroy`

Destroy a cluster and clean up all AWS resources.

```
ocp-engine destroy --work-dir <path>
```

| Flag | Required | Description |
|---|---|---|
| `--work-dir` | Yes | Path to cluster directory (must contain `metadata.json`, `openshift-install`, and `cluster.yaml`) |

### `ocp-engine gen-config`

Generate `install-config.yaml` without running any install phases. Useful for validating configuration.

```
ocp-engine gen-config --config <path>
```

| Flag | Required | Description |
|---|---|---|
| `--config` | Yes | Path to `cluster.yaml`. Parent directory is used as the cluster directory. |

## Configuration Reference

The config file has two parts:

1. **`ocp_engine`** -- engine-specific settings (credentials, file paths, release image)
2. **Everything else** -- native OpenShift `install-config.yaml` fields, passed through directly

### Full `cluster.yaml` example

```yaml
# --- ocp-engine settings ---
ocp_engine:
  pull_secret_file: /path/to/pull-secret.json    # Required. Read and inlined as pullSecret.
  ssh_public_key_file: /path/to/id_rsa.pub       # Optional. Read and inlined as sshKey.
  additional_trust_bundle_file: /path/to/ca.pem  # Optional. Read and inlined as additionalTrustBundle.
  release_image: quay.io/openshift-release-dev/ocp-release:4.20.18-multi  # Optional. Override release image.
  credentials:                                   # Required. One of the 4 modes below.
    access_key_id: "AKIA..."
    secret_access_key: "..."

# --- Native install-config.yaml (pass-through) ---
apiVersion: v1
baseDomain: example.com
metadata:
  name: my-cluster
platform:
  aws:
    region: us-east-1
    tags:
      environment: staging
controlPlane:
  name: master
  replicas: 3
  platform:
    aws:
      type: m6a.xlarge
compute:
  - name: worker
    replicas: 3
    platform:
      aws:
        type: m6a.xlarge
networking:
  clusterNetwork:
    - cidr: 10.128.0.0/14
      hostPrefix: 23
  serviceNetwork:
    - 172.30.0.0/16
publish: External
fips: false
```

### Required fields

- `ocp_engine.pull_secret_file`
- `ocp_engine.credentials` (at least one mode)
- `baseDomain`
- `metadata.name`
- `platform.aws.region`

### AWS Credential Modes

All credential settings go under `ocp_engine.credentials`:

**Inline credentials:**
```yaml
credentials:
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

**Credentials file:**
```yaml
credentials:
  credentials_file: /path/to/aws/credentials
```

**Named profile:**
```yaml
credentials:
  profile: my-aws-profile
```

**STS assume role (not yet implemented):**
```yaml
credentials:
  role_arn: arn:aws:iam::123456789:role/ocp-installer
```

### Install-config pass-through

All fields outside `ocp_engine` are written directly to `install-config.yaml`. This means any option supported by `openshift-install` works without waiting for ocp-engine to explicitly support it -- subnets, proxy, custom AMIs, feature gates, etc. See the [OpenShift install-config reference](https://docs.openshift.com/container-platform/4.20/installing/installing_aws/ipi/installing-aws-customizations.html) for all available fields.

## Cluster Directory

Each cluster gets its own directory containing the config and all artifacts:

```
clusters/my-cluster/
  cluster.yaml              # Your config file
  install-config.yaml       # Generated install config
  openshift-install         # Cached binary from release image
  manifests/                # Generated by openshift-install
  openshift/                # Generated by openshift-install
  auth/
    kubeconfig              # Cluster access (on success)
    kubeadmin-password      # Admin password (on success)
  metadata.json             # Cluster metadata (needed for destroy)
  .openshift_install.log    # Consolidated installer log
  _phase_extract_complete   # Phase completion markers
  _phase_install-config_complete
  _phase_manifests_complete
  _phase_ignition_complete
  _phase_cluster_complete
  _pid                      # PID of running process
```

## Exit Codes

- **0** -- Success
- **1** -- Failure (details in JSON output on stdout)

## Error Handling

All errors are returned as structured JSON on stdout:

```json
{
  "category": "phase_error",
  "phase": "cluster",
  "message": "bootstrap timeout after 30 minutes",
  "log_tail": "last 20 lines of installer log...",
  "has_metadata": true,
  "requires_destroy": true
}
```

**Error categories:**

| Category | Meaning | What to do |
|---|---|---|
| `config_error` | Invalid config (bad region, missing pull secret, etc.) | Fix config and retry |
| `prereq_error` | Missing prerequisite (`oc`, container runtime) | Install missing tool and retry |
| `phase_error` | `openshift-install` failed during a phase | Check `requires_destroy` (see below) |
| `already_running` | Another operation is running in this work directory | Wait or check status |
| `workdir_error` | Work directory issue (missing metadata for destroy, etc.) | Check work directory |

### Handling Failures

**Failed before `cluster` phase** (`requires_destroy: false`):
No AWS resources were created. Delete the cluster directory and retry.

```bash
rm -rf clusters/my-cluster
mkdir clusters/my-cluster
cp cluster.yaml clusters/my-cluster/
ocp-engine provision --config clusters/my-cluster/cluster.yaml
```

**Failed during `cluster` phase** (`requires_destroy: true`):
AWS resources may exist. Destroy before retrying.

```bash
ocp-engine destroy --work-dir clusters/my-cluster
# Then retry with a fresh cluster directory
mkdir clusters/my-cluster-2
cp cluster.yaml clusters/my-cluster-2/
ocp-engine provision --config clusters/my-cluster-2/cluster.yaml
```

## Running Multiple Clusters

Each cluster uses its own directory. Run as many as you want in parallel:

```bash
ocp-engine provision --config clusters/a/cluster.yaml &
ocp-engine provision --config clusters/b/cluster.yaml &
ocp-engine provision --config clusters/c/cluster.yaml &
wait
```

There is no shared state between clusters. Each is an independent process with its own `openshift-install` invocation.

## AWS Resource Tagging

`openshift-install` automatically tags all AWS resources with:

```
kubernetes.io/cluster/<infraID>: owned
```

The `infraID` is auto-generated during install and stored in `metadata.json`. The destroy command uses these tags to find and delete all resources belonging to a cluster.

Any custom tags you specify in `platform.aws.tags` are applied on top of the infrastructure tags.

## Platform Integration

`ocp-engine` is designed to be called by an external management platform. The platform is responsible for:

- **State tracking** -- which clusters exist, what state they're in
- **Retry logic** -- when and whether to retry failed provisions
- **Scheduling** -- when to provision/destroy clusters
- **Credential management** -- providing AWS credentials and pull secrets

The engine just does what it's told and returns structured results. Parse the JSON output from stdout to drive your automation.

### Integration example (bash)

```bash
#!/bin/bash
CLUSTER_DIR="clusters/001"
mkdir -p "$CLUSTER_DIR"
cp cluster.yaml "$CLUSTER_DIR/"

output=$(ocp-engine provision --config "$CLUSTER_DIR/cluster.yaml" 2>/dev/null)
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Cluster provisioned successfully"
    kubeconfig="$CLUSTER_DIR/auth/kubeconfig"
else
    requires_destroy=$(echo "$output" | tail -1 | jq -r '.requires_destroy // false')
    if [ "$requires_destroy" = "true" ]; then
        echo "Provision failed with AWS resources created. Destroying..."
        ocp-engine destroy --work-dir "$CLUSTER_DIR"
    else
        echo "Provision failed before AWS resources were created. Safe to retry."
    fi
fi
```

## Future Considerations

Features not yet implemented but worth considering for future iterations:

- **STS assume-role** — Cross-account AWS provisioning via `sts:AssumeRole` with ExternalID. The config field (`role_arn`) exists but is not yet implemented.
- **S3 log upload** — Upload install logs and gathered diagnostics to S3 for post-mortem analysis.
- **Failure log gathering** — Run `openshift-install gather bootstrap` or `oc adm must-gather` on install failure to collect detailed diagnostic logs.
- **Manifest injection** — Copy custom manifests into the work directory between the manifests and ignition phases for cluster customization (security hardening, network policies, OIDC integration).
- **Day-2 operations** — SyncSets, MachinePool management, hibernation, certificate rotation.
- **Multi-platform support** — Extend beyond AWS to Azure, GCP, vSphere, etc.
- **Platform retry logic** — Currently the platform is responsible for retry decisions. Consider whether the engine should support built-in retry strategies.
- **On-disk log scrubbing** — The raw `.openshift_install.log` file is written directly by `openshift-install` and may contain secrets (AWS keys, passwords). The log pipeline scrubs output sent to stderr and JSON events, but the raw log file on disk is not scrubbed. Consider whether to scrub the file in place after install completes, write a parallel scrubbed copy, or leave as-is with access controls.

### FleetShift Integration

- **gRPC callback** — Add `--callback-url` and `--cluster-id` flags. ocp-engine reports phase results, milestones, completion, and failures to the calling platform via gRPC. This replaces stdout JSON parsing for inter-process communication when running as a container. Callback authentication via short-lived per-provision tokens scoped to the cluster ID.
- **CCO STS mode (ccoctl)** — Automate the `ccoctl aws create-all` workflow as part of the provision pipeline. Extract `ccoctl` from the release image, extract CredentialsRequest manifests, run `ccoctl aws create-all` to create S3 bucket (OIDC discovery + JWKS), IAM OIDC provider, and per-operator IAM roles. Set `credentialsMode: Manual` and inject ccoctl output manifests at the manifests phase. On destroy, run `ccoctl aws delete` to clean up IAM OIDC provider, IAM roles, and S3 bucket. This eliminates all long-lived AWS keys from the cluster.
- **Single STS session credential mechanism** — The OCP agent calls `AssumeRoleWithWebIdentity` once with the caller's valid OIDC token, requesting a 2-hour STS session (`DurationSeconds=7200`). The resulting credentials are passed as env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) to the ocp-engine subprocess. The 2-hour window comfortably covers any provision (~45 min) or destroy (~30 min). No token files, no refresh goroutines, no stored refresh tokens. The IAM role's `MaxSessionDuration` must be set to at least 7200 seconds. If the management server is compromised, the attacker gets at most the in-flight STS session with no ability to refresh or extend it.
- **External OIDC manifest injection** — Inject the OpenShift `Authentication` CR (`type: OIDC`), CA bundle ConfigMap, and client Secret as manifests during the manifests phase. This configures external OIDC on the cluster at install time, avoiding a 20-minute post-install kube-apiserver rollout. The cluster comes up with external OIDC already configured.
- **Break-glass kubeadmin kubeconfig** — When external OIDC is configured at install time, the kubeadmin password no longer works (OAuth server is disabled). The kubeadmin kubeconfig (certificate-based auth) still works and should be stored in vault as a break-glass emergency credential. If the OIDC provider goes down and the platform SA token expires, this is the only way into the cluster.

### Credential Lifecycle

- **Just-in-time credential acquisition** — Long-term goal is zero stored credentials. AWS credentials acquired via STS `AssumeRoleWithWebIdentity` using the caller's OIDC token. Pull secret acquired via Red Hat SSO token exchange (`POST /api/accounts_mgmt/v1/access_token`). SSH key auto-generated per provision. All credentials discarded after use. Only `infra_id`, `cluster_id`, and `region` are persisted (in target properties, not vault).
- **Pluggable credential provider interface** — `CredentialProvider` abstraction with implementations for passthrough (initial, dev/testing) and SSO/JIT (future production). The OCP agent resolves credentials through the provider — doesn't know or care how they're obtained.
- **Red Hat SSO pull secret flow** — Proven working via `ocm` CLI: device code flow against `sso.redhat.com`, exchange access token for pull secret via `api.openshift.com`. Currently uses the `ocm-cli` client ID (Red Hat's public OAuth client for their CLI tool). This works but is not ideal for production — we're impersonating another tool's client. Red Hat could change its scopes, rate limits, or redirect URIs without notice. For production, register `fleetshift` as its own OAuth client with Red Hat SSO (requires coordination with Red Hat). This is a client ID swap — no architectural changes needed.
- **AWS credential provider options** — (A) AWS SSO / Identity Center login with `sso:GetRoleCredentials`, (B) Keycloak OIDC federation with `sts:AssumeRoleWithWebIdentity` (no extra login step — user's Keycloak token IS their AWS auth). Option B is preferred for production as it eliminates the separate AWS login step.

### Containerization

- **Separate container/pod per deployment** — ocp-engine runs as an ephemeral container, one per cluster provision. FleetShift server orchestrates and receives results via gRPC callback. Container only needs network access to FleetShift's callback endpoint and to AWS APIs. No database access.
- **Podman / same-pod deployment** — Near-term: run in podman alongside fleetshift-server containers in the same pod. ocp-engine container communicates with fleetshift-server via localhost gRPC.
- **Kubernetes deployment** — Longer-term: fleetshift-server as a Deployment, spawns ocp-engine as Jobs or Pods per provision. Credentials injected via Kubernetes Secrets as env vars or projected volumes.
- **In-memory credential handling** — Production containers should never write credentials to persistent filesystem. Use tmpfs mounts for work directories containing install-config.yaml and ignition configs (which contain baked-in pull secret and SSH key).

### AWS IAM Setup for JIT/SSO Credential Flow

To use the just-in-time credential flow (no stored AWS credentials), two AWS resources must be created once per AWS account. This allows fleetshift users to exchange their OIDC token (from the management plane IdP, e.g., Keycloak) for temporary AWS credentials via `AssumeRoleWithWebIdentity`.

**This is separate from the per-cluster OIDC providers that `ccoctl` creates for cluster operators.** The setup below is for the management plane → AWS trust relationship. The `ccoctl` OIDC providers are created automatically per cluster during provisioning.

#### Step 1: Register your IdP as an IAM OIDC Identity Provider

This tells AWS "trust tokens from this issuer." One per AWS account.

```bash
# Get your IdP's TLS certificate thumbprint
# Replace with your IdP's URL
IDP_URL="https://keycloak.example.com/realms/master"

# Get thumbprint (SHA1 of the root CA cert for the IdP's TLS)
openssl s_client -connect keycloak.example.com:443 -servername keycloak.example.com \
  < /dev/null 2>/dev/null | openssl x509 -fingerprint -noout -sha1 \
  | sed 's/://g' | cut -d= -f2

# Register the OIDC provider in AWS IAM
aws iam create-open-id-connect-provider \
  --url "$IDP_URL" \
  --client-id-list "fleetshift" \
  --thumbprint-list "<thumbprint from above>"
```

**Parameters:**
- `--url` — Your IdP's issuer URL. Must match the `iss` claim in the OIDC tokens exactly.
- `--client-id-list` — The OAuth client ID(s) that fleetshift uses. Must match the `aud` claim in the tokens. Can list multiple.
- `--thumbprint-list` — SHA1 fingerprint of the root CA certificate for the IdP's HTTPS endpoint. AWS uses this to verify the JWKS endpoint is authentic.

**Verify:**
```bash
aws iam list-open-id-connect-providers
# Should show: arn:aws:iam::<account>:oidc-provider/keycloak.example.com/realms/master
```

#### Step 2: Create an IAM Role with trust policy + permissions

The trust policy controls WHO can assume the role (which IdP, which users/groups). The permissions policy controls WHAT the role can do in AWS.

```bash
aws iam create-role \
  --role-name OCP-Provisioner \
  --assume-role-policy-document file://trust-policy.json
```

**trust-policy.json:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/keycloak.example.com/realms/master"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "keycloak.example.com/realms/master:aud": "fleetshift"
        }
      }
    }
  ]
}
```

**Scoping the trust policy (who can assume the role):**

Allow all authenticated users from the IdP:
```json
"Condition": {
  "StringEquals": {
    "keycloak.example.com/realms/master:aud": "fleetshift"
  }
}
```

Restrict to specific users:
```json
"Condition": {
  "StringEquals": {
    "keycloak.example.com/realms/master:aud": "fleetshift",
    "keycloak.example.com/realms/master:sub": "user1@example.com"
  }
}
```

Restrict to users matching a pattern (e.g., group-based sub claims):
```json
"Condition": {
  "StringEquals": {
    "keycloak.example.com/realms/master:aud": "fleetshift"
  },
  "StringLike": {
    "keycloak.example.com/realms/master:sub": "platform-team:*"
  }
}
```

**Attach permissions policy:**
```bash
aws iam put-role-policy \
  --role-name OCP-Provisioner \
  --policy-name ocp-provision-permissions \
  --policy-document file://permissions-policy.json
```

**permissions-policy.json** (covers openshift-install IPI + ccoctl requirements):
```json
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
```

#### Step 3: Verify the setup

**Test the OIDC federation manually:**
```bash
# Get an OIDC token from your IdP (method depends on your IdP)
# For Keycloak, you can use the token endpoint:
TOKEN=$(curl -s -X POST \
  "https://keycloak.example.com/realms/master/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=fleetshift&username=<user>&password=<pass>" \
  | jq -r .access_token)

# Try to assume the role
aws sts assume-role-with-web-identity \
  --role-arn "arn:aws:iam::<ACCOUNT_ID>:role/OCP-Provisioner" \
  --role-session-name "test-session" \
  --web-identity-token "$TOKEN"
```

**Expected output:**
```json
{
  "Credentials": {
    "AccessKeyId": "ASIA...",
    "SecretAccessKey": "...",
    "SessionToken": "...",
    "Expiration": "2026-04-13T23:00:00Z"
  },
  "AssumedRoleUser": {
    "AssumedRoleId": "AROA...:test-session",
    "Arn": "arn:aws:sts::<ACCOUNT_ID>:assumed-role/OCP-Provisioner/test-session"
  }
}
```

**If it fails**, check:
- OIDC provider URL matches `iss` claim in token exactly (trailing slashes matter)
- Client ID in IAM OIDC provider matches `aud` claim in token
- Thumbprint is correct (re-extract if IdP cert was rotated)
- Trust policy `Federated` ARN matches the OIDC provider ARN
- Trust policy conditions match the token's claims

#### Step 4: Register the target in fleetshift

```bash
# The target stores the role ARN and region — not credentials
fleetctl target register \
  --id ocp-aws-dev-east \
  --type ocp \
  --name "AWS Dev us-east-1" \
  --property region=us-east-1 \
  --property role_arn=arn:aws:iam::<ACCOUNT_ID>:role/OCP-Provisioner \
  --property account_id=<ACCOUNT_ID>
```

#### Summary: What exists in AWS

| Resource | Count | Contains secrets? | Purpose |
|---|---|---|---|
| IAM OIDC Identity Provider | 1 per account | No — just issuer URL + thumbprint | Tells AWS to trust your IdP |
| IAM Role (OCP-Provisioner) | 1 per account/team/env | No — just trust policy + permissions | Defines who can assume + what they can do |

No AWS credentials are stored in fleetshift, ocp-engine, or vault. The IAM role ARN is infrastructure configuration, not a secret.

#### Runtime flow

```
User logs into fleetshift UI (Keycloak OIDC)
    │ gets OIDC token (short-lived, e.g., 15 min)
    ▼
User creates deployment targeting "ocp-aws-dev-east"
    │
    ▼
OCP Agent reads target properties: role_arn, region
    │
    ▼
AssumeRoleWithWebIdentity(user's OIDC token, role_arn)
    │
    ▼
AWS validates:
  ✓ Is keycloak.example.com a registered OIDC provider?
  ✓ Is the token signature valid against Keycloak's JWKS?
  ✓ Does the audience claim match ("fleetshift")?
  ✓ Does the trust policy allow this subject?
    │
    ▼
AWS returns: temporary AccessKeyID + SecretAccessKey + SessionToken (1 hour)
    │
    ▼
Written to web identity token file → openshift-install uses it
    │ SDK auto-refreshes by re-reading the file
    ▼
Credentials discarded when provision/destroy completes
```

### Security: Work Directory Credential Hygiene

- **Audit work directory for secrets** — The ocp-engine work directory contains sensitive data written by `openshift-install`: `install-config.yaml` (pull secret, SSH key), `auth/kubeconfig`, `auth/kubeadmin-password`, `.openshift_install.log` (may contain AWS keys), and ignition configs (embedded pull secret). These must be identified and scrubbed or discarded as soon as they are no longer needed — do not leave credentials on disk after the operation completes. The platform (FleetShift OCP agent) is responsible for extracting what it needs (infra_id, cluster_id, kubeconfig for bootstrap), then cleaning up the work directory. For containerized deployments, use ephemeral storage (tmpfs, emptyDir) so credentials are destroyed when the container exits.
