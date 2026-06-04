# FleetShift Credential Lifecycle Analysis

**Date:** 2026-04-09
**Scope:** Analysis of credentials required for AWS OCP cluster provisioning, operations, and lifecycle management via ocp-engine
**Related:** [Zero-Trust Auth Analysis](zero-trust-auth-analysis.md), [Addon Agent Fleetlet Analysis](addon-agent-fleetlet-analysis.md)

---

## Background

The ocp-engine CLI provisions AWS OCP clusters using `openshift-install` (IPI mode). The minimum config (`cluster.yaml`) requires three security-sensitive inputs:

1. **Pull secret** — Red Hat registry authentication (dockerconfig.json)
2. **SSH public key** — Node debug access
3. **AWS credentials** — IAM credentials for provisioning AWS infrastructure

This analysis investigates which of these credentials must be stored by the management platform, for how long, and for what operations — with the goal of minimizing persistent credential storage in line with the zero-trust security model.

---

## Part 1: Live Cluster Investigation

### Method

Investigated a live OCP 4.20 cluster provisioned via ocp-engine IPI on AWS (us-west-2, 3 control-plane + 3 worker nodes). Examined secrets, credential requests, and Cloud Credential Operator (CCO) configuration directly on the cluster.

### Pull Secret

**Location on cluster:** `openshift-config/pull-secret`

**Registries authenticated:**
- `cloud.openshift.com`
- `quay.io`
- `registry.connect.redhat.com`
- `registry.redhat.io`

**Finding:** The cluster holds its own copy of the pull secret. It is used ongoing for pulling operator images, OCP updates, and OperatorHub content. This is the same data provided at install time — the cluster does not create or derive a new pull secret.

**Verdict:** The management platform does NOT need to store the pull secret after provisioning. The cluster is self-sufficient. The pull secret is only needed again to provision additional clusters.

### SSH Public Key

**Location on cluster:** Baked into `MachineConfig 99-master-ssh` as an authorized key on all nodes.

**Finding:** Immutable after install. Embedded in node ignition configs at provision time. The cluster does not use the SSH key for any operational purpose — it exists only for emergency node-level debug access via SSH.

**Verdict:** The management platform does NOT need to store the SSH key after provisioning. It is baked into the nodes and cannot be changed without reprovisioning.

### AWS Credentials

**Cloud Credential Operator Mode:** Mint (default, `credentialsMode: ""`)

**Root credential:** Stored in `kube-system/aws-creds`. This is the same access key provided at install time via `~/.aws/credentials`.

**Minted credentials:** The CCO used the root credential to create 6 separate IAM users, each with scoped permissions and their own access key:

| Namespace | Secret Name | Purpose |
|---|---|---|
| `kube-system` | `aws-creds` | Root admin credential (used by CCO to mint others) |
| `openshift-cloud-credential-operator` | `cloud-credential-operator-iam-ro-creds` | Read-only IAM access for CCO self-verification |
| `openshift-cloud-network-config-controller` | `cloud-credentials` | VPC/subnet/ENI management for networking |
| `openshift-cluster-csi-drivers` | `ebs-cloud-credentials` | EBS volume provisioning (CSI driver) |
| `openshift-image-registry` | `installer-cloud-credentials` | S3 bucket access for image registry |
| `openshift-ingress-operator` | `cloud-credentials` | Route53/ELB management for ingress |
| `openshift-machine-api` | `aws-cloud-credentials` | EC2 instance lifecycle for machine management |

**Total unique AWS access key IDs on cluster: 7** (1 root + 6 minted)

**Key insight:** The root provisioning credential IS stored on the cluster. The CCO uses it as the master key to create scoped IAM users. The 6 minted credentials are self-sufficient for day-to-day cluster operations.

---

## Part 2: Full Lifecycle Credential Requirements

### Which operations need AWS credentials, and where do they come from?

| Lifecycle Operation | Needs AWS Creds? | Source of Creds | Management Side Required? |
|---|---|---|---|
| **Day-to-day operations** (pod scheduling, EBS volumes, networking, ingress) | Yes | Cluster's 6 minted credentials (self-sufficient) | No |
| **Node scaling** (MachineSet replica changes) | Yes | `openshift-machine-api/aws-cloud-credentials` (minted, on cluster) | No |
| **Z-stream upgrades** (e.g., 4.20.17 to 4.20.18) | Possibly, if new CredentialsRequests added | Root cred in `kube-system/aws-creds` (on cluster) | No — CCO processes automatically if root secret is present |
| **Minor upgrades** (e.g., 4.20 to 4.21) | Yes — critical | Root cred in `kube-system/aws-creds` must be present | Maybe — only if root secret was removed post-install |
| **Credential rotation** | Yes | New admin credentials must be pushed to `kube-system/aws-creds`; CCO re-mints all scoped users | Yes — someone must provide new creds |
| **Cluster destroy** | Yes | Management-side credentials passed to `openshift-install destroy cluster` | Yes — must be available on management side |

### Upgrade Credential Details

From the OCP documentation on mint mode:

> "The automatic, continuous reconciliation of cloud credentials in mint mode allows actions that require additional credentials or permissions, such as upgrading, to proceed."

However, if the root secret has been removed post-install (a supported hardening option):

> "Prior to a non z-stream upgrade, you must reinstate the credential secret with the administrator-level credential. If the credential is not present, the upgrade might be blocked."

This means:
- **Root secret present on cluster (default):** Upgrades are fully self-service. CCO automatically processes new CredentialsRequests from the updated release. No management-side intervention needed.
- **Root secret removed post-install (hardened):** Must push root credential back to `kube-system/aws-creds` before every minor upgrade. Management side must store creds for both upgrades AND destroy.

### Destroy Credential Details

`openshift-install destroy cluster` runs from the management side, not from the cluster. It:
1. Reads `metadata.json` to get the `infraID` and AWS region
2. Uses AWS credentials (from env vars or credentials file) to scan the region for resources tagged `kubernetes.io/cluster/<infraID>: owned`
3. Deletes all matching resources (EC2 instances, VPCs, subnets, ELBs, Route53 records, IAM users, S3 buckets, etc.)

The destroy process does NOT use the cluster's own credentials — it runs entirely from the management server.

### Critical Finding: Credential Portability for Destroy

The destroy operation is **not tied to the provisioning credentials**. `openshift-install destroy cluster` does not verify that the credentials used for destroy are the same ones used for provisioning. It only needs:

1. `metadata.json` — to know the `infraID` and region
2. **Any valid AWS credentials** with sufficient IAM permissions to delete resources tagged `kubernetes.io/cluster/<infraID>: owned`

This means:
- User A provisions a cluster. User B can destroy it using their own AWS credentials, as long as User B has the required IAM permissions (EC2, VPC, ELB, Route53, IAM, S3 delete actions + `tag:GetResources` in us-east-1).
- **The management platform does not need to store the provisioning user's credentials for later destroy.** Any authorized user/role with the right AWS permissions can perform the destroy.
- The only artifact that MUST be persisted for destroy is `metadata.json` (cluster identity: infraID + region), not any AWS credentials.

**Impact on credential storage:** This significantly weakens the argument for persistent credential storage on the management platform. The platform needs to persist `metadata.json` per cluster, but AWS credentials can be provided at the time of the lifecycle operation by whoever is performing it — or resolved from an IAM role attached to the management server, or assumed via STS at operation time.

---

## Part 3: Credential Storage Requirements Summary

### What the management platform must store

| Credential | Store for provisioning? | Store after provisioning? | Reason |
|---|---|---|---|
| Pull secret | Yes (passed to ocp-engine) | **No** | Cluster holds its own copy. Only needed for provisioning new clusters. |
| SSH public key | Yes (passed to ocp-engine) | **No** | Baked into node ignition at install time. Immutable. |
| AWS credentials | Yes (passed to ocp-engine) | **No** | Destroy/lifecycle operations accept any valid AWS credentials at invocation time — not tied to provisioning credentials. |
| `metadata.json` | N/A (generated during install) | **Yes** | Contains `infraID` and region. Required for cluster destroy. This is the only artifact that must be persisted per cluster. |

### What the cluster holds independently

| Credential | Where on cluster | Self-sufficient? |
|---|---|---|
| Pull secret | `openshift-config/pull-secret` | Yes — used for image pulls and updates |
| SSH public key | `MachineConfig 99-master-ssh` | Yes — baked into all nodes |
| AWS root credential | `kube-system/aws-creds` | Yes — CCO uses to mint scoped creds |
| 6 minted AWS creds | Various namespaces (see table above) | Yes — each scoped to specific operator needs |

---

## Part 4: Zero-Trust Credential Handling Options

Since AWS credentials MUST be available on the management side for at least cluster destroy, the question becomes: how do we hold them in a zero-trust-aligned way?

### Option 1: Short-Lived STS Credentials

Don't store long-lived IAM access keys. Instead, store an IAM role ARN. At destroy/upgrade time, call `sts:AssumeRole` to get temporary credentials (configurable duration, typically 1 hour max). The role can require:
- MFA for assumption
- External ID for cross-account access
- Condition keys restricting who/when/where the role can be assumed

**Pros:** No long-lived keys in the management platform. Credentials expire automatically. Role assumption can be audited via CloudTrail.
**Cons:** Requires IAM role setup per cluster or per account. AssumeRole itself requires some base credential.

### Option 2: Just-in-Time Credential Retrieval

Store only a pointer to credentials (e.g., AWS Secrets Manager ARN, HashiCorp Vault path). At lifecycle operation time, retrieve the credential, use it, discard it. The secrets manager enforces access control independently of the management platform.

**Pros:** Management platform never caches credentials. Access logged by the secrets manager. Can enforce approval workflows.
**Cons:** Adds a dependency on an external secrets manager. Network availability required at lifecycle time.

### Option 3: External Secrets Manager with Signed Intent

Combine with the fleetshift attestation model: the management platform requests credentials from a broker, but the broker requires a valid signed deployment/lifecycle intent (user-signed attestation) before releasing credentials. A compromised management platform cannot obtain credentials without also possessing a valid user signature.

**Pros:** Strongest zero-trust alignment. Credential access is tied to provable user intent. Even with full management platform compromise, no credentials without user key.
**Cons:** Most complex to implement. Requires credential broker infrastructure.

---

## Part 5: Full Credential Handling Options Analysis

Complete range of options from zero storage to full storage, with trade-offs for each.

### Option 1: Store Nothing — Credentials Provided at Invocation Time

The management platform stores only `metadata.json` per cluster. Every lifecycle operation requires the caller to provide credentials at the time of the request.

**Provision:** User provides pull secret + AWS creds + SSH key
**Destroy:** User (same or different) provides AWS creds
**Upgrade (if root secret removed):** User provides AWS creds to push to cluster

**Pros:**
- Zero credential exposure on management platform
- Compromised management platform leaks nothing
- Perfect zero-trust alignment
- Simplest to implement

**Cons:**
- Every operation requires the user to have credentials available
- Can't automate lifecycle operations (destroy, upgrade) without human providing creds
- No scheduled/automated cluster teardown (e.g., TTL-based expiry clusters)
- Pull secret must be available to every user who provisions — shared secret management becomes the user's problem

### Option 2: Store Nothing — Management Server IAM Role (Instance Profile)

The management server runs on an EC2 instance (or ECS/EKS) with an IAM instance profile that has permissions to provision/destroy OCP clusters. Pull secret is on a shared filesystem or fetched from AWS Secrets Manager at runtime without caching.

**Provision:** Pull secret fetched from Secrets Manager, AWS creds from instance profile
**Destroy:** AWS creds from instance profile
**Upgrade:** AWS creds from instance profile if needed

**Pros:**
- No credentials stored in the management platform's database or config
- AWS handles credential rotation automatically (instance profile creds rotate every ~6 hours)
- Enables automation (scheduled destroys, TTL clusters)
- Management platform compromise doesn't expose persistent credentials — instance profile creds are short-lived and role-scoped

**Cons:**
- Ties the management platform to AWS (not cloud-agnostic)
- The IAM role is powerful — anyone with access to the management server gets the role's permissions
- No per-user attribution for AWS API calls (all calls come from the instance role)
- Pull secret still needs to be accessible somewhere (Secrets Manager, shared mount)

### Option 3: Store Role ARN Only — STS AssumeRole at Operation Time

The management platform stores an IAM role ARN per AWS account (not per cluster). At lifecycle operation time, it calls `sts:AssumeRole` to get short-lived credentials (15 min to 1 hour). Pull secret referenced by path or Secrets Manager ARN.

**Provision:** AssumeRole -> temp creds, fetch pull secret from reference
**Destroy:** AssumeRole -> temp creds
**Upgrade:** AssumeRole -> temp creds if needed

**Pros:**
- No long-lived keys stored
- Temporary credentials expire automatically
- Role assumption is audited in CloudTrail
- Can add conditions: MFA required, external ID, source IP restrictions
- Per-account role scoping (different roles for dev vs prod accounts)

**Cons:**
- Requires a base credential to call AssumeRole (instance profile, or one set of long-lived keys)
- IAM role setup required per AWS account
- If the base credential is compromised, attacker can assume the role (mitigated by MFA/conditions)
- More complex than Option 1 or 2

### Option 4: Store References Only — External Secrets Manager

The management platform stores pointers (ARN, vault path) to credentials in an external secrets manager (AWS Secrets Manager, HashiCorp Vault, CyberArk). Credentials are fetched just-in-time, used, and discarded.

**Stored per cluster:** `metadata.json`, secrets manager reference for AWS creds, secrets manager reference for pull secret
**Provision:** Fetch pull secret + AWS creds from secrets manager
**Destroy:** Fetch AWS creds from secrets manager

**Pros:**
- Management platform never holds actual credentials
- Secrets manager provides its own access control, audit logging, rotation
- Supports credential rotation transparently (update in secrets manager, platform fetches latest)
- Can enforce approval workflows in the secrets manager

**Cons:**
- Adds hard dependency on external secrets manager availability
- Network connectivity to secrets manager required at operation time
- The management platform needs credentials to authenticate to the secrets manager itself (chicken-and-egg, typically solved by IAM role or workload identity)
- More operational complexity

### Option 5: Store References + Signed Intent — Credential Broker

Combines Option 4 with the fleetshift attestation model. A credential broker sits between the management platform and AWS. The broker requires **both** a valid platform identity **and** a cryptographically signed user intent (fleetshift attestation) before releasing credentials.

**Stored per cluster:** `metadata.json`, broker endpoint
**Provision:** User signs intent -> platform sends signed intent to broker -> broker validates attestation -> broker returns short-lived AWS creds + pull secret
**Destroy:** User signs destroy intent -> same flow

**Pros:**
- Strongest zero-trust alignment
- Compromised management platform cannot obtain credentials without a valid user signature
- Full audit trail: who requested, what they signed, when credentials were issued
- Credential access tied to provable, verifiable user intent
- Broker can enforce policies (only destroy clusters you created, require approval for prod)

**Cons:**
- Most complex to implement — requires building the credential broker
- Requires the attestation/signing infrastructure to be fully operational
- Cannot perform unattended operations (no automation without a signer) unless you build a machine identity signing path
- Additional infrastructure to deploy and maintain

### Option 6: Store Encrypted Credentials — Envelope Encryption

The management platform stores AWS credentials and pull secret directly, but encrypted with envelope encryption. Each secret encrypted with a data encryption key (DEK), DEK encrypted with a key encryption key (KEK) from an external KMS.

**Stored per cluster:** `metadata.json`, encrypted AWS creds, encrypted pull secret
**Provision:** Decrypt creds via KMS, use
**Destroy:** Decrypt creds via KMS, use

**Pros:**
- Enables full automation (scheduled destroys, TTL clusters, auto-upgrades)
- No external secrets manager dependency at operation time (creds are local, just encrypted)
- KMS compromise required to decrypt — separate blast radius
- Simple programming model (read, decrypt, use)

**Cons:**
- Credentials ARE stored, just encrypted — a compromised platform + compromised KMS = full access
- Long-lived credentials that need manual rotation
- Key-per-cluster DEKs add management overhead
- Doesn't prevent a privileged platform operator from decrypting if they have KMS access

### Option 7: Store Credentials in External Vault — Full Storage

The management platform stores AWS credentials and pull secret in an external vault (HashiCorp Vault, AWS Secrets Manager) with the platform having direct read access. No signed intent required.

**Stored:** AWS creds + pull secret in vault, platform fetches on demand
**Provision/Destroy:** Platform reads from vault, uses directly

**Pros:**
- Simplest automation model — platform can do anything anytime
- Vault provides encryption at rest, access logging, rotation policies
- Supports all lifecycle operations without human involvement

**Cons:**
- Platform has standing access to all credentials — compromised platform = compromised credentials
- No zero-trust property — the platform is fully trusted
- Vault access control is the entire security boundary

### Comparison Matrix

| Option | Credentials Stored | Automation? | Zero-Trust Level | Complexity | Survives Platform Compromise? |
|---|---|---|---|---|---|
| 1. Nothing (user provides) | None | No | Highest | Lowest | Yes — nothing to steal, no credentials on platform at any time |
| 2. Instance profile | None (IAM role on host) | Yes | High | Low | No while compromised — attacker can use the instance role as long as they have server access. Damage stops when server is isolated. Creds are short-lived (~6h rotation) but continuously available. |
| 3. Role ARN + STS | Role ARN only | Yes | High | Medium | No while compromised — attacker can assume the role on demand. Damage stops when base credential is revoked or role trust policy is updated. All assumptions auditable in CloudTrail. |
| 4. References + Secrets Manager | Pointers only | Yes | High | Medium | No while compromised — attacker can fetch secrets on demand. Damage stops when secrets manager access is revoked. All reads auditable. |
| 5. References + Signed Intent | Pointers only | With signer | Highest | High | Yes — compromised platform alone cannot obtain credentials. Requires a valid cryptographic signature from a user's private key (held on their device, not on the platform). |
| 6. Encrypted credentials | Encrypted creds | Yes | Medium | Medium | No while compromised — if attacker also has KMS access (same AWS account, compromised IAM), they can decrypt. Damage stops when KMS access is revoked. If KMS is in a separate security domain, attacker needs to compromise both independently. |
| 7. Full vault storage | Creds in vault | Yes | Low-Medium | Low | No while compromised — platform has standing read access. Damage scope depends on vault access policy: broad read = full exposure; scoped policies with audit and approval workflows = limited exposure. |

### CCO STS Mode (Orthogonal Option)

Independent of the above options, OCP supports running clusters with STS-based credentials instead of long-lived IAM keys. The cluster uses OIDC federation (IRSA-style) to get short-lived tokens from AWS STS. No long-lived IAM access keys are stored on the cluster itself.

**Pros:** Eliminates long-lived keys on the cluster side. AWS-native. Well-supported by OCP.
**Cons:** More complex cluster setup (requires pre-created OIDC provider and IAM roles). Still need some credential on the management side for destroy (the OIDC provider itself must be cleaned up). Can be combined with any of the 7 options above for the management-side credential handling.

---

## Part 6: Just-In-Time Credential Acquisition — Proven Flows

This section documents the proven just-in-time credential acquisition methods for each of the three credentials, with the goal of OME storing nothing — all credentials are acquired at operation time and discarded after use.

### SSH Key — Auto-Generate (Proven, Trivial)

- OME auto-generates an ED25519 key pair at provision time
- User is offered the option to upload their own public key instead
- Private key is offered for download to the user
- OME discards both keys after provisioning completes
- No storage required

### Pull Secret — Red Hat SSO OAuth (Proven)

**Verified working flow using `ocm` CLI and Red Hat SSO:**

The pull secret can be fetched programmatically via the Red Hat accounts management API after authenticating through Red Hat SSO. This was tested and confirmed working on 2026-04-10.

**OAuth/API Details:**

| Parameter | Value |
|---|---|
| Authorization endpoint | `https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/auth` |
| Token endpoint | `https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token` |
| Client ID | `ocm-cli` |
| Scopes | `openid` |
| Pull secret API | `POST https://api.openshift.com/api/accounts_mgmt/v1/access_token` |
| Request body | `{}` (empty JSON object) |
| Auth header | `Authorization: Bearer <access_token>` |

**Tested CLI flow (device code grant):**

```bash
# Step 1: SSO login via device code (opens browser on another device)
ocm login --use-device-code
# Prints: "To login, navigate to https://sso.redhat.com/device and enter code XXXX-XXXX"
# User authenticates in browser, CLI receives tokens

# Step 2: Fetch pull secret
echo '{}' | ocm post /api/accounts_mgmt/v1/access_token
# Returns: full pull secret JSON with auths for cloud.openshift.com, quay.io,
#          registry.connect.redhat.com, registry.redhat.io
```

**What `ocm` stores after login (`~/.config/ocm/ocm.json`):**

```json
{
  "access_token": "<JWT Bearer token>",
  "refresh_token": "<token>",
  "client_id": "ocm-cli",
  "scopes": ["openid"],
  "token_url": "https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token",
  "url": "https://api.openshift.com"
}
```

**Two implementation options for OME:**

**Option A: Browser-based OAuth redirect (authorization code flow) — Preferred for production**

1. OME opens popup to Red Hat SSO authorization endpoint with `response_type=code&client_id=ocm-cli&scope=openid&redirect_uri=http://localhost:8080/callback/redhat`
2. User logs into Red Hat SSO
3. Red Hat redirects back to OME with auth code
4. OME exchanges auth code for access token at the token endpoint
5. OME calls `POST https://api.openshift.com/api/accounts_mgmt/v1/access_token` with Bearer token
6. Pull secret received, all tokens discarded

**Caveat:** The `client_id=ocm-cli` may not allow arbitrary `redirect_uri` values — Red Hat SSO likely has a whitelist. May need to register OME as its own OAuth client, or fall back to device code flow.

**Option B: Device code flow — Proven, works from any environment**

1. OME backend initiates device code flow with Red Hat SSO
2. OME UI shows the user: "Go to https://sso.redhat.com/device and enter code XXXX-XXXX"
3. User authenticates in their browser
4. OME backend polls the token endpoint until the user completes authentication
5. OME gets access token, fetches pull secret, discards everything

**This option is proven working and avoids redirect_uri restrictions.** Recommended for the POC.

**Note:** Red Hat is actively moving away from offline tokens toward SSO-based authentication. The Red Hat API tokens page now shows a warning recommending SSO credentials over API tokens. This confirms that the OAuth-based flow is the right long-term direction.

### AWS Credentials — Two Proven Options

**Option A: AWS SSO / Identity Center login (explicit user login) — Selected for POC**

User authenticates to AWS through the OME UI, OME gets short-lived STS credentials.

1. User clicks "Create Cluster" in OME UI
2. OME opens popup/redirect to AWS SSO login page
3. User authenticates with their AWS identity
4. OME receives short-lived credentials via `sso:GetRoleCredentials`
5. Credentials used for provisioning, discarded after completion

Requires the org to have AWS Identity Center (AWS SSO) configured.

**Option B: Keycloak OIDC federation with AWS (`AssumeRoleWithWebIdentity`) — Documented alternative**

User's existing Keycloak OIDC session is used to obtain AWS credentials with no additional login step.

1. Admin one-time setup: register Keycloak as an OIDC identity provider in AWS IAM, create IAM roles with trust policies referencing Keycloak issuer
2. User logs into OME via Keycloak (already done at step 1 of the overall flow)
3. OME backend calls `sts:AssumeRoleWithWebIdentity` with the user's Keycloak OIDC token
4. AWS validates the token against Keycloak's JWKS, checks the role's trust policy conditions
5. AWS returns short-lived credentials (15 min to 1 hour)
6. Credentials used for provisioning, discarded after completion

**Advantages over Option A:** Eliminates the separate AWS login step — the user's Keycloak login IS their AWS authentication. Per-user attribution in CloudTrail. Role trust policy controls which Keycloak users/groups can provision.

**Requires:** One-time admin setup (Keycloak registered as IAM identity provider + IAM role with trust policy).

### Combined Zero-Storage Flow (Target UX)

```
1. User logs into OME UI (Keycloak OIDC) ← single login
2. User clicks "Create Cluster", fills in cluster config
3. OME UI shows Red Hat device code: "Go to sso.redhat.com/device, enter XXXX-XXXX"
   User authenticates → OME fetches pull secret → held in memory
4. AWS credentials acquired:
   - Option A: AWS SSO popup → user authenticates → temp creds
   - Option B: Keycloak token → AssumeRoleWithWebIdentity → temp creds (no extra login)
5. OME auto-generates SSH key pair
6. ocp-engine provision runs with all three credentials
7. Credentials discarded as each phase completes:
   - Pull secret: discarded after install-config phase
   - SSH public key: discarded after install-config phase
   - AWS creds: discarded after cluster phase (or failure)
   - SSH private key: offered for download, then discarded
8. Only metadata.json (infraID + region) retained for destroy operations

For destroy: User provides AWS creds again via the same flow (step 4).
Any user with appropriate AWS permissions can destroy — not tied to provisioning user.
```

**OME stores nothing. All credentials are just-in-time and discarded after use.**

### Production Considerations (Not for POC)

- **In-memory only:** Production implementation should never write credentials to filesystem — all credential handling in memory. POC uses disk via ocp-engine/openshift-install as-is.
- **Tmpfs for installer artifacts:** install-config.yaml and ignition configs contain baked-in pull secret and SSH key. Production should use ephemeral tmpfs mounts.
- **Red Hat OAuth client registration:** Production deployment should register OME as its own OAuth client with Red Hat SSO rather than using `ocm-cli` client ID, enabling proper redirect_uri support.
- **AWS federation vs SSO:** Production deployments should evaluate Option B (Keycloak federation) to eliminate the extra AWS login step entirely.

### POC Limitation: STS Temp Creds with Default CCO Mode

The POC demo uses `credentialsMode: Manual` in install-config.yaml because the default CCO mint mode rejects STS temporary credentials (it requires long-lived IAM user keys to create scoped IAM users). With `credentialsMode: Manual`, the installer accepts STS creds for provisioning, but the cluster's operators won't get auto-minted scoped credentials. Once the STS session expires (~1 hour), operators that need AWS access (MachineAPI, CSI driver, ingress, image registry) will stop functioning.

**This is acceptable for a POC demo** — the cluster installs and operates for the duration of the demo. The production solution (CCO STS mode with OIDC federation) eliminates this limitation entirely.

### POC Limitation: STS Token Expiry During Long Operations

The `aws configure export-credentials` command exports a snapshot of the current STS token (~10-15 minute TTL). When baked into `cluster.yaml`, these credentials cannot auto-refresh. For provisioning (30-45 minutes) or destroy operations, the token may expire mid-operation.

**Observed behavior:** `openshift-install` fails with `RequestExpired: Request has expired` when the baked-in STS token expires during a long-running phase.

**Required improvement for ocp-engine:** Instead of baking AWS credentials into the config file, pass them as environment variables to the `openshift-install` subprocess. Re-resolve credentials from the AWS SDK credential chain (which reads from `~/.aws/login/cache/` and auto-refreshes) before each phase. This allows the AWS SDK's built-in token refresh mechanism to keep credentials valid for the full duration of the operation.

**For the POC:** Use long-lived IAM keys (credentials file) for actual provisioning, and demonstrate the STS flow in `--dry-run` mode only.

---

## Part 7: Production Path — CCO STS Mode with OIDC Federation

### Overview

CCO STS mode is the production-grade approach for using temporary credentials with OCP on AWS. It eliminates long-lived IAM keys everywhere — both on the management side and on the cluster. Each cluster component gets its own IAM role and obtains short-lived STS tokens via OIDC federation that auto-refresh every hour.

This is the same architecture that ROSA (Red Hat's managed OpenShift on AWS) uses.

### How It Works

```
Pod (Operator/Workload)
    │
    ├── 1. Has projected service account token (JWT) + IAM Role ARN
    │      (token mounted at /var/run/secrets/cloud/token, auto-refreshed)
    │
    ▼
AWS IAM OIDC Identity Provider
    │
    ├── 2. Validates JWT signature using public key (stored in S3 bucket)
    ├── 3. Checks: token not expired, issuer URL matches, audience matches
    ├── 4. Verifies: service account subject matches IAM role trust policy
    │
    ▼
AWS STS (AssumeRoleWithWebIdentity)
    │
    ├── 5. Returns temporary credentials (access key, secret key, session token)
    │      Valid for ~1 hour, auto-refreshed by the SDK
    │
    ▼
Pod uses temp credentials to call AWS APIs
    (EC2, S3, ELB, Route53, etc. — scoped to what the IAM role allows)
```

**Key properties:**
- No long-lived IAM access keys stored anywhere on the cluster
- Each operator gets the minimum permissions it needs (least privilege)
- Credentials auto-refresh every hour — no expiry/degradation problem
- All credential assumptions are auditable in CloudTrail
- Compromise of one operator's role doesn't grant access to other operators' permissions

### Setup Steps (Pre-Install)

#### Step 1: Extract the `ccoctl` binary

The Cloud Credential Operator utility (`ccoctl`) is bundled in the OCP release image alongside `openshift-install`.

```bash
# Extract ccoctl from the release image
oc adm release extract \
  --command=ccoctl \
  --to=/usr/local/bin \
  quay.io/openshift-release-dev/ocp-release:4.20.18-x86_64
```

#### Step 2: Extract CredentialsRequest manifests

Each OCP release defines a set of `CredentialsRequest` objects — one per operator that needs AWS access. These specify what IAM permissions each operator requires.

```bash
# Extract all CredentialsRequest manifests from the release image
oc adm release extract \
  --credentials-requests \
  --included \
  --to=./credrequests \
  quay.io/openshift-release-dev/ocp-release:4.20.18-x86_64
```

This produces ~6-8 YAML files, one per operator (machine-api, ingress, image-registry, CSI driver, cloud-network-config, CCO read-only, etc.).

#### Step 3: Create all AWS resources with `ccoctl`

```bash
ccoctl aws create-all \
  --name=<cluster-name> \
  --region=<aws-region> \
  --credentials-requests-dir=./credrequests
```

This single command creates:

1. **RSA key pair** — Used to sign service account tokens
2. **S3 bucket** (`<name>-oidc`) — Stores the OIDC discovery document and JWKS (public keys)
3. **IAM OIDC Identity Provider** — Registered in AWS IAM, trusts tokens signed by the key pair
4. **IAM roles** (one per CredentialsRequest) — Each with:
   - A **trust policy** that allows the specific service account (e.g., `system:serviceaccount:openshift-machine-api:machine-api-controllers`) to assume the role via the OIDC provider
   - A **permissions policy** with the exact AWS API actions the operator needs
5. **Kubernetes Secret manifests** — One per role, containing the IAM role ARN (not keys) for the operator to reference

Output directory structure:
```
<output-dir>/
  manifests/                           # Copy these into the install dir
    openshift-machine-api-aws-cloud-credentials-credentials.yaml
    openshift-ingress-operator-cloud-credentials-credentials.yaml
    openshift-image-registry-installer-cloud-credentials-credentials.yaml
    openshift-cluster-csi-drivers-ebs-cloud-credentials-credentials.yaml
    ...
    cluster-authentication-02-config.yaml   # Sets the OIDC issuer URL
  tls/
    bound-service-account-signing-key.key   # Private signing key
```

#### Step 4: Prepare install-config.yaml

```yaml
credentialsMode: Manual
```

#### Step 5: Generate manifests and inject ccoctl output

```bash
# Generate install manifests
openshift-install create manifests --dir=<install-dir>

# Copy ccoctl-generated manifests into the install directory
cp <ccoctl-output>/manifests/* <install-dir>/manifests/

# Copy the signing key
cp <ccoctl-output>/tls/bound-service-account-signing-key.key <install-dir>/tls/
```

#### Step 6: Run the installer

```bash
openshift-install create cluster --dir=<install-dir>
```

The installer uses the provided AWS credentials (which CAN be STS temp creds in manual mode) to create infrastructure. The cluster's operators use the OIDC federation path for ongoing AWS access — no dependency on the installer's credentials after bootstrap.

### What the Cluster Looks Like After Install

| Component | Credential Source | Key Type |
|---|---|---|
| `kube-system/aws-creds` | **Does not exist** — no root credential | N/A |
| Machine API | IAM role via OIDC federation | STS temp (auto-refresh) |
| CSI driver | IAM role via OIDC federation | STS temp (auto-refresh) |
| Ingress operator | IAM role via OIDC federation | STS temp (auto-refresh) |
| Image registry | IAM role via OIDC federation | STS temp (auto-refresh) |
| Cloud network config | IAM role via OIDC federation | STS temp (auto-refresh) |
| CCO (read-only) | IAM role via OIDC federation | STS temp (auto-refresh) |

**No long-lived keys anywhere on the cluster.** Every AWS API call uses credentials that expire and auto-refresh.

### Upgrade Considerations

CCO STS mode requires manual credential management during upgrades:

1. Extract `CredentialsRequest` manifests from the **new** release image
2. Run `ccoctl aws create-iam-roles` with the new manifests (creates new roles or updates existing)
3. Apply the updated manifests to the cluster
4. Annotate the `CloudCredential` resource to indicate permissions are updated
5. Proceed with the upgrade

This is more work than mint mode (which handles upgrades automatically), but eliminates persistent credentials entirely.

### Destroy with CCO STS Mode

`openshift-install destroy cluster` still works the same way — it uses the management-side credentials to tear down tagged AWS resources. Additionally, you need to clean up the OIDC resources:

```bash
# Destroy the cluster
openshift-install destroy cluster --dir=<install-dir>

# Clean up OIDC resources (S3 bucket, IAM identity provider, IAM roles)
ccoctl aws delete \
  --name=<cluster-name> \
  --region=<aws-region>
```

---

## Part 8: How ROSA Does It

ROSA (Red Hat OpenShift Service on AWS) uses the same STS + OIDC architecture as CCO STS mode, but fully automated by the ROSA service. Understanding ROSA's approach informs what OME's production implementation should look like.

### ROSA STS Architecture

ROSA uses AWS STS to provide temporary, limited-permission credentials for cluster operations. Credentials expire after one hour and are auto-refreshed. Neither Red Hat nor the cluster stores long-lived AWS keys.

### IAM Resources ROSA Creates

Before deploying a ROSA STS cluster, the following AWS IAM resources are created:

1. **Account-wide roles** (created once per AWS account):
   - `ManagedOpenShift-Installer-Role` — Used by the installation program
   - `ManagedOpenShift-ControlPlane-Role` — Used by the control plane
   - `ManagedOpenShift-Worker-Role` — Used by compute nodes
   - `ManagedOpenShift-Support-Role` — Used by Red Hat SRE for support

2. **Account-wide Operator policies** — Define permissions for cluster operators

3. **Cluster-specific Operator IAM roles** — One per operator, scoped to the specific cluster via trust policies that reference the cluster's OIDC provider

4. **OIDC provider** — Per-cluster, stores JWKS in S3, registered as an IAM identity provider

### ROSA Credential Flow (Detailed)

1. **Cluster installation:**
   - ROSA CLI creates the account-wide roles (one-time)
   - ROSA CLI creates the OIDC provider and cluster-specific operator roles
   - The Red Hat installation program assumes `ManagedOpenShift-Installer-Role` in the customer's account via STS
   - Temporary credentials are returned from AWS STS
   - Installation program uses temp creds to create VPCs, EC2 instances, etc.
   - Once installed, operators authenticate via OIDC federation (see below)

2. **Runtime operator authentication:**
   - Each operator pod has a projected service account token (JWT) mounted
   - The JWT contains: issuer (OIDC provider URL), subject (service account name), audience (`sts.amazonaws.com`)
   - Operator passes the JWT to `sts:AssumeRoleWithWebIdentity`
   - AWS validates the JWT against the OIDC provider's JWKS (public keys in S3)
   - AWS checks the IAM role's trust policy: does the subject match?
   - AWS returns temp credentials (access key + secret + session token)
   - Operator uses temp creds for AWS API calls
   - Credentials auto-refresh before expiry

3. **User workloads (IRSA):**
   - User creates a service account with `eks.amazonaws.com/role-arn` annotation
   - Pod identity webhook intercepts pod creation and injects token volume + env vars
   - Same OIDC flow as operators — pod assumes the specified IAM role

### What ROSA Proves for OME

ROSA demonstrates that the full OCP lifecycle can run without any long-lived AWS credentials:

| Aspect | ROSA Approach | OME Production Target |
|---|---|---|
| Install credentials | STS via role assumption | STS via browser login (SSO or federation) |
| Cluster operator credentials | OIDC federation, auto-refresh | Same — CCO STS mode |
| Credential storage | Zero long-lived keys | Zero long-lived keys |
| Upgrade credentials | Automated role/policy updates | Manual `ccoctl` update (can be automated) |
| Destroy credentials | STS via role assumption | STS via browser login |
| Credential rotation | Automatic (1-hour tokens) | Automatic (1-hour tokens) |

### OME Production Implementation Plan

For production, OME would automate the `ccoctl` workflow:

1. **User clicks "Create Cluster" in OME UI**
2. OME acquires temp AWS credentials (via SSO/federation — proven in POC)
3. OME acquires pull secret (via Red Hat SSO — proven in POC)
4. OME auto-generates SSH key pair (proven in POC)
5. **OME runs `ccoctl aws create-all`** using the temp AWS credentials — creates OIDC provider, IAM roles, signing key
6. OME generates install-config.yaml with `credentialsMode: Manual`
7. OME injects `ccoctl` output manifests into the install directory
8. OME runs `openshift-install create cluster`
9. All temp credentials discarded after install
10. Cluster operates entirely on OIDC federation — no stored credentials anywhere

**For destroy:** OME acquires fresh temp AWS credentials, runs `openshift-install destroy cluster`, then runs `ccoctl aws delete` to clean up OIDC resources.

---

## References

- [Using mint mode - Cloud Credential Operator (OCP 4.13)](https://docs.openshift.com/container-platform/4.13/authentication/managing_cloud_provider_credentials/cco-mode-mint.html)
- [Preparing to update a cluster with manually maintained credentials (OCP 4.11)](https://docs.openshift.com/container-platform/4.11/updating/preparing-manual-creds-update.html)
- [How does openshift-install destroy cluster work on AWS IPI](https://access.redhat.com/solutions/7063308)
- [How to replace AWS credentials used during installation](https://access.redhat.com/solutions/4277801)
- [About the Cloud Credential Operator (OCP 4.8)](https://docs.openshift.com/container-platform/4.8/authentication/managing_cloud_provider_credentials/about-cloud-credential-operator.html)
- [Using manual mode with AWS STS (OCP 4.9)](https://docs.openshift.com/container-platform/4.9/authentication/managing_cloud_provider_credentials/cco-mode-sts.html)
- [ROSA STS IAM Resources](https://docs.redhat.com/en/documentation/red_hat_openshift_service_on_aws_classic_architecture/4/html/introduction_to_rosa/rosa-sts-about-iam-resources)
- [ROSA OIDC Overview](https://docs.redhat.com/en/documentation/red_hat_openshift_service_on_aws_classic_architecture/4/html/introduction_to_rosa/rosa-oidc-overview)
- [AWS STS and ROSA HCP Explained](https://docs.redhat.com/en/documentation/red_hat_openshift_service_on_aws/4/html/about/cloud-experts-rosa-hcp-sts-explained)
- [Fine-grained IAM roles for ROSA workloads with STS (AWS Blog)](https://aws.amazon.com/blogs/containers/fine-grained-iam-roles-for-red-hat-openshift-service-on-aws-rosa-workloads-with-sts/)
- [openshift/installer#4596 — Support more auth options in manual mode](https://github.com/openshift/installer/pull/4596)
- [CCO Manual Mode Docs](https://github.com/openshift/cloud-credential-operator/blob/master/docs/mode-manual-creds.md)
