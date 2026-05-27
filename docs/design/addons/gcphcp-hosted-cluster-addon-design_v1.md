# GCP HCP Hosted Cluster Addon V1 Design

**Purpose:** Architectural design for the `gcphcp` FleetShift addon.

This document describes the intentionally simplified first implementation, not the full long-term
shape of the addon. It emphasizes the smallest useful end-to-end model that fits FleetShift's
current reconciliation and workflow behavior, even when that means deliberately leaving out
capabilities that a later version could add.

For the explicit list of intentional omissions and deferred capabilities, see
[`11. Known Limitations And Future Work`](#11-known-limitations-and-future-work).

The design centers on a **managed-resource front door** backed by **self-targeted delivery**:

- the user-facing object is a managed resource (`api.gcphcp.cluster`)
- the addon seeds targets from config, one per GCP project + region
- the managed resource derives to a target through `RegisteredSelfTarget`
- the delivery agent executes provisioning and teardown against the external HCP control plane

---

## 1. Architectural Context

### 1.1 Managed-resource + delivery hybrid

The `gcphcp` addon is a managed-resource plus delivery hybrid, the strongest model for a real
cluster-management addon within FleetShift:

- declares `DeliveryCapability`
- declares `ManagedResourceCapability`
- connects with a delivery agent
- connects with a managed-resource schema
- seeds a self target
- derives managed resources to that self target through `RegisteredSelfTarget`

### 1.2 Dynamic addon-owned APIs

Managed-resource schemas compile into dynamic runtime APIs:

- dynamic gRPC service
- dynamic HTTP routes
- dynamic reflection descriptors

The `gcphcp` addon does **not** require core protobuf changes to expose its own cluster resource.

---

## 2. Addon Shape

### 2.1 Descriptor

The addon declares both capabilities:

```
DeliveryCapability   { TargetType:    "gcphcp" }
ManagedResourceCapability { ResourceType: "api.gcphcp.cluster" }
```

### 2.2 Connect-time assets

At connect time, the addon provides:

- the delivery agent
- one `TargetInfo` per entry in the config file's `targets:` list, with `gcp_project`, `region`,
  and identity federation fields stored in `TargetInfo.Properties`
- the managed-resource schema for `api.gcphcp.cluster`

### 2.3 Fulfillment relation

The fulfillment relation is:

```
RegisteredSelfTarget
```

`RegisteredSelfTarget` derives **static placement to exactly one addon target**.

V1 uses one active `gcphcp` addon target, with all `api.gcphcp.cluster` managed resources routing
to that target.

Multi-target routing is **not** supported by this relation today. The current platform placement
model does not inspect `TargetInfo.Properties`, and `RegisteredSelfTarget` itself emits static
placement to one target ID. Future multi-target routing requires platform work or a different
fulfillment-relation shape; it is not an already-supported v1 path.

Managed resources derive to:

- managed-resource manifest strategy
- static placement to the addon target
- immediate rollout

### 2.4 Internal components

The addon code is organized into the following boundaries:

- **agent** - `DeliveryAgent` entry points and lifecycle boundary to FleetShift. Owns `Deliver()`
  and `Remove()`, including the platform-facing contract: validate incoming manifests, validate
  caller/auth context, launch long-running provisioning work asynchronously, and report
  completion. Coordinates the other subsystems (auth exchange, infrastructure preparation,
  management-plane client calls, reconciliation, readiness polling, guest bootstrap). Owns
  addon-level trust-bundle input state: stores the current trust-bundle set in agent-owned
  memory keyed by issuer URL, replaces same-issuer entries rather than accumulating history,
  prunes stored entries when trust-bundle manifests are removed. The reconciler consumes a snapshot
  of that agent-owned state when building guest-target output.

- **descriptor** - Addon declaration and managed-resource registration surface. Defines the addon
  ID, name, capabilities, and managed-resource schema. Binds the schema to `RegisteredSelfTarget`
  for the single active addon target used in v1.

- **auth** - Management-plane identity translation boundary. Encapsulates the two-step auth
  exchange (Workforce STS exchange, then broker service account `generateIdToken`) and returns
  both the broker ID token for gateway requests and the Workforce access token for hypershift
  workspace and direct GCP API calls. Sets `X-User-Email` to the broker SA email from target
  config. Hides identity federation mechanics from the rest of the addon.

- **client** - CLS backend / gateway API boundary. Owns management-plane HTTP calls: create
  cluster, get cluster, update cluster, list clusters, get cluster status, create nodepool, update
  nodepool, list nodepools, get nodepool status, delete cluster, delete nodepool, and resolve
  cluster ID by name. Centralizes request construction, headers (`Authorization`, `X-User-Email`),
  response parsing, and backend error handling. Does not own tenant-project side effects or
  higher-level reconciliation decisions.

- **cluster_spec** - Addon-facing resource model and validation boundary. Defines the user-facing
  cluster spec shape. Validates and normalizes: endpoint access, desired nodepool set (including
  ID uniqueness and format constraints), release version, and channel group. Cluster name
  validation is separate because the name is derived from the managed resource ID, not from the
  user-provided spec. Clearly separates user intent from derived implementation fields such as
  `infraID`, service-account-signing-key material, and backend-generated IDs.

- **reconcile** - Desired-vs-actual lifecycle coordinator. Decides how the addon moves from
  requested state to provider state: whether the hosted cluster exists, whether the desired
  nodepool set matches, whether any updates are needed, and whether the cluster is ready enough
  to advance to later phases. Sequences the major steps rather than implementing all side effects
  itself. Consumes `client` and `infra` as dependencies. Handles ambiguous create failure recovery.

- **infra** - Tenant-project infrastructure boundary, intentionally distinct from the CLS backend
  client. Owns IAM and network preparation via hypershift CLI (`create iam gcp`, `create infra
  gcp`). Owns teardown steps that cluster delete alone does not complete (`destroy infra gcp`,
  `destroy iam gcp`). Owns PSC endpoint cleanup waiting via direct GCP Compute API calls. Wraps
  all hypershift binary invocations. Returns infrastructure-derived data needed by cluster
  creation: `infraID`, network name, subnet name, workload-identity/service-account mapping data.

- **status** - Readiness model and polling logic. Turns raw backend status surfaces into lifecycle
  decisions. Owns polling loops for cluster phase readiness, cluster deletion, and per-nodepool
  readiness. Owns timeout and retry policy, and translation of backend status into progress,
  warning, and error messages. Captures the distinction between control plane created, nodepools
  created, cluster reachable, and cluster delivery-ready. Owns failure status snapshot collection
  for operator-facing debug output on reconcile failure.

- **bootstrap** - Guest-cluster bootstrap boundary, isolated from management-plane provisioning.
  Resolves the guest API endpoint from CLS backend status. Authenticates to the guest cluster
  using the broker ID token. Creates a platform delivery `ServiceAccount` and RBAC inside the
  guest cluster. Requests a bounded-lifetime `ServiceAccount` token via `TokenRequest`. Returns
  the credentials needed for `ProvisionedTarget` registration.

- **cluster_output** - Conversion boundary between addon-internal results and FleetShift
  platform outputs. Assembles `ProvisionedTarget` and `ProducedSecrets` data. Decides which
  properties belong on the resulting target (endpoint, trust bundle) and which values belong in the
  vault (ServiceAccount token). Keeps sensitive or implementation-only data out of target
  properties. Enforces the rule that a guest `kubernetes` target is only emitted when the guest
  cluster is actually delivery-ready.

The split between **infra** and **client** matters because the hosted-cluster lifecycle is not one
API:

- cluster create uses CLS backend HTTP calls
- IAM and network preparation use separate hypershift commands
- delete is not complete when the cluster object disappears; tenant PSC artifacts and infra
  resources still need cleanup

This boundary also captures an important identifier distinction: some cleanup steps key off the
backend cluster ID, while other cleanup steps key off the `infraID` / cluster name.

---

## 3. Configuration And Target Model

### 3.1 Config tiers

Configuration is split into four tiers with distinct lifecycles:

| Tier | What | Where it lives | Who sets it |
|------|------|----------------|-------------|
| Caller identity | OIDC token for GCP STS exchange | `DeliveryAuth.Token` from FleetShift | Platform (transparent to addon) |
| Gateway config | HCP backend service endpoint and audience | Addon config file, `gateway:` section | Operator at addon startup |
| Target config | Identity federation, broker SA, GCP project, region | Addon config file, `targets:` list; seeded as `TargetInfo.Properties` | Operator at addon startup |
| Cluster spec | Full cluster shape: name, endpoint access, release version, channel group, nodepools (all required) | Managed resource spec (`api.gcphcp.cluster`) | User at cluster creation |

`oidc_issuer_url` and `oidc_client_id` are not part of the addon config. The addon receives the
caller's OIDC token via `DeliveryAuth.Token` and exchanges it directly with GCP Workforce STS.

Only `gateway_url` and `gateway_audience` are addon-level constants -- they identify the shared HCP
backend service. Target config is strictly infrastructure wiring: identity federation path
(`workforce_pool`, `workforce_provider`, `broker_sa_email`) and provisioning destination
(`gcp_project`, `region`).

### 3.2 Config file

The addon loads a single YAML config file at startup. The config enforces strict field validation:
all gateway and target fields are required, and exactly one target must be present in v1.

```yaml
# gcphcp.yaml

gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "<google-client-id>.apps.googleusercontent.com"

targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    workforce_provider: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example-hcp-target-project.iam.gserviceaccount.com"
```

Each target is strictly infrastructure wiring. No cluster shape defaults live on the target.

### 3.3 Cluster spec -- all fields required

The addon does not apply any default values. Every field in the cluster spec must be explicitly
provided by the user at creation time.

The cluster name is not part of the user-facing spec. It is derived from the managed resource ID
(`--id` in fleetctl). The addon receives the resource name through the delivery manifest and uses
it as the CLS cluster name. The resource ID must conform to CLS naming constraints (max 15 chars,
lowercase alphanumeric + hyphens, must start with a letter).

**Cluster-level required fields:**

| Field | Description |
|-------|-------------|
| `endpointAccess` | Control plane access mode (e.g. `"PublicAndPrivate"`, `"Private"`) |
| `releaseVersion` | OCP release version (e.g. `"4.22.0"`) |
| `channelGroup` | Release channel (e.g. `"stable"`, `"fast"`, `"candidate"`) |
| `nodepools` | At least one nodepool (see below) |

**Per-nodepool required fields:**

| Field | Description |
|-------|-------------|
| `id` | Short nodepool identifier (max 10 chars, lowercase alphanumeric + hyphens, must start with a letter). The full CLS nodepool name is derived as `{clusterName}-{id}`. |
| `replicas` | Replica count (must be > 0) |
| `instanceType` | GCP machine type (e.g. `"n1-standard-4"`) |
| `rootVolumeSize` | Root disk size in GB (must be > 0) |
| `rootVolumeType` | Root disk type (e.g. `"pd-standard"`, `"pd-ssd"`) |
| `autoRepair` | Whether to enable node auto-repair (`true` or `false`) |
| `upgradeType` | Node upgrade strategy (e.g. `"Replace"`, `"InPlace"`) |

Nodepool IDs must be unique within a cluster spec. Duplicate IDs are rejected at validation time.

Validation is enforced at two layers: proto annotations (`buf.validate`) on the managed resource
API surface, and Go validation in the internal delivery path. Cluster name validation is performed
by the addon when it reads the resource name from the delivery manifest.

**Example: creating a cluster via fleetctl**

Current `fleetctl` uses the generic managed-resource surface:

- list available reflected resource collections with `fleetctl resource types`
- create a cluster with `fleetctl resource create <plural> --id <id> --spec-file <path-or->`
- for `gcphcp`, the reflected plural is `GCPHCPClusters`
- the `--id` becomes the CLS cluster name

```bash
fleetctl resource create GCPHCPClusters --id my-cluster --spec-file - <<'EOF'
{
  "endpointAccess": "PublicAndPrivate",
  "releaseVersion": "4.22.0",
  "channelGroup": "stable",
  "nodepools": [
    {
      "id": "np1",
      "replicas": 2,
      "instanceType": "n1-standard-4",
      "rootVolumeSize": 128,
      "rootVolumeType": "pd-standard",
      "autoRepair": true,
      "upgradeType": "Replace"
    }
  ]
}
EOF
```

This produces: cluster name `my-cluster`, nodepool name `my-cluster-np1`, guest target ID
`k8s-my-cluster`.

### 3.4 Startup sequence

At startup, the addon:

1. parses the config file and validates all required fields
2. stores the gateway config (shared across all targets)
3. validates that exactly one target entry is active
4. builds a `TargetInfo` for that target, storing all target fields in `TargetInfo.Properties`
5. passes that `TargetInfo` to the addon connect flow

At delivery time, the reconciler retrieves target config from `TargetInfo.Properties` and
constructs a broker auth client for that target's identity federation path.

### 3.5 Target model

Each target represents a provisioning destination: identity federation path plus GCP project +
region.

Target IDs are human-readable and encode scope (e.g. `gcphcp-example-region-staging`), but the
addon keys off `Properties` values, not the ID string.

V1 uses exactly one active target for managed-resource fulfillment.

### 3.6 Target config vs cluster spec boundary

`gcp_project` and `region` live on the target, not in the managed resource spec. The user does not
pick a project or region when creating a cluster. In v1, those values are implicit because every
managed resource routes to the one active addon target, and that target carries the project/region.

All cluster shape fields (`endpointAccess`, `replicas`, `instanceType`, `rootVolume`,
`release_version`, `channel_group`, management config) live in the managed resource spec and are
required at creation time. The target carries no cluster shape configuration.

### 3.7 Target config drift

If an operator changes a target's `gcp_project` or `region` in the config file and restarts the
addon, existing clusters provisioned under the old values break -- the addon attempts to reconcile
against the wrong project/region.

V1 does not guard against this. Target config drift detection and replacement conditions are
deferred (see section 11).

### 3.8 Cluster naming and idempotency

Cluster names are derived from the managed resource ID. The resource ID is passed to the addon
through the delivery manifest's `Name` field, and the addon uses it as both the CLS cluster name
and the infrastructure identity (`infraID`). This makes the resource self-identifying: given a
cluster in the CLS backend, you can immediately determine which FleetShift managed resource owns it.

Nodepool names are derived deterministically from the cluster name and a short user-provided
identifier (`id`): `{clusterName}-{id}`. The identifier is stable across reconcile passes, so
reordering nodepools in the spec does not cause unintended deletes or recreates.

Idempotency comes from the reconciler checking whether a cluster with that name already exists
before creating. An existing cluster is updated rather than failing on create. This aligns with the
guarded-authoritative reconciliation posture described in section 5.3.

---

## 4. Resource Model

### 4.1 One managed resource type

V1 uses one cluster resource type:

- `api.gcphcp.cluster` (singular: `GCPHCPCluster`, plural: `GCPHCPClusters`)

Nodepools are nested in the cluster spec.

This is the right first increment because the operational model is a single cluster lifecycle with
child nodepools: one cluster create, reconciled nodepool set, one delete flow.

### 4.2 User-facing spec vs raw transport body

The user-facing managed-resource spec expresses desired intent, not the exact raw CLS request body.

The addon derives or hides implementation details such as:

- `infraID` (derived from the managed resource name)
- local temp JWKS file usage
- `serviceAccountSigningKey` (generated 4096-bit RSA keypair)
- workload identity service-account mapping plumbing (from hypershift IAM output)
- backend-specific generated IDs

### 4.3 Proto schema

The managed resource schema uses protobuf with `buf.validate` annotations for API-level validation.
The schema defines `GCPHCPClusterSpec` as the top-level message with nested `Nodepool` messages.
The addon embeds the proto source and registers it at connect time for dynamic API compilation.

---

## 5. Delivery Lifecycle

### 5.1 Execution model

Provisioning follows the platform's delivery shell:

1. validate manifests and cluster spec
2. validate auth context (caller token must be non-empty)
3. check per-cluster generation ordering (reject stale deliveries)
4. launch long-running work asynchronously with per-cluster serialization
5. report completion through the delivery reporter

The agent separates incoming manifests into two categories: trust-bundle manifests (stored
immediately in the agent's in-memory trust map) and cluster manifests (routed to the reconciler).
Trust-bundle-only deliveries complete immediately. Cluster deliveries expect exactly one cluster
manifest.

### 5.2 Trigger model

Reconciliation is strictly **FleetShift-driven**:

- a new reconciliation pass starts when FleetShift observes a managed-resource spec change
- FleetShift's existing fulfillment generation / orchestration workflow is the only trigger
- the addon does long-running polling within a single accepted delivery
- the addon does **not** ask FleetShift to schedule a follow-up reconcile after completion

Explicitly out of scope for v1:

- addon-driven requeue / invalidation
- backend-status-driven reverse triggers
- periodic resync independent of spec changes

### 5.3 Reconciliation posture

V1 uses a simple **authoritative** reconciler: the addon treats the FleetShift spec as desired
state and reconciles all fields toward it. If the user changes a spec field, the addon sends the
updated value to the CLS backend.

For existing clusters, the update path preserves observed bootstrap and infrastructure fields
(such as `infraID`, `serviceAccountSigningKey`, and workload identity configuration) while
overlaying the desired mutable fields (`endpointAccess`, `releaseVersion`, `channelGroup`).

V1 does not classify fields as safe vs blocked. Changing any field (including fields like
`endpointAccess` or `instanceType` that may not be safely mutable in the backend) is passed through
without guardrails. Field safety classification is deferred to a later version (see section 11).

### 5.4 Reconcile flow

The reconcile flow sequences through the following phases:

```
Exchange caller token for broker credentials
  -> Create CLS client with broker token and broker email
  -> Resolve cluster by name
  -> If cluster exists:
       -> Fetch observed cluster state
       -> Build update spec (preserve bootstrap/infra fields, overlay desired fields)
       -> Update cluster via CLS API
     If cluster does not exist:
       -> Generate 4096-bit RSA keypair and JWKS
       -> Prepare hypershift workspace with caller token and JWKS
       -> Create IAM resources via hypershift (with retry recovery)
       -> Create infrastructure via hypershift (with retry recovery)
       -> Build CLS cluster spec from addon spec + infra outputs
       -> Create cluster via CLS API
       -> Handle ambiguous create failures (see section 5.6)
  -> Reconcile desired nodepool set (create missing, update existing, delete removed)
  -> Poll cluster status until Ready phase (or fail on Failed phase)
  -> Resolve guest API endpoint and bootstrap guest cluster (see section 6)
  -> Poll desired nodepools until all report Ready (or fail on Failed)
  -> Build cluster output with endpoint, credentials, and trust bundles
```

The reconciler runs within a 55-minute timeout context. If the context expires, the current pass
fails and FleetShift can retry with a new delivery.

On reconcile failure, the addon attempts to emit a redacted failure status snapshot before
reporting the error (see section 8).

### 5.5 Nodepool reconciliation

The nodepool reconciler performs a three-way diff between desired and observed nodepools:

1. **Create:** desired nodepools not present in the backend are created
2. **Update:** desired nodepools already present are updated to match the current spec
3. **Delete:** observed nodepools not in the desired set are deleted

Nodepool names are derived deterministically (`{clusterName}-{nodepoolID}`), so the diff is
name-stable across reconcile passes. Reordering nodepools in the spec or running the same spec
twice does not produce unintended side effects.

### 5.6 Ambiguous create failure recovery

When the CLS cluster create call returns an error or a response without a cluster ID, the addon
does not immediately assume failure. The API call may have succeeded on the backend even though the
HTTP response indicated an error.

The addon probes for the cluster by name over a short polling window. If the cluster appears in the
backend:

1. the addon "adopts" it
2. re-ensures IAM resources via hypershift (idempotent)
3. re-ensures infrastructure via hypershift (idempotent)
4. fetches the current observed cluster state
5. issues an update to align the adopted cluster with the desired spec
6. continues with normal reconciliation (nodepool reconcile, readiness polling, guest bootstrap)

If the cluster does not appear within the probe window, the addon cleans up any partially created
IAM and infrastructure resources before failing the delivery.

IAM and infrastructure creation calls themselves are also wrapped in a retry-with-recovery loop
(up to 3 attempts) to handle ambiguous subprocess failures from the hypershift binary.

### 5.7 Credential file handling

The addon does not write credential files to disk unless an external tool requires it.

Workforce STS exchange, `generateIdToken()`, and CLS gateway requests are all handled through Go
HTTP clients using in-memory tokens.

The only external binary the addon invokes is `hypershift`, for IAM and infrastructure
bootstrap/teardown. The addon creates a temporary workspace directory for each hypershift
invocation containing:

- a subject token file (raw caller token)
- a workforce credential config file (`external_account` type, pointing at the subject token file)
- an isolated environment (`HOME`, `CLOUDSDK_CONFIG`, `XDG_CONFIG_HOME` all point into the temp
  dir to prevent fallback to ambient ADC)
- for create operations only: a JWKS file containing the generated public key

The workspace is cleaned up immediately after the hypershift subprocess completes, with
cleanup-on-error guaranteed via deferred removal. Temp directories are never reused across
reconcile passes.

The workspace credential config uses the `workforce_pool_user_project` field to enable billing
against the target GCP project. The hypershift subprocess runs as the workforce identity derived
from the caller token, not as the broker service account. This is distinct from the
management-plane path, which explicitly mints a broker ID token via `generateIdToken`.

---

## 6. Readiness Model And Guest Registration

### 6.1 Multi-gate readiness

The addon treats "cluster ready" as only one gate in a broader registration sequence. V1 has
**five distinct gates**:

1. **Management-plane ready:** `cluster.status.phase == Ready`
2. **Guest API discoverable/reachable:** the CLS status exposes an `APIServer` URL and the addon
   can successfully connect to that endpoint
3. **Guest bootstrap ready:** the broker token can perform the required bootstrap writes inside the
   guest cluster (RBAC may not be ready immediately after the cluster phase flip)
4. **Desired nodepool set ready:** every desired nodepool is present and reports `Ready`
5. **Registered target ready:** the addon has all durable output data and can emit
   `ProvisionedTarget` + `ProducedSecret`

These gates intentionally separate "the hosted cluster finished provisioning" from "FleetShift can
now treat the guest cluster as a normal Kubernetes delivery target."

**Phase 1 -- Provision:** Poll `cluster.status.phase` until `Ready` (15-second intervals,
20-minute timeout). If the phase reaches `Failed`, the reconcile fails immediately. This gate only
proves control-plane / management-plane readiness. Captured status examples show that
`cluster.status.phase == Ready` can coexist with a desired nodepool whose own
`status.phase == Progressing`, so this gate is necessary but not sufficient for registration.

**Phase 2 -- Bootstrap:** Once cluster phase is `Ready`, the addon resolves the guest API endpoint
from the `APIServer` condition in `controller_status`, then attempts guest bootstrap with the broker
ID token. This phase uses a **retry loop** (up to 10 attempts, 15-second intervals) because
several adjacent conditions may lag behind the phase flip: the API endpoint may not be reachable
yet, or the CLS backend's RBAC setup job (which grants the broker SA `cluster-admin` inside the
guest cluster) may not have completed yet. The bootstrap gate is only satisfied when the addon can
complete the real registration prerequisites: connect to the guest API, create the delivery
`ServiceAccount` and RBAC, and obtain the `ServiceAccount` token.

**Phase 3 -- Desired nodepool readiness:** After guest bootstrap succeeds, poll the desired
nodepool set until every desired nodepool is present and reports `Ready` (15-second intervals,
20-minute timeout). This extra gate is required because top-level cluster readiness does not
guarantee that the requested worker/nodepool capacity has converged yet. If any desired nodepool
reports `Failed`, the addon fails the current delivery immediately.

**Phase 4 -- Register:** Emit `ProvisionedTarget` + `ProducedSecret` only after bootstrap
succeeds, the desired nodepool set is ready, and the durable output contract is complete. If
bootstrap or desired nodepool readiness is still pending when the bounded retry window is
exhausted, the addon fails the delivery with an explicit `postProvisionRegistrationError` message
indicating that the hosted cluster is provisioned and management-plane ready, but guest target
registration did not complete.

### 6.2 Managed resource as the stable user-facing object

Even if the addon cannot yet emit a `ProvisionedTarget`, the user still has:

- a typed `gcphcp` cluster resource
- a fulfillment-backed lifecycle
- a durable object for later status and reconciliation

A provisioning-only MVP remains structurally clean.

### 6.3 Guest API endpoint resolution

Guest API endpoint resolution uses the CLS backend status API:

- primary: scan `controller_status[].conditions[]` for a condition with `type: APIServer` whose
  `message` starts with `https://`
- fallback: read `api_endpoint` from the cluster status object

This endpoint is only available after the cluster reaches `Ready` phase and the control plane API
server is operational.

### 6.4 Guest-cluster bootstrap

The addon performs guest-cluster registration programmatically through direct Kubernetes API calls
using Go client libraries.

The registration sequence after the cluster reaches `Ready`:

```
Resolve guest API endpoint from CLS backend status
  -> Retry until the endpoint is actually reachable
  -> Build a Kubernetes client config with:
       - host: resolved guest API endpoint
       - bearer token: broker ID token
  -> Create a "fleetshift-platform" ServiceAccount in kube-system
  -> Create a ClusterRoleBinding granting cluster-admin to that ServiceAccount
  -> Request a bounded-lifetime ServiceAccount token (24-hour expiry) via TokenRequest API
  -> Return endpoint, token reference, and token value for ProvisionedTarget registration
```

The broker ID token provides initial privileged access because the CLS backend grants the broker SA
`cluster-admin` inside the guest cluster at creation time. Both the `ServiceAccount` creation and
the `ClusterRoleBinding` creation are idempotent -- they handle `AlreadyExists` errors gracefully
and update existing bindings if the subjects have drifted.

Bootstrap uses the host's normal system trust store to verify the guest API endpoint TLS
certificate.

### 6.5 Emitted guest target contract

The emitted `ProvisionedTarget`:

- **type:** `kubernetes`
- **accepted resource types:** the Kubernetes manifest resource type used by FleetShift's
  Kubernetes delivery path
- **target ID:** `k8s-{clusterName}`

Target properties:

- `api_server` -- the resolved guest cluster API endpoint
- `service_account_token_ref` -- a vault reference for the delivery `ServiceAccount` token
  created during bootstrap (format: `targets/{targetID}/sa-token`)
- `trust_bundle` -- the trust configuration used by FleetShift's Kubernetes delivery agent to
  verify attestation before it uses the platform `ServiceAccount` token (serialized as JSON,
  sorted by issuer URL for deterministic output)
- `ca_cert` -- conditionally set from the bootstrap result; in the current environment the
  CLS-exposed guest API endpoint uses a publicly trusted certificate chain (Let's Encrypt), so
  this property is not populated

The corresponding `ProducedSecret`:

- the addon emits the `ServiceAccount` token as a produced secret
- the secret ref matches `service_account_token_ref` on the emitted target
- cleanup logic treats that ref as delivery-owned output state (see section 7.6)

### 6.6 Trust-bundle source and distribution

The authoritative source of `trust_bundle` entries is FleetShift's provisioned OIDC auth methods:

1. FleetShift auth-method provisioning resolves OIDC discovery and produces `TrustBundleEntry`
   records
2. those entries are distributed through the existing `idp-trust-bundle` resource type
3. the seeded `gcphcp` target receives those trust-bundle manifests as part of its normal addon
   input
4. the addon retains the current trust-bundle set needed during provisioning, with one active entry
   per issuer URL
5. when the addon emits a guest `kubernetes` target, it serializes that trust set into the target's
   `trust_bundle` property in deterministic issuer-sorted order

Trust-bundle state is process-local addon state owned by the agent, not the reconciler:

- a delivery for an issuer replaces any prior entry for that same issuer rather than appending
  duplicates
- `Remove()` prunes a stored issuer when the corresponding `idp-trust-bundle` manifest is removed
- repeated same-issuer deliveries and different arrival orders converge to the same `trust_bundle`
  bytes within a running addon process

The guest cluster does **not** need to trust FleetShift's OIDC issuer for this to work.
Attestation verification happens in FleetShift's Kubernetes delivery agent, while the guest cluster
itself only sees the bootstrapped platform `ServiceAccount` credential.

### 6.7 TLS trust strategy

The addon uses the host's normal trust store for both the initial guest bootstrap connection and
later FleetShift delivery to the emitted guest target.

The CLS-exposed guest API endpoint is expected to present a publicly trusted certificate chain. In
the current environment that chain is Let's Encrypt, so the addon does not persist `ca_cert` on the
emitted target.

If a future deployment exposes the guest API behind a private or self-signed CA, the addon needs a
backend-supported path to surface the correct trust material before that configuration can be
supported cleanly (see section 11).

---

## 7. Delete And Cleanup

### 7.1 Delete sequence

The provider-side teardown sequence:

```
Exchange caller token for broker credentials
  -> Create CLS client
  -> Resolve cluster by name
     -> If cluster is already absent, skip CLS delete and proceed to cleanup
     -> If cluster exists:
          -> Delete hosted cluster via CLS API (force=true)
          -> Poll until cluster returns 404
  -> Wait for PSC endpoint cleanup (forwarding rule and address)
  -> Prepare hypershift workspace with caller token
  -> Destroy tenant network infrastructure via hypershift
  -> Destroy IAM resources via hypershift
```

### 7.2 Delete is synchronous

V1 blocks and polls inside `Remove()` for the full delete sequence. This can take 10-20 minutes
and ties up the delivery agent for the entire duration.

The platform's `Deliver()` path supports async completion via the delivery reporter, but `Remove()`
is synchronous (`Remove(...) error`). Async delete support is deferred (see section 11).

### 7.3 PSC cleanup waiting

PSC cleanup keys off the backend cluster ID. The addon polls the GCP Compute API for two specific
resources using the Workforce access token from the broker auth exchange:

- forwarding rule: `psc-{clusterID}-endpoint`
- address: `psc-{clusterID}-ip`

Polling runs at 30-second intervals with a 20-minute timeout. If both resources are already absent
at the first check, cleanup returns immediately with no delay.

If the cluster was already absent (not found by name), PSC cleanup is skipped because there is no
cluster ID to derive the resource names from.

### 7.4 IAM destroy failure handling

After infrastructure destroy succeeds, an IAM destroy failure still fails the current delete pass.
The error message explicitly states that cluster deletion and infrastructure cleanup completed but
IAM cleanup failed. This gives the operator a clear signal of what remains to be cleaned up.

### 7.5 Delete handles absent clusters

If the cluster is not found by name when delete begins, the addon does not fail. It logs that the
cluster is already absent and proceeds directly to infrastructure and IAM cleanup. This handles
cases where the CLS backend cluster was removed externally but tenant-project resources remain.

### 7.6 Cleanup ownership

The platform does not symmetrically clean up:

- emitted provisioned targets in a generic way
- inventory items
- vault secrets
- arbitrary addon-owned external artifacts

The preferred v1 cleanup model is a **hybrid of addon changes and core platform changes**:

- addon changes define what `gcphcp` emits and which secret refs must be cleaned up
- platform changes preserve ownership metadata for those emitted outputs and remove them during
  teardown

The cleanup model:

1. delivery completion produces outputs
2. output registration persists ownership metadata tying emitted artifacts back to the delivery that
   created them
3. delete-time cleanup uses that ownership link to remove emitted artifacts automatically

Concretely:

- emitted guest targets are treated as delivery-owned outputs, not independent long-lived roots
- the inventory items created for those targets retain the originating delivery identity
- cleanup looks up all inventory/target artifacts created by a delivery and removes them generically
  during fulfillment teardown
- vault secrets referenced by `service_account_token_ref` on the emitted target are deleted as part
  of removing the target and its inventory item

---

## 8. Auth And Identity

### 8.1 What the addon consumes

The addon expects:

- caller identity in `DeliveryAuth.Caller`
- raw caller token in `DeliveryAuth.Token`

Unlike some other addons, this token is not optional context. It is an active input to the
management-plane auth chain.

### 8.2 Auth sequence

Inside the addon, the auth exchange performs a two-step flow:

```
DeliveryAuth.Token (caller's OIDC token)
  -> Step 1: Workforce STS exchange
       Exchange caller token for a Workforce access token via Google STS
       (audience: workforce pool/provider, token type: access_token)
  -> Step 2: Broker ID token generation
       Use the Workforce access token to call IAM generateIdToken()
       for the broker service account (audience: CLS gateway client ID)
  -> Result:
       - Broker ID token (Google-signed JWT for CLS gateway requests)
       - Broker email (service account email for X-User-Email header)
       - Workforce access token (retained for hypershift workspace and PSC cleanup)
```

### 8.3 Auth error classification

The addon classifies auth failures by HTTP status:

- **401 Unauthorized** from the STS endpoint, IAM endpoint, or CLS backend: wrapped as an
  auth-expired error, which maps to `DeliveryStateAuthFailed` in the delivery result. This causes
  the platform to transition the fulfillment to a paused-auth state.
- **OAuth `invalid_grant`** from the STS endpoint: treated the same as 401 (auth expired).
- **403 Forbidden**: NOT wrapped as auth-expired, because it indicates a permission or
  configuration issue that fresh user credentials will not resolve.

### 8.4 Security rules

The addon:

1. sets `X-User-Email` to the broker SA email from target config, not from user input
2. logs the auth chain with enough correlation to debug token exchange and backend calls

### 8.5 Credential paths

The addon uses two distinct credential paths:

**Management-plane path (in-memory):**

The broker auth exchange produces an in-memory broker ID token and Workforce access token. These
are used directly for:
- CLS gateway HTTP requests (broker ID token as `Authorization: Bearer`, broker email as
  `X-User-Email`)
- PSC cleanup waiting during delete (Workforce access token for GCP Compute API calls)

**Hypershift subprocess path (file-based):**

Hypershift is invoked as an external binary and requires file-based credentials. The addon writes
a temporary `external_account` credential config that tells the Google ADC inside the subprocess to:
1. read the raw caller token from a subject token file
2. exchange it via STS for a Workforce access token
3. use `workforce_pool_user_project` for billing against the target GCP project

This means the hypershift subprocess performs its own STS exchange, independent of the
management-plane exchange. The subprocess runs as the workforce identity derived from the caller
token, not as the broker service account.

### 8.6 Token lifetime

The v1 auth model does not implement mid-flight credential refresh. The entire reconcile pass
(create or delete) is expected to complete within the lifetime window of every credential it
touches:

- the broker ID token (minted via `generateIdToken`, short-lived)
- the caller token (used as the STS subject token in hypershift workspaces)
- the Workforce access token (used for PSC cleanup during delete)

If a reconcile pass outlives any of these credential windows, it fails and is retried by FleetShift
rather than attempting in-place refresh.

Current gap: once `Deliver()` has returned and async work is in progress, auth failures are
detected (401 responses produce `DeliveryStateAuthFailed` results), but the platform side of
pausing for fresh credentials and cleanly resuming is not yet fully wired. Auth expiry during
async work currently surfaces through the delivery result mechanism.

---

## 9. Status And Observability

### 9.1 Delivery events

V1 uses delivery events for run-time status reporting. The addon emits progress and warning events
at each phase of the reconcile flow. These events are intended to be logged/observed during the run
rather than treated as durable managed-resource status:

```
[progress] Exchanging caller token for broker credentials
[progress] Creating CLS client
[progress] Reconciling cluster via CLS API
[progress] Generating cluster keypair
[progress] Preparing hypershift workspace
[progress] Creating IAM resources
[progress] Creating infrastructure
[progress] Building CLS cluster spec
[progress] Creating cluster via CLS API
[progress] Cluster created with ID: <cluster-id>
[progress] Polling for cluster ready state
[progress] Cluster status: phase=Progressing reason=... message="..."
[progress] Cluster readiness satisfied; proceeding with guest bootstrap and desired nodepool health checks
[progress] Resolving guest API endpoint
[warning]  Bootstrap failed, retrying in 15s: ...
[progress] Bootstrap successful
[progress] Waiting for desired nodepools to become healthy
[progress] Nodepool <name> status: phase=Ready reason=...
[progress] Desired nodepools are healthy; building cluster output
[progress] Cluster provisioning complete
```

### 9.2 Failure snapshots

On **failure only**, the addon emits a **redacted, curated failure snapshot** derived from the CLS
backend's cluster and nodepool status surfaces. The snapshot captures useful operator-facing debug
fields without dumping raw backend payloads.

The failure snapshot includes:

- cluster ID and cluster name
- cluster phase, reason, and message
- release version when available
- whether an `APIServer` endpoint was present in CLS status
- selected degraded / failed / unknown / progressing controller conditions for the cluster
- per-nodepool ID, name, phase, reason, and message
- selected problem conditions for each nodepool

The failure snapshot does **not** include:

- raw cluster `spec`
- `serviceAccountSigningKey`
- secret or kubeconfig object names
- full controller metadata payloads
- project numbers
- service-account email addresses

Condition inclusion is governed by a filter: conditions with `status: False` or `status: Unknown`
are always included, and conditions with `status: True` are included only for problem types
(`Degraded`, `Failed`, `Failing`, `Progressing`, `Deleting`).

Successful runs end with normal progress/completion events only and do not emit a status snapshot.

### 9.3 Delivery result classification

Terminal delivery results are classified into three categories:

- **Delivered:** cluster provisioned, guest target registered, `ProvisionedTarget` and
  `ProducedSecrets` attached to the result.
- **Auth failed:** credentials expired during reconciliation (401 or `invalid_grant`). The platform
  transitions the fulfillment to a paused-auth state.
- **Failed (post-provision):** the hosted cluster reached management-plane readiness, but guest
  target registration did not complete (bootstrap or nodepool readiness not satisfied within the
  bounded retry window). The error message explicitly calls out what succeeded and what remains
  incomplete.
- **Failed (general):** cluster provisioning failed at any earlier stage.

### 9.4 Status is frozen after delivery

After delivery completes, status is frozen -- FleetShift has no live health view of the cluster
until the next spec-triggered delivery. Structured status reporting on the managed resource is
deferred (see section 11).

---

## 10. Generation And Concurrency

### 10.1 Per-cluster generation ordering

The agent tracks a per-cluster generation high-water mark. When a delivery arrives, the agent
checks the generation against the highest previously accepted generation for that cluster name.
Stale deliveries (generation strictly less than the current high-water mark) are rejected
immediately with a failed delivery result. Equal-generation deliveries are permitted to support
orchestration retries.

This prevents out-of-order deliveries from overwriting newer spec state with older spec state.

### 10.2 Per-cluster serialization

The agent holds a per-cluster mutex to prevent concurrent reconcile or delete operations against
the same cluster. The lock is acquired before launching async work and released when the async
operation completes. Different clusters can be reconciled concurrently.

---

## 11. Known Limitations And Future Work

The initial implementation leaves several capabilities out on purpose so the first addon flow stays
small, understandable, and aligned with what FleetShift already implements.

### 11.1 Reverse-triggered reconciliation

V1 does not implement addon-driven requeue or invalidation:

- FleetShift starts reconciliation when the managed-resource spec changes
- the addon does not ask FleetShift to schedule another reconcile pass
- backend status changes do not independently trigger a new FleetShift reconcile

### 11.2 Token refresh and pause/resume

V1 does not implement mid-flight credential refresh or pause/resume semantics for long-running
reconciliation. The addon assumes all credentials remain valid for the duration of a single
reconcile pass.

What can be stated today:

- the broker token is minted through Google's `generateIdToken()` path
- Google identity tokens are short-lived and expire no later than one hour
- create, delete, and update flows are expected to complete within the lifetime window of every
  credential they touch
- if a reconcile pass outlives that window, the pass fails and is retried

Current gap to revisit:

- once `Deliver()` has already returned, async auth failures are detected through the delivery
  result mechanism, but the platform side of cleanly pausing and resuming with fresh credentials
  is not yet fully wired
- create and delete hypershift subprocesses materialize the raw caller token into temp
  `external_account` workspaces, so those paths have their own STS lifetime constraints in addition
  to the broker token used for CLS requests
- delete-side PSC cleanup uses the minted Workforce access token directly, so one long delete pass
  may span broker-token lifetime, caller-token lifetime for hypershift STS exchange, and Workforce
  access-token lifetime for direct GCP API calls

### 11.3 Periodic resync

V1 does not add periodic resync or background polling loops that create new reconcile passes
independent of spec changes.

### 11.4 Per-user identity for CLS backend and guest-cluster auth

V1 sends the broker SA email as `X-User-Email` on all CLS backend requests. The CLS backend
records the broker SA as the cluster owner and grants it `cluster-admin` inside the guest cluster.
All clusters share one owner identity regardless of which human initiated the request.

This happens because both the CLS gateway and the guest cluster's Kubernetes API require a
Google-signed JWT. The caller's federated OIDC token (Workforce STS) is a Google access token, not
an identity token these systems accept. The broker SA `generateIdToken()` bridges the gap but
collapses caller identity into the broker SA in the process.

### 11.5 Field safety classification and blocked-field reconciliation

V1 reconciles all spec fields authoritatively. Some fields (e.g. `endpointAccess`, `instanceType`,
`rootVolume`, `region`) may not be safely mutable in the backend and could leave the cluster in a
broken state.

Future work: classify fields into safe-mutable vs blocked, detect unsafe drift, and surface
replacement conditions instead of blindly forwarding changes.

### 11.6 Dynamic target registration

V1 loads target config from a static config file at startup and supports one active fulfillment
target.

Making multiple registered `gcphcp` targets usable for one managed-resource type requires a routing
model beyond the current single-target `RegisteredSelfTarget` behavior.

### 11.7 Asynchronous delete

V1 blocks inside `Remove()` for the full delete sequence (10-20 minutes). The platform's
`Deliver()` path supports async completion, but `Remove()` is synchronous. Future work: extend
the platform delete semantics to support the async pattern (return accepted, perform cleanup in the
background, signal completion when done).

### 11.8 Structured status reporting

V1 reports status through delivery events (message strings) and a curated failure snapshot on
failed completion. This is an execution-time observability model, not durable managed-resource
status. Significant gaps:

- after delivery completes, status is frozen
- status data is unstructured (version, endpoint, node counts are embedded in message strings)
- the CLS backend exposes rich per-controller and per-nodepool conditions (30+ HostedCluster
  conditions) that are lost in the event log
- the addon intentionally does not emit raw full CLS payloads because those payloads can contain
  sensitive bootstrap/provider data

Future work: add structured status to the managed resource, likely requiring a platform-level
managed resource status mechanism and periodic resync (see 11.3) to keep it current.

### 11.9 Private guest API trust material

The current implementation assumes the CLS-exposed guest API endpoint presents a publicly trusted
certificate chain. If a future deployment exposes the guest API behind a private or self-signed CA,
that likely depends on CLS backend changes to surface the CA bundle in the status payloads.

### 11.10 Durable trust-bundle reconstruction across addon restart

V1 treats `idp-trust-bundle` input as process-local addon state. Within a running addon process,
trust-bundle handling is canonicalized as the current set keyed by issuer URL with deterministic
serialization.

The remaining gap is durability, not in-process correctness:

- addon restart loses the in-memory set until trust-bundle deliveries happen again
- reconcile after restart does not rebuild `trust_bundle` from durable platform state
- convergence for removed issuers depends on the platform delivering corresponding trust-bundle
  removals to the addon

### 11.11 Destroy-data contract

Before `gcphcp` can fully rely on `Remove()` for hosted-cluster teardown, a clear answer is needed
for what cluster-specific destroy data is guaranteed to be available to `Remove()`.

The current platform pattern does not yet make that guarantee generic. Teardown may need data such
as backend cluster ID, `infraID` / cluster name, project and region context, and references to any
emitted guest target or produced secrets.

V1 mitigates this by rediscovering destroy state where possible: the cluster name comes from the
managed resource ID, which is always available; the cluster ID is resolved by name from the CLS
backend; and infrastructure identity is derived from the cluster name. However, this rediscovery
approach has limits, and missing destroy-data contracts are called out as a platform gap rather than
treated as normal addon behavior.
