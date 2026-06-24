# Managed resources

## Problem

How do we offer an extensible core, but allow addons to offer "managed resource" like semantics, that incorporate decade+ of best practices?

Example: imagine a full featured cluster management addon. It should handle many domain specific use cases:

- Provision & configure managed clusters directly targeting managed provider APIs, likely with passthrough auth (e.g. ROSA or ARO clusters)
- Provision & configure clusters through native self managed options (e.g. wrapping openshift assisted installer)
- Provision & configure clusters through operators like CAPI or HyperShift
- Import existing clusters
- Upgrade these clusters (either individually or through a campaign, see [https://redhat.atlassian.net/jira/software/c/projects/FM/list?selectedIssue=FM-81](https://redhat.atlassian.net/jira/software/c/projects/FM/list?selectedIssue=FM-81) )
- Full exploitation of the core placement and rollout strategy abstractions for progressive delivery, maintenance windows, etc., encoding specific cluster management best practices
- Assist in knowable operational issues in the course of provisioning, upgrades, or other configuration changes
- Manage cluster pooling strategies
- View the state of clusters and their underlying nodes (inventory)
- Integration with other cluster-related addons, like ACS, MCOA, ODF, ...

You could imagine similar domain specific experiences for other managed resource types:

- VMs
- Argo instances
- Model serving
- ...

These are the consumer-facing "nouns" of the platform, in contrast to the addon-facing core abstractions. In some sense, the whole interesting architecture of FleetShift is getting these two competing halves right:

- An agnostic "fleet core" that encodes only the most generic, consistent, "meta" or cross cutting best practices. Things like durability, resilience, extensibility, pooling, fleet-awareness, placement and rollout control, inventory, IAM, metering, ...
- Thoroughly domain specific "nouns" that are opinionated and encode all of the best practice and real world experience we can muster

## Proposal

*Managed resources* are the "consumer-facing nouns" of the platform. They are addon-driven. Addons register to provide the functions for one or more managed resource types.

Managed resources are driven by the core Fulfillment abstraction. A managed resource is a *registered resource type* (as in, a manifest resource type). An addon defines how Fulfillments are derived from managed resources. In a typical case, a managed resource maps to a single, immediate placement with the addon itself as the target.

Example: a cluster management addon registers the `clusters` managed resource type. A consumer requests a ROSA cluster. (**These examples illustrate the structural relationships between managed resources, derived Fulfillments, and addon registration — not a specification of the exact API surface. Field names, nesting, and conventions are assumed for readability. In the two-layer API model, the consumer talks to the addon's extension API, for example `/apis/{addon_service}/v1/clusters`, while the shared platform identity lives separately at `//fleetshift.io/clusters/{id}`.**)

#### Consumer-facing managed resource

```json
POST /apis/kind.fleetshift.io/v1/clusters
{
  "name": "prod-us-east-1",
  "spec": {
    "provider": "rosa",
    "version": "4.16.2",
    "region": "us-east-1",
    "compute_pools": [
      {
        "name": "workers",
        "instance_type": "m5.2xlarge",
        "replicas": 3,
        "autoscaling": { "min_replicas": 3, "max_replicas": 12 }
      }
    ],
    "network": {
      "machine_cidr": "10.0.0.0/16",
      "service_cidr": "172.30.0.0/16",
      "pod_cidr": "10.128.0.0/14"
    },
    "encryption": {
      "etcd_encryption": true,
      "kms_key_arn": "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123"
    }
  }
}
```

The consumer's agent signs this request. The platform validates the spec against the addon-registered schema, stores the resource, and returns it with platform-managed fields:

```json
{
  "name": "clusters/prod-us-east-1",
  "uid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "spec": { "..." },
  "state": "PROVISIONING",
  "reconciling": true,
  "conditions": [
    {
      "type": "Provisioning",
      "status": "True",
      "message": "Creating ROSA cluster infrastructure"
    }
  ],
  "create_time": "2026-04-21T14:30:00Z",
  "provenance": {
    "signature": {
      "signer": {
        "subject": "priya@acme.corp",
        "issuer": "https://sso.acme.corp"
      },
      "content_hash": "sha256:9f86d08...",
      "signature_bytes": "MEUCIQ..."
    }
  }
}
```

The `spec` is entirely addon-defined — the platform stores it opaquely but validates it against the addon's registered schema. The `state`, `reconciling`, `conditions`, `provenance`, and timestamps are platform-managed, following the same patterns as Deployment (AIP-128 declarative-friendly).

Within the extension API, the `name` field remains the relative resource name (`clusters/prod-us-east-1`). The corresponding full resource names are `//{addon_service}/clusters/prod-us-east-1` for the extension API and `//fleetshift.io/clusters/prod-us-east-1` for the shared platform identity.

> [!NOTE]
> **Resolved.** Extensions define their own gRPC services in addon-specific proto packages. Resource type names do not collide because each extension's service is differentiated at the transport level (gRPC package, HTTP path prefix). Multiple extensions can model the same logical resource identity through the two-layer API model. See [architecture/resource_identity_and_api.md](architecture/resource_identity_and_api.md) for the full design.

#### Derived Fulfillment

The platform mechanically derives a Fulfillment from the managed resource. Because the addon is the target, the derivation is fixed — no configurable transformation:

```json
{
  "name": "fulfillments/_managed/clusters/dev-cluster",
  "manifest_strategy": {
    "type": "MANAGED_RESOURCE",
    "managed_resource": {
      "resource_type": "kind.fleetshift.io/Cluster",
      "resource_name": "clusters/dev-cluster"
    }
  },
  "placement_strategy": {
    "type": "STATIC",
    "static": {
      "targets": ["targets/kind-local"]
    }
  },
  "rollout_strategy": {
    "type": "IMMEDIATE"
  },
  "provenance": {
    "signature": { "..." },
    "managed_resource_ref": "clusters/dev-cluster"
  }
}
```

- **Manifest strategy**: `MANAGED_RESOURCE` is a reference to the stored resource spec. When the platform delivers to the addon, it sends the full managed resource document as a `Manifest` with a `ManifestType` that the addon declares it accepts. Manifest types are opaque dispatch labels decoupled from the API resource type — they identify what kind of payload a manifest carries so agents can route and validate without understanding manifest content. The addon interprets it — in this case, provisioning the requested cluster with the specified configuration.
- **Placement**: a single static target — the addon's own delivery endpoint. The addon registered this target during capability registration. Since the addon is a delivery agent for its own target type, it receives the managed resource through the standard delivery channel.
- **Rollout**: immediate. A single managed resource means a single target means a single delivery — rollout strategy is degenerate.
- **Provenance**: derived from the original managed resource's signature. A verifier can chain from the Fulfillment's provenance back to the user's signed resource intent. The `managed_resource_ref` links the two, and the derivation rule is mechanically fixed (the addon is always the target), so a verifier can confirm the Fulfillment was correctly derived without trusting the platform.

This Fulfillment flows through the standard orchestration pipeline: Resolve → Delta → Plan → Generate → Deliver. The only difference from a directly-authored Fulfillment is how it was created (derived from a managed resource) and how its provenance chains (through the managed resource rather than directly signed).

#### Addon resource type registration

When an addon connects, it provides its managed resource schemas as part of the connect handshake. Schemas are proto-based: the addon transmits inline proto source content, and the platform compiles it to build the API surface. This parallels how an application carries its DB migration SQL — the workload owns and transmits its schema.

The addon first declares its capabilities at enable time:

```go
domain.AddonDescriptor{
    ID:   "kind",
    Name: "Kind Cluster Provider",
    Capabilities: []domain.Capability{
        domain.DeliveryCapability{TargetType: "kind"},
        domain.ManagedResourceCapability{ResourceType: "kind.fleetshift.io/Cluster"},
    },
}
```

Then at connect time, the workload provides the full schema:

```go
domain.ManagedResourceSchema{
    ResourceType: "kind.fleetshift.io/Cluster",
    ServiceName:  "kind.fleetshift.io",
    Singular:     "Cluster",
    Plural:       "clusters",
    ProtoPackage: "kind.fleetshift.v1",
    ProtoFiles: map[string]string{
        "addons/kind/v1/kind_cluster_spec.proto": kindClusterSpecProto,
    },
    SpecMessage: "kind.fleetshift.v1.ClusterSpec",
    Relation:    domain.RegisteredSelfTarget{AddonTarget: "kind-local"},
}
```

- `**ServiceName**`: the AIP-122 service name (versionless) used in full resource names (e.g. `//kind.fleetshift.io/clusters/dev-cluster`). The addon chooses this; the platform imposes no naming convention beyond uniqueness.
- `**ProtoPackage**`: the versioned proto package for gRPC service registration (e.g. `kind.fleetshift.v1.ClusterService`). This replaces the previous hardcoded `fleetshift.v1` package.
- `**Singular**` / `**Plural**`: `Singular` names the gRPC service and singular RPCs (e.g. `CreateCluster`). `Plural` is the shared AIP collection identifier used in relative resource names and HTTP paths (e.g. `clusters/dev-cluster`, `POST /apis/kind.fleetshift.io/v1/clusters`, `GET /apis/kind.fleetshift.io/v1/clusters/{id}`). Keeping the same collection identifier across extension and platform services preserves identity equivalence for `clusters/foo`.
- `**ProtoFiles**`: inline proto source content, keyed by virtual filename. The platform's compiler resolves imports within this map first, then falls back to well-known types (`google/protobuf/*`, `buf/validate/*`). This means addon specs can use `protovalidate` annotations that the platform enforces at the API boundary.
- `**SpecMessage**`: the fully qualified proto message name for the addon-defined spec. The platform compiles this message, wraps it in a generated `Resource` envelope (with platform-managed `uid`, `state`, `provenance`, etc.), and exposes CRUD operations.
- `**Relation**`: the fulfillment derivation rule. `RegisteredSelfTarget` means the derived Fulfillment always targets the addon itself. This is the common case — and the only case where the derivation is fixed and the attestation chain is trivial (the addon is trusted by virtue of its registration, and the platform is a courier). Future relation types (CEL-based derivation, multi-target mappings) can be added to the typed union.

The platform validates at connect time that every schema matches a declared `ManagedResourceCapability`. On reconnection, schemas are reconciled: removed types are deactivated, unchanged types are left in place (content-hashed), and changed types are atomically replaced. See [addon lifecycle](architecture/addon_integration.md#addon-lifecycle) for details.

This registration is itself signed by the addon and stored as part of the addon's capability record. A delivery-side verifier uses it as evidence: "the addon claimed ownership of `api.kind.cluster` resources with `delivery_target: self`, so a Fulfillment derived from an `api.kind.cluster` resource that targets this addon is consistent with the addon's registration."

### Attestation

Managed resources use the same signed input model as deployments. The attestation envelope (`SignedInput`) accepts a typed content variant — `ManagedResourceContent` alongside `DeploymentContent` — through a common `InputContent` protocol. Both are signed, verified, and derived through the same pipeline; constraint derivation and identity extraction dispatch on the content type.

`ManagedResourceContent` carries the resource spec and the addon reference (`addon_id`). The user signs the "what" and the "who" — not the "how" or the trust path. This parallels how a deployment's signed content includes the strategy type and addon identity, but not the trust anchor used to verify the addon's output.

The fulfillment relation — addon-signed evidence describing how the resource maps to a fulfillment — is external evidence in the `VerificationBundle`, not part of what the user signs. Relation types are platform-defined — the verifier has built-in logic for each — so they use strong typing rather than an open attributes pattern.

The first relation type is `RegisteredSelfTarget`: 1:1 manifest delivery to the addon itself. The addon signs over `{relation_type, resource_type, manifest_type}` to claim: "I own resources of this type, and fulfillments derived from them target me directly with this manifest type." At verification time, the verifier looks up the matching relation from the bundle by `(addon_id, resource_type)`, verifies the relation's signature cryptographically and against the trust store (the signer's key must be recognised by the claimed trust anchor), checks consistency (relation resource type matches content resource type, relation signer matches the declared addon), and derives constraints: placement is static to the addon, manifests use the declared manifest type, and content must match the user's signed spec (the content is deterministic — like `inline` for deployments). Future relation types (CEL-based derivation, multi-target mappings) add to the typed union, each with platform-defined verification logic.

Additionally, we expect addons to eventually produce other platform objects as part of delivery — a managed resource may trigger related resource or deployment creations. In this case, there needs to be trusted evidence that constrains the resulting artifacts within the user's original resource intent. The fulfillment relation is a plausible foundation: a verifier could test that the original intent was to a resource owned by this addon, that the addon was an appropriate target based on its signed relation, and that resulting manifests and placement are signed by the authorized addon. Whether the current relation model is sufficient for this multi-artifact case, or whether it needs additional evidence (e.g. an addon-signed production manifest linking the managed resource to the artifacts it spawns), is an open question.

```mermaid
sequenceDiagram
    participant User as Consumer (Priya)
    participant Platform as Platform API
    participant Store as Resource Store
    participant Pipeline as Orchestration Pipeline
    participant Fleetlet as Addon Fleetlet
    participant Addon as Cluster Mgmt Addon
    participant Cloud as Cloud Provider (ROSA API)

    User->>User: Sign managed resource spec
    User->>Platform: POST /apis/kind.fleetshift.io/v1/clusters (signed)
    Platform->>Platform: Validate spec against<br/>addon-registered schema
    Platform->>Store: Store managed resource<br/>with provenance

    Platform->>Pipeline: Create derived Fulfillment<br/>(manifest=resource ref,<br/>placement=addon, rollout=immediate)

    Note over Pipeline: Standard orchestration:<br/>Resolve → Delta → Plan → Generate → Deliver

    Pipeline->>Pipeline: Resolve placement → [addon target]
    Pipeline->>Pipeline: Generate manifest → managed resource document
    Pipeline->>Fleetlet: Deliver (resource spec + attestation)
    Fleetlet->>Addon: Delivery channel

    Addon->>Addon: Interpret resource spec
    Addon->>Cloud: Create ROSA HostedCluster,<br/>configure networking, KMS, etc.
    Cloud-->>Addon: Cluster provisioning started

    Addon-->>Fleetlet: State: PROVISIONING
    Fleetlet-->>Pipeline: Status channel
    Pipeline-->>Store: Update Fulfillment state

    Note over Addon, Cloud: Minutes later...

    Cloud-->>Addon: Cluster ready, API URL available
    Addon->>Platform: Register new target<br/>(the provisioned cluster)
    Addon-->>Fleetlet: State: ACTIVE
    Fleetlet-->>Pipeline: Status channel
    Pipeline-->>Store: Update Fulfillment state
    Addon-->>Fleetlet: Properties: {api_url, console_url}
    Fleetlet-->>Pipeline: Index channel
    Pipeline-->>Store: Update inventory item properties

    User->>Platform: GET /apis/kind.fleetshift.io/v1/clusters/prod-us-east-1
    Platform-->>User: state: ACTIVE,<br/>properties: {api_url: https://...,<br/>console_url: https://...}
```



The key property: the platform is a courier throughout. It stores the user's signed intent, mechanically derives a Fulfillment, and delivers the resource spec to the addon through the standard pipeline. The addon — a separate process with its own identity — is the only component that interprets the spec and interacts with the cloud provider. Provenance chains from the user's signature through the managed resource to the derived Fulfillment to the delivery attestation, without the platform ever needing to understand what a "ROSA cluster" is.

### Architectural layering

The platform separates the core orchestration primitive from user-facing concepts:

**Fulfillment** (kernel primitive): the internal unit of orchestration. Each Fulfillment maintains independently versioned strategy streams (manifest, placement, rollout) that drive the reconciliation loop — resolve → delta → plan → generate → deliver. Fulfillments are not directly created or edited by users. Any strategy version advance bumps the Fulfillment's generation, providing configuration history at the kernel level for audit, debugging, and rollback.

**User-facing concepts** (thin layers over Fulfillments):

- **Managed resource**: continuous reconciliation, addon-driven or config-only, typed collections (`/clusters`, `/monitoring-stacks`). Creates and drives a Fulfillment.
- **Campaign**: run-to-completion fleet operations, immutable once started. Creates Fulfillments for each stage.
- **Deployment**: the "just deploy this" convenience. A thin wrapper that creates a single Fulfillment. Simple edit/status API.

Each user-facing concept defines its own API surface, lifecycle rules, and editability. The Fulfillment has no direct edit API — it's driven exclusively through the concept that owns it. This avoids the problem where higher-level concepts must restrict a general-purpose primitive ("you can't edit this deployment because it's owned by a managed resource"). Instead, the primitive is inert by default, and each concept grants specific capabilities.

### Versioned intent

Managed resource specs are stored as immutable versions — the user-facing version history. Each update to a managed resource creates a new version; the managed resource HEAD table tracks which version is current.

```sql
CREATE TABLE resource_intents (
  resource_type TEXT NOT NULL,
  name          TEXT NOT NULL,
  version       INTEGER NOT NULL,
  spec          TEXT NOT NULL,       -- jsonb in Postgres
  provenance    TEXT,
  created_at    TEXT NOT NULL,
  PRIMARY KEY (resource_type, name, version)
);
```

Each version is immutable — INSERT only, never UPDATE. Postgres: `PARTITION BY LIST (resource_type)`, partitions at addon registration. Benefits: rollback (point to version N-1), audit (what spec when), attestation (per-version signature), debugging (diff versions).

Resource intent versioning is a managed resource layer concern, distinct from Fulfillment strategy versioning (see [below](#fulfillment-strategy-versioning)). When a new resource intent version is created, the platform creates a corresponding manifest strategy version on the Fulfillment (referencing the new intent version), which bumps the Fulfillment's generation and triggers reconciliation. But the two version histories are independent: the resource intent tracks what the user asked for; the Fulfillment's manifest strategy tracks what the system was configured to deliver.

### Fulfillment strategy versioning

Each strategy type on a Fulfillment has its own append-only version stream scoped to that Fulfillment. The Fulfillment tracks the current version of each strategy. Any strategy version advance bumps the Fulfillment's generation, which is the orchestration loop's signal to reconcile.

```
fulfillment:
  id: F1
  manifest_strategy_version:  3
  placement_strategy_version: 2
  rollout_strategy_version:   1
  generation: N

manifest_strategies:   (fulfillment_id, version, type, content, created_at)
placement_strategies:  (fulfillment_id, version, type, content, created_at)
rollout_strategies:    (fulfillment_id, version, type, content, created_at)
```

Strategy versions are scoped to a Fulfillment — each Fulfillment maintains independent version counters per strategy type. This cleanly separates two distinct version histories:

- **Resource version history** (what the user asked for over time) — stored in `resource_intents`, tracked by the managed resource HEAD table
- **Fulfillment configuration history** (what the system was configured to do at each point) — stored in per-Fulfillment strategy versions

A managed resource spec change creates a new `resource_intent` version AND a new manifest strategy version on the Fulfillment (referencing the new intent version). But the Fulfillment's manifest strategy can also change for reasons unrelated to the resource intent (e.g. addon invalidation). And placement or rollout strategies change independently of either.

**Shared definitions.** Strategies can reference shared, reusable definitions through an additional layer of indirection — the same pattern as managed resources referencing `resource_intents`. A placement strategy version might reference a shared placement definition used across many Fulfillments. When the shared definition changes, each affected Fulfillment gets a new strategy version, which bumps generation and triggers reconciliation. The reusability lives in the referenced definition, not in the strategy version stream itself.

Stable generated values — things like `api_url`, `provider_id`, `oidc_issuer` that are produced once and rarely change — are stored as inventory properties on the inventory item for the managed resource (see [Inventory](#inventory)). A single Fulfillment can fan out to many targets, each producing its own properties, so properties are per-object rather than per-Fulfillment.

### Delivery model

Deliveries are keyed on `(fulfillment_id, target_id)`. For bundled intents with multiple manifests, the delivery reports per-manifest apply outcomes via **manifest_results**:

```json
{
  "ns": { "applied": true },
  "crds": { "applied": true },
  "deploy": { "applied": true },
  "svc": { "applied": true },
  "rbac": {
    "applied": false,
    "error": "forbidden: requires elevated privileges"
  }
}
```

manifest_results has a narrow purpose: "what happened when I tried to apply each manifest." Written at apply time, overwritten on next apply. Not historical — delivery condition events cover the transition story. Failed applies don't produce inventory; manifest_results covers failures.

**Delivery conditions** are addon-reported *operational* status on this target — the addon's own assessment of its ability to function ("can't reach API server", "auth failed", "apply batch completed"). These are NOT a semantic health aggregate of deployed resources. The addon is the thing doing the delivery, so if there's aggregate status only it knows about, it needs a place to put it.

**Fulfillment conditions** are platform-aggregated from delivery conditions via CEL. Single-target = pass-through. Built-in defaults when no CEL registered. Two stored condition levels: delivery (addon-reported) and Fulfillment (platform-aggregated). No per-manifest condition layer on deliveries — per-manifest health comes from inventory.

#### Condition events (history)

Condition transitions stored as discrete events. Server deduplicates — only records actual transitions.

```sql
CREATE TABLE condition_events (
    fulfillment_id TEXT NOT NULL,
    target_id      TEXT NOT NULL,
    condition_type TEXT NOT NULL,
    status         TEXT NOT NULL,
    reason         TEXT,
    message        TEXT,
    generation     INTEGER NOT NULL,
    observed_at    TEXT NOT NULL,
    PRIMARY KEY (fulfillment_id, target_id, condition_type, observed_at)
);
```

Same pattern applies for inventory condition events.

### Inventory

Inventory is the system for all historical observations — a point-in-time report of what an observer saw about a resource. It covers both managed and inventory resources. Comparable to ACM Search (stolostron/search-v2-api) in scope, but optimized for observation history and health querying.

Inventory stores per-resource projections keyed by extension resource identity and linked to the platform resource via identity equivalence. Read-only extension resources that exist solely to report observed state are inventoried resources in the `inventory` representation role. Managed resources can also have inventory projections without becoming inventoried resources themselves. See [architecture/resource_identity_and_api.md](architecture/resource_identity_and_api.md) for the resource identity model and representation roles.

Inventory is a projection, not a literal copy of the source resource. The addon extracts relevant fields, like ACM search collectors extract a subset of K8s fields.

Each inventory item has:

- **Identity**: resource type, name, source association (Fulfillment + target, optional manifest_key for intent-correlated resources, null for side-effect resources)
- **Properties**: stable generated values (api_url, provider_id, console_url) produced once and rarely changed. Not historical — the latest value is the only one that matters. Properties are per-object because a single Fulfillment can target many objects, each with its own properties.
- **Observations**: opaque, addon-defined. The latest point-in-time report of runtime state (replica counts, image versions, allocatable resources, etc.) as seen by the observer. Historical observations are kept over time.
- **Conditions**: structured, platform-queryable health and progress signals. Historical transitions tracked via condition events. Gives the platform a uniform health query surface across all resource types without understanding observation internals.

Aliases and semantic relationships live on the linked platform resource identity. Inventory reporting may contribute them, but the platform resource remains the canonical owner of that aggregated identity data.

For managed resources, the managed thing itself may have an inventory projection. A single intent may explode into many inventory items — the managed resource's own projection plus sub-resources created by the addon (nodes under a cluster, operators, etc.). Side-effect resources associate with the delivery but have no manifest_key.

**Condition types** are both platform-defined and addon-defined. The platform defines conventional types (`Lifecycle`, `Healthy`) for universal fleet dashboards. Addons define domain-specific types (`ReplicationHealthy`, `KeyValid`, `Progressing`) without platform coordination. The platform defines conventions, not constraints — ecosystem conventions emerge from usage.

Validated against real cloud APIs: EKS (health issues list), GKE (gRPC-coded conditions), AKS (provisioningState/powerState only), RDS (health encoded in status enum). Each maps onto the inventory shape with addon-side translation. The inconsistency across cloud APIs is the reason conditions exist as a platform abstraction.

#### Managed resource API projection

The managed resource consumer API projects from three sources:

- **Spec** → `resource_intents` (the version the managed resource tracks). The user's declared intent.
- **State** → Fulfillment lifecycle enum (PROVISIONING, ACTIVE, FAILED, DELETING). The AIP-compliant lifecycle state.
- **Properties** → inventory item for this managed resource (if present). Stable generated values (api_url, provider_id, console_url). Written once, rarely change, no history needed. Lives on the inventory item because a single Fulfillment can fan out to many objects, each with its own properties.
- **Observations** → inventory item for this managed resource (if present). The latest point-in-time report of what the observer saw. Historical observations are kept over time.
- **Conditions** → always from Fulfillment (aggregated from delivery conditions via CEL), plus inventory conditions for this managed resource if a matching inventory item exists. Fulfillment conditions are always available because every managed resource has a Fulfillment; they provide the operational/management view ("is the delivery pipeline working?"). Inventory conditions, when present, add resource-health signals ("is the resource itself healthy?"). Both are merged into a single conditions list — condition types are self-describing, so the consumer doesn't need to distinguish the source.

```
GET /apis/kind.fleetshift.io/v1/clusters/prod-us-east-1
  spec:         → from resource_intents @current_version (managed resource HEAD)
  state:        → from Fulfillment (PROVISIONING/ACTIVE/FAILED/DELETING)
  properties:   → from inventory item (if present)
  observations: → from inventory item (if present)
  conditions:   → from Fulfillment (always) + inventory item (if present)
```

> OPEN QUESTION: Could / should we support managed resources backed by addons with their own state. We know some addons will have their own state (ACS in a relational db, MCOA in prometheus/thanos, ...).

#### Managed resource HEAD table

A thin identity/HEAD table provides the entry point for all managed resource queries. It holds only pointers — no denormalized state, no cached lifecycle state.

```sql
CREATE TABLE managed_resources (
  resource_type TEXT NOT NULL,
  name          TEXT NOT NULL,
  uid           TEXT NOT NULL UNIQUE,
  current_version INTEGER NOT NULL,  -- → resource_intents.version
  fulfillment_id  TEXT NOT NULL,     -- → fulfillments.id
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL,
  deleted_at    TEXT,                 -- soft delete
  PRIMARY KEY (resource_type, name)
);
```

The managed resource is the query entry point, not the Fulfillment. Listing all resources of a type (for example `GET /apis/kind.fleetshift.io/v1/clusters`) is a direct scan on this table, joined to `resource_intents` for the spec and to the Fulfillment for lifecycle state:

```sql
SELECT mr.*, ri.spec, ri.provenance, f.state
FROM managed_resources mr
JOIN resource_intents ri
  ON ri.resource_type = mr.resource_type
  AND ri.name = mr.name
  AND ri.version = mr.current_version
JOIN fulfillments f ON f.id = mr.fulfillment_id
WHERE mr.resource_type = 'clusters'
  AND mr.deleted_at IS NULL;
```

State is not denormalized — the Fulfillment join is already required for lifecycle state. Conditions are sourced from both the Fulfillment (always) and a matching inventory item (if present). Properties and observations come from inventory.

Writes: `INSERT` on create, `UPDATE current_version` on spec change, `UPDATE deleted_at` on delete. Updating `current_version` is a managed resource layer operation; it triggers a corresponding manifest strategy version on the Fulfillment (see [Fulfillment strategy versioning](#fulfillment-strategy-versioning)), but the two version counters are independent.

### Placement and groupings

A managed resource's placement is defined by the addon. It generally represents a single managed "thing" and therefore a single placement. But technically we could probably support multiple placements. In which case, rollout strategy becomes relevant.

> NOTE: Currently a Fulfillment supports a single rollout. I intend to adjust this so it is more MWRS-like: many placement×rollout pairs, where each pair independently replicates to its resolved targets. It's a modest change to the core loop, but makes it more flexible and maps more closely to MWRS.

The design here is incomplete but landing on a first increment that is likely correct: just supporting the managed resource model described here.

Further grouping concepts are likely to be added to the core platform kernel in some shape or form, along these orthogonal axis:

- **Reconciliation**. Does the grouping reconcile against a "group-level" definition or not? That is, is the grouping itself a "Deployment" that reconciles, and what of it reconciles? Sometimes you have a definition of a group that allows edits of the sub-resources but corrects for drift where unintended or newly managed.
- **Targeting.** Does the grouping contain delivery targets which can themselves accept manifests or not?
- **Growing.** Is the grouping able to "grow" by provisioning more like members with little input?

A group of targets is already semi-defined: it's the initial placement pool. There is ongoing design work around making this growable and reconciliable.

As established above, we also expect addons to be able to produce platform artifacts (beyond targets or inventory–for example, Deployments or other managed resources) as a result of delivery.

This leads to the following options we should expect in the future by composing one or both of these:

- Mappings directly to platform groupings (for example, a managed resource that is not a single-placement Fulfillment directly but instead maps to a "growable reconciling pool of targets")
- Addons which produce platform groupings as a result of delivery

These are ultimately quite similar to the end user. They differ in architecture (trust, responsibility boundaries, and order of operations.)

### Addon invalidation

In the deployment architecture, we have invalidation signals. For example, if an addon's manifest strategy changes, it will trigger an invalidation of all Fulfillments it generates manifests for, so it can regenerate them and reconciliation takes over.

This design highlights a missing invalidation signal: when the delivery agent itself has a behavior change. If the delivery would now do something different, all manifests for that target must be redelivered. Managed resource addons are one such case, but any delivery agent technically could have its behavior change.

### Domain specific operations

**WIP / DRAFT**

I think a reasonable path here would be to model these as managed resources / sub resources themselves. They need to be REST-friendly, anyway. The trick is that these could result in associated new platform artifacts, like other managed resources, or Deployments. So an addon could implement a smart transformation from a high level cluster upgrade campaign input to low level deployment-that-updates-resources with rollout strategy.

Non-reconciling fleet-wide operations (e.g. upgrade campaigns, rolling patches) are expected to be a separate top-level platform concept alongside managed resources, with their own lifecycle semantics (immutable, terminal, stoppable) and their own design. They build on the same deployment orchestration but differ in lifecycle: a managed resource reconciles continuously, while a campaign runs to completion. This is a separate design effort.

### Domain specific queries

**WIP / DRAFT**

These could possibly be inventory or addon-directed queries (hand waving a bit over what that is -- maybe an extension of the federated query mechanism discussed in [architecture/platform_hierarchy.md](./architecture/platform_hierarchy.md)) that are pre-configured templates. We'd like to define a read projection up front and not have to hit the addon to process this.

### Integration (observability, security, ...)

**WIP / DRAFT**

Addons need to be able to integrate with each other.

### API (gRPC / REST)

Managed resource types extend both the gRPC and HTTP API surface at runtime. Schema activation performs **dual registration**: the extension resource type in the addon's own package, and a corresponding platform resource type in the platform package for the canonical identity layer. See [architecture/resource_identity_and_api.md](architecture/resource_identity_and_api.md) for the two-layer model.

When a schema is activated, the platform:

1. **Compiles** the addon's inline proto sources into a file descriptor set, resolving well-known imports (`buf/validate/`*, `google/protobuf/`*) from a built-in registry.
2. **Builds** a dynamic gRPC `ServiceDesc` with Create, Get, List, and Delete methods. The service name follows the addon's proto package: `{proto_package}.{Singular}Service` (e.g. `kind.fleetshift.v1.ClusterService`). Request/response messages are constructed dynamically from the compiled spec descriptor.
3. **Registers** the service in the `DynamicServiceMux`, which is wired as the gRPC server's `UnknownServiceHandler`. Requests to services not registered at server creation time are routed here instead of being rejected. Composite reflection merges dynamic services with static ones so they are discoverable via `grpcurl`.
4. **Registers HTTP routes** in the `DynamicHTTPMux` — a wrapper around `http.ServeMux` that uses handler indirection for zero-downtime replacement. HTTP handlers proxy to the gRPC service, providing REST access at `/apis/{service_name}/{version}/{plural}` and `/apis/{service_name}/{version}/{plural}/{id}`.
5. **Registers the platform resource type** in the platform API layer (generic schema), enabling the canonical identity surface at `/apis/fleetshift.io/v1/{plural}/{id}`. For a cluster-capable addon this means the extension API is at `/apis/kind.fleetshift.io/v1/clusters/{id}`, while the shared identity surface is at `/apis/fleetshift.io/v1/clusters/{id}`.

Atomic replacement is a first-class operation. When an addon reconnects with a changed schema, the `DynamicSchemaActivator` detects the change via content hashing (SHA-256 over proto files, spec message, singular, plural, proto package, and service name), recompiles, and atomically swaps both the gRPC and HTTP service entries. In-flight requests that already resolved the old entry complete normally; new requests route to the replacement immediately. Unchanged schemas skip recompilation entirely.

The transport-layer components live across three packages under `internal/transport/`: shared infrastructure (`dynamicapi` — muxes, file registry, compiler, helpers), extension services (`managedresource` — service builder, activator), and platform-canonical services (`platformresource` — builder and handler). They are documented in [addon_integration.md — API extensibility](architecture/addon_integration.md#api-extensibility--dynamic-grpc-and-http). See also [addon lifecycle](architecture/addon_integration.md#addon-lifecycle) for how schemas flow from addon connect to API registration.

### Durability

> NOTE: This topic has been expanded on in [target_delivery_contract.md](./architecture/target_delivery_contract.md).

How does this design guarantee that (under failure conditions)...

- no resources are orphaned
- resources eventually reconcile (or explicitly fail)

When an addon [re]connects...

- it has to ask about what work it has left to do
- it has to ensure it's reported the state it knows about. This may require querying for all the things it *should* know about and updating those.

### What was eliminated

- **Mutable managed_resources table** — replaced by versioned intent
- **Separate managed resource status table** — observations through inventory
- **Single Fulfillment field as the sole observation mechanism** — per-object knowledge (properties, observations) lives on inventory items; conditions are sourced from both Fulfillment (aggregated operational) and inventory (resource health); the Fulfillment owns lifecycle state and aggregated conditions
- **Per-manifest condition layer on deliveries** — inventory is the per-resource condition system
- **Three-level condition hierarchy** — two levels sufficient (delivery + Fulfillment)
- **Multiple intent pointers per Fulfillment** — bundled intent preferred; strategy versioning is per strategy type, not per intent
- **Delivery conditions as semantic health aggregate** — delivery conditions are addon operational status; resource health through inventory
- **Direct user access to the kernel primitive** — user-facing concepts (managed resource, campaign, deployment) are the API; Fulfillment is internal
- **Fulfillment as composite version snapshot** — replaced by per-strategy version streams; the Fulfillment tracks current versions per strategy type, and generation derives from strategy version advances

### Open questions

#### Pre-Fulfillment lifecycle states

Can a managed resource exist before its Fulfillment (pending approval, schema validation)?

#### Condition event compaction/TTL

Policy needed for both Fulfillment condition events and inventory condition events.

#### Inventory data model details

- Concrete table schema for inventory items and observations
- Relationship to the existing `resource` / `resource_versions` tables in `docs/design/archive/inventory_resource_storage.md`
- Compaction and TTL for inventory observations

#### Config-only resource types

Whether resource types can be defined through configuration alone (no addon process), providing typed collections and managed-resource-like API without the overhead of a separate addon. Spectrum from full addon-managed to pure config-only.

#### Fulfillment rename in codebase

Completed in OME-43. The codebase now has `Fulfillment` as its own aggregate with `FulfillmentRepository`, and `Deployment` is a thin aggregate holding a `FulfillmentID` reference. Orchestration operates on `Fulfillment` directly. A `DeploymentView` read model joins the two for the user-facing API. The user-facing proto/gRPC layer still uses "Deployment" terminology.

### Phasing

1. **Phase 1:** Versioned intent table (`resource_intents`) + per-Fulfillment strategy versioning (manifest, placement, rollout) + basic lifecycle state. No properties, observations, or conditions. Proves the managed resource → Fulfillment flow end-to-end with the two version histories cleanly separated.
2. **Phase 2:** Inventory as the per-object knowledge system. Inventory items with properties + observations + conditions. Condition events for inventory. manifest_results on deliveries. Delivery conditions (addon-reported). Fulfillment conditions (CEL aggregation, single-target pass-through). Shared strategy definitions (reusable placement across Fulfillments).
3. **Phase 3:** CEL-based aggregation for multi-target. Fleet-wide condition and inventory queries. Campaign and deployment as user-facing concepts over Fulfillments.

