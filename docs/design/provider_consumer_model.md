# Provider/consumer management model

This document proposes extensions to the FleetShift architecture to support the provider/consumer/factory topology observed in every managed infrastructure service at scale. These proposals emerge from analysis of real systems (ROSA Cluster Service, IBM Cloud IKS/ROKS, ARO HCP, RAZEE, HyperShift, ACM) and from the observation that the provider/consumer split is not unique to cloud providers -- it is the natural structure of any organization managing infrastructure at scale, including enterprise IT teams and mid-market customers.

The core argument: the provider/consumer/factory/neighborhood model should be first-class structure in FleetShift's kernel, not a pattern assembled from generic primitives. The generic Manifest x Placement x Rollout model remains the foundation, but provider-side concepts (managed resource types, factory pools, neighborhoods, signed intents) are built-in alongside it.

## Context: the topology

Every managed infrastructure service implements the same structural pattern:

- A **consumer** requests managed resources (clusters, ArgoCD instances, VMs) through a front door.
- A **provider** fulfills those requests by scheduling onto pools of infrastructure ("factory clusters") organized by workload type ("neighborhoods").
- A **curtain** separates the consumer surface from the factory infrastructure, enforcing network separation, identity boundaries, and blast radius containment.

This pattern repeats at every scale:

1. **Laptop**: one instance, a few clusters. No explicit provider/consumer split -- the admin is both.
2. **Small fleet**: neighborhoods start to emerge (workload clusters vs. virt clusters). Still one instance, but workspace structure reflects the separation.
3. **Enterprise platform team**: full provider/consumer split. Platform team operates a provider instance with factory pools. Internal teams consume managed resources through the provider's API using their own tenant identity and IdP(s). Teams that also want fleet-wide workload management over their provisioned clusters can optionally run their own FleetShift instance.
4. **Managed service provider**: same topology as (3) with hard cryptographic boundaries, signed intents, and network curtain. Consumers interact with the provider API directly. Per-tenant FleetShift instances are available for consumers who want their own fleet management plane on top of the managed clusters provisioned for them.

The progression is continuous. The product experience should guide users along this path naturally, not require a product switch at any step. Critically, consumers do not need their own FleetShift instance to use a provider -- they can interact with the provider's API directly. The provider must know the consumer's tenant identity and IdP(s) in order to store, deliver, and validate deployments on their behalf. A consumer's own FleetShift instance is an additive capability, not a prerequisite.

## 1. First-class pool concept

**Current state**: the "workspace target pool" is a passive set -- all targets in a workspace's subtree. Placement strategies receive this set and select from it. There is no capacity tracking, no expansion trigger, no pool-level lifecycle.

**Problem**: the provider model requires active pools. An HCP management cluster pool must track how many hosted control planes each member is running (capacity), detect when all members are at their density limit (exhaustion), and trigger provisioning of new management clusters (expansion). Each neighborhood type (HCP, Argo, Virt, AI) has its own density constraints and provisioning template.

**Proposal**: a **Pool** is a first-class platform resource, distinct from a workspace's implicit target set. A pool defines:

- **Membership criteria**: which targets belong to this pool (label selector, explicit list, or provisioning-correlated).
- **Target template**: a provisioning deployment spec that defines what a new pool member looks like. When the pool needs to expand, the platform creates a new provisioning deployment from this template. This ensures every member follows the same well-known configuration (e.g., an Argo pool's template includes best-practice ArgoCD operator installation, network policies, UDN configuration, etc.).
- **Capacity model**: a pool-type-specific definition of capacity per target. For HCP management clusters: number of hosted control planes (DNS entries consumed, resource utilization). For Argo pools: number of ArgoCD instances. For Virt: VM count, storage utilization, GPU availability. The capacity model is an extension point -- each neighborhood type registers its own definition.
- **Expansion policy**: conditions under which new targets should be provisioned (e.g., "when average pool utilization exceeds 80%" or "when no target has capacity for the next allocation"). Contraction policy (draining and deprovisioning underutilized targets) is the inverse.
- **Health aggregates**: pool-level health derived from member health, capacity utilization, and provisioning status. Visible to the provider administrator.

Placement strategies that target a pool receive the pool's membership rather than a raw workspace target set. The platform's placement view (ID, name, labels) is unchanged -- it remains minimal, governing only when the platform triggers re-evaluation. Capacity-aware placement is addon-driven: a binpacking scored strategy runs as an out-of-process addon, continuously monitoring target capacity through its own data sources (Prometheus, cloud APIs, management cluster metrics), maintaining a live scoring model, and signaling the platform to re-resolve when scores shift. When `Resolve` is called, the strategy returns results from its already-maintained state. The pool provides the membership set; the addon-driven strategy provides the scoring intelligence. This is the existing architecture's intended model for stateful placement, applied to the pool concept.

**Relationship to existing concepts**: a pool is not a workspace. Workspaces define organizational scope and access boundaries. Pools define operational groupings of infrastructure with capacity semantics. A pool's targets may span multiple workspaces (e.g., a global HCP management pool with members in several regional workspaces), or a single workspace may contain targets in multiple pools (management pool + argo pool in the same region).

## 2. Provider-surfaced managed resource types

**Current state**: the `platform` target type enables a parent platform to deploy to child platform instances. But the consumer-side model -- how consumers request managed resources and how those requests flow to the provider -- isn't explicitly modeled. The implicit assumption is that consumers always have their own FleetShift instance, which isn't true.

**Problem**: the contract between consumers and providers needs to work at two levels: (1) consumers using the provider's API directly with their own tenant identity and IdP(s), and (2) consumers who also run their own FleetShift instance and want provisioned clusters registered as targets in their fleet. The current design only addresses (2), making recursive instantiation a prerequisite rather than an optional enhancement.

**Proposal**: the provider platform registers **managed resource types** that define the consumer-facing contract. A managed resource type is:

- **Schema**: what the consumer can request (region, version, size, capabilities). Validated by the provider platform on receipt.
- **Manifest generator (provider-side)**: takes a validated request and generates the low-level manifests for the factory (HostedCluster CRDs, ArgoCD instances, etc.). This is the provider's internal addon.
- **Placement strategy (provider-side)**: how the generated manifests are scheduled onto the factory pool. Bespoke per managed resource type (binpacking for HCP, different binpacking for Argo, etc.).
- **Health model (provider-side)**: what "healthy" means for this managed resource type. HCP healthy means the control plane pods are running and the API server is reachable. Argo healthy means the ArgoCD instance is reconciling. Each type defines its own health.
- **Consumer-facing status**: the subset of health and lifecycle information surfaced back to the consumer. The consumer sees "my managed cluster is Provisioning / Ready / Degraded" without seeing which management cluster it's on or how many pods it has.

### Direct consumer integration (baseline)

A consumer interacts with the provider's API directly, authenticated by their own IdP. The provider registers the consumer's tenant identity and IdP(s) so it can store, deliver, and validate deployments on the consumer's behalf. The consumer requests managed resources against the provider's managed resource type schemas. The provider handles provisioning, placement, and lifecycle internally. The consumer observes status through the provider's API.

This is the default integration mode. The consumer does not need a FleetShift instance, a `platform` target type, or any cross-platform delivery. They use the provider's managed resource API the same way they would use any managed service API.

### Enhanced consumer integration (own FleetShift instance)

A consumer can also run their own FleetShift instance. This adds fleet-wide workload management on top of the managed clusters provisioned for them: the consumer can register provisioned clusters as targets in their own fleet, deploy workloads across them, search indexed resources, and use the full deployment pipeline (Manifest x Placement x Rollout) for their own applications.

When the provider provisions a cluster on behalf of such a consumer, the provider configures the new cluster's fleetlet to connect to the consumer's FleetShift instance. The managed resource type's lifecycle includes this cross-instance target registration -- the provider knows the consumer instance's endpoint and injects it into the provisioned cluster's bootstrap configuration.

The provider's managed resource types are also advertised to the consumer instance. The consumer instance can use a generic "managed resource" addon that adapts to whatever types the provider exposes, allowing the consumer to request managed resources through their own deployment pipeline rather than calling the provider API directly. This is the recursive instantiation path described in architecture.md section 12, but it is opt-in -- not the only way consumers interact with providers.

## 3. Signed intent verification in delivery

**Current state**: the security model (security.md) discusses signed intents for GitOps (user signs manifests, cluster-side admission validates the signature). The delivery contract does not currently include any cryptographic attestation from the originator.

**Problem**: in the provider/consumer/factory chain, the provider platform holds credentials for factory clusters. If the provider platform is compromised, the attacker has those credentials and can create/modify/delete tenant resources on any factory cluster. Per-factory-cluster credential scoping limits blast radius between factories but not within a factory (a single HCP management cluster hosts ~80 tenants' control planes).

**Proposal**: delivery to factory targets requires a **signed operation intent** from the originating consumer platform. The signed intent is:

- **Produced by the consumer platform** when it makes a request to the provider platform. Signed with the consumer platform's signing identity (Fulcio-issued short-lived certificate bound to the consumer platform's OIDC identity, logged in a transparency log; no long-lived signing keys).
- **Carried by the provider platform** as an opaque, unmodifiable attestation alongside the deployment. The provider cannot forge or alter the signed intent without invalidating the signature.
- **Verified by the factory-side fleetlet** (or factory-side agent in buffer mode) before delivery is passed to the local delivery agent. The fleetlet validates: signature is from a recognized consumer platform, claims match the operation (tenant, resource type, scope), intent has not expired. If validation fails, delivery is rejected.
- **Verified again by a validating admission webhook** on the factory cluster's Kubernetes API server (defense in depth). The webhook enforces: the manifest's namespace and resource type are consistent with the signed intent's claims, and the provider platform's credential is also valid.

Both the provider platform's credential AND a valid signed intent are required. Neither alone is sufficient.

**Standing intents for background operations**: reconciliation, upgrades, and healing require the provider to act without the consumer being online. The consumer platform issues standing intents with bounded lifetime (e.g., 24 hours): "tenant Acme approves ongoing management of HCP X." These are automatically renewed by the consumer platform. If the consumer platform stops renewing (outage, compromise), standing intents expire and background operations on that tenant's resources pause until renewal. This is fail-safe: the system degrades toward less access.

**Delivery envelope extension**: the delivery channel's request message gains an `attestation` field carrying the signed intent. The fleetlet validates this field before invoking the local delivery agent. For non-factory targets (consumer workload clusters), the attestation field is optional -- the existing identity model (token passthrough, delegation SAs) applies.

## 4. Verifiable per-tenant claims for factory credential retrieval

**Current state**: the provider platform holds per-factory-cluster credentials (or retrieves them from a secret store). These credentials are scoped to specific operations on specific clusters but are accessible to the provider platform process without external authorization.

**Problem**: if the provider platform is compromised, the attacker can retrieve all factory credentials from the secret store (or use in-memory credentials) and bypass signed intent enforcement by talking directly to factory clusters outside the fleetlet delivery path.

**Proposal**: factory credentials are stored in a vault that requires **dual authorization** for retrieval:

- The provider platform's own identity (mTLS certificate or JWT).
- A valid, non-expired signed tenant intent (or standing intent) for the specific operation.

The vault issues a **short-lived credential** (minutes) scoped to the operation described in the signed intent: specific factory cluster, specific namespace (tenant's namespace), specific resource types and verbs.

This means the provider platform never holds persistent factory credentials. Every factory operation requires a just-in-time credential from the vault, and the vault enforces the same dual-authorization that the factory-side fleetlet enforces. Defense in depth: even if the fleetlet validation has a bug, the vault independently requires the signed intent.

The vault can also rate-limit credential issuance and alert on anomalous patterns (unusual volume, off-hours access, access patterns inconsistent with normal operations).

**Relationship to signed intent verification**: this is complementary, not alternative. Signed intent verification (proposal 3) enforces at the factory boundary. Vault-based dual authorization (this proposal) enforces at the credential retrieval boundary. Together: the attacker must bypass both the vault AND the factory-side fleetlet/admission webhook, using a signed intent they cannot forge.

## 5. Fleetlet buffer mode for factory targets

**Current state**: the fleetlet maintains a persistent, bidirectional gRPC connection to the platform. All channels (delivery, status, index, access, protocol) are multiplexed over this connection.

**Problem**: a persistent bidirectional connection to a factory cluster is a tunnel through the network curtain. Even though the factory initiates the connection (outbound-only), the platform can send messages through the channel once established. If the platform is compromised, the attacker can send crafted delivery messages, and -- critically -- if protocol channels (K8s API proxy) are enabled, the attacker has arbitrary Kubernetes API access to the factory cluster through the tunnel.

**Proposal**: factory targets use a **restricted channel profile** with an optional **buffer transport**.

**Factory target profile (restricted channels)**:

- **Delivery channel**: enabled, with signed intent validation in the fleetlet before passing to the local delivery agent.
- **Status channel**: enabled (target -> platform only). Accepts the confidentiality risk of the platform knowing factory status, because the provider needs to know whether delivery succeeded.
- **Protocol channel (K8s API proxy)**: structurally absent. Not disabled -- absent from the factory fleetlet's capability set. The factory fleetlet binary (or configuration) does not include the protocol channel handler. No mechanism exists for the platform to tunnel API requests to the factory cluster through normal operation.
- **Index channel**: disabled by default. Factory cluster resource state is not indexed by the platform. Factory observability is a provider-internal concern handled by provider-internal tooling (Prometheus, etc.) that does not flow through the management plane. See proposal 7 for controlled exceptions.
- **Access channel**: disabled. No consumer-facing addon proxies run on factory clusters.

**Buffered transport (hardened mode)**:

For the highest security requirements, the factory-side agent reads manifests from a buffer (S3, Kafka, NATS) rather than maintaining a persistent connection to the platform. The platform writes to the buffer; the factory-side agent reads from it. Status flows back through the same buffer (or a separate one). No persistent channel exists.

The buffer is in a **separate security domain** (e.g., a different cloud account) accessible to both the platform and the factory via cross-account credentials, but neither side has network access to the other. If the platform is compromised, the attacker can write to the buffer but cannot reach the factory cluster directly. The factory-side agent validates signed intents before applying anything read from the buffer.

This maps to the fleetlet's existing transport pluggability. The factory fleetlet uses a Kafka, NATS, or S3 transport instead of gRPC/TLS. Locally, the delivery agent sees the same interface. The delivery contract (Manifest x Placement x Rollout, signed intents, status reporting) is identical regardless of transport.

**Trade-offs of buffer mode**: no real-time delivery (polling or message queue latency), no protocol channel (no K8s API access through the platform), delayed status. For factory clusters, these are acceptable -- HCP provisioning is not latency-sensitive, the provider doesn't need to proxy API requests to factory clusters, and status can be delayed by seconds without operational impact.

## 6. Break-glass access sessions

**Current state**: if protocol channels are disabled on factory targets (proposal 5), there is no mechanism for a provider operator (SRE) to access a factory cluster through the platform when debugging is needed.

**Problem**: SREs sometimes need direct access to factory clusters (investigating a stuck HCP, diagnosing management cluster issues). A separate, out-of-band break-glass system is the conventional approach, but it fragments audit trails and doesn't compose with the platform's identity model.

**Proposal**: the platform models break-glass access as a first-class resource -- an **access session** with a lifecycle (Requested -> Approved -> Active -> Expired/Revoked).

The flow:

1. **SRE requests access** through the platform API: target, scope (read-only, namespace-scoped write, full write), reason, incident ID, requested duration.
2. **Platform creates a pending access session** and notifies the approver(s).
3. **Approver signs the authorization** independently of the platform -- using their own OIDC identity via Fulcio, or their own signing key. The platform facilitates notification but does not hold the approver's signing capability. The signed authorization includes: target, scope, requester identity, approver identity, reason, expiry.
4. **Signed authorization is delivered to the factory-side fleetlet** through the delivery channel (or buffer). The fleetlet validates: signature is from a recognized approver, not expired, target matches.
5. **Fleetlet temporarily enables the protocol channel** for the approved duration and scope. The SRE's API requests flow through the tunnel, authenticated by the SRE's own OIDC token at the factory cluster's API server (token passthrough -- the factory cluster validates the SRE directly).
6. **Session expires or is explicitly revoked.** The fleetlet tears down the protocol channel. The platform records the full session lifecycle in its audit log.

**Security properties**:

- No standing access to factory clusters through the platform. The protocol channel is structurally absent until an approved session activates it.
- Three independent factors required: the SRE's identity (OIDC token for the factory cluster), the approver's signed authorization (for the fleetlet to enable the channel), and network reachability through the platform. Compromising any one is insufficient.
- Unified audit: every break-glass session is a platform resource with full lifecycle tracking, visible alongside deployments and delivery status. "Show me all break-glass sessions for factory clusters in the last 30 days" is a platform query.

**Scope gradation**: the signed authorization includes a scope. Read-only (`get`, `list`, `watch`) for investigation. Namespace-scoped write for targeted remediation. Full write is rare and may require multiple approvers. The factory-side fleetlet can enforce scope by intercepting mutating requests on the protocol channel (or by configuring the SRE's K8s RBAC on the factory cluster).

## 7. Controlled indexing of managed targets

**Current state**: the index channel streams resource deltas from targets to the platform for fleet-wide search. This is designed for consumer-visible clusters.

**Problem**: indexing factory clusters would expose provider-internal state (tenant namespace names, HCP resource configurations, scheduling decisions) through the platform's search API. But some indexing of consumer-visible managed resources is valuable -- the consumer wants to know "what pods are running in my managed cluster" without needing direct API access to the cluster.

**Proposal**: distinguish between **substrate indexing** (resources on the factory cluster itself -- management-internal) and **managed target indexing** (resources on clusters that were provisioned for consumers and are consumer-visible).

- **Substrate indexing (factory clusters)**: disabled by default (proposal 5). Factory cluster resource state is not indexed through the platform. Provider-internal observability uses provider-internal tooling.
- **Managed target indexing (consumer clusters)**: enabled. Consumer workload clusters provisioned by the provider have fleetlets that connect to the consumer platform and index resources there. The consumer sees their own cluster's resources through the platform's search API.

The boundary is clean: factory clusters connect to the provider platform with restricted channel profiles (no index channel). Consumer clusters connect to the consumer platform with full channel profiles (including index channel). The two never overlap -- a cluster is either infrastructure (provider-visible only) or a managed target (consumer-visible).

If the provider needs operational visibility into what's running on factory clusters (e.g., "which management clusters are running HCPs for tenant Acme"), this is a provider-internal query answered by the provider platform's own deployment status and health reporting -- not by indexing the factory cluster's resources through the fleetlet.

## 8. Provider-adjacent domain-specific abstractions

**Current state**: FleetShift's core model is fully generic: Manifest x Placement x Rollout, with pluggable strategies, target types, and addon contracts. The provider/consumer/factory topology is partially achievable by composing these primitives (managed resource type APIs for direct consumer integration, recursive instantiation via the `platform` target type for consumers with their own instances, bespoke addons), but it is not structurally supported -- the framework doesn't know about providers, consumers, neighborhoods, or managed resources.

**Problem**: if the provider/consumer/factory topology is the universal pattern for infrastructure management at scale, requiring users to assemble it from generic primitives is both error-prone and undiscoverable. The framework can't guide users toward the right structure if the structure isn't part of its vocabulary.

**Proposal**: elevate provider-side concepts to first-class kernel abstractions alongside the generic deployment model.

**Managed resource type**: a registered type with a schema (consumer-facing request format), a manifest generator (provider-side rendering), a placement strategy (provider-side scheduling), and a health model (provider-side health definition with consumer-facing status projection). This replaces the pattern of manually wiring a consumer-side addon, a provider-side addon, and a cross-platform delivery path. The kernel handles the wiring; the extension point is "define what this managed resource looks like."

**Neighborhood**: a named grouping of pools that share a workload type, lifecycle policy, and security profile. A neighborhood defines: which managed resource type it serves, which pools it uses, what the factory target profile is (restricted channels, buffer mode, etc.), and what the upgrade lifecycle looks like (management clusters upgrade more frequently than workload clusters). Neighborhoods are the structural equivalent of Derek Carr's "city planning" metaphor -- each neighborhood has a purpose, and the platform guides the user toward planning them.

**Target profile**: a configuration that determines which fleetlet channels are available, which transport is used, and what validation happens at the fleetlet layer. Three built-in profiles: **consumer** (full channels, including protocol and index), **factory** (delivery + status only, signed intent validation, no protocol channel), and **hardened factory** (buffered transport, no persistent connection). Custom profiles are possible. The target profile is assigned per-pool or per-target and enforced by the fleetlet.

**Relationship to the generic model**: these abstractions sit ON TOP of Manifest x Placement x Rollout, not beside it. A managed resource type's manifest generator IS a ManifestStrategy. Its placement strategy IS a PlacementStrategy. Its health model extends DeliveryStatus. The generic model is the implementation layer; the provider-side abstractions are the domain layer. Consumers deploying their own applications to their own workload clusters still use the generic model directly. The provider-side abstractions only apply to the provider/consumer/factory pattern.

**Practical effect**: registering a new neighborhood (e.g., "AI service clusters") becomes: define the managed resource type schema, implement the manifest generator, implement the placement strategy (with capacity model), define the target profile, and define the pool template. The kernel handles: consumer-facing API generation from the schema, cross-platform delivery with signed intents, pool management with capacity tracking and expansion, factory-side validation, audit logging, and status projection back to the consumer. This is the "register a managed resource type" extension point rather than "compose addons and platform targets to approximate the provider topology."

## Open questions

### Consumer instance binpacking (own-instance mode only)

For consumers who opt into running their own FleetShift instance (the enhanced integration mode), those instances run on clusters. Should consumer instances be binpacked onto shared "consumer instance clusters" (with tenant density as a knob), or should each tenant get a dedicated cluster? This is another neighborhood type (the "consumer instance neighborhood") with its own density limits, blast radius considerations, and security profile. A density of 1 is the most isolated; higher density is more cost-efficient but increases blast radius. This question does not apply to consumers using direct API integration, who have no instance to host.

### Standing intent renewal protocol

What is the exact protocol for standing intent renewal? Does the consumer platform push new standing intents to the provider platform periodically? Does the provider platform request renewal? What happens during the grace period between expiry and renewal (allow operations with a warning, or hard-stop)?

### Vault integration specifics

Which vault systems should be supported for dual-credential factory credential retrieval? HashiCorp Vault, cloud-native KMS (AWS KMS, Azure Key Vault), or a platform-internal credential service? How are vault policies provisioned and managed -- are they part of the pool template (automatically configured when a new factory cluster joins a pool)?

### Provider operational tooling

The provider needs operational dashboards, capacity planning, and incident management tools that are distinct from the consumer-facing UI. Are these provider-platform-side addons with their own UI plugins? Or are they a separate operational plane entirely? The break-glass access session model suggests the platform can serve both, but the operational needs (aggregate capacity views, cross-neighborhood health, upgrade orchestration across factory clusters) may require provider-specific UI components that the generic addon UI plugin model should accommodate.

### Scope of the domain-specific kernel

How far does the domain-specific vocabulary extend? Managed resource types, neighborhoods, pools, and target profiles are the proposals here. But the provider/consumer model also implies: tenant management (create, suspend, delete tenants), subscription/entitlement tracking (which managed resource types a tenant can use), usage metering (how much of each managed resource type a tenant consumes), and quota enforcement (limits on managed resources per tenant). Are these kernel concerns or addon concerns? If kernel: the kernel grows significantly. If addon: the "managed resource" addon needs a rich contract with the kernel for metering and quota.