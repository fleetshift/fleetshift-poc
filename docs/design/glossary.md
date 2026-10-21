# OCM Fleet Glossary

This glossary defines the terms used across the OCM Fleet architecture. The
definitions describe the intended architecture; individual APIs or prototypes
may expose only a subset of these concepts.

## A–C

### Add-on (addon)

An independently deployed OCM Fleet integration unit that declares one or
more capabilities. An add-on may provide strategy implementations, delivery
[agents](#agent) and [target](#target) types, [managed resource](#managed-resource)
or inventory schemas, APIs, or UI [extensions](#extension). Add-ons are
separate execution and trust boundaries; they are not just libraries loaded
into the [resource manager](#resource-manager-rm).

Examples include a Kubernetes integration add-on that supplies a delivery
agent and inventory schema, or a managed-cluster add-on that exposes a typed
managed-resource API and provisions clusters through the normal fulfillment
pipeline.

An add-on is a platform extension: its capabilities may span clients and
independently running workloads, each with its own execution and trust
boundary. An add-on may participate in an [OCM Fleet](#ocm-fleet) deployment
without belonging to the OCM Fleet project.

### Agent

A process or component that performs domain or target-side work on behalf of
the platform. In delivery discussions, **delivery agent** means the component
that receives a [fulfillment](#fulfillment) payload, verifies the applicable
[delivery authorization](#delivery-authorization), applies it to a [target](#target),
and reports progress and health. A [fleetlet](#fleetlet) is transport
infrastructure, not an agent, and does not perform this business logic.

### Attestation

A delivery-carried package of authenticated evidence and a typed action. The
the [common evaluator](#common-evaluator) uses it to determine whether the action is authorized before
the delivery agent applies it. An attestation can include root intent,
supporting evidence, generated output, placement evidence, or trust updates.

### Authority

In [provenance](#provenance), the canonical external namespace that vouches for [principals](#principal)
under an identity scheme. It is represented with a scheme as a **principal
authority** `(scheme, authority)`, such as an OIDC issuer or SPIFFE trust
domain.

### AuthorityConfig

The authenticated configuration for one principal [authority](#authority). It defines the
credential methods, [provenance profiles](#provenance-profile), and ordered
[delivery policies](#delivery-policy)
accepted under that authority, along with their trust anchors and parameters.
It is installed at bootstrap and changed through authenticated trust-config
updates. Those updates are delivered to the [agents](#agent) and other
verifiers that must enforce the authority, where they update local retained
trust state before relying on the new configuration. The [resource manager](#resource-manager-rm)
also uses the configuration when accepting and assembling evidence. Distribution
is scoped to relevant verifiers and targets, not necessarily every process in
the platform. `AuthorityConfig` selects installed implementations but does not
supply executable code.

### Channel

A named, bidirectional gRPC stream multiplexed by a [fleetlet](#fleetlet). Channels are the
unit of routing, access control, traffic prioritization, and independent
stream lifecycle. Built-in channels include control, access, delivery,
status, and index; add-ons may define additional channels.

### Common evaluator

The shared, mechanism-neutral semantic evaluation layer for a delivery. After
a [provenance profile](#provenance-profile) verifies individual evidence, the
common evaluator evaluates the authenticated [attestation](#attestation) graph:
it checks relationships between assertions, content and generation bindings,
tenant and scope constraints, and whether the selected root authorizes the
requested action. It determines whether the delivery is authorized under the
applicable [delivery authorization](#delivery-authorization), but it does not
choose or cryptographically verify the provenance profile and does not apply
the payload; the [delivery agent](#agent) performs application at the
[target](#target).

The common evaluator is used by:
- The resource manager, when accepting evidence, assembling deliveries, and updating the evidence log.
- The delivery agent, independently of the resource manager, before applying a delivery.

### Console

The browser application, introduced as the **OCM Fleet console**. The console
is a client of the [resource manager server](#resource-manager-rm), not the
[resource manager](#resource-manager-rm) itself.

### Credential presentation

The choice of whose live credential performs the concrete operation at a
[target](#target): for example, the user's credential, a delegated workload
identity, a delivery agent's local identity, or a standalone service identity.
Credential presentation and [provenance](#provenance) are separate axes of
[delivery authorization](#delivery-authorization).

## D–G

### Delivery authorization

The target-side authorization model for a delivery:

```text
Delivery authorization = Credential presentation × Provenance
```

It determines both who applies the payload and what authenticated intent or
supporting assertions the [target](#target) requires. It is distinct from orchestration,
which determines what, where, and when to deliver.

### Delivery policy

An ordered, authenticated rule in an [`AuthorityConfig`](#authorityconfig) that matches delivery
context such as tenant partition and assertion purpose. A policy states
whether [credentials](#credential-presentation) and [provenance](#provenance) are
required or allowed, applies relevant constraints, determines whether [evidence-log](#evidence-log)
inclusion is required, and names the acceptable [provenance profile](#provenance-profile)
configurations. The first matching policy owns verification; verification failure does not fall
through to a later policy.

### Deployment

A user-facing convenience concept representing a request to deploy something.
It is a thin aggregate that owns or references one [fulfillment](#fulfillment) and exposes a
simple edit and status API. A deployment is not the kernel's orchestration
primitive: the fulfillment owns the strategy versions, target state, and
reconciliation state.

### Evidence log

The common append-only log of accepted [`TypedEvidence`](#typed-evidence)
identities. It addresses the threat of a compromised [resource manager](#resource-manager-rm)
rewriting a [verifier](#verifier)'s accepted history: replaying the same evidence as new,
omitting an earlier assertion, presenting a rollback, or assigning inconsistent positions.
The resource manager assigns each evidence identity one canonical position, at most once,
and delivers inclusion and consistency proofs to [targets](#target), which retain checkpoints.
This provides ordering, inclusion, append-only consistency, and local rollback protection.
It does not prevent the resource manager from withholding evidence or causing denial of
service, and appending evidence does **not** by itself authorize its content.

Possible implementations include an RFC 6962-style Merkle tree with signed
checkpoints, an externally witnessed transparency log, or a signed hash-chain
log with durable verifier checkpoints. These choices provide different
inclusion, witnessing, and fork-detection properties.

### `fleetctl`

The command-line client for [OCM Fleet](#ocm-fleet). It authenticates users,
invokes resource-manager APIs, and can provide operator workflows without
being part of the [resource manager server](#resource-manager-rm).

### Extension

An [add-on](#add-on-addon)-owned addition to an OCM Fleet API or capability
surface. Most commonly, an extension defines a typed extension resource and its API package,
which can represent, manage, inventory, or provide delivery connectivity for
a platform resource. An extension adds domain-specific behavior while using
the platform's common identity, authorization, and orchestration contracts.

Examples include a `clusters` managed-resource API, an inventory schema for
Kubernetes objects, a delivery capability for a `kubernetes` target type, or
an add-on-provided UI plugin.

### Fleetlet

A canonical component that connects local add-on processes with the [platform](#platform)
through channels. It provides transport and shared cross-cutting services,
including application authorization and provenance verification for delivery.
It centralizes connection lifecycle, authentication handoff, reconnection,
backpressure, routing, and channel access control. Add-ons provide domain- and
resource-specific behavior such as manifest generation, constraint enforcement,
and resource application; the fleetlet does not run that business logic.
Delivery, status, access, control, and index traffic can all use fleetlet
channels.

For example, a fleetlet running beside a target-side delivery agent maintains
one outbound platform connection while carrying delivery and status channels.
Multiple deployments are [fleetlet instances](#fleetlet-instance), potentially
connected to different add-ons.

### Fleetlet instance

A deployment of the canonical [fleetlet](#fleetlet), potentially connected to a
particular set of local add-ons and a different platform endpoint.

### Fulfillment

The internal kernel unit of orchestration and reconciliation. A fulfillment
owns the manifest, placement, and rollout strategies; resolved target state;
delivery authorization context; lifecycle state; and generation tracking. It
executes the common resolve → delta → plan → generate → deliver pipeline.
User-facing concepts such as deployments and [managed resources](#managed-resource) drive a
fulfillment; users do not edit a fulfillment directly.

For example, a deployment that selects three Kubernetes targets has one
fulfillment whose placement resolves those targets and whose manifest and
rollout strategies drive delivery. A managed-cluster resource can likewise
own a fulfillment that targets a provisioning add-on.

## G-M
### Generation

A monotonically advancing version of a [fulfillment](#fulfillment)'s effective intent or
configuration. It fences stale deliveries and lets the platform distinguish
the generation being reconciled from the generation already observed or
accepted by a target.

### Manifest

The declarative payload produced for a [target](#target). The term is generic: it may be
Kubernetes YAML, an OCM Fleet API object, an add-on configuration document, or
a provisioning specification. A manifest strategy determines what payload to
generate, potentially using target information for per-target customization.

### Management plane

The APIs, services, and user interfaces through which a [platform instance](#platform-instance)
represents, observes, configures, and governs [targets](#target) and coordinates
their lifecycle. It includes the [resource manager server](#resource-manager-rm)
and management capabilities supplied by participating [add-ons](#add-on-addon).
It is the operational control layer, distinct from the target environments where
delivered workloads run.

### Managed resource

An [add-on](#add-on-addon)-driven, user-facing resource type such as a cluster,
VM, or monitoring stack. A managed resource has a typed API and normally owns a
[fulfillment](#fulfillment) for continuous reconciliation. It is distinct from
a platform resource: the managed resource is an [extension](#extension)'s domain
representation, while the [platform](#platform) resource provides canonical
cross-extension identity.

## O

### Open Cluster Management (OCM)

The community, project family, and multicluster ecosystem surrounding OCM.
When referring to a particular integration, name the relevant OCM component,
such as the ManifestWork controller or klusterlet.

### OCM Resource Manager

Former project name. Use [OCM Fleet](#ocm-fleet) instead. The names OME,
FleetShift, Resource Manager, and OCM Resource Manager referred to this same
project or its evolving resource-manager design; they are historical names,
not separate products in this glossary.

### OCM Fleet

The current name for the experimental OCM variant and project: the [resource manager server](#resource-manager-rm),
[console](#console), [`fleetctl`](#fleetctl), [fleetlet](#fleetlet), and
add-on/[extension](#extension) interfaces. Included core add-ons and extensions
should be identified explicitly; deployment participation alone does not imply
project membership.

The OCM Fleet architecture is a [resource manager architecture](#resource-manager-architecture).
Its logical centrality does not require one process, replica, or global
deployment. Choices such as fleetlet channels and add-on contracts belong to
OCM Fleet and are not requirements of every resource manager architecture.

Use the full project or component name when establishing context, then the
scoped short form. For example: “OCM Fleet includes a console. The console
calls the resource manager server.”

Qualify overloaded uses of **platform**: a Kubernetes distribution, the
platform API layer, and the `platform` target type have distinct meanings in
their own contexts. For architectural comparisons, name the mechanism being
compared, such as **OCM's ManifestWork controller** and **the resource
manager's delivery contract**.

## P

### Placement

The orchestration axis that determines where a [fulfillment](#fulfillment) should go. A
placement strategy resolves a candidate [target pool](#pool) into a target set:

```text
Resolve(pool) -> targets
```

Placement is declarative and is separate from manifest generation and rollout
sequencing.

Examples include selecting every target with `environment=production`, using
an explicit list of target IDs, or selecting a single local platform target.

### Placement decision

The concrete result of evaluating a [placement](#placement) strategy for a
[fulfillment](#fulfillment) and [generation](#generation): the [targets](#target)
selected from the candidate [pool](#pool), together with any
strategy-specific constraints or rationale. A placement decision is not
automatically authority. When placement must be independently trusted by a
target, an authorized add-on or service signs it as placement evidence.

### Principal

An externally authenticated identity represented as `(scheme, authority,
tenant partition?, subject)`. Successful credential or [provenance](#provenance) verification
produces a canonical principal. A subject string alone is not globally
unique; its scheme, authority, and applicable tenant partition are part of its
identity.

### Provenance

Durable cryptographic evidence that a [principal](#principal) authenticated exact,
purpose-typed content. Provenance answers who authenticated an assertion, what
bytes and purpose were covered, and whether the target's accepted trust state
recognizes the evidence. It can authenticate user intent, add-on output,
placement decisions, relations, or trust updates. Provenance does not replace
[resource manager](#resource-manager-rm) authorization or the credential used
to apply the payload.

### Provenance profile

A configured implementation of the common [provenance](#provenance) contract
for a specific verification mechanism. A profile defines how producers create
evidence, how the [resource manager](#resource-manager-rm) stores and assembles
it, and how [targets](#target) verify it to
produce a canonical principal, tenant partition, and content binding. It may
also define trust anchors, retained verifier state, and historical-verification
rules. Sigstore, TUF, and continuity/v3 are examples of profile families, not
interchangeable policy names.

### Provider

The operator and provider [tenant](#tenant) of an OCM Fleet [platform](#platform).
The provider controls platform-level concerns such as add-on installation,
topology, root trust, and tenant onboarding. Each platform has exactly one
provider tenant; consumer tenants operate within the provider's platform under
authorization isolation. A provider is not the same as an external identity
authority.

For example, a hosted infrastructure service can operate the provider tenant
while customer organizations use consumer tenants on the same platform.

### Pool

The candidate population against which [placement](#placement) resolves. A pool produces a
target set from nothing, while placement selects from that set. Depending on
the design, a pool may be a static or label-based grouping or a richer
operational resource with capacity, health, and expansion semantics.

### Platform

The complete deployed system or a particular [platform instance](#platform-instance),
including its clients, enabled integrations, agents, supporting services, and
[management plane](#management-plane). Its stateful center is the [resource
manager](#resource-manager-rm), but it also includes orchestration, APIs,
[fleetlets](#fleetlet), installed [add-ons](#add-on-addon), [targets](#target),
and their contracts. A platform can itself be a target of another platform.
Participation in a deployment does not imply project membership.

### Platform instance

A particular installation of the [platform](#platform), including its clients,
enabled [add-ons](#add-on-addon), agents, and supporting services. Use
**parent platform instance** and **child platform instance** for recursive
installations. Multiple resource-manager replicas can belong to one platform
instance.

## R–W

### Reconciliation

The process of comparing a [fulfillment](#fulfillment)'s current target state with its latest
desired intent and taking the delivery actions needed to converge them. Unlike Kubernetes controllers,
reconciliation uses [durable execution](https://temporal.io/blog/what-is-durable-execution) in
[resource manager server](#resource-manager-rm). A reconciliation pass resolves placement, computes
 a delta, plans rollout, generates payloads, delivers changes, and evaluates resulting status.

For example, a reconciliation pass may notice that a newly labeled target now
matches placement, generate its payload, deliver it, and update fulfillment
status.

### Resource manager (RM)

The **resource manager server**, the customer-facing, stateful service at the
center of an OCM Fleet platform. It authenticates and authorizes
API requests, stores desired state and evidence, coordinates orchestration,
and routes deliveries. The RM is the
platform's primary stateful authorization engine and a courier for provenance;
it is deliberately not the sole authority for end-to-end provenance or target
application.

Introduce **resource manager server** first; **resource manager** or **RM** can
then refer to that component within the same context.

### Resource manager architecture

An architectural approach organized around a logically central service
responsible for management APIs, authorization, persistent management state,
and orchestration. This project explores it with a dedicated [resource manager
server](#resource-manager-rm) owning domain APIs and database persistence
directly. The term is descriptive here and does not assume an industry-standard
taxonomy.

For example, a target-side delivery agent may verify a signed deployment
assertion before applying manifests, while the resource manager verifies the
same evidence when assembling the delivery.

### Rollout

The sequencing and pacing of delivery and removal actions across a [placement](#placement)
delta. A rollout strategy can deliver immediately or introduce batches,
approvals, delays, health gates, and other progression rules. Rollout answers
how and when changes occur, not what is generated or where it is placed.

### Target

A logical endpoint or location that participates in the delivery contract. A
target accepts a declarative payload, applies it, and reports health and
delivery status. It may also report inventory. A target can be a Kubernetes
cluster, VM, cloud project, child platform, or an add-on-defined backend; it
does not have to be a cluster or Kubernetes API server.

### Tenant

An authorization-isolated organizational boundary within a [platform](#platform).
Tenants contain [workspace](#workspace) trees and their resources. The [provider](#provider)
tenant additionally governs platform-level trust and installed abstractions;
consumer tenants use the shared platform API within their own authorization scope.

### Typed evidence

The immutable outer storage object for [provenance](#provenance) material:

```text
TypedEvidence = provenance type + media type + exact bytes
```

The outer type selects routing and profile semantics only after authenticated
policy selection. The inner assertion's purpose, such as `deployment/v1` or a
placement decision, is established by the selected profile and the [common
evaluator](#common-evaluator)'s semantic evaluation.

### UI plugin

A module loaded by the [console](#console) to provide UI capabilities. An
[add-on](#add-on-addon) may supply a UI plugin, while core and pre-installed
plugins may also exist.

### Verifier

A component that independently checks whether credentials, provenance, trust
state, and delivery constraints authorize an operation. A verifier uses the
authenticated [`AuthorityConfig`](#authorityconfig), invokes the applicable
[provenance profile](#provenance-profile), and passes the authenticated
assertions to the [common evaluator](#common-evaluator). A target-side verifier
is commonly part of the [delivery agent](#agent); verification must complete
before that agent applies the payload to a [target](#target). The [resource
manager](#resource-manager-rm) may also verify and assemble evidence, but a
target must not treat the resource manager's result as a substitute for its
own verification.

### Workspace

A tenant-scoped node in the hierarchical organizational resource tree. Users
receive access through workspace grants, [targets](#target) belong to
workspaces, and a [fulfillment](#fulfillment) may select only targets within
its authorized workspace scope.
