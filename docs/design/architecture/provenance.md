# Provenance And Trust Configuration

## What this doc covers

The trust machinery used to authenticate FleetShift delivery evidence:

- bootstrapped and subsequently updated authority configuration
- canonical principal identity and multi-tenant authority partitioning
- provenance profiles and deterministic profile selection
- the boundary between profile verification and common attestation semantics
- credential presentation and request signing where they intersect provenance
- verifier-owned profile state and the common append-only delivery log
- trust updates and historical verification
- the initial TUF, Sigstore, and continuity/v3 profiles

## When to read this

Read this when implementing or reviewing how a client creates provenance, how
the resource manager assembles evidence, how a target selects a verifier, or
how trust configuration changes without making the resource manager a trust
root.

## What is intentionally elsewhere

- The governing delivery threat model, time/space problem, security choices,
  and shared trust-anchor model: [security.md](../security.md)
- Live credential-presentation modes, request authentication, and
  `PausedAuth`: [authentication.md](../authentication.md)
- The target apply, acknowledgement, generation, and recovery contract:
  [target_delivery_contract.md](target_delivery_contract.md)
- OIDC enrollment, user-key continuity, authenticated history, and rotation
  cutoffs for the continuity/v3 profile: [trust_model_v3.md](../trust_model_v3.md)
- The provider, tenant, workspace, and resource-manager permission model:
  [tenancy_and_permissions.md](tenancy_and_permissions.md)
- Bootstrap and pivot for recursively provisioned platforms:
  [platform_hierarchy.md](platform_hierarchy.md)

## Related docs

- [architecture.md](../architecture.md)
- [security.md](../security.md)
- [core_model.md](core_model.md)
- [addon_integration.md](addon_integration.md)
- [authentication.md](../authentication.md)
- [trust_model_v3.md](../trust_model_v3.md)
- [Archived JWT and raw-key provenance](../archive/jwt_and_raw_key_provenance.md)
- [Hybrid attestation prototype](../../../poc/attestation/hybrid/README.md)
- [Sigstore/TUF bundle prototype](../../../poc/attestation/sigstore_tuf_bundle/README.md)

## Overview

FleetShift deliveries often outlive the credential or interactive session that
created them, and they cross a resource manager that the target should not have
to trust as the author of user or workload intent. The target therefore needs
durable cryptographic evidence answering three questions:

- Which principal authenticated an assertion?
- Which exact typed content and purpose did that assertion cover?
- Why does the target's accepted trust state recognize that principal and
  evidence now, including when the evidence is historical?

That durable evidence is **provenance**. A user may authenticate a deployment
intent, an addon may authenticate generated manifests, a placement service may
authenticate a target decision, and an administrator may authenticate a trust
update. The resource manager stores and couriers these independently
authenticated assertions so the target can verify them itself.

There is no single best cryptographic mechanism for every authority or
environment. Sigstore can bind an OIDC identity to a short-lived certificate
and Bundle; continuity/v3 can bind a principal to an authenticated key history
and delivery-log position; TUF can authenticate content published by a
repository role. They differ in signing workflow, trust anchors, retained
state, historical verification, availability, and compromise guarantees.

A **provenance profile** is a configured implementation of a common contract
across those mechanisms. The contract covers:

- client or publisher creation of evidence for exact typed content;
- RM storage and assembly of the evidence and profile-specific support
  material; and
- target verification against authenticated profile configuration and retained
  state, producing a canonical principal, tenant partition, and content
  binding.

Authenticated delivery policy selects an ordered set of acceptable profiles.
Once one profile verifies an assertion, the common attestation and delivery
machinery consumes the same authenticated meaning regardless of whether the
proof came from Sigstore, continuity/v3, TUF, or a future well-known profile.

```text
exact typed assertion
        |
        v
provenance-profile evidence
        |
        v
RM storage, assembly, and delivery
        |
        v
profile selected from authenticated policy and verified at the target
        |
        v
canonical principal + tenant + exact authenticated content
        |
        v
common attestation evaluation and delivery application
```

Provenance composes with live credential presentation and the resource
manager's primary authorization decision, but does not replace either. It also
stops at authenticated assertions: common attestation evaluation decides how
those assertions justify a delivery, and the target delivery contract applies
the resulting action.

## Security Objective And Threat Model

The broader delivery security model is defined in
[security.md](../security.md). This section states the narrower guarantee and
threat boundary of provenance verification.

The resource manager remains FleetShift's primary API and authorization
engine. It authenticates callers, evaluates stateful permissions, stores
intent and evidence, coordinates addons, builds deliveries, and routes them to
targets. Reproducing all of that policy at every target is not a goal.

This is still a substantial change from the usual management-plane boundary.
In many fleet and GitOps systems, the controller's accepted input and
privileged apply path share one trust boundary: after controller compromise,
the target cannot distinguish authentic user or workload intent from a
fabricated command. FleetShift instead carries content-bound assertions
through the management plane for independent verification at the target.

The provenance design therefore adds a narrower end-to-end guarantee:

> A resource-manager compromise cannot forge a user or workload's provenance,
> alter the content authenticated by that provenance, substitute trust relative
> to an established verifier's accepted state, or make an unauthorized trust
> transition self-authorizing.

This boundary applies established identity, signature, attestation,
continuity, transparency, and authenticated-update techniques to typed
management deliveries. The continuity/v3 baseline provides verifier-local
history and ordering, not trusted time or global fork detection. Profiles may
add a TSA to constrain backdating, require log or witness proofs for
acceptance, and use monitors or gossip to expose equivocation. Target-retained
state makes the RM a courier rather than the source of provenance.

The common baseline still permits a compromised resource manager to:

- bypass its own tenant, workspace, or resource authorization rules;
- route authentic evidence in ways ordinary service policy would reject;
- omit evidence, credentials, updates, or deliveries;
- delay work or cause `PausedAuth` and other denial of service;
- return incorrect query results unless a separate request-integrity mechanism
  covers them; and
- present different not-yet-pinned histories to different new verifiers.

With a more operationally intensive profile and target policy—using
independent authorities, trusted time, witnessed transparency, and
monitoring—the RM could approach an untrusted courier even for delivery
authorization; it would still control availability and remain trusted for any
stateful policy not reproduced at the target.

Only constraints deliberately retained in authenticated target trust state or
signed attestation content are expected to survive resource-manager
compromise. The controlled client, provenance implementation, delivery agent,
bootstrap path, configured external authorities, and target enforcement path
remain trusted for their stated roles. A direct target mutation path that
bypasses the delivery agent is outside this guarantee.

## Authenticated Trust Configuration

### Bootstrap and steady state

A verifier has two trust lifecycle states:

```text
uninitialized
    -> bootstrap installs the initial authenticated trust configuration

initialized
    -> every change is an authenticated trust-config-update delivery
```

Bootstrap or TOFU is permitted only for a genuinely uninitialized verifier.
The bootstrapped configuration contains the initial authority registry,
provider governance policy, delivery policies, profile configuration, and any
initial profile state required by the installed profiles.

An initialized verifier never silently returns to TOFU because it is offline,
has fallen behind a retention boundary, cannot contact the resource manager,
or has lost state. It fails closed or enters an explicit recovery procedure.
The normal update path is described in [Trust updates](#trust-updates).

Provider workload authorities use the same authority registry as other
principals, but their configuration remains governed by the bootstrapped
provider policy. A provider workload cannot authorize its own authority entry
or change the policy under which its evidence is accepted.

Provider users are not a separate identity class or trust structure. They are
ordinary principals under an authority whose authenticated policies grant the
appropriate provider-wide scope.

### Authority registry

Trust configuration is organized by canonical **principal authority**, not by
FleetShift tenant and not by a globally named trust-domain object.

Conceptually:

```text
AuthorityConfig {
    principal_authority: (scheme, authority)
    tenant_mapping
    credential_methods[]
    provenance_profiles[]
    delivery_policies[]
}
```

`scheme` is a well-known, versioned identity scheme whose implementation owns
canonical parsing and comparison. `authority` is the canonical external
namespace for subjects under that scheme. Examples include:

- exact OIDC `iss` for `oidc-sub/v1`;
- a canonical SPIFFE trust domain for a SPIFFE identity scheme;
- a trust-anchor SPKI digest for an X.509 identity with no more stable
  certificate-native namespace; or
- a repository authority for a TUF provenance profile.

Common code does not apply generic URL, distinguished-name, Unicode, or other
normalization to these values. It compares the canonical values produced by
the trusted scheme implementation.

An authority config contains the credential and provenance mechanisms allowed
to establish principals under that authority. Those mechanisms' anchors do
not become separate principal-authority axes. For example, an OIDC authority
may configure:

- direct JWT validation for live credential presentation;
- a Sigstore profile that trusts a Fulcio CA to encode identities from that
  OIDC issuer; and
- a continuity/v3 profile whose accepted enrollment history originates from
  the same issuer.

The Fulcio CA and v3 map are trust anchors for verifying an OIDC principal;
they do not replace the OIDC issuer as that principal's authority. For an
X.509-native workload identity, by contrast, the certificate-native namespace
or CA-key digest may itself be the principal authority.

Credential methods under the authority describe validation of live JWTs,
X.509 credentials, and request-signing key bindings independently from the
provenance profiles used for durable evidence.

One authority config may serve one FleetShift tenant or many tenant
partitions. A provider-operated multi-tenant IdP or CA therefore does not
require every tenant to duplicate the same trust configuration.

### Canonical principals and tenant mapping

Successful verification yields a canonical principal:

```text
Principal = (scheme, authority, tenant_partition?, subject)
```

The optional external tenant partition is scoped to its own `(scheme,
authority)`. A value such as `acme` from one issuer has no relationship to the
same string from another issuer. Equal subject strings under different
authorities or tenant partitions identify different principals.

An authority config defines at most one applicable tenant-mapping rule for the
verified credential or provenance form. Supported rule shapes include:

- an authenticated JWT claim;
- an authenticated X.509 OID or SAN component;
- a tenant value proven through continuity/v3 authenticated state; or
- a static FleetShift tenant for a single-tenant authority.

The rule runs on verified material, not on an RM-supplied label. It maps the
external tenant partition to a FleetShift tenant ID. Zero mappings and
ambiguous mappings fail closed. Any tenant claimed in the delivery or by the
resource manager must exactly match the verified result.

Tenant mapping isolates principals sharing one authority. It does not move the
resource manager's complete tenant/workspace authorization state into the
target. The resource manager remains responsible for ordinary permission
evaluation after authentication produces the same canonical principal.

### Well-known provenance types

Every provenance profile selects a well-known, versioned provenance type.
Implementations arrive through the client's, resource manager's, and
verifier's trusted software supply chains. Authenticated configuration selects
and constrains installed implementations; it never supplies executable code.
An unknown provenance type fails closed.

A provenance type owns:

- its evidence and support-material encodings;
- cryptographic verification and trust-anchor interpretation;
- principal derivation under its configured authority;
- any profile-specific retained state;
- historical-verification behavior; and
- any type-specific lifecycle API.

The provenance type also defines its permitted media types. Delivery policy
does not need a second generic media-type allowlist. If a future profile needs
to prohibit a representation otherwise supported by a type, it can introduce
a typed constraint or a stricter provenance-type version.

Profile configuration is an authenticated entry inside an `AuthorityConfig`.
It is not named by an RM-maintained or client-visible profile ID. An
implementation may derive local storage keys or configuration references for
profile state, but package data cannot use such a key to grant authority or
select code.

## Delivery Policy And Profile Selection

Each authority defines deterministic delivery policies. A policy matches
bounded delivery context such as:

- verified tenant partition;
- delivery content type;
- root authorization versus supporting graph evidence; and
- other explicitly defined scope constraints.

A matched policy states:

- whether live credential presentation is required or allowed;
- whether provenance is required or allowed;
- constraints on the principal, content, freshness, or configured mechanism;
  and
- an ordered `any-of` list of provenance profiles.

Profile combination has only `any-of` semantics in this design. Thresholds,
principal diversity, and requirements for several verification paths are not
part of profile selection.

### Selection algorithm

Evidence is not authoritative until verification completes. Selection follows
this sequence for each graph assertion:

1. Parse the untrusted provenance type and enough type-specific material to
   obtain a tentative principal scheme, authority, and optional tenant hint.
2. Use the tentative `(scheme, authority)` only to locate the corresponding
   authenticated `AuthorityConfig`. Missing configuration fails.
3. Use delivery context and tentative fields to locate a candidate delivery
   policy. Policy matching must resolve unambiguously.
4. Filter that policy's ordered profile list to the evidence's well-known
   provenance type.
5. Try matching profiles in authenticated policy order. The first profile that
   fully verifies the evidence and its constraints is selected.
6. Derive the canonical principal and verified tenant mapping from the
   authenticated result, then re-evaluate the policy and every supplied hint.
   Any mismatch fails.

Trying the next configured profile after a failure is deliberate `any-of`
policy, not evidence-controlled downgrade. The evidence and RM may narrow the
search with hints, but they cannot introduce a profile, reorder the policy, or
change a profile's anchors or constraints.

Supporting addon, workload, placement, or relation evidence may authenticate
under a different principal authority from the root authorization. The common
attestation graph and authenticated addon relationships determine whether that
evidence is relevant; success under some installed profile is not enough by
itself.

## Evidence And Attestation Semantics

### Profile-owned proof, common meaning

The common unit of provenance is one exact, purpose-typed assertion
authenticated by one provenance profile. A profile proves the assertion's
canonical bytes or digest; it does not interpret what the assertion means in
the larger delivery graph.

Examples include:

- a user's delivery authorization;
- an addon's exact manifest set;
- a placement decision;
- a fulfillment relation;
- a signed update or derivation; and
- a trust-configuration update.

Sigstore may carry an assertion in a DSSE/in-toto Sigstore Bundle. Continuity/v3
may carry a signature plus authenticated history and delivery-log proofs. TUF
may authenticate the assertion as a repository target. Those representations
remain profile-owned.

Successful profile verification produces a deliberately small common result:

```text
AuthenticatedEvidence {
    principal
    mapped_fleetshift_tenant?
    content_type
    content_digest
    provenance_type
    authority_config_digest
    profile_config_digest
    satisfied_constraints
}
```

The configuration digests bind the result to the exact authenticated policy
and anchors used during verification; they are audit and state-precondition
data, not profile selectors. `satisfied_constraints` contains only outcomes
defined by the matched authenticated policy. Profile-specific attributes may
influence authorization through typed constraints that define how those
attributes are validated.

Profile-specific certificate, timestamp, map, history, repository-role, or
transparency details may remain typed audit output. Generic delivery machinery
does not consume an unbounded universal facts map.

### Independent assertions

Each user, addon, or workload authenticates its own assertion. The RM retains
and couriers the exact evidence bytes; it does not translate them, replace
their authorship, or create a signature that speaks for the aggregate.

An aggregate evidence package is therefore not a new provenance authority.
It is a collection assembled by the RM so the target can evaluate one delivery.
No evidence author is assumed to have seen evidence created later by another
author.

Purpose is part of verification. A manifest assertion cannot be reinterpreted
as placement, a derivation cannot be interpreted as deployable manifests, and
a trust update cannot be interpreted as ordinary content merely because the
same profile could verify its bytes.

### Common attestation graph

The attestation graph remains shared, profile-neutral trust machinery. It owns:

- root-input selection;
- canonical statement and content digests;
- subject and predicate binding;
- derived-input recipes and preconditions;
- addon manifest, placement, and fulfillment relationships;
- constraint evaluation; and
- the relationship between verified intent and one concrete delivery action.

Graph selectors, indexes, and outer references locate evidence but confer no
authority. Every accepted relationship must appear in authenticated content or
follow from a deterministic transformation checked by the common evaluator.
Changing an index or reference cannot substitute a different principal,
content item, target, fulfillment, or generation.

The concrete put or removal action comes from the target delivery contract; it
is not duplicated as a second authoritative graph output. Outer tenant,
target, fulfillment, generation, and routing values are compared with signed
or locally authenticated facts and are never authoritative alone.

Unused evidence cannot satisfy a graph requirement, endorsement, or
constraint. A bounded unused item may be ignored rather than turning an
otherwise valid delivery into an outage. Package structure, item digests,
counts, byte sizes, graph depth, and proof work remain bounded and
integrity-checked before untrusted input can cause unbounded work.

### Intent and output authentication

An authenticated root intent may describe desired state rather than the final
target payload. For example, a user can authenticate a deployment spec and its
manifest and placement strategies before an addon renders manifests or a
placement service selects targets. The common evaluator checks that every
derived output is permitted by the authenticated intent and supporting
assertions.

This is the normal **intent-authentication** path. It avoids requiring a new
user interaction whenever deterministic or separately authenticated output is
regenerated. Its guarantee depends on the constraints relating intent to
output: an unconstrained opaque derivation would give the RM or addon more
authority than an exact binding.

For higher-assurance, lower-churn operations, an assertion may instead
authenticate the exact manifests, target decision, or other concrete output.
That removes the corresponding derivation freedom but requires new evidence
whenever those bytes change. Intent and exact-output assertions can also
compose: the root authenticates which addon or derivation is allowed, while a
supporting assertion authenticates the exact result.

Provenance profiles only establish the assertions. The common graph determines
whether their subjects, purposes, and relationships justify the one delivery
action.

### Derived inputs and update chains

An ordinary content update may be authenticated directly as a new root intent
or derived from an earlier authenticated input. A derived input contains or
references:

- the exact prior input;
- an independently authenticated update assertion;
- a deterministic transformation or well-known update operation;
- preconditions over the prior content; and
- any constraints governing the new result.

The evaluator verifies the prior input and update assertion independently,
checks their subject and fulfillment relationship, evaluates the preconditions,
applies the deterministic transformation, and confirms that protected identity
or scope fields were not rewritten. The resulting content digest must equal the
digest used by later graph relationships and delivery.

Constraints apply at the layer whose output they govern. A prior input's
constraint is not blindly copied onto every descendant, and a later update
cannot erase the authenticated relationship needed to justify its own input.
Strategy-implied constraints are derived from the final applicable strategy.
Chain depth and cumulative transformation work are bounded.

This derivation mechanism concerns application content. A
`trust-config-update/v1` is likewise ordinary authenticated content, but its
predecessor and successor checks are defined separately in
[Trust Updates](#trust-updates).

### Strategy, placement, and removal constraints

Known strategy types contribute deterministic constraints from trusted
verifier code. Unknown strategy types fail closed. Common examples are:

- **Inline manifests:** delivered manifests must exactly match the authenticated
  manifest content.
- **Addon manifests:** the root intent identifies the permitted addon or addon
  relationship, and a separate assertion from that addon authenticates the
  exact manifest set.
- **Predicate placement:** a put is allowed only when the predicate matches
  target-local authenticated identity or labels; removal is allowed only when
  it no longer matches.
- **Addon placement:** a separately authenticated placement decision must bind
  the applicable fulfillment and target set; the put or removal must agree with
  that decision.

An addon signature alone never authorizes its output for arbitrary tenant
content. The graph must connect the addon's authenticated identity and exact
output to a root intent or authenticated addon registration that permits that
relationship. Optional structural constraints—such as allowed resource types,
namespaces, or label requirements—can further limit correctly signed but
unexpected addon output.

Removal is authenticated as rigorously as put. The action's fulfillment must
match the enclosing delivery and the owning authenticated intent. An
owner-resource ID, RM-selected graph edge, or unrelated placement assertion
cannot substitute for that fulfillment relationship.

### Generation and replay checks

When an operation requires exact compare-and-swap semantics, its authenticated
content includes an expected fulfillment generation. The target compares that
value with target-local state before applying the concrete action. Directly
authenticated and derived updates advance the same lineage; an old assertion
cannot be replayed over a newer generation merely because its signature still
verifies.

Dynamic-scope assertions may intentionally omit one concrete generation when
they authorize a standing or selector-based relationship. The applicable
content type and constraints must define whether authorization is standing,
once per target, or snapshot-scoped. The common delivery log supplies durable
ordering and rollback resistance but does not silently turn a reusable
assertion into single-use authorization.

The [hybrid prototype](../../../poc/attestation/hybrid/README.md) exercises
these graph, derivation, placement, removal, and constraint semantics using an
older raw-key evidence representation and some deployment-shaped field names.
A provenance profile neither replaces nor branches the common semantics.

### Shared-authority delivery example

Consider a provider-operated OIDC issuer and Fulcio CA serving several managed
service tenants. The authority config is keyed by the OIDC issuer and maps a
verified `tenant` claim to a FleetShift tenant. Its root-delivery policy allows
an ordered set of Sigstore profiles configured with the shared Fulcio CA.

Alice signs tenant A's delivery authorization through Sigstore. The RM claims
that the request is for tenant A and attaches Alice's Bundle. That claim only
locates the authority config. The target verifies the Fulcio chain and OIDC
identity extensions, obtains Alice's canonical OIDC principal and tenant
partition, maps that partition to tenant A, and rechecks the root policy.

The signed intent names a provider addon as its manifest strategy. That addon
signs the exact manifests using its provider workload authority, such as a
SPIFFE identity under a provider trust domain. The target verifies this
supporting assertion through the provider authority config, then the common
attestation evaluator checks that the signed intent and authenticated addon
registration permit that exact addon and manifest set.

The shared IdP and CA need one authority configuration rather than one copy per
tenant. The verified tenant partition prevents Alice's evidence from being
replayed into tenant B, and the separate provider workload authority prevents
the RM or a tenant user from manufacturing addon evidence.

## Credential Presentation And Request Signing

Credential presentation is associated with the root authorization by default.
Supporting historical or addon assertions ordinarily carry durable provenance,
not live bearer credentials.

The credential modes, target exchanges, secret-lifetime concerns, and
`PausedAuth` workflow are defined in
[authentication.md](../authentication.md). This section defines only their
intersection with provenance selection and verification.

A delivery policy may allow:

- credential-only root authorization;
- provenance-only root authorization; or
- both credential presentation and provenance.

When both are required, their root-authorizing identities must produce the
same canonical principal and tenant partition. A credential cannot match an
unrelated supporting addon or workload signature.

Credential-only acceptance explicitly delegates the credential's effective
scope to its holder. A bearer token does not prove authorization of exact
delivery content unless another mechanism binds it. This provides less
protection against a compromised courier than durable provenance, but remains
a supported policy choice.

### Request signing

Request signing is credential presentation, not provenance. With an
authenticated binding between a principal and request-signing key, a
short-lived signature can sender-constrain an otherwise bearer credential
without requiring IdP-issued proof-of-possession support.

The signature covers the relevant request components, credential binding,
freshness data, audience or destination, and body or intent digest as
applicable. Verification reuses authority lookup, credential/key validation,
canonical identity, tenant mapping, and policy constraints.

A request signature authorizes only the live root request. It is not durable
historical evidence and cannot authenticate supporting graph assertions. The
same mechanism can protect operations that have no delivery or provenance,
such as read-only queries.

Request signing is owned by a typed credential method. The exact
request-signature representation, freshness mechanism, and non-IdP
key-binding ceremony are credential-method-specific. An implementation may
share key or verification machinery with a provenance profile, but the
provenance profile does not become the protocol owner merely because
machinery is shared.

## Component Responsibilities

### Client

The client produces canonical typed assertions, presents live credentials, and
uses the selected provenance implementation to create evidence. Common client
code identifies the allowed provenance type and principal authority; it does
not obtain an RM-maintained profile or anchor ID.

The profile-specific path owns its signing ceremony. Depending on the profile,
that may include a device key, operating-system key handle, OIDC redirect,
Fulcio issuance, repository publication, or collection of proof material. A
client or addon receives purpose-specific signing operations over known
content, not unrestricted access to private key bytes or a general signing
oracle.

### Resource manager

The RM:

- authenticates the request and performs primary stateful authorization;
- verifies any request credential and provenance needed for its own decision;
- requires credential and provenance identities to agree when policy composes
  them;
- stores original immutable evidence and content-addressed support objects;
- records resource intent and history;
- invokes profile-specific work needed by the mutation's durable transaction;
- commits durable deliveries to the common delivery log;
- assembles the attestation graph and replaceable verification material for
  the target's retained state; and
- routes the resulting package to the target.

RM verification is authoritative for whether the RM accepts an API request.
It is not a substitute for target verification. The RM may repeat target-like
checks before dispatch to catch errors early, but this is an optional local
optimization and has no protocol authority.

The RM is a provenance courier even though it remains the primary authorization
engine. It cannot alter authenticated content, create a missing assertion,
choose a profile outside authenticated target policy, or make unverified proof
material satisfy target-retained constraints.

Acceptance of an RM mutation must durably couple the resource change, original
evidence, resource history, and required delivery-log or profile work. A
database transaction, transactional outbox, or equivalent durable workflow may
provide that boundary. Profile-specific code participates in the operation but
does not own or bypass the RM's transaction and authorization rules.

### Target

The target or delivery agent:

- begins from bootstrapped and subsequently authenticated trust state;
- parses hints only to locate authenticated authority configuration;
- verifies credentials and provenance under matched delivery policy;
- validates the attestation graph and target-local constraints;
- applies authenticated content through profile-neutral delivery machinery;
- retains profile and delivery-log state required by its accepted profiles; and
- acknowledges only after it has durably guaranteed safe progress or retry.

The boundary is conceptual rather than a required pair of protocol methods:

```text
profile verification
    -> authenticated principal, tenant, and exact typed content

common attestation evaluation
    -> authenticated concrete delivery action

delivery handling
    -> target effect and durable progress
```

Delivery handling does not reparse raw credentials, signatures, authority
claims, or profile-specific proofs.

## Authority And Profile Lifecycle Operations

Some provenance types and credential methods need FleetShift-mediated
lifecycle operations; others do not. These APIs remain strongly typed per
mechanism rather than being forced into a universal `RegisterKey` or opaque
control payload.

Examples:

- continuity/v3 defines enrollment, key rotation, recovery, and tombstoning;
- an OIDC request-signing credential method may register and renew an
  IdP-authenticated public-key binding;
- adapted TUF may define RM-mediated role/key initialization, metadata
  publication, or rotation when bootstrap does not wholly establish them; and
- Fulcio keyless normally obtains a short-lived certificate externally during
  signing and needs no FleetShift key-registration operation.

Common FleetShift API conventions still apply: authenticate the caller,
perform RM authorization, route through authenticated authority config, use
idempotency keys where needed, and integrate with durable workflows and the
delivery log where ordering has security meaning. The provenance type or
credential method owns its operation schema, validation, and state transition.

## Retained State And Delivery Ordering

### Profile-owned verifier state

A provenance profile may require retained verifier state, such as:

- a continuity/v3 authenticated map root and bounded exception index;
- TUF updater metadata and repository root state; or
- a future transparency-log or witness checkpoint.

The state is associated with the authenticated authority/profile
configuration, never with an RM-supplied profile ID.

Profile verification may safely advance independently authenticated state
during verification, or it may return an opaque pending transition to be
persisted later. The profile defines which transitions are independently
meaningful and which depend on acceptance of the current delivery.

The common requirement is crash-consistent, idempotent durable progression:

- before acknowledgement, the target has persisted enough verifier, delivery,
  or pending-work state to recover or retry safely;
- partially persisted state cannot make unverified content authoritative;
- an older or unverified delivery cannot overwrite newer accepted state; and
- retry after any crash converges without duplicating the effect.

This does not require profile state, the delivery-log checkpoint, target
generation, and external apply effects to share one isolation transaction.
Transactions, ordered idempotent writes, compare-and-swap, leases, generation
fences, and durable workflow records are all valid implementations when they
preserve the invariants above.

### Common append-only delivery log

Every durable mutation is committed to a common append-only log before
dispatch. A delivery commitment binds the exact typed delivery, fulfillment,
target, generation, attestation graph, immutable evidence set, and applicable
authenticated configuration context. Trust updates and profile-specific
ordering records use purpose-separated commitment types in the same ordering
infrastructure.

The log supports:

- inclusion of the exact durable mutation;
- append-only consistency from a target's retained checkpoint;
- catch-up without replaying unrelated delivery bodies;
- idempotent retry and recovery from lost acknowledgements;
- local rollback protection for established targets; and
- ordering-sensitive profile rules such as v3 rotation cutoffs.

Refreshable Merkle proofs, profile proof paths, and other reconstructable
support material need not be part of the immutable delivery commitment. The
profile cross-checks that refreshed material against the committed evidence,
accepted configuration, and retained checkpoints.

Appending a record does not authorize its content. The log orders commitments;
the applicable credential, provenance profile, graph, constraints, and target
checks establish authority. An inert or invalid profile-control marker remains
inert.

The log provides protocol order, not trusted wall-clock time, public
transparency, or global fork detection. A timestamp authority, witnesses, or
gossip may extend those guarantees later. Ordinary live queries need not be
logged merely because they use request signing.

Log scope, compaction, permanent-rejection progression, and optional TSA
integration remain open design work. An established target that cannot extend
its retained checkpoint fails closed rather than restarting from TOFU.

## Trust Updates

### Trust update as ordinary content

`trust-config-update/v1` is a well-known delivery content type, not a separate
cryptographic universe. Its canonical content identifies the configuration
scope and binds:

- the complete successor configuration bytes;
- the accepted predecessor configuration digest; and
- the successor configuration digest.

It traverses the same credential, provenance-profile, attestation, ordering,
and delivery machinery as other typed content. The delivery policy in the
predecessor configuration decides which profiles and credentials may
authenticate it. Neither the RM, an evidence hint, nor the successor
configuration chooses how the transition is authorized.

After provenance verification, the profile-independent trust-update handler
checks:

- exact predecessor matching and consecutive catch-up;
- update scope and tenant-partition authority;
- successor schema, policy, and profile validity;
- delivery-log ordering where applicable; and
- consistency between successor bytes and the committed digest.

It then installs the successor configuration and required profile state using
a crash-consistent, idempotent durable transition. Safe non-authorizing
checkpoints may advance earlier, but the successor configuration cannot become
usable before its authorization and state dependencies hold.

There is no separate `TrustUpdatePolicy`: ordinary authority delivery policies
match the `trust-config-update/v1` content type and its scope. They may require
durable provenance, a live credential, a request signature, or a supported
composition of them.

### Update authority

The predecessor authorizes its successor, including changes to:

- delivery policies and tenant mappings;
- allowed credentials and provenance profiles;
- trust anchors and profile parameters;
- profile lifecycle or update authority; and
- historical-verification constraints.

A newly introduced profile cannot authorize its own installation. Adding a new
principal authority requires an already accepted authority whose predecessor
policy grants authority-registry update scope. A principal in one tenant
partition cannot modify shared authority-wide policy unless the predecessor
explicitly grants that broader scope.

Provider workload entries remain governed by bootstrapped provider policy; a
workload cannot make its own key, issuer, or profile authoritative merely by
signing a candidate update.

A delivery may carry a non-authoritative digest or state requirement indicating
the trust state against which the RM assembled it. This is a catch-up signal,
not permission to use stale policy. A target with newer authenticated state
uses current revocations and constraints; a target missing required state
requests or waits for authenticated updates rather than accepting supplied
trust bytes at face value.

No separate security-significant revision counter is required. Predecessor
digests and delivery-log positions establish transition ordering.

### Profiles used for trust updates

TUF, Sigstore, and continuity/v3 are peers at the trust-update boundary:

| Profile       | How it authenticates `trust-config-update/v1`                             | Profile-owned state and constraints                                                                     |
| ------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| TUF           | Publishes the exact canonical trust-update content as a repository target | Retained TUF root/metadata; configured role, target path, version, and freshness checks                 |
| Sigstore      | Signs the exact canonical content in a Sigstore Bundle                    | Configured Fulcio CA, OIDC issuer constraints, trusted TSA/time, and optional transparency requirements |
| Continuity/v3 | Signs the exact canonical content with an authenticated continuity state  | Current map/history checkpoint, delivery-log branch, key-event semantics, and rotation cutoffs          |

TUF is especially suitable for low-churn trust updates, but it is neither
mandatory nor coupled to Sigstore. A policy may also admit TUF for other
delivery content when repository-publisher authority has the desired meaning.

Sigstore and continuity/v3 can authorize trust updates without TUF. Permission
to authenticate ordinary content never implies permission to update trust;
the predecessor delivery policy must allow the profile, principal, content
type, and update scope.

Adapted TUF administration may begin entirely in bootstrap or may expose
strongly typed RM-mediated APIs for key/role initialization, target publication,
or rotation. That publishing-side choice does not change target verification:
the target accepts only repository evidence chained from its retained TUF state
and admitted by predecessor policy.

## Historical Verification

Current authenticated authority configuration controls whether historical
evidence remains admissible. The baseline supports two retention shapes:

1. A current profile retains the old anchors, states, and cutoffs it needs to
   verify historical evidence.
2. The authority config retains older profiles in its ordered `any-of` list,
   constrained for their permitted historical interval and purpose.

The second shape lets a CA, issuer, or mechanism change without inventing a
generic proof accumulator for profile configuration. An old retained profile
is not automatically valid for fresh evidence: trusted signing-time windows,
TSA or delivery-log cutoffs, content constraints, and current distrust policy
limit its use.

The RM remains a courier for historical objects:

- Sigstore evidence carries or references the historical certificate and
  timestamp material; current authenticated configuration retains the CA/TSA
  roots and trusted-time constraints needed to verify it.
- Continuity/v3 retains a compact current checkpoint that commits accepted
  history. The RM supplies historical event bodies, the immediate successor
  when needed, and current-branch map/history/log proofs. Those couriered
  objects gain authority only by verifying against the retained checkpoint.
- TUF retains the repository state and target metadata required by its
  configured historical policy, or the authority retains an older constrained
  TUF profile.

Historical verification answers whether evidence was authentic under an
allowed historical state. It does not grant standing permission to act now.
Current delivery policy, revocation, distrust, content scope, and cutoff rules
still govern current use. Conversely, a later policy change does not rewrite a
delivery that was already durably accepted and applied.

If the RM omits an event body, old Bundle, retained target, or refreshable
proof, verification pauses or fails closed. Missing historical material is
denial of service, never permission to switch to an unrelated profile or
authority.

## Profile Mappings And Guarantees

Profiles share a common verification boundary and API conventions, not
identical security guarantees.

### Continuity/v3

The continuity/v3 profile uses ordinary OIDC for initial identity enrollment,
then user-controlled continuity, device, and session keys for durable
provenance. An authenticated map commits each principal's history head, while
the common delivery log orders deliveries against rotation markers.

An established verifier retains a compact map/history checkpoint and local
delivery-log checkpoint. The RM constructs selective proofs for the exact
signing state used by an assertion. The profile protects that verifier from
key substitution, accepted-prefix rollback, and movement of an already pinned
rotation cutoff.

The guarantee is local to each verifier's retained branch. Without witnesses
or gossip, the RM can withhold transitions and keep another verifier on a
stale branch. V3 supplies logical delivery ordering but no trusted wall-clock
time.

### Sigstore

The initial Sigstore profile authenticates a standard Bundle over the exact
FleetShift assertion. A configured Fulcio CA authenticates the certificate and
its OIDC identity extensions. A configured RFC 3161 TSA supplies trusted
signing time for historical validation of the short-lived certificate.

The authority config is keyed by the OIDC principal authority. The Fulcio CA,
TSA, and any Rekor, CT, or witness configuration remain profile anchors and
constraints, not alternative principal authorities.

A logless Sigstore profile with trusted timestamps provides no public detection
of Fulcio misissuance or equivocation. Adding Rekor, CT, or witnesses changes
the profile's guarantee and retained state. The well-known profile type or its
authenticated parameters must make that distinction explicit.

### TUF

The TUF profile authenticates exact content as a repository target using
retained TUF root and metadata state. It verifies standard root rotation,
threshold, version, expiry, snapshot, target-hash, rollback, and freeze rules
applicable to the configured profile.

Its canonical principal identifies the configured repository authority and
the authenticated role that published the target. Individual threshold-key
holders are profile-specific audit detail unless a future policy explicitly
needs signer-level identity.

For trust updates, policy constrains the repository principal, delegated role,
target path or namespace, and update scope. The authenticated target is the
canonical trust-update content itself, not merely successor bytes and not a
second transition object.

TUF is a provenance mechanism for repository publication. It does not become a
high-frequency identity database, and using it does not require Sigstore or a
separately deployed repository service. The RM may store and courier the
metadata and targets while repository signing authority remains external to
the RM's ordinary database authority.

## Failure And Recovery

Failures are classified at the layer that detects them:

- unknown authority, provenance type, or configuration;
- no applicable or successful profile in the ordered policy list;
- invalid credential, request signature, provenance, tenant mapping, or
  identity agreement;
- missing or invalid profile state and historical material;
- invalid graph relationship, constraint, placement, removal, or generation;
- delivery-log inconsistency or retained-state rollback; and
- target apply or durable-progress failure after successful authentication.

Missing, expired, or temporarily unavailable authorization material normally
causes `PausedAuth` when a user can provide fresh authorization or credentials.
Malformed or contradictory evidence is rejected. Neither outcome causes
fallback to TOFU, an unrelated authority, or a profile outside the matched
policy.

Verification explanations should identify the failed layer without exposing
credentials, private material, or other secrets. Profile-specific errors are
nested beneath the common authority, profile-selection, attestation, and
delivery explanation.

Recovery preserves previously accepted trust. A target may accept an explicit
out-of-band or currently authorized recovery transition, but state loss alone
never makes it an uninitialized verifier.

## Security Invariants

1. **Bootstrapped authority:** only bootstrap initializes trust; every normal
   change chains from authenticated predecessor configuration.
2. **Known implementations:** authenticated config selects well-known,
   trusted-code provenance types and cannot install verifier code.
3. **Tentative hints:** provenance type, authority, tenant, subject,
   certificate, key, and repository labels are untrusted until verification.
4. **Canonical identity:** authorization compares the complete canonical
   principal and verified tenant mapping; equal strings in other authorities
   or partitions do not merge identities.
5. **Policy-owned selection:** one unambiguous delivery policy supplies the
   ordered `any-of` profile list; the first complete success wins.
6. **Exact content:** a profile authenticates the exact typed content and
   purpose consumed by the common evaluator.
7. **Independent authority:** one evidence author, aggregate, graph selector,
   or RM operation cannot speak for another evidence author.
8. **Authenticated relationships:** graph edges and indexes locate evidence;
   signed content or deterministic verification establishes relationships.
9. **Root credentials:** live credentials and request signatures apply to the
   root authorization unless a future signed relationship explicitly says
   otherwise.
10. **Primary RM authorization:** provenance does not claim to reproduce all
    resource-manager permission policy at the target.
11. **Courier limitation:** RM preverification, storage, routing, and proof
    construction do not replace target verification or grant provenance.
12. **Predecessor-controlled trust:** successor config, new authorities, and
    new profiles cannot select or authorize their own installation.
13. **Current historical policy:** old anchors or profiles are usable only
    where current authenticated constraints admit their historical interval.
14. **Append-only continuity:** established targets extend retained delivery
    and profile checkpoints or fail closed; they do not roll back or rebootstrap.
15. **Crash-safe progression:** acknowledgement follows durable safe progress;
    partial state cannot authorize unverified content or apply an older
    delivery over a newer one.
16. **Bounded verification:** package size, evidence count, graph depth,
    indexes, proof sizes, and profile-specific work have enforced limits.

## Validation

Common protocol tests apply to every provenance implementation:

- shared and single-tenant authorities, static and claim-derived tenant
  mappings, identical subjects in different partitions, and cross-tenant
  replay rejection;
- missing, forged, cross-authority, overlapping, and ambiguous authority or
  tenant claims;
- ordered `any-of` profile selection, overlapping candidates, deterministic
  first success, and rejection when no allowed profile succeeds;
- credential-only, provenance-only, composed, bearer-only, and request-signed
  root authorization, including identity mismatch, expiry, replay, and
  rejection of request signatures as supporting provenance;
- immutable independent evidence, purpose/type confusion, graph/reference
  retargeting, supporting evidence under another authority, harmless unused
  evidence, and bounded package/proof processing;
- trust updates with forged, stale, wrong-predecessor, rollback, wrong-scope,
  and self-authorizing successor configurations;
- new authorities and profiles authorized only by predecessor policy;
- delivery-log inclusion/consistency tamper, stale checkpoints, lost
  acknowledgements, state drift, and crash recovery at every durable boundary;
- historical evidence, cutoffs, retained-profile constraints, missing history,
  and rejection of fresh evidence under constraints limited to past intervals;
- unbound workload JWTs presented as durable supporting provenance;
- provider-authority updates authenticated only by the workload being changed;
- profile evidence used for a content type that the matched policy does not
  admit; and
- RM assembly and forwarding of profile-specific proof material without
  authority to change profile selection or substitute for target verification.

Each profile additionally tests the guarantees it claims:

- continuity/v3 tests enrollment substitution, map/history tamper, exceptions,
  stale branches, rotation cutoffs, and historical-state proof coverage;
- Sigstore tests Fulcio proof of possession, certificate paths, identity
  extensions, TSA and trusted-time checks, configured transparency behavior,
  and rejection of unsupported Bundle forms; and
- TUF tests root rotation, thresholds, expiry, rollback/freeze protection,
  snapshot/target integrity, delegated role/path constraints, exact trust-update
  content, and crash-safe updater recovery.

Common interface behavior is tested consistently across profiles. Failure and
attack tests remain profile-specific where the profiles intentionally make
different security and availability tradeoffs.

## Open Questions

- Should clients sign claimed provenance type and authority attributes, or
  should verifiers derive all of them from authenticated evidence? A focused
  threat-model and POC comparison should decide this.
- What are the concrete strongly typed lifecycle APIs for continuity/v3,
  OIDC request-signing key binding, and adapted TUF administration?
- What canonical encodings and cross-language test vectors define each common
  assertion and evidence digest?
- How are profile-owned state records associated across routine config updates
  without making an RM-supplied storage key authoritative?
- What delivery-log scope, compaction, permanent-rejection progression, and
  recovery policy fit the target scale range?
- Where optional TSA, witness, or gossip extensions are enabled, are they
  authority-wide or partition-specific?
- When concrete requirements arise, should multiple endorsements require
  principal diversity, verification-path diversity, or both? Thresholds are
  intentionally not part of initial profile selection.
