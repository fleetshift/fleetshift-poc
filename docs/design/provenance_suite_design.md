# Pluggable provenance suites

Status: initial design draft for discussion

This document proposes interfaces between FleetShift's stable attestation and
delivery semantics and a provider-selected **provenance suite**. It maps two
initial implementations:

- the continuity, authenticated-map, and ordered-log model from
  `docs/design/trust_model_v3.md` and `poc/trust-model-v3`; and
- the Fulcio, RFC 3161, Sigstore Bundle, and TUF model from
  `poc/attestation/sigstore_tuf_bundle`.

The aim is not to make cryptography interchangeable at arbitrary individual
call sites. The aim is to give providers a coherent, securely selected suite
while keeping FleetShift's authorization language, evidence relationships,
delivery contract, policy evaluation, and apply path the same.

## 1. Problem statement

FleetShift needs a secure default that works with infrastructure a provider is
likely to have already: controlled clients, an OIDC provider, the resource
manager, delivery agents, and the resource manager's database. Trust model v3
is a good fit. It gives established delivery agents protection from
resource-manager key substitution and history rollback without requiring a
new CA, timestamp service, or public transparency service.

Some providers can operate or consume more specialized infrastructure. A
Sigstore-based suite can use short-lived Fulcio certificates, standard
Sigstore bundles, trusted timestamps, and standard TUF metadata. It replaces
custom identity continuity with an established certificate issuance and
verification ecosystem, at the cost of additional online services and
operational trust.

Future systems may make other trade-offs. An embeddable key-transparency
service, a witnessed transparency log, or a hardware-backed enterprise signer
should be adoptable without changing FleetShift's attestation language or
target apply logic.

The architecture therefore needs to separate:

```text
what was authorized and what may be delivered

from

how a signer obtained a key, how that key was bound to an identity,
how trust reached a verifier, and how the resulting signature is verified
```

## 2. Goals

- Providers select a provenance suite without creating different fulfillment,
  managed-resource, placement, removal, or generation semantics.
- Every independently signed evidence item is retained and later delivered
  exactly as signed; the resource manager does not translate, combine, or
  re-sign those items.
- Clients use one high-level signing API even though acquiring a signer may
  mean loading a continuity key or performing OIDC + Fulcio issuance.
- Common resource-manager code assembles the FleetShift attestation evidence
  bundle, while one suite boundary assembles the additional verification
  material; v3 must construct map/history proofs and Sigstore usually does not.
- The delivery agent has one attestation-verifier boundary. It receives a
  normalized authenticated principal from the suite instead of inspecting
  public keys, certificates, or signer-selected labels itself.
- Trust configuration and ordered delivery reuse as much implementation as is
  defensible.
- Suite-specific state advances transactionally with delivery-agent state.
- A new suite can be added by implementing bounded client, manager, and agent
  interfaces plus a conformance test contract.

## 3. Non-goals

- Dynamically downloading verifier code from the resource manager.
- Mixing arbitrary cryptographic components independently in the first
  version, such as v3 enrollment with Fulcio verification and an unrelated
  rotation protocol. A suite is deliberately cohesive.
- Reproducing the resource manager's complete tenant/workspace authorization
  policy at every delivery agent.
- Treating a management-plane preflight verification as delivery authority.
- Making TUF a high-churn database of every identity key or delivery.
- Claiming that a local append-only journal provides wall-clock time, public
  transparency, or global fork detection.
- Solving credential presentation. Run-as-me, run-as-workload, and
  run-as-platform remain orthogonal to provenance.

## 4. Terminology

- **Attestation semantics:** FleetShift's meaning for signed input, derived
  input, manifests, placement, fulfillment relations, constraints, put/remove
  actions, and generations.
- **Signed statement:** A purpose-separated in-toto statement carried in a DSSE
  envelope. The predicate contains FleetShift authorization or evidence data.
- **Signed evidence item:** One producer's independently signed assertion. It
  consists of one in-toto statement and its DSSE envelope and signature,
  encoded with the suite's stable per-item verification material. Additional
  dynamic proof material may travel separately. A Sigstore Bundle is one
  signed-evidence-item format; despite its name, it is not the aggregate
  FleetShift evidence bundle. The v3 suite needs an analogous continuity bundle
  for each signed item.
- **Attestation evidence bundle:** FleetShift's aggregate collection of the
  independently signed evidence items needed to verify one delivery. The
  resource manager assembles it over time from user requests, addon outputs,
  placement decisions, fulfillment relations, and later signed updates. The
  aggregate is not covered by one DSSE signature.
- **Verifiable relationship:** A relationship among evidence items or delivery
  content whose identity-affecting fields are carried by a signed statement or
  are deterministically derived from signed statements. Outer references and
  indexes may locate evidence, but do not make a relationship authoritative.
- **Provenance suite:** A versioned implementation of signer acquisition,
  identity lifecycle, signed-evidence construction, identity/key binding,
  suite verification-material assembly, and target-side cryptographic
  verification.
- **Trust distribution:** The authenticated update mechanism for relatively
  low-churn suite selection, trust roots, and verification policy. The initial
  implementation is TUF.
- **Delivery journal:** FleetShift's per-tenant append-only sequence of delivery
  commitments and suite control events. Delivery agents retain local
  checkpoints. This is the v3 delivery log generalized into shared
  infrastructure.
- **Credential facts:** Mechanism-level facts produced only after cryptographic
  verification, such as OIDC issuer and subject, issuing authority, key/state
  identifier, and trusted signing time.
- **Authenticated principal:** FleetShift's stable signer identity and class
  after trusted policy maps and constrains credential facts.

## 5. Proposed architecture

```text
user request          addon output          placement/relation       later update
    |                     |                         |                      |
    | signs statement     | signs statement         | signs statement      | signs statement
    v                     v                         v                      v
signed evidence A    signed evidence B         signed evidence C     signed evidence D
    \_____________________|_________________________/_____________________/
                              |
                              v
resource manager assembles, but does not sign as a whole:
    concrete delivery content
    + FleetShift attestation evidence bundle {A, B, C, D, ...}
    + trust, journal, and suite verification support
                              |
                              v
delivery agent:
    suite verifies every signed evidence item independently
    common verifier follows only relationships proven by signed content
    constraints -> placement/removal -> generation -> durable apply
```

The suite boundary is deliberately below attestation semantics and above raw
cryptography. A suite does not decide whether a manifest matches an inline
strategy, whether removal is permitted, or whether a generation is current.
It decides whether each particular statement is authentically signed by a
principal recognized under the target's accepted trust state. There is no
assumption that one producer saw, assembled, or signed the complete evidence
set.

### 5.1 Common versus suite-owned responsibilities

| Concern                                         | Common FleetShift code          | Provenance suite                  |
| ----------------------------------------------- | ------------------------------- | --------------------------------- |
| Authorization statement schemas                 | Yes                             | No                                |
| DSSE/in-toto semantic profile                   | Yes                             | Encodes each signed evidence item |
| Evidence relationships and derivation           | Yes                             | No                                |
| CEL and strategy-implied constraints            | Yes                             | No                                |
| Put/remove and generation fencing               | Yes                             | No                                |
| Normal API authorization                        | Yes                             | No                                |
| Signer acquisition                              | No                              | Yes                               |
| Enrollment/rotation/recovery protocol           | Transport only                  | Yes                               |
| Signed-evidence media type                      | Registry and routing            | Yes                               |
| Cryptographic signature verification            | No                              | Yes                               |
| Credential-to-external-identity proof           | No                              | Yes                               |
| Credential facts -> FleetShift principal policy | Prefer common                   | Supplies verified facts           |
| Low-churn trust update                          | Common interface; initially TUF | Defines/parses its targets        |
| Durable delivery commitment ordering            | Yes                             | May add namespaced control events |
| Per-suite proof construction                    | No                              | Yes                               |
| Per-suite verifier checkpoint                   | Opaque storage and transport    | Defines and validates             |
| Verification explanations                       | Common result tree              | Adds suite-specific children      |
| Apply and acknowledgement                       | Yes                             | No                                |

## 6. Stable signed and delivered data model

### 6.1 Independently signed statements

The unit of signing is one evidence statement, not the final delivery package
or the complete evidence graph. Different producers create statements at
different times:

- a user signs the input they authorize;
- a manifest addon later signs the manifests it generates;
- a placement addon signs a placement decision;
- an addon signs a fulfillment relation;
- another user or workload may later sign an update operation; and
- the update and manifest addons may produce more signed evidence for that
  operation.

Each statement is independently signed and cryptographically verifiable,
although its semantic use may depend on other verified statements. The
resource manager collects the resulting signed items; it does not produce a
new signature that speaks for all of their authors.

When a later operation updates a fulfillment, the next delivery's attestation
evidence bundle contains the reachable prior signed items plus the newly signed
update and output items needed for that generation. Earlier signatures and
statements remain byte-for-byte unchanged. Evidence retention or compaction may
eventually replace a long reachable history with a separately defined durable
anchor, but ordinary delivery assembly never rewrites the authorship chain.

The Sigstore/TUF POC already has useful statement boundaries. The initial
shared profile should retain these predicate families:

- `delivery-authorization/v1` for signed input, validity constraints, explicit
  output constraints, and expected generation;
- `manifest-set/v1` for exact addon-produced manifests;
- `placement/v1` for an addon placement decision; and
- `fulfillment-relation/v1` for a managed-resource fulfillment relation.

Future signed operation kinds use a new predicate type, not a generic bag of
fields. Every statement must bind at least:

- a stable FleetShift authorization/trust domain;
- tenant;
- purpose/evidence kind;
- the content or content digest;
- the identifiers and digests of the subjects or prior content to which the
  assertion applies;
- every relationship field that affects interpretation; and
- validity and expected generation where applicable.

The statement MUST NOT contain a signer-selected trusted anchor. A signer may
include identity and key lookup hints, but the verifier derives authority and
principal from verified evidence plus accepted policy.

### 6.2 DSSE, in-toto, and per-item suite representation

The three formats serve different layers for each independently signed item:

```text
in-toto Statement
    semantic assertion and signed relationships
        |
        v
DSSE envelope
    payload type + statement bytes + producer's signature
        |
        v
per-item suite representation
    DSSE envelope + material needed to verify that one signature
```

The DSSE signature is produced by that evidence item's user, workload, or
addon signing session at the time the statement is created. It covers only the
typed in-toto statement in that envelope—not later evidence and not the final
FleetShift aggregate.

For the Sigstore suite, the representation of one signed evidence item is one
standard Sigstore Bundle. In the initial profile it contains one DSSE envelope
and signature together with the producer's Fulcio certificate, RFC 3161
timestamp, and any configured transparency evidence. “Bundle” in the Sigstore
type name does not mean that it contains the other FleetShift attestations for
the delivery.

For v3, the analogous representation is one continuity bundle containing one
DSSE envelope plus stable signer-evidence selectors such as identity ID and
signing-state digest. Large dynamic map/history proofs can be carried once in
delivery-wide suite verification material and keyed by the digest of each
continuity bundle that uses them.

The common layer treats either representation as immutable typed bytes:

```text
SignedEvidenceItem {
    media_type
    bytes
}
```

Initial media types could be:

```text
application/vnd.dev.sigstore.bundle.v0.3+json
application/vnd.fleetshift.continuity-bundle.v1+json
```

The resource manager stores each item by digest and later delivers the exact
same bytes. It may add more independently signed items as fulfillment evolves,
but it never translates an existing signature or wraps the aggregate evidence
set in a manager-generated provenance signature.

The suite parses and verifies one item's bytes and returns its common in-toto
statement. Trying to normalize certificates, continuity state, timestamps,
Rekor entries, and future transparency proofs into one universal `Signature`
struct would leak every implementation into the common verifier. The complete
standard Sigstore Bundle should therefore remain intact rather than becoming a
custom almost-Sigstore wire format.

### 6.3 Verifiable relationships and derived input

The FleetShift evidence bundle may contain outer IDs, maps, or indexes to find
evidence efficiently. These are selectors, not provenance claims. A verifier
accepts a relationship only when signed statement content establishes it or
when it can deterministically derive the relationship from signed content.

Examples include:

| Relationship                                       | Authenticated source                                                                       |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| User authorization applies to input content        | Subject and predicate of the signed `delivery-authorization/v1` statement                  |
| Manifests are an addon's output for an attestation | Signed `manifest-set/v1` subject and predicate                                             |
| Placement applies to a deployment                  | Signed `placement/v1` deployment ID and target set                                         |
| Addon can fulfill a managed-resource type          | Signed `fulfillment-relation/v1` relation type, addon ID, resource type, and manifest type |
| Update may operate on prior content                | Signed update input, signed placement/scope, preconditions, and prior-content identity     |
| Final delivery follows from signed input           | Common constraint evaluation over verified statements and the concrete output              |

A derived input does not require the original user to sign evidence that does
not yet exist. The verifier instead establishes the chain from independently
signed pieces:

```text
verified prior input
    + verified update authorization and scope
    + verified signed spec update / derivation operation
    + deterministic precondition and transformation evaluation
    = verified derived input
```

An outer `prior_input_id` or `update_attestation_id` can help locate the pieces,
but changing it cannot create a relationship. The resolved signed statements
must identify the expected prior content, operation, subject, and scope, and
the verifier must reproduce the derived content. If a relationship cannot be
proven from the existing signed predicates, the protocol needs another signed
predicate or another field in an existing predicate; it must not rely on an
authoritative unsigned graph edge.

The same rule applies to the root selector. The resource manager can nominate
which evidence item allegedly justifies the concrete delivery, but the common
verifier accepts it only if the independently verified statement graph reaches
and authorizes that exact delivery.

### 6.4 Aggregate evidence bundle and delivery package

The FleetShift attestation evidence bundle is an aggregate of independently
signed evidence items. It has no aggregate DSSE signature:

```text
AttestationEvidenceBundle {
    root_evidence_id
    items {
        item_digest -> SignedEvidenceItem
    }
    lookup_indexes // optional, non-authoritative
}
```

Every map key must equal the digest of the exact encoded signed item. Semantic
IDs inside verified statements remain distinct from these storage digests;
lookup indexes may map semantic IDs to candidate item digests, but the verifier
checks the mapping against the decoded signed statements.

The complete delivery package keeps three concerns separate:

```text
DeliveryPackage {
    delivery: DeliveryContent
    attestations: AttestationEvidenceBundle
    verification: DeliveryVerificationMaterial
}

DeliveryContent {
    protocol_version
    provenance_domain
    tenant_id
    target_id
    delivery_id
    fulfillment_id
    generation
    action
    concrete_output
}

DeliveryVerificationMaterial {
    trust_update
    journal_proof
    suite {
        suite_id
        media_type
        bytes
    }
}
```

`DeliveryContent` is what the target may apply. `AttestationEvidenceBundle` is
the server-assembled set of independently signed statements used to decide
whether that content is authorized. `DeliveryVerificationMaterial` supplies
replaceable trust, ordering, identity-history, or transparency proofs needed
to verify the signed items; it is not content attestation evidence itself.

The delivery journal commits the digest of `DeliveryContent` and a canonical
attestation-evidence digest covering the nominated root and the sorted exact
signed-item digests and bytes. Reconstructable lookup indexes are excluded.
That commitment proves exact delivery ordering and prevents later substitution
relative to an agent's checkpoint, but it does not make the resource manager an
attestation signer. Trust updates and reconstructable proofs are likewise not
included because they may be refreshed without changing the delivery or its
signed evidence.

The common verifier must compare outer routing and delivery fields to the
cryptographically verified statements. An outer tenant, target, action,
generation, root selector, reference, or lookup index is never authoritative
on its own.

The package contains a `suite_id` for diagnostics and dispatch, but it does
not select trust. The agent first loads the suite pinned by its provisioned or
authenticated trust configuration and then requires the package to match.

## 7. Suite selection and configuration

A provider configures a provenance profile for an authorization domain. In
the first version, a tenant/authorization domain has exactly one active suite:

```text
ProvenanceProfile {
    domain_id
    suite_id
    suite_protocol_version
    signed_evidence_media_types[]
    trust_updater_id
    identity_policy_target
    suite_target_paths[]
    journal_profile
}
```

The profile is authenticated by the target's already provisioned trust root.
It is not accepted merely because it arrived in a delivery package.

The suite ID names a protocol, not a code package or deployment instance. For
example:

```text
fleetshift.dev/provenance/continuity/v1
fleetshift.dev/provenance/sigstore-keyless/v1
```

All relevant binaries have a static registry keyed by suite ID. Providers may
compile or deploy only the suites they support. Unknown suites fail closed.
Code is never loaded from the couriered package.

The initial single-suite rule prevents downgrade and identity-confusion bugs.
A later profile could select suites by signer class or evidence kind, but it
would need explicit, authenticated rules. A signed evidence item MUST NOT
choose from a set of verifiers by trying each until one accepts it.

### 7.1 Migration

Changing suite is a trust-root transition, not an ordinary configuration
edit. An established delivery agent must accept a migration only through one
of:

- a transition authorized under the currently accepted profile and the
  successor profile;
- a previously configured higher-level tenant/provider authority; or
- an explicit out-of-band recovery/reprovisioning ceremony.

The resource manager cannot turn a failed suite verification into a retry
under another suite. During a staged migration, a trusted profile may require
dual signatures, but the dual requirement is policy—not fallback behavior.

## 8. Concrete interfaces

The interfaces below are Go-like pseudocode. They describe process boundaries
and ownership; they are not yet proposed production Go APIs.

### 8.1 Shared types

```go
type SuiteID string

type TypedBytes struct {
    MediaType string
    Bytes     []byte
}

type EvidenceKind string // input, output, placement, relation, ...

type UnsignedStatement struct {
    PredicateType string
    Statement     []byte // canonical in-toto Statement bytes
}

type SignedEvidenceItem = TypedBytes

type CredentialFacts struct {
    ExternalIssuer  string
    ExternalSubject string

    AuthorityType string // continuity-map, x509-ca, key-transparency-log, ...
    AuthorityID   string // authenticated ID/URI, never a signer-selected label

    KeyID          string
    SigningTime    *time.Time
    Attributes     map[string]Value
}

type AuthenticatedPrincipal struct {
    ID         string
    Class      string // user, workload, addon, configuration-authority, ...
    AnchorID   string
    Attributes map[string]Value
}

type VerifiedStatement struct {
    PredicateType string
    Statement     []byte
    Principal     AuthenticatedPrincipal
    Facts         CredentialFacts
}

type VerifierCheckpoint struct {
    SuiteID          SuiteID
    ProfileDigest    string
    TrustState       TypedBytes // owned by the configured trust updater
    Journal          JournalCheckpoint
    SuiteState       TypedBytes // opaque to common code
}
```

`CredentialFacts` are mechanism facts, not an open authorization surface.
Implementations register typed fact schemas per suite/version. Policy may
expose a bounded projection to CEL, but arbitrary suite-provided attributes
must not silently become trusted authorization claims.

### 8.2 Suite registry

The client, resource manager, and delivery agent do not necessarily ship in
one binary, so each role has its own factory registry rather than one giant
`ProvenanceSuite` object.

```go
type SuiteDescriptor struct {
    ID                         SuiteID
    ProtocolVersion            string
    AcceptedEvidenceMediaTypes []string
    SuiteVerificationMaterialType string
    SuiteCheckpointType        string
}

type ClientSuiteFactory interface {
    Descriptor() SuiteDescriptor
    Open(ClientDependencies, TrustedProfile) (ClientSuite, error)
}

type ManagerSuiteFactory interface {
    Descriptor() SuiteDescriptor
    Open(ManagerDependencies, TrustedProfile) (ManagerSuite, error)
}

type AgentSuiteFactory interface {
    Descriptor() SuiteDescriptor
    Open(AgentDependencies, TrustedProfile) (AgentSuite, error)
}
```

Descriptor capabilities are useful for compatibility checks and UX. They are
not a substitute for authenticated policy and are not consulted to weaken a
failed security check.

### 8.3 Client signing interface

```go
type SignerRequest struct {
    DomainID       string
    TenantID       string
    PrincipalClass string
    EvidenceKind   EvidenceKind
    Interactive    bool
}

type ClientSuite interface {
    AcquireSigner(
        context.Context,
        SignerRequest,
        IdentityControlClient,
    ) (SigningSession, error)

    Capabilities(context.Context) ClientCapabilities
}

type SigningSession interface {
    Sign(context.Context, UnsignedStatement) (SignedEvidenceItem, error)
    PrincipalHint() PrincipalHint
    Close() error
}

type IdentityControlClient interface {
    SubmitSuiteOperation(
        context.Context,
        SuiteControlOperation,
    ) (SuiteControlReceipt, error)
}

type SuiteControlOperation struct {
    SuiteID   SuiteID
    Kind      string
    OperationID string
    Payload   TypedBytes
}
```

`AcquireSigner` owns the entire key-acquisition ceremony. It receives narrow
OIDC, secure-key-store, WebAuthn, and user-presentation dependencies; it does
not return private-key bytes. Addon code and mutable UI plugins never receive
a general-purpose signing handle. The returned session is scoped to the
requested domain, tenant, principal class, and evidence kind, and rejects a
statement whose signed context does not match that scope. `PrincipalHint` is
only for UX and evidence lookup; it never grants identity or authority.

Lifecycle operations remain suite-defined because pretending that enrollment,
continuity rotation, Fulcio issuance, and future key-transparency registration
are one protocol would create a leaky abstraction. FleetShift shares their
authenticated API transport, idempotency, authorization hook, status, and
receipt shape.

For v3, `AcquireSigner` loads or creates the continuity/device/session
hierarchy and may submit an enrollment operation before returning. Explicit
rotation is a suite-specific client action using the same control transport.
For Sigstore, it creates an ephemeral key, completes OIDC/Fulcio issuance, and
obtains required timestamp material while signing; it normally submits no
FleetShift identity-control operation.

### 8.4 Resource-manager suite interface

```go
type ManagerSuite interface {
    VerifyRequest(
        context.Context,
        RequestVerificationRequest,
    ) (VerifiedStatement, *VerificationResult, error)

    PrepareControl(
        context.Context,
        AuthorizedCaller,
        SuiteControlOperation,
    ) (PreparedControl, error)

    AssembleVerificationMaterial(
        context.Context,
        VerificationMaterialAssemblyRequest,
    ) (VerificationMaterialAssembly, error)

    ValidateReportedCheckpoint(
        context.Context,
        TypedBytes,
    ) error
}

type PreparedControl interface {
    // Nil means this operation does not need a journal serialization point.
    JournalCommitment() *SuiteControlCommitment

    // Finalize is idempotent. When JournalCommitment was non-nil, record is
    // the exact common-journal record assigned to that commitment.
    Finalize(context.Context, *JournalRecord) (SuiteControlReceipt, error)
    Close() error
}

type VerificationMaterialAssembly struct {
    SuiteVerificationMaterial TypedBytes
    JournalSelectors          []JournalSelector
}

type VerificationMaterialAssemblyRequest struct {
    Delivery                DeliveryContent
    DeliveryDigest          Digest
    AttestationBundleDigest Digest
    Attestations            AttestationEvidenceBundle
    JournalCommitment       JournalRecord
    TargetCheckpoint        VerifierCheckpoint
    TrustedTargets          TrustedTargetSet
}

type RequestVerificationRequest struct {
    Evidence       SignedEvidenceItem
    EvidenceKind   EvidenceKind
    DomainID       string
    TenantID       string
    TrustedTargets TrustedTargetSet
}
```

Common resource-manager code performs ordinary caller authentication and
authorization around `VerifyRequest`, `PrepareControl`, and delivery
submission. For a signed API mutation, it independently verifies the request's
signed evidence item, compares the authenticated signing principal to the live
API caller and operation, performs normal tenant/workspace authorization, and
stores the original item. This result is authoritative for the resource
manager's own request handling, but it is not target delivery authority; the
delivery agent repeats verification from its independent checkpoint and trust
state.

`PrepareControl` validates suite continuity and returns any control commitment
that needs a common-journal serialization point. Common code appends it and
passes the exact assigned record to idempotent `Finalize`; v3 uses that record
in the key event that completes rotation. A crash after append but before
finalization leaves an inert marker and resumes the durable workflow on retry.
A suite cannot use successful cryptographic validation to bypass ordinary
FleetShift authorization.

`AssembleVerificationMaterial` is proof construction, not authorization or
attestation construction. Common code has already assembled the independently
signed evidence items. The suite may read authenticated-map nodes, key
histories, or native transparency services to build the additional proof
material needed to verify those exact items. It returns suite verification
material plus the suite-control records that common journal code must disclose
alongside the delivery commitment. This material and its selectors are
untrusted until the common journal verifier and agent suite cross-check them.

The resource manager may additionally preflight a complete derived delivery
to reject malformed work early. That delivery preflight is advisory and MUST
NOT be represented as target authority or replace target-side verification.

### 8.5 Delivery-agent suite interface

```go
type AgentSuite interface {
    BeginVerification(
        context.Context,
        BeginVerificationRequest,
    ) (VerificationSession, error)
}

type BeginVerificationRequest struct {
    Delivery                 DeliveryContent
    Attestations              AttestationEvidenceBundle
    SuiteVerificationMaterial TypedBytes
    TrustedTargets            TrustedTargetSet
    JournalView               VerifiedJournalView
    CurrentState              TypedBytes
}

type VerificationSession interface {
    VerifyEvidenceItem(
        context.Context,
        SignedEvidenceItem,
        EvidenceKind,
    ) (VerifiedStatement, *VerificationResult, error)

    CandidateState() TypedBytes
    Close() error
}
```

`BeginVerification` performs delivery-wide work once. For v3 this includes
advancing the authenticated map, validating new key events or recording
exceptions in a candidate state, and indexing selectively supplied identity
proofs. For Sigstore it parses accepted roots/policy and prepares verification
of every Sigstore Bundle in the aggregate FleetShift evidence bundle.

The common attestation engine calls `VerifyEvidenceItem` for each independently
signed item in the aggregate FleetShift bundle. Each call returns the same
normalized `VerifiedStatement` shape. The engine builds its working graph from
the verified statements, treats outer references only as lookup aids, and
evaluates derivation, placement, output constraints, removal, and generation
without branching on suite ID.

`CandidateState` is never persisted merely because suite cryptography passed.
It is committed only after the entire common attestation verification succeeds
and the delivery contract has durably recorded apply or pending work.

### 8.6 Identity policy boundary

The suites should share the mapping from verified mechanism facts to
FleetShift principals where possible:

```go
type PrincipalPolicy interface {
    Authenticate(
        context.Context,
        CredentialFacts,
        EvidenceKind,
        VerifiedPredicate,
    ) (AuthenticatedPrincipal, *VerificationResult, error)
}
```

An authenticated policy rule can match:

- authority type and ID;
- external issuer and exact subject or constrained subject pattern;
- principal class and evidence kind;
- stable key/transparency namespace where relevant;
- anchor attributes and CEL constraints over the verified predicate.

It then produces the stable FleetShift principal ID. Zero matches and multiple
matches fail closed. This is the generalized form of the Sigstore POC's
`_authenticate_identity` and the hybrid POC's `TrustAnchor.verify_signer`.

The v3 suite authenticates the OIDC issuer/subject from the accepted enrollment
and continuity history before supplying facts. The Sigstore suite authenticates
them from the Fulcio certificate and certificate chain. Neither suite accepts
a signed evidence item's claimed FleetShift signer ID as authority.

Some suite-specific policy remains unavoidable. For example, allowed
continuity algorithms and recovery transitions belong to v3, while accepted
certificate extensions, transparency logs, and timestamp authorities belong
to Sigstore. Those stay in typed suite targets and run before common principal
policy.

## 9. Shared trust distribution

### 9.1 Initial decision: TUF for low-churn trust configuration

The first implementation should always use a stateful TUF updater for:

- the provenance profile and suite selection;
- the common principal/anchor policy;
- suite trust roots and parameters;
- trust-root and policy rotation with rollback/freeze protection.

The provisioned bootstrap is a complete TUF root or a stronger out-of-band
anchor. A delivery may courier newer root metadata in a valid TUF root chain,
but it can never supply the bootstrap root for an established agent.

Using TUF here means reusing its metadata, updater, and role semantics; it does
not require a separately deployed TUF service. The resource manager can store
and courier repository bytes through its existing database and delivery path.
The signing authority for those bytes must still follow the tenant/provider's
accepted root policy and cannot collapse into an unrestricted resource-manager
key. This keeps the default v3 deployment within the existing component set
while using an off-the-shelf trust-update protocol.

TUF is not used for every v3 enrollment, continuity rotation, delivery
commitment, or future key-transparency leaf. Those are high-churn suite or
journal data with their own authenticated proofs.

The code should still depend on a small interface:

```go
type TrustUpdater interface {
    Stage(
        context.Context,
        CurrentTrustState,
        TrustUpdate,
    ) (CandidateTrustState, TrustedTargetSet, *VerificationResult, error)
}
```

The initial and expected implementation is TUF. Keeping the interface separate
allows a future environment with a genuinely equivalent authenticated updater
without changing suite or attestation APIs. The selected updater is itself
pinned at provisioning; it is not chosen by a delivery.

### 9.2 Candidate TUF targets

Common targets:

```text
fleetshift/provenance-profile.json
fleetshift/principal-policy.json
```

V3 suite targets:

```text
fleetshift/suites/continuity-v1/trust-manifest.json
```

The trust manifest carries OIDC enrollment issuer/client, accepted algorithms,
map and journal parameters, recovery constraints, and trust-update policy that
is not already represented by TUF roles.

Sigstore suite targets:

```text
fleetshift/suites/sigstore-keyless-v1/trusted-root.json
```

This is the standard Sigstore `TrustedRoot` containing the applicable Fulcio,
TSA, Rekor, and CT authorities. The common principal policy replaces the POC's
suite-local FleetShift identity-mapping target if the normalized policy proves
sufficient.

### 9.3 Transactional TUF state

The Sigstore POC's standard updater writes accepted metadata during refresh.
Production integration must stage those writes. A valid TUF update followed by
an invalid attestation must not partially advance the agent's durable trust
state outside the delivery transaction. Viable implementations include a
transactional metadata store or refresh in a candidate directory followed by
an atomic state swap.

## 10. Shared append-only delivery journal

### 10.1 Initial decision: use it for every provenance suite

The v3 delivery log should become common FleetShift delivery infrastructure.
Every durable mutation is committed before dispatch:

```text
DeliveryCommitment {
    domain_id
    tenant_id
    target_id
    delivery_id
    fulfillment_id
    generation
    action
    delivery_content_digest
    attestation_evidence_bundle_digest
}
```

Suites may define purpose-separated control records:

```text
SuiteControlCommitment {
    domain_id
    suite_id
    operation_kind
    operation_digest
}
```

The journal interface is not suite-specific:

```go
type DeliveryJournal interface {
    AppendDelivery(context.Context, DeliveryCommitment) (JournalRecord, error)
    AppendSuiteControl(context.Context, SuiteControlCommitment) (JournalRecord, error)
    Prove(context.Context, JournalCheckpoint, []JournalSelector) (JournalProof, error)
}

type JournalVerifier interface {
    Verify(
        context.Context,
        JournalCheckpoint,
        JournalProof,
    ) (VerifiedJournalView, JournalCheckpoint, *VerificationResult, error)
}
```

Common code owns inclusion, append-only consistency, stale checkpoint recovery,
acknowledgement, compaction watermarks, and retry behavior. A suite receives a
`VerifiedJournalView` and interprets only its namespaced control records.

### 10.2 What this gains

- V3 keeps its exact rotation-cutoff semantics without owning a private
  delivery transport protocol.
- Both suites get the same established-agent rollback protection for delivery
  history and the same lost-acknowledgement/catch-up path.
- Retention, tiling, compaction, and checkpoint reporting are implemented once.
- A future key-transparency suite can bind control events into the same
  delivery ordering domain when useful.

### 10.3 What this does not gain

For the Sigstore suite, the journal does not replace RFC 3161 trusted time.
A resource-manager-operated Merkle log proves ordering relative to an agent's
locally retained checkpoint; it does not prove that a signature existed during
a certificate's wall-clock validity period to a cold verifier.

The journal also does not provide Sigstore/Rekor-style public auditability or
global fork detection. Witnessing or gossip can be added independently.

## 11. Delivery verification transaction

The target-side flow should be the same for every suite:

1. Load the provisioned provenance profile and current durable verifier state.
2. Select the pinned trust updater and suite implementation. Reject any package
   suite/profile mismatch without fallback.
3. Canonicalize `DeliveryContent` and the exact set of independently signed
   evidence items, recompute their digests, and match the outer route to the
   agent's tenant and target.
4. Stage the trust update and obtain authenticated common and suite targets.
5. Verify journal consistency from the retained checkpoint, inclusion of the
   exact delivery-content and attestation-bundle digests, and all selected
   suite-control records.
6. Call `AgentSuite.BeginVerification` with current suite state, trusted
   targets, and the verified journal view.
7. Ask the verification session to verify every signed evidence item
   independently and return its normalized statement and principal. Do not
   infer authority from the aggregate bundle or its indexes.
8. Build the working evidence graph from verified statement subjects,
   predicates, IDs, and digests. Treat outer references as selectors and reject
   every relationship that the signed content does not establish.
9. Resolve the nominated root, reconstruct derived inputs, and evaluate common
   constraints, placement, removal, graph limits, and generation fencing
   against the concrete delivery.
10. Atomically or crash-consistently persist:

- candidate TUF/trust-updater state;
- candidate journal checkpoint;
- candidate suite checkpoint;
- common fulfillment generation/state; and
- either completed apply state or enough pending work to guarantee retry.

11. Acknowledge according to the target delivery contract.

Failures before step 10 discard all candidate trust state. An invalid principal
event that v3 deliberately records as an exception is not a failure to stage;
it becomes candidate v3 state, but the current delivery still fails if it
depends on that identity.

## 12. Mapping the two initial suites

### 12.1 Summary

| Boundary                      | Continuity / trust-model-v3                                | Sigstore keyless / TUF bundle                              |
| ----------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- |
| Client key acquisition        | Load/create continuity/device key; create session key      | Create ephemeral P-256 key                                 |
| Identity ceremony             | Nonce-bound OIDC enrollment; later continuity              | OIDC-authenticated Fulcio issuance per signing session     |
| FleetShift control operations | Enrollment, rotation, recovery, tombstone                  | Normally none                                              |
| Signed evidence item          | One FleetShift continuity bundle containing one DSSE item  | One standard Sigstore Bundle v0.3 containing one DSSE item |
| Aggregate attestation bundle  | Common FleetShift collection of independently signed items | Common FleetShift collection of independently signed items |
| Trusted wall-clock time       | Not required for user delivery                             | RFC 3161 token in current profile                          |
| Low-churn trust               | TUF-carried v3 trust manifest and common policy            | TUF-carried Sigstore TrustedRoot and common policy         |
| High-churn identity state     | Principal history + authenticated head map                 | Short-lived certificate; optional Rekor/CT evidence        |
| Manager proof work            | Map/history/event proofs keyed to signed item digests      | Usually only courier each producer's existing bundle       |
| Shared journal                | Deliveries + rotation markers                              | Deliveries; no required control marker                     |
| Suite checkpoint              | Map root + bounded exceptions                              | Empty initially; later native log/witness checkpoints      |
| Common checkpoint             | TUF state + journal root/size                              | TUF state + journal root/size                              |
| Verified authority facts      | OIDC issuer/sub bound through accepted enrollment/history  | Fulcio CA URI + certificate OIDC issuer/sub + timestamp    |
| Historical signer rule        | Adjacent history events and marker interval                | Cert chain + trusted timestamp + retained historical roots |

### 12.2 V3 signed items and suite verification material

The current v3 `ContentAttestation` should be replaced by the common
`delivery-authorization/v1` statement, carried as one independently signed
continuity bundle. Other evidence kinds use the same per-item shape with their
own in-toto predicates and their own producer signatures:

```text
ContinuityBundle {
    media_type
    dsse_envelope
    identity_id_hint
    signing_state_digest_hint
    delegation_chain[]
}
```

The identity and state fields are proof-selection hints. The signature and
delivered evidence must demonstrate them; neither is trusted directly.

Delivery-wide v3 verification material contains identity proofs keyed to the
exact independently signed items that need them:

```text
ContinuityVerificationMaterial {
    map_advances[]
    identities {
        signed_item_digest -> {
            current_head_and_map_proof
            signing_event
            successor_event?
        }
    }
}
```

Rotation records and their inclusion paths live in the common journal proof.
The v3 suite cross-checks exact marker index/hash/package references against
the already verified journal view.

The v3 suite checkpoint is the existing map root and exceptional-event set.
The delivery-log checkpoint moves to common state:

```text
ContinuitySuiteState {
    map_root
    exceptional_events[] {
        identity_id
        sequence
        event_digest
        resulting_state_digest
    }
}
```

The existing `SyncMap` plus identity verification is a strong starting point
for `BeginVerification` and `VerifyEvidenceItem`. The current `Deliver` method
is too broad: it mixes suite verification, journal verification, generation,
application, and acknowledgement, which are exactly the responsibilities this
design separates.

### 12.3 Sigstore signed items and suite verification material

Each signed evidence item is one unmodified Sigstore Bundle already used by the
POC. Its DSSE payload uses the same FleetShift predicate schemas as the
continuity suite. A delivery with a user input, addon manifests, placement,
fulfillment relation, and update history therefore carries several independent
Sigstore Bundles created by their respective producers. FleetShift's aggregate
attestation evidence bundle collects them but has no aggregate DSSE signature.

Most per-item verification material remains inside each standard bundle:

- Fulcio leaf certificate;
- DSSE signature;
- RFC 3161 timestamp;
- optional future Rekor inclusion material.

Delivery-wide suite verification material can initially be an empty, versioned
object. The common trust update carries the Sigstore `TrustedRoot`, and the
common journal proof commits the concrete delivery and exact aggregate set of
signed evidence items.

`verify_sigstore_bundle` maps naturally to `VerifyEvidenceItem` and runs once
for each Sigstore Bundle. Certificate and timestamp checks yield credential
facts. The current `_authenticate_identity` logic should move behind the common
`PrincipalPolicy`, retaining exact-one-match and evidence-kind separation.

The Sigstore suite initially has no suite-specific checkpoint. Its persistent
TUF state and journal checkpoint are common. If Rekor, CT, or witness
consistency becomes part of acceptance, their retained checkpoints become
suite state without changing the common verifier interface.

### 12.4 Important guarantee differences remain visible

The common interface must not imply that both suites provide identical
security:

- v3 protects an established agent against continuity-history rollback but
  accepts local-view fork limitations and has no trusted wall-clock time;
- logless Sigstore has short-lived credentials and trusted timestamps but no
  public detection of Fulcio misissuance or equivocation;
- adding Rekor/CT/witnesses changes the Sigstore profile's guarantees;
- a common FleetShift journal adds local delivery-history continuity to both
  but does not erase these differences.

Each suite descriptor and provider-facing documentation should publish a
human-readable guarantee profile. Capability metadata is explanatory; the
actual verifier behavior is determined by the pinned protocol and policy.

## 13. Future suite example: key transparency

An industrialized key-transparency implementation should fit without changing
the common engine:

- `AcquireSigner` creates or loads a client-held key and registers it through
  a suite control operation or native service.
- Each signed evidence item carries DSSE plus a stable key/transparency
  selector.
- TUF distributes the transparency service's bootstrap keys, policy, and
  witness configuration unless the provider provisions an equivalent anchor.
- `AssembleVerificationMaterial` obtains inclusion, consistency, and
  non-equivocation material for each signing identity.
- `BeginVerification` advances retained transparency checkpoints.
- `VerifyEvidenceItem` verifies each key binding and signature, then emits the
  same normalized issuer/subject/authority facts.
- `CandidateState` contains accepted log and witness checkpoints.

The independently signed statement model, principal policy, aggregate
FleetShift evidence bundle, delivery journal, apply path, and acknowledgement
contract do not change.

## 14. Security invariants

1. **Pinned selection:** suite and trust updater are selected from provisioned
   or authenticated local state, never from untrusted package data.
2. **No fallback:** parse, trust, or verification failure in the selected suite
   is terminal for that attempt.
3. **Original signed items:** every exact user, workload, or addon signed item
   is stored and delivered. The manager does not translate or replace it.
4. **Independent authority:** no signature by one producer or by the resource
   manager is treated as a signature over the aggregate evidence bundle or as
   authority for another producer's statement.
5. **Authenticated relationships:** outer indexes and references locate
   evidence but never establish a relationship. Every accepted relationship is
   contained in or deterministically derived from verified signed statements.
6. **Purpose separation:** predicate type, DSSE payload type, signed-evidence
   media type, evidence kind, and suite protocol are checked together.
7. **Derived identity:** principal and anchor IDs come from verified evidence
   and accepted policy, never from an unauthenticated signer label.
8. **Exact-one policy match:** zero or ambiguous identity-policy matches fail.
9. **Manager is courier:** manager preflight and ordinary authorization do not
   substitute for target verification; both are required.
10. **Proof isolation:** suite verification material and trust updates are
    untrusted input until independently verified from retained
    checkpoints/anchors.
11. **Transactional state:** trust, journal, suite, generation, and durable
    apply state do not advance independently in a way that grants authority
    after a failed delivery.
12. **Rollback protection:** established agents never silently re-enter new-
    agent bootstrap after checkpoint loss, expiry, or compaction lag.
13. **Bounded verification:** graph size/depth, proof sizes, signed-item count,
    statement sizes, and suite-specific work have explicit limits.
14. **Explainable failure:** errors identify suite selection, trust update,
    journal, identity binding, signature, policy, attestation, constraint, or
    generation failure without exposing secret material.
15. **Controlled signer surface:** key handles are purpose-restricted and are
    not exposed to addon UI code or arbitrary byte-signing callers.
16. **Migration is authorization:** a suite change must chain from current
    trust or use explicit recovery; it is never a compatibility fallback.

## 15. Tempting but incorrect abstractions

### `Verify(signature, publicKey) bool`

This omits identity binding, accepted authority, evidence kind, trust history,
trusted time, policy constraints, and state advancement. It is below the useful
security boundary.

### One DSSE signature over the complete evidence bundle

No single actor owns or even observes all evidence at the time it is created.
The user signs an authorization before later addon output, placement, and
update evidence may exist. Making the resource manager sign the assembled set
would authenticate only the manager's collection and would incorrectly turn it
into the provenance authority for independently authored statements.

The correct unit is one DSSE envelope per producer assertion. FleetShift
aggregates those independently signed items and verifies every relationship
from their signed content. The delivery journal may commit the exact aggregate
for ordering and substitution resistance, but that commitment is not an
attestation signature.

### A universal `KeyBinding` struct

V3 history evidence and a Fulcio certificate are not the same object. Forcing
them into common optional fields creates invalid combinations and pushes suite
branching throughout the verifier. The shared output should be credential facts
and an authenticated principal, not a universal input representation.

### One interface method per v3 concept

Making `Enroll`, `RotateContinuityKey`, `ProveMapLeaf`, and
`VerifyRotationMarker` part of the common API would merely rename v3 as the
abstraction. Lifecycle payloads and suite evidence must remain typed and
suite-owned behind common transport/session boundaries.

### Letting the package select the verifier

Trying all installed suites or accepting a package-declared suite enables
downgrade and cross-protocol confusion. Local authenticated policy selects one
suite; package metadata only has to agree.

### Putting all mutable identity state in TUF

TUF is valuable for roots and policy, but per-signature identity churn would
turn it into a centralized high-frequency signing bottleneck and duplicate the
v3/key-transparency proof systems. Use it as the control plane, not the event
database.

### Treating the delivery journal as a timestamp service

The resource manager can delay append. Journal position gives protocol order,
not trustworthy signature creation time. Sigstore's short-lived certificate
profile still needs a trusted timestamp or an explicitly different online-
verification policy.

## 16. Conformance contract

Every suite implementation should run the same semantic and adversarial
contract, with mechanism-specific assertions where necessary.

Common cases include:

- exact parity for input, derived input, manifest, placement, relation,
  put/remove, managed-resource, constraint, and generation tests;
- cross-tenant and wrong-target package rejection;
- signed/outer generation mismatch;
- purpose and predicate confusion;
- signed-evidence media-type relabeling;
- injection or omission of an independently signed evidence item;
- outer-reference or lookup-index retargeting without a corresponding signed
  relationship;
- attempted reuse of valid manifests, placement, updates, or fulfillment
  relations under a different subject or attestation;
- rejection of any relationship that exists only in unsigned aggregate
  metadata;
- signer-label and anchor-label substitution;
- missing, ambiguous, and wrong-evidence-kind policy matches;
- manager modification of signed content or interpretation fields;
- trust-update rollback, freeze, expiry, and target tamper;
- journal leaf/root/inclusion/consistency tamper;
- stale manager checkpoint and lost acknowledgement;
- state remains unchanged after every verification failure boundary;
- graph cycles, excessive graph depth, proof amplification, and oversized
  signed-evidence-item rejection;
- migration/downgrade rejection.

V3-specific cases retain enrollment substitution, map/history proof tamper,
rotation cutoff, stale-agent, exception, and historical-state coverage.

Sigstore-specific cases retain Fulcio proof-of-possession, certificate path,
identity extension, RFC 3161, TrustedRoot, and optional transparency evidence
coverage.

The existing Sigstore parity inventory is a useful precedent: test-name parity
alone is insufficient, so negative tests should pin the intended rejection
layer and forbid conversion into soft boolean assertions.

## 17. Suggested proof-of-concept sequence

1. Define common in-toto predicate schemas and canonical cross-language test
   vectors for each independently signed evidence kind. Replace v3's simple
   `ContentAttestation` with one DSSE continuity bundle per signed assertion,
   without introducing a signature over the aggregate evidence set or changing
   its continuity proofs.
2. Extract the v3 delivery log into a common journal package. Keep all existing
   cutoff, catch-up, stale checkpoint, and lost-ack tests.
3. Introduce `DeliveryContent`, `AttestationEvidenceBundle`,
   `DeliveryVerificationMaterial`, `AgentSuite`, and the staged verifier
   transaction. Move v3 map/identity work behind the suite and keep signed
   relationship evaluation, generation, and apply outside it.
4. Introduce the common principal-policy evaluator. Make v3 enrollment/history
   and Sigstore certificates produce normalized credential facts and run the
   same policy tests.
5. Add a common stateful TUF trust updater. Publish profile, principal policy,
   v3 trust manifest, and Sigstore TrustedRoot as separate authenticated
   targets.
6. Implement the Sigstore agent suite using the existing bundle verifier and
   put both implementations through one attestation semantic contract.
7. Add crash injection around candidate TUF, journal, suite, generation, and
   apply state before treating the interface as viable.

The smallest useful first spike is steps 1-3 in `poc/trust-model-v3`: it tests
whether the proposed seam is real before porting all hybrid semantics or
introducing production storage.

## 18. Provisional decisions

These are recommendations for the first POC, not final product commitments:

1. Use common in-toto statement predicates and DSSE semantics for both suites.
2. Make each user, workload, or addon assertion an independently signed
   evidence item. Do not sign the aggregate FleetShift evidence bundle.
3. Require every evidence relationship to be expressed by, or
   deterministically verified from, signed statement content; aggregate indexes
   are lookup aids only.
4. Keep each official Sigstore Bundle whole; create a separate v3 continuity
   bundle rather than a fake common certificate/key-binding structure.
5. Use TUF for profile, policy, and low-churn trust material in every initial
   deployment.
6. Use one common per-tenant delivery journal for every suite.
7. Give each authorization domain exactly one active suite in v1.
8. Normalize verified mechanism evidence into credential facts, then share
   exact-one principal/anchor mapping and constraint evaluation.
9. Stage all verifier state and let the common delivery transaction commit it.
10. Keep suite control protocols typed and opaque to the core beyond transport,
    authorization, idempotency, and receipts.

## 19. Open questions for iteration

### Suite selection scope

Is the selection unit a provider, tenant, authorization domain, target
profile, or some combination? A tenant/authorization domain is the safest
initial unit. Provider defaults can instantiate that profile without becoming
an implicit target-side trust decision.

Platform addon identities complicate a strict tenant-wide selection. Should a
v3 tenant use v3 for users but a platform Sigstore authority for addons, or
must the v3 suite grow a native workload/addon profile first? The first version
should either require a cohesive suite or model an authenticated per-principal-
class rule explicitly; it must not fall through between verifiers.

### Common journal

Is the operational cost of journaling every durable delivery acceptable for
providers that select Sigstore? The proposed answer is yes because it also
unifies retry, rollback, and audit semantics, but the first POC should measure
append and proof costs independently of the suite.

Should the common journal be per tenant, or should lower-scope logs be anchored
into a tenant log? V3 rotation needs one ordering domain spanning every target
a key can authorize.

### TUF ownership and granularity

Does each tenant have an independent TUF root, or does a provider root delegate
tenant targets? Per-tenant roots maximize isolation; provider delegation may
be substantially easier to operate. Either shape must prevent the resource
manager from becoming the unilateral root authority.

How are common principal-policy changes authorized relative to suite-specific
trust-manifest changes? TUF roles and delegations may make the v3 trust
manifest's separate update-policy machinery smaller than currently designed.

### Signed evidence item details

Should a continuity bundle put identity/state selectors in DSSE `keyid`, in a
typed verification-material block, or both? They must remain lookup hints and
must be checked against authenticated history.

Do request authorization statements need a stable signed provenance-domain ID
in addition to tenant ID and predicate type to prevent cross-instance replay?
The provisional answer is yes.

What exact canonical statement encoding and cross-language vectors will be
normative? The current v3 Go JSON encoding is explicitly not sufficient.

### Stateful verification

Can TUF metadata, suite state, common journal state, and target generation be
stored in one transactional database in the fleetlet? If not, the durable
ordering protocol and recovery states need to be specified before production.

Should verification of a trust update be retained when the accompanying
delivery fails for a content-policy reason? This draft says no for simple
transactional reasoning. Retaining independently valid trust progress could
improve availability, but it requires a separate trust-sync operation with its
own acknowledgement semantics.

### Time, transparency, and history

Is RFC 3161 mandatory for the first Sigstore profile, or can a provider choose
online verification during certificate validity? Those are distinct profiles
with different historical-verification guarantees and should have different
suite protocol IDs or authenticated parameters.

Will the production Sigstore option require Rekor/CT, allow the current logless
profile, or expose both? The guarantee profile must clearly distinguish them.

How long must v3 event bodies, old Sigstore CA/TSA roots, bundles, and journal
proof material remain available for current fulfillment reconciliation?

### Migration and multiple signatures

What authorizes v3 -> Sigstore and Sigstore -> v3 migrations? Requiring one
valid signed evidence item under each suite over the same migration statement
is a strong starting point, but recovery and unavailable-old-suite cases need
explicit policy.

Should the common attestation model support several signatures over one
statement immediately? If so, threshold policy and suite mixing must be
evaluated as an authenticated rule, not as "any installed verifier accepts."

### Client UX

What does `AcquireSigner` return when user interaction or an external service
is unavailable: a synchronous error, a durable challenge, or a `PausedAuth`
continuation? The interface likely needs a challenge/result state before it is
made production-ready.

How narrowly can browser and addon signing APIs be scoped so the user sees the
tenant, target scope, action, and content identity at the actual signing
boundary for both suites?
