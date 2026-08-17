# Pluggable provenance suites

Status: design draft for discussion; interfaces remain provisional

This document proposes interfaces between FleetShift's stable attestation and
delivery semantics and an authenticated **provenance profile** composed from
one or more **provenance-suite routes**. It maps two initial suite
implementations:

- the continuity, authenticated-map, and ordered-log model from
  `docs/design/trust_model_v3.md` and `poc/trust-model-v3`; and
- the Fulcio, RFC 3161, Sigstore Bundle, and TUF model from
  `poc/attestation/sigstore_tuf_bundle`.

The attestation and authorization semantics come primarily from
`docs/design/authentication.md` and `poc/attestation/hybrid`. The hybrid POC is
not another provenance suite: it is the existing semantic model that the suite
abstractions must be able to carry, with wire-format, purpose-separation, and
strategy-selectable replay-hardening changes identified below.

The aim is not to make cryptography interchangeable at arbitrary individual
call sites. The aim is to let providers compose explicit trust domains—for
example, tenant user authority and provider workload authority—while keeping
FleetShift's authorization language, evidence relationships, delivery
contract, policy evaluation, and apply path the same.

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

which authority domain is expected to speak, how its signer obtained a key,
how that key was bound to an identity, how trust reached a verifier,
and how the resulting signature is verified
```

## 2. Goals

- Providers select an authenticated provenance profile without creating
  different fulfillment, managed-resource, placement, removal, or generation
  semantics. A profile may route tenant users and provider workloads through
  different suites without allowing evidence-controlled fallback.
- Every independently signed evidence item is retained and later delivered
  exactly as signed; the resource manager does not translate, combine, or
  re-sign those items.
- Clients use one high-level signing API even though acquiring a signer may
  mean loading a continuity key or performing OIDC + Fulcio issuance.
- Common resource-manager code assembles the FleetShift attestation evidence
  bundle, while each selected route assembles its additional verification
  material; v3 must construct map/history proofs and Sigstore usually does not.
- The delivery agent has one common attestation-verifier boundary. It receives
  normalized authenticated principals from selected route suites instead of
  inspecting public keys, certificates, or signer-selected labels itself.
- Trust configuration and ordered delivery reuse as much implementation as is
  defensible, while independently governed authority domains retain separate
  roots, policies, issuers, and verifier state.
- The common provenance data model stays deliberately small: an aggregate of
  media-typed signed items plus additional typed verification material. Initial
  suites may reuse the same standard format inside that extension point without
  making the core depend permanently on it.
- Route-specific suite state advances transactionally with the content delivery
  that consumes it. Independently authenticated trust updates advance through
  their own trust-delivery transaction.
- A new suite can be added by implementing bounded client, manager, and agent
  interfaces plus a conformance test contract.

## 3. Non-goals

- Dynamically downloading verifier code from the resource manager.
- Unstructured mixing or evidence-controlled fallback among cryptographic
  components. Each route selects one cohesive suite profile; explicit
  composition across authority domains is part of the authenticated provenance
  profile.
- Reproducing the resource manager's complete tenant/workspace authorization
  policy at every delivery agent.
- Treating a management-plane preflight verification as delivery authority.
- Making TUF a high-churn database of every identity key or delivery.
- Making a content delivery carry the trust update that causes that same
  delivery to become acceptable. Trust synchronization is a separate
  authenticated delivery operation.
- Claiming that a local append-only delivery log provides wall-clock time, public
  transparency, or global fork detection.
- Designing the first POC around user output signing. The primary model is
  intent signing; addon signatures over opaque outputs remain independently
  verified evidence rather than user re-approval of every rendered output.
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
  encoded as an immutable, media-typed `SignedEvidence` value. Additional
  dynamic proof material may travel separately. Both initial suites use a
  standard Sigstore Bundle for this per-item encoding, but the common type does
  not require that format. Despite its name, a Sigstore Bundle is not the
  aggregate FleetShift evidence bundle.
- **Attestation evidence bundle:** FleetShift's aggregate collection of the
  independently signed evidence items needed to verify one delivery. The
  resource manager assembles it over time from user requests, addon outputs,
  placement decisions, fulfillment relations, and later signed updates. The
  aggregate also carries the minimal, untrusted derivation recipe needed to
  navigate those items. It is not covered by one DSSE signature.
- **Attestation graph:** The root-input selector and deterministic derivation
  recipes within an attestation evidence bundle. It is execution structure,
  not authority: every edge must be confirmed from signed statements or by a
  deterministic transformation of verified content.
- **Delivery content:** The one concrete, typed action proposed for a target,
  such as `PutManifests` or `RemoveByFulfillmentId`, plus delivery routing and
  generation fields. It is present once in the delivery package and is the
  candidate output evaluated against the verified root input.
- **Delivery verification material:** Replaceable common and route-specific
  proof material needed to verify one content delivery, such as a delivery-log
  proof or v3 map/history proofs. It is neither signed content evidence nor a
  trust update.
- **Verifiable relationship:** A relationship among evidence items or delivery
  content whose identity-affecting fields are carried by a signed statement or
  are deterministically derived from signed statements. Outer references and
  indexes may locate evidence, but do not make a relationship authoritative.
- **Authority domain:** An independently governed namespace of principals,
  issuers, policy, roots, and verifier state, such as `tenant:acme/users` or
  `provider:fleetshift/workloads`. This is broader than, and must not be
  confused with, a SPIFFE trust-domain string.
- **Provenance domain:** A stable FleetShift installation/security namespace
  bound into signed statements and delivery commitments to prevent valid
  evidence from being replayed into another FleetShift instance or protocol
  domain. It is not a signer authority or verifier selector.
- **Provenance profile:** Authenticated target configuration that composes
  exact routes by expected authority domain, principal class, and evidence
  kind.
- **Provenance route:** One profile entry selecting a cohesive suite protocol,
  accepted evidence representation, trust policy, and verification-material
  type for a bounded set of expected producers and evidence kinds.
- **Provenance suite:** A versioned implementation of signer acquisition,
  identity lifecycle, signed-evidence construction, identity/key binding,
  suite verification-material assembly, and target-side cryptographic
  verification. A suite is instantiated for a provenance route; it is not
  necessarily the sole suite used by a delivery.
- **Trust distribution:** The authenticated update mechanism for relatively
  low-churn profile, trust roots, and verification policy. The initial
  implementation is TUF, transported as a first-class trust delivery rather
  than embedded in content verification material.
- **Trust delivery:** A separately verified and acknowledged meta-delivery that
  advances an authority domain's trusted TUF state. TUF metadata signatures,
  thresholds, versions, expiry, and root rotation authenticate the update.
- **Delivery log:** FleetShift's cryptographic, scoped append-only sequence of
  content, trust, and suite-control commitments. Delivery agents retain local
  checkpoints. This is the v3 delivery log generalized into shared
  infrastructure; the exact tenant/provider/target-group scope remains open.
  It is distinct from the journal in the
  [target delivery protocol](architecture/target_delivery_contract.md#journaling),
  which records target-side work and recovery state.
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
 tenant-user route     provider-workload route  provider-workload route  selected route
    \_____________________|_________________________/_____________________/
                              |
                              v
resource manager assembles, but does not sign as a whole:
    one concrete delivery action
    + FleetShift attestation evidence bundle {graph, A, B, C, D, ...}
    + delivery-log and per-route verification support
                              |
                              v
delivery agent:
    authenticated profile selects exactly one route for each expected role
    selected suite instance verifies each signed evidence item independently
    common verifier follows only relationships proven by signed content
        or deterministic policy over verified statements and authoritative facts
    constraints -> placement/removal -> generation -> durable apply
```

The suite boundary is deliberately below attestation semantics and above raw
cryptography. A suite does not decide whether a manifest matches an inline
strategy, whether removal is permitted, or whether a generation is current.
It decides whether each particular statement is authentically signed by a
principal recognized under one selected authority domain's accepted trust
state. The common verifier decides which producer role is required and asks
the authenticated profile for that role's single route. There is no assumption
that one producer saw, assembled, or signed the complete evidence set.

### 5.1 Common versus suite-owned responsibilities

| Concern                                         | Common FleetShift code          | Provenance suite                  |
| ----------------------------------------------- | ------------------------------- | --------------------------------- |
| Authorization statement schemas                 | Yes                             | No                                |
| DSSE/in-toto semantic profile                   | Yes                             | Encodes each signed evidence item |
| Evidence relationships and derivation           | Yes                             | No                                |
| CEL and strategy-implied constraints            | Yes                             | No                                |
| Put/remove and generation fencing               | Yes                             | No                                |
| Normal API authorization                        | Yes                             | No                                |
| Expected producer role and route resolution     | Profile-driven                  | No fallback                       |
| Signer acquisition                              | No                              | Yes                               |
| Enrollment/rotation/recovery protocol           | Transport only                  | Yes                               |
| Signed-evidence media type                      | Registry and routing            | Yes                               |
| Cryptographic signature verification            | No                              | Yes                               |
| Credential-to-external-identity proof           | No                              | Yes                               |
| Credential facts -> FleetShift principal policy | Prefer common                   | Supplies verified facts           |
| Low-churn trust delivery                        | Common interface; initially TUF | Defines/parses its targets        |
| Durable delivery commitment ordering            | Yes                             | May add namespaced control events |
| Per-route suite proof construction              | No                              | Yes                               |
| Per-route verifier checkpoint                   | Opaque storage and transport    | Defines and validates             |
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
shared profile should retain and tighten these predicate families:

- `delivery-authorization/v1` for signed input, validity constraints, explicit
  output constraints, and optional expected generation;
- `manifest-set/v1` for exact addon-produced manifests and any input, request,
  or target binding required by the signed manifest strategy;
- `placement/v1` for an addon placement decision and its fulfillment or owner-
  resource/content scope;
- `fulfillment-relation/v1` for a managed-resource fulfillment relation; and
- a purpose-specific derivation predicate (name to be chosen) for an externally
  produced `spec_update`.

> [!NOTE]
> The last item is a small wire-format tightening of the hybrid model, not a new
> update-authorization model. Hybrid already has the addon sign a manifest
> envelope whose `resource_type` is `spec_update`. The shared profile should
> express that same assertion as a derivation predicate so it cannot be confused
> with deployable manifests. The predicate is the payload of the addon's existing
> signature and covers the transformation, preconditions, and any binding
> required by the selected strategy; it does not require another signature.
> Inline derivations need no separate addon evidence item.

Update authorization, applicability, placement, and generation semantics remain
those described in `docs/design/authentication.md` and demonstrated by
`poc/attestation/hybrid`. This document only requires their signed assertions
to be purpose-separated evidence items that both suite implementations can
verify. Every item binds the provenance domain, tenant, evidence kind, and its
semantic content or relationship, and it never selects its own trusted anchor.

### 6.2 DSSE, in-toto, and per-item suite representation

The formats serve different layers for each independently signed item:

```text
in-toto Statement
    semantic assertion and signed relationships
        |
        v
DSSE envelope
    payload type + statement bytes + producer's signature
        |
        v
signed-evidence representation
    DSSE envelope + material needed to verify that one signature
        |
        v
SignedEvidence
    media type + immutable serialized representation
```

The DSSE signature is produced by that evidence item's user, workload, or
addon signing session at the time the statement is created. It covers only the
typed in-toto statement in that envelope—not later evidence and not the final
FleetShift aggregate.

Both initial suites use one standard Sigstore Bundle v0.3 as the representation
of each signed evidence item. They select different standard verification-
material forms inside it:

- The Sigstore keyless suite uses a Fulcio leaf certificate, RFC 3161
  timestamp, and any configured transparency evidence.
- The v3 suite uses the Bundle's public-key identifier form. Its hint and the
  DSSE signature's matching `keyid` select the signing-state proof and key, but
  grant no authority themselves. Dynamic authenticated-map, key-history,
  rotation, and delivery proofs travel in per-route delivery verification
  material and may be shared by many items.

The authenticated provenance route, not the Bundle, determines which form is
valid. The initial v3 profile rejects certificate-based Bundle verification
material; the initial Sigstore keyless profile rejects a public-key-only
Bundle. Sharing the media type must not become an implicit fallback between
trust mechanisms.

“Bundle” in the Sigstore type name does not mean that it contains the other
FleetShift attestations for the delivery. It contains one independently signed
item and the stable material or selector needed to begin verifying that item.

The common layer treats every representation as immutable typed bytes:

```text
SignedEvidence {
    media_type
    bytes
}
```

Both initial suites emit:

```text
application/vnd.dev.sigstore.bundle.v0.3+json
```

This is an initial profile rule, not a permanent restriction on the extension
point. A future suite may register another signed-evidence media type if a new
standard or trust mechanism does not fit the Sigstore Bundle model. The common
layer stores, digests, bounds, and routes the typed bytes; only the selected
suite parses their format. The common verifier sees the resulting
`VerifiedStatement`, not certificates, public-key hints, or format-specific
proofs.

The resource manager stores each item by a domain-separated digest over both
`media_type` and the exact `bytes`, then later delivers the same typed value.
The media type is part of the identity because changing parsers must not
preserve an item's digest. When a format also carries an internal media type,
as Sigstore Bundle does, the suite requires it to match the outer value. The
manager may add more independently signed items as fulfillment evolves, but it
never translates an existing signature or wraps the aggregate evidence set in
a manager-generated provenance signature.

The suite parses and verifies one item's bytes and returns its common in-toto
statement. Trying to normalize certificates, continuity state, timestamps,
Rekor entries, and future transparency proofs into one universal `Signature`
struct would leak every implementation into the common verifier. Each standard
Sigstore Bundle should therefore remain intact rather than becoming a custom
almost-Sigstore wire format. Reuse happens inside the extension point because
both initial suite implementations can share Bundle and DSSE parsing without
making those details part of the core abstraction.

DSSE itself permits multiple signatures over one payload. The initial
Sigstore Bundle v0.3 profile nevertheless requires exactly one DSSE signature
per Bundle because one Bundle carries one signer's verification-material set.
Several endorsements therefore travel as several `SignedEvidence` items,
normally with identical canonical statement bytes and one signer each. Common
policy groups them by the canonical statement digest and evaluates threshold or
principal-class requirements; it never interprets “any installed verifier
accepts” as a quorum.

Repeating a small statement in several Bundles is acceptable for the initial
profile and compresses well at the aggregate transport layer. If large
statements make that material, a future signed endorsement predicate may refer
to the canonical statement digest, payload type, purpose, and provenance
domain. An unsigned pointer to another Bundle is insufficient, and a reference
to the entire Bundle digest is unnecessarily brittle because it also binds
certificate, timestamp, signature, and serialization details. A future native
multi-signature evidence media type remains possible without changing the
aggregate abstraction.

This deliberately accepts low-single-KiB representation overhead in exchange
for using a complete standard Bundle. In the current POC samples, a lean custom
continuity representation could save roughly 1.4 KiB per item, while a keyless
Bundle containing a 770-byte statement was about 4.2 KiB. Replacing a repeated
statement with a signed digest endorsement saved only about 0.6 KiB before
compression and was slightly larger after aggregate gzip in that sample. These
figures are illustrative rather than protocol limits, but they support keeping
the standard format intact until realistic payloads show a material problem.

### 6.3 Verifiable relationships and derived input

The hybrid model demonstrates that an aggregate needs a little more than a bag
of signed bytes. `SignedInput` and `DerivedInput` form an assembly recipe: a
derived input identifies a prior input and an independently authorized update
whose result is applied deterministically. That recipe is necessary to execute
verification, but it is not itself provenance authority.

The common representation therefore has a minimal `AttestationGraph`:

```text
AttestationGraph {
    root_input_id

    inputs {
        input_id -> SignedInputRef {
            authorization_evidence_digest
        }

        input_id -> DerivedInput {
            prior_content_id
            prior_content_type
            prior_input_id
            update_authorization_input_id
            derivation_evidence_digest? // external addon output only
        }
    }
}
```

The two variants above are a tagged union. Their IDs and references are
untrusted assembly selectors, not authority. Common code resolves them to
verified, purpose-typed statements and then applies the existing hybrid
`SignedInput`/`DerivedInput` semantics. It also checks that the declared prior
content identity matches the resolved prior. An external derivation reference
is required only when the verified strategy expects addon-produced output and
is forbidden for an inline derivation. Cycles, missing or ambiguous references,
and excessive depth fail closed.

This graph deliberately has no root `output` node. The content-delivery package
already contains exactly one concrete action. Verification is conceptually:

```text
verify(attestation_graph.root_input, content_delivery.delivery, signed_evidence)
```

The current hybrid `Attestation { input, output }` remains a useful internal
verification view, but its `output` is the already decoded package delivery;
it is not a second wire copy. For a direct delivery the graph contains one
`SignedInputRef`. Only updates need `DerivedInput` recipes.

The bundle may also contain optional lookup indexes—for example predicate type,
semantic content ID, statement digest, or `(addon_id, resource_type)` to signed-
item digests. Indexes are reconstructable acceleration structures. The
verifier checks every result against the decoded verified statement, rejects
ambiguous matches, and may simply scan a bounded bundle in the first POC.

Examples of accepted relationships are:

| Relationship                                       | Authenticated or deterministic source                                                                                                                  |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| User authorization applies to input content        | Subject and predicate of the verified `delivery-authorization/v1` statement                                                                            |
| Addon manifests are permitted for an input         | User-signed manifest strategy joined with the verified `manifest-set/v1` producer and content; exact input binding only when that strategy requires it |
| Placement applies to a fulfillment or content item | User-signed static/selector scope or delegated placement strategy joined with verified `placement/v1` evidence and authoritative target facts          |
| Addon can fulfill a managed-resource type          | Verified `fulfillment-relation/v1` binding relation type, addon ID, resource and manifest type                                                         |
| Update may operate on prior content                | Existing hybrid derivation verification over the referenced, purpose-typed evidence                                                                    |
| Concrete delivery follows from root input          | Common constraint evaluation over the verified graph, evidence, and one package action                                                                 |

The graph can nominate an authorization chain but cannot add authority to it.
Every selected relationship must still follow from verified statements or the
deterministic hybrid evaluator; changing a graph reference cannot fill a
missing signed relationship or strategy requirement.

The initial profile should require every signed item in the aggregate to be
consumed by one recognized graph/evidence role or by an explicit endorsement
rule. Unreachable extras are rejected rather than ignored. This preserves the
promise that every delivered item is independently verified and prevents an
attacker from hiding unrouteable or malformed material inside a logged
aggregate.

### 6.4 Aggregate evidence bundle and delivery package

The FleetShift attestation evidence bundle is an aggregate of independently
signed evidence items. It has no aggregate DSSE signature:

```text
AttestationEvidenceBundle {
    graph: AttestationGraph
    signed_items {
        item_digest -> SignedEvidence
    }
    lookup_indexes // optional, non-authoritative
}
```

Every map key must equal the domain-separated digest of the exact
`SignedEvidence` media type and bytes. The canonical aggregate digest covers
the graph and the sorted signed-item digests, media types, and exact bytes.
Semantic IDs inside verified statements remain distinct from storage digests;
lookup indexes may map semantic IDs to candidate item digests, but are excluded
from the aggregate digest because they can be reconstructed and the verifier
checks every mapping against decoded verified statements.

Within the provenance portion of a delivery, there are two top-level payloads:

1. `AttestationEvidenceBundle`, the aggregate of independently signed items;
   and
2. `DeliveryVerificationMaterial`, the additional common and route-specific
   support needed to verify those items for this delivery.

The complete content-delivery package keeps those two provenance payloads
separate from the applicable action:

```text
ContentDelivery {
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
    action:
        PutManifests {
            manifests[]
        }
      | RemoveByFulfillmentId {
            fulfillment_id
        }
}

DeliveryVerificationMaterial {
    required_trust_state {
        profile_digest
        authority_domains {
            authority_domain_id -> required_state
        }
    }
    delivery_log_proof
    routes {
        route_id -> TypedBytes
    }
}
```

`DeliveryContent` is what the target may apply. `AttestationEvidenceBundle` is
the server-assembled set of independently signed statements used to decide
whether that content is authorized. `DeliveryVerificationMaterial` supplies
replaceable ordering, identity-history, or transparency proofs needed to verify
the signed items against already accepted trust; it is not content attestation
evidence itself.
Some per-item verification material remains inside a signed-evidence format,
such as the certificate and timestamp in a Sigstore keyless Bundle or the
public-key hint in a v3 Bundle. `DeliveryVerificationMaterial` holds additional
material that is delivery- or route-wide, shareable, replaceable, or not
representable in that per-item format. There is no generic `concrete_output`:
the tagged action is the concrete output. For a put, it is the ordered manifest
envelopes; for a remove, it is the fulfillment identity to remove. The
fulfillment ID in `RemoveByFulfillmentId` must equal the enclosing
`DeliveryContent.fulfillment_id`.

`fulfillment_id` names the internal orchestration and delivery lineage common
to all owner-resource types. A user-facing `Deployment` still exists, but it is
a thin resource that owns and points to a Fulfillment, just as a managed
resource or future campaign may own one. User-facing resource identity belongs
in the verified input semantics; delivery generation, placement, application,
and removal are scoped to the resolved Fulfillment. Verification must not
silently equate a `DeploymentID` or another owner-resource ID with a
`FulfillmentID`.

`target_id` is routing and correlation data, not placement evidence. The agent
establishes its own target identity from local authenticated state, requires
the outer target to match it, and then evaluates the verified placement
predicate, signed placement statement, or fulfillment relation against that
local identity. Editing the outer `target_id` cannot authorize placement.

`required_trust_state` does not provide trust bytes or select a verifier. It
states the profile digest and authority-domain state against which the manager
assembled the delivery. The agent compares it to authenticated local state and
may require a prior trust delivery; it never rolls back or switches profiles to
satisfy package metadata. Route material is keyed by profile route only for
lookup. The locally accepted profile determines the suite and required media
type, and all material remains untrusted until that route verifies it.

A TUF update is intentionally absent. Trust updates are independently
authenticated meta-deliveries described in Section 9, rather than
`DeliveryVerificationMaterial` that a content delivery can use to establish
its own trust policy.

The delivery log commits the digest of `DeliveryContent` and the canonical
attestation-evidence digest covering the exact graph and signed items.
Reconstructable lookup indexes are excluded.
That commitment proves exact delivery ordering and prevents later substitution
relative to an agent's checkpoint, but it does not make the resource manager an
attestation signer. Reconstructable delivery-log and route proofs are likewise
not included because they may be refreshed without changing the delivery or
its signed evidence.

The common verifier must compare outer routing and delivery fields to the
cryptographically verified statements. An outer tenant, target, fulfillment,
action, generation, root selector, reference, or lookup index is never
authoritative on its own.

Package route IDs are diagnostic selectors, not trust decisions. The agent
loads the authenticated profile, resolves exactly one route for each expected
producer role, and requires package media types and material to match.

### 6.5 Concrete intent-signed put

Consider an addon-rendered, addon-placed user-facing `Deployment` named
`cluster-01`. It owns Fulfillment `fulfillment-cluster-01`, which is the object
actually reconciled and delivered:

```text
ContentDelivery
├── delivery
│   ├── tenant_id: tenant-a
│   ├── target_id: cluster-prod-1
│   ├── fulfillment_id: fulfillment-cluster-01
│   ├── generation: 4
│   └── action: PutManifests
│       └── manifests: [Cluster, MachineDeployment, ...]
├── attestations
│   ├── graph.root_input_id: input-4
│   ├── graph.inputs.input-4: SignedInputRef -> digest(A)
│   └── signed_items
│       ├── digest(A) -> Alice's delivery-authorization/v1
│       ├── digest(M) -> capi-provisioner's manifest-set/v1
│       └── digest(P) -> capacity-planner's placement/v1
└── verification
    ├── required trust state for tenant users and provider workloads
    ├── delivery-log proof
    └── route material, if required
```

`A` says that Alice authorizes the user-facing `Deployment` `cluster-01`, whose
verified input resolves to Fulfillment `fulfillment-cluster-01` at generation
4, using `capi-provisioner` for manifests and `capacity-planner` for placement,
subject to its validity and output constraints. In this example, Alice's
manifest strategy requires input-specific rendering, so `M` signs the exact
ordered manifests and binds them to the resolved input or revision it fulfills.
A strategy that deliberately authorizes reusable addon output would not require
that extra binding. `P` signs `fulfillment_id = fulfillment-cluster-01` and the
allowed targets `[cluster-prod-1, cluster-prod-2]`.

The target verifies `A` through the tenant-user route and `M` and `P` through
the provider-workload routes. Those routes may use different authorities,
policies, issuers, or suites. Common code then verifies that the actual
principals and authority domains match the addons Alice authorized and their
authenticated registrations, `M` exactly matches the one `PutManifests` action
and satisfies the input-specific strategy, `P` applies to Fulfillment
`fulfillment-cluster-01`, the locally established target is in `P`, and all
constraints and generation checks pass. The outer graph and indexes only
nominate candidates. Replacing a manifest, input reference, placement item, or
target either fails a required signed binding/deterministic check or selects
another output that the signed strategy deliberately authorizes.

## 7. Provenance profile and route selection

An authorized operator configures one authenticated provenance profile for a
target or target class. The profile composes independently governed authority
domains and selects exactly one verifier route for every supported combination
of expected principal class and evidence kind:

```text
ProvenanceProfile {
    profile_id
    version

    authority_domains {
        authority_domain_id -> {
            trust_updater_id
            principal_policy_target
        }
    }

    routes {
        route_id -> {
            authority_domain_id
            applies_to[] {
                principal_class
                evidence_kind
            }

            suite_id
            suite_protocol_version
            accepted_evidence_media_types[]
            verification_material_media_type

            suite_target_paths[]
        }
    }

    delivery_log_profile
}
```

For example, a target may use a tenant-controlled v3 or Fulcio route for user
authorizations and a provider-controlled SPIFFE, Fulcio, or future workload
route for manifest and placement addons. Different tenants can name different
user authority domains even when they use the same suite implementation. The
same tenant can—and normally will—use different routes for people and
workloads because their issuers, lifecycle, policy, and software differ.

The FleetShift default profile instantiates the continuity/v3 suite for every
producer role that its initial v3 implementation supports. Selecting keyless
Sigstore or another workload mechanism requires an authenticated profile
change; merely presenting that evidence never enables it.

The profile separates ownership from mechanism: authority-domain entries name
independently advanced trust and principal policy, while routes choose a suite
for particular uses of that authority. The profile and every referenced state
are authenticated from provisioned state and later trust deliveries. The
profile is not accepted merely because it arrived in a content delivery.

`applies_to` enumerates allowed `(principal_class, evidence_kind)` pairs rather
than taking the Cartesian product of two allowlists. This prevents a route
trusted for user authorizations and addon manifests from accidentally becoming
trusted for user-produced manifests or addon-produced authorizations.

The suite ID names a protocol, not a code package or running installation. For
example:

```text
fleetshift.dev/provenance/continuity/v1
fleetshift.dev/provenance/sigstore-keyless/v1
```

All relevant binaries have a static registry keyed by suite ID. Providers may
compile or deploy only the suites they support. Unknown suites fail closed.
Code is never loaded from the couriered package.

Route resolution is driven by expected semantics, never by trying package
evidence against several verifiers:

1. The common engine determines the evidence kind and expected producer role
   from the operation being verified, the root context, or an already verified
   parent statement.
2. The authenticated profile must resolve that expectation to exactly one
   route. Zero or multiple applicable routes fail.
3. That route's suite verifies the signed evidence and emits credential facts.
4. Common principal policy maps those facts and requires the resulting
   authority domain, principal class, and stable identity to match the expected
   producer.

For the root authorization, expected tenant and producer class come from the
target's locally accepted profile and API/delivery contract, not from an
untrusted signer claim. Once the root verifies, its signed strategies can
establish the expected identities and evidence kinds for addon outputs,
placement, relations, and updates.

A signed strategy names the stable FleetShift addon or producer it delegates
to; it does not name a trust anchor. Authenticated addon registration and
profile policy map that stable identity to its expected authority domain and
principal class. The profile then resolves that expectation to a route, and
the accepted endorsement set must satisfy policy for that same stable
principal. This prevents two authority domains that use the same
human-readable addon ID from becoming interchangeable.

A package may carry route IDs to make lookup efficient, but an item cannot
select its verifier by declaring an authority, class, suite, or trusted anchor.
Failure under the resolved route is terminal; common code never falls through
to another route.

### 7.1 Migration

Changing the route for any authority domain, principal class, or evidence kind
is a trust-root and policy transition, not an ordinary content edit. An
established delivery agent must accept a migration only through one of:

- a transition authorized under the currently accepted profile and the
  successor profile;
- a previously configured higher-level tenant/provider authority; or
- an explicit out-of-band recovery/reprovisioning ceremony.

The resource manager cannot turn a failed route verification into a retry
under another suite. During a staged migration, a trusted profile may require
endorsements under both old and new routes over the same canonical statement
digest, but the dual requirement is explicit policy—not fallback behavior.

## 8. Concrete interfaces

The interfaces below are Go-like pseudocode. They describe process boundaries
and ownership; they are not yet proposed production Go APIs.

### 8.1 Shared types

```go
type SuiteID string
type RouteID string
type AuthorityDomainID string

type TypedBytes struct {
    MediaType string
    Bytes     []byte
}

// authorization, manifest, derivation, placement, relation, ...
type EvidenceKind string

type UnsignedStatement struct {
    PredicateType string
    Statement     []byte // canonical in-toto Statement bytes
}

type SignedEvidence struct {
    MediaType string
    Bytes     []byte
}

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
    ID                string
    Class             string // user, workload, addon, configuration-authority, ...
    AuthorityDomainID AuthorityDomainID
    AnchorID          string
    Attributes        map[string]Value
}

type VerifiedEndorsement struct {
    Principal AuthenticatedPrincipal
    Facts     CredentialFacts
}

type VerifiedStatement struct {
    PredicateType   string
    Statement       []byte
    StatementDigest Digest
    Endorsements    []VerifiedEndorsement
}

type VerifierCheckpoint struct {
    ProfileDigest Digest
    TrustStates   map[AuthorityDomainID]TypedBytes // owned by trust updaters
    DeliveryLog   DeliveryLogCheckpoint
    RouteStates   map[RouteID]TypedBytes           // opaque to common code
}
```

`CredentialFacts` are mechanism facts, not an open authorization surface.
Implementations register typed fact schemas per suite/version. Policy may
expose a bounded projection to CEL, but arbitrary suite-provided attributes
must not silently become trusted authorization claims.

`StatementDigest` is distinct from the signed-evidence-item digest. It
domain-separately commits the verified DSSE payload type and exact canonical
statement bytes, so common policy can recognize several endorsements of the
same assertion without binding their certificates, timestamps, signatures, or
Bundle serialization.

An initial standard Bundle yields exactly one `VerifiedEndorsement`. Keeping
endorsements as a list lets a future registered media type carry several
cryptographically verified signatures over one statement without changing the
aggregate or verification-session interface. Common threshold policy combines
and de-duplicates endorsements across items by `StatementDigest`. Signatures
that require different routes remain separate items because no one route may
verify on another authority domain's behalf.

`VerifierCheckpoint` is the common target report and durable-state envelope.
Common code validates its profile, trust, and delivery-log portions and passes
only the selected opaque route state to the corresponding suite instance.

### 8.2 Suite registry

The client, resource manager, and delivery agent do not necessarily ship in
one binary, so each role has its own factory registry rather than one giant
`ProvenanceSuite` object.

```go
type SuiteDescriptor struct {
    ID                           SuiteID
    ProtocolVersion              string
    AcceptedEvidenceMediaTypes   []string
    VerificationMaterialType     string
    RouteStateType               string
}

type ClientSuiteFactory interface {
    Descriptor() SuiteDescriptor
    Open(ClientDependencies, TrustedRoute) (ClientSuite, error)
}

type ManagerSuiteFactory interface {
    Descriptor() SuiteDescriptor
    Open(ManagerDependencies, TrustedRoute) (ManagerSuite, error)
}

type AgentSuiteFactory interface {
    Descriptor() SuiteDescriptor
    Open(AgentDependencies, TrustedRoute) (AgentSuite, error)
}
```

Descriptor capabilities are useful for compatibility checks and UX. They are
not a substitute for authenticated policy and are not consulted to weaken a
failed security check. `TrustedRoute` is the already authenticated projection
of one `ProvenanceProfile.routes` entry plus its accepted trust targets; it is
not constructed from content-delivery metadata.

### 8.3 Client signing interface

```go
type SignerRequest struct {
    ProfileID         string
    RouteID           RouteID
    AuthorityDomainID AuthorityDomainID
    TenantID          string
    PrincipalClass    string
    EvidenceKind      EvidenceKind
    Interactive       bool
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
    Sign(context.Context, UnsignedStatement) (SignedEvidence, error)
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
    RouteID           RouteID
    AuthorityDomainID AuthorityDomainID
    SuiteID           SuiteID
    Kind              string
    OperationID       string
    Payload           TypedBytes
}
```

`AcquireSigner` owns the entire key-acquisition ceremony. It receives narrow
OIDC, secure-key-store, WebAuthn, and user-presentation dependencies; it does
not return private-key bytes. Addon code and mutable UI plugins never receive
a general-purpose signing handle. The returned session is scoped to the
requested authority domain, tenant, principal class, and evidence kind, and
rejects a statement whose signed context does not match that scope.
`PrincipalHint` is only for UX and evidence lookup; it never grants identity or
authority.

Common client code resolves `RouteID` from an authenticated profile before
opening the suite. A mutable request or signer cannot redirect a user signing
operation into a provider-workload route, even if both happen to use the same
Bundle media type.

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
    // Nil means this operation does not need a delivery-log serialization point.
    DeliveryLogCommitment() *SuiteControlCommitment

    // Finalize is idempotent. When DeliveryLogCommitment was non-nil, record is
    // the exact common delivery-log record assigned to that commitment.
    Finalize(context.Context, *DeliveryLogRecord) (SuiteControlReceipt, error)
    Close() error
}

type VerificationMaterialAssembly struct {
    RouteID                   RouteID
    RouteVerificationMaterial TypedBytes
    DeliveryLogSelectors      []DeliveryLogSelector
}

type VerificationMaterialAssemblyRequest struct {
    RouteID                  RouteID
    Delivery                 DeliveryContent
    DeliveryDigest           Digest
    AttestationBundleDigest  Digest
    Attestations             AttestationEvidenceBundle
    RouteEvidenceDigests     []Digest
    DeliveryLogRecord        DeliveryLogRecord
    CurrentRouteState        TypedBytes
    TrustedRoute             TrustedRoute
}

type RequestVerificationRequest struct {
    RouteID           RouteID
    AuthorityDomainID AuthorityDomainID
    Evidence          SignedEvidence
    EvidenceKind      EvidenceKind
    TenantID          string
    TrustedRoute      TrustedRoute
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
that needs a common delivery-log serialization point. Common code appends it
and passes the exact assigned record to idempotent `Finalize`; v3 uses that
record in the key event that completes rotation. A crash after append but before
finalization leaves an inert marker and resumes the durable workflow on retry.
A suite cannot use successful cryptographic validation to bypass ordinary
FleetShift authorization.

`AssembleVerificationMaterial` is called once for each route used by the
delivery. It is proof construction, not authorization or attestation
construction. Common code has already assembled the independently signed
evidence items and resolved their expected routes. The suite may read
authenticated-map nodes, key histories, or native transparency services to
build the additional proof material needed to verify those exact items. It
returns route verification material plus the suite-control records that common
delivery-log code must disclose alongside the delivery commitment. This
material and its selectors are untrusted until the common delivery-log verifier
and selected agent-suite instance cross-check them.

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
    RouteID                   RouteID
    AuthorityDomainID         AuthorityDomainID
    Delivery                  DeliveryContent
    Attestations              AttestationEvidenceBundle
    RouteEvidenceDigests      []Digest
    RouteVerificationMaterial TypedBytes
    TrustedRoute              TrustedRoute
    DeliveryLogView           VerifiedDeliveryLogView
    CurrentRouteState         TypedBytes
}

type ExpectedEvidence struct {
    Kind                    EvidenceKind
    AuthorityDomainID       AuthorityDomainID
    AllowedPrincipalClasses []string
}

type VerificationSession interface {
    VerifyEvidenceItem(
        context.Context,
        SignedEvidence,
        ExpectedEvidence,
    ) (VerifiedStatement, *VerificationResult, error)

    CandidateState() TypedBytes
    Close() error
}
```

`BeginVerification` performs route-wide work once. For v3 this includes
advancing the authenticated map, validating new key events or recording
exceptions in a candidate state, and indexing selectively supplied identity
proofs. For Sigstore it parses accepted roots/policy and prepares verification
of the Sigstore Bundles assigned to that route.

The common attestation engine resolves an expected role to one route before it
calls `VerifyEvidenceItem`. Each call returns the same normalized
`VerifiedStatement` shape. The engine follows the `AttestationGraph`, validates
every graph edge against the verified statements, and evaluates derivation,
placement, output constraints, removal, and generation without branching on
suite ID. It additionally requires every accepted endorsement's authority
domain and principal class to satisfy `ExpectedEvidence`; suite success alone
is not semantic authorization.

Each route's `CandidateState` is never persisted merely because suite
cryptography passed. All candidate route states are committed only after the
entire common attestation verification succeeds and the content-delivery
contract has durably recorded apply or pending work. Trust-updater state is not
part of this candidate set; it advances through a separate `TrustDelivery`.

### 8.6 Identity policy boundary

The suites should share the mapping from verified mechanism facts to
FleetShift principals where possible:

```go
type PrincipalPolicy interface {
    Authenticate(
        context.Context,
        TrustedRoute,
        CredentialFacts,
        EvidenceKind,
        VerifiedPredicate,
    ) (AuthenticatedPrincipal, *VerificationResult, error)
}
```

An authenticated policy rule can match:

- authority type and ID;
- authority-domain ID selected by the route;
- external issuer and exact subject or constrained subject pattern;
- principal class and evidence kind;
- stable key/transparency namespace where relevant;
- anchor attributes and CEL constraints over the verified predicate.

It then produces the stable FleetShift principal ID and repeats the route's
authority-domain and principal-class constraints in its result. Zero matches
and multiple matches fail closed. This is the generalized form of the Sigstore
POC's `_authenticate_identity` and the hybrid POC's
`TrustAnchor.verify_signer`.

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

The first implementation should use a stateful TUF updater for:

- the provenance profile and route selection;
- each authority domain's principal/anchor policy;
- each route's suite trust roots and parameters;
- trust-root and policy rotation with rollback/freeze protection.

The provisioned bootstrap for each independently rooted trust repository is a
complete TUF root or a stronger out-of-band anchor. A later `TrustDelivery` may
courier newer root metadata in a valid TUF root chain, but it can never supply
the bootstrap root for an established agent.

Using TUF here means reusing its metadata, updater, and role semantics; it does
not require a separately deployed TUF service. The resource manager can store
and courier repository bytes through its existing database and transport. The
signing authority for those bytes must still follow the applicable tenant or
provider root policy and cannot collapse into an unrestricted resource-manager
key. This keeps the default v3 installation within the existing component set
while using an off-the-shelf trust-update protocol.

TUF is not used for every v3 enrollment, continuity rotation, delivery
commitment, or future key-transparency leaf. Those are high-churn suite or
delivery-log data with their own authenticated proofs.

Trust updates are their own authenticated meta-deliveries:

```text
TrustDelivery {
    authority_domain_id
    previous_trust_state_digest
    update: TypedBytes // initially a complete TUF refresh set
}
```

`previous_trust_state_digest` is an ordering/idempotency precondition, not the
source of trust. The updater authenticates root rotation and current metadata
using its retained state, then enforces TUF thresholds, versions, expiry,
rollback, freeze, snapshot, and target-hash rules. The TUF metadata signatures
are sufficient for target-side trust-update authentication; wrapping the same
metadata in DSSE would not add authority. Provenance for the administrative
act of publishing a target is a separate control-plane concern.

`authority_domain_id` is likewise only a selector for locally pinned updater
state. The verified repository identity, delegated target namespace, and
resulting profile must all bind that same domain; relabeling an update from one
domain to another either fails those checks or resolves to the identical
already-authorized state.

The code should still depend on a small interface:

```go
type TrustUpdater interface {
    Stage(
        context.Context,
        CurrentTrustState,
        TrustDelivery,
    ) (CandidateTrustState, TrustedTargetSet, *VerificationResult, error)
}
```

The initial and expected implementation is TUF. Keeping the interface separate
allows a future environment with a genuinely equivalent authenticated updater
without changing suite or attestation APIs. The selected updater for an
authority domain is itself pinned by authenticated state; it is not chosen by
the trust or content delivery.

### 9.2 Initial TUF targets

Profile targets:

```text
fleetshift/profiles/<profile-id>.json
```

Authority-domain targets:

```text
fleetshift/authorities/<authority-domain-id>/principal-policy.json
```

V3 suite targets:

```text
fleetshift/authorities/<authority-domain-id>/suites/continuity-v1/trust-manifest.json
```

The trust manifest carries OIDC enrollment issuer/client, accepted algorithms,
map and delivery-log parameters, recovery constraints, and trust-update policy
that is not already represented by TUF roles.

Sigstore suite targets:

```text
fleetshift/authorities/<authority-domain-id>/suites/sigstore-keyless-v1/trusted-root.json
```

This is the standard Sigstore `TrustedRoot` containing the applicable Fulcio,
TSA, Rekor, and CT authorities. The common principal policy replaces the POC's
suite-local FleetShift identity-mapping target if the normalized policy proves
sufficient. A provider TUF root may delegate tenant and provider authority
namespaces, or independently provisioned domains may use separate roots; the
route and delivery abstractions do not require them to share an operator.

### 9.3 Trust delivery ordering and transaction boundary

The Sigstore POC's standard updater writes accepted metadata during refresh.
Production integration must stage those writes and commit them atomically for
the `TrustDelivery`, using a transactional metadata store or candidate
directory followed by an atomic swap.

A transport may batch ordered events when content needs newly published trust:

```text
DeliveryEvent = ContentDelivery | TrustDelivery

DeliveryBatch {
    events: DeliveryEvent[]
}
```

Each event retains its own verification, durable commit, and acknowledgement.
The agent first verifies and commits the trust event from previously retained
trust, then verifies content against the now-local authenticated profile and
authority states. An invalid content policy, signature, placement, or apply
must not roll back an independently valid trust update. Conversely, a content
delivery cannot smuggle a trust update inside its verification material and
make itself valid in one inseparable step.

`ContentDelivery.verification.required_trust_state` lets an agent report that
it needs a particular prior trust sync. It never authorizes use of stale state:
the locally current authenticated policy remains authoritative, and a newer
revocation or route change cannot be bypassed by asking for an older version.

## 10. Shared append-only delivery log

### 10.1 Initial decision: use it for every provenance suite

The v3 delivery log should become common FleetShift delivery infrastructure.
Every durable mutation is committed before dispatch:

```text
DeliveryCommitment {
    provenance_domain
    tenant_id
    target_id
    delivery_id
    fulfillment_id
    generation
    action
    provenance_profile_digest
    delivery_content_digest
    attestation_evidence_bundle_digest
}
```

Trust meta-deliveries can use the same ordered transport and delivery log
without becoming content attestations:

```text
TrustDeliveryCommitment {
    authority_domain_id
    previous_trust_state_digest
    update_digest
    resulting_trust_state_digest
}
```

Suites may define purpose-separated control records:

```text
SuiteControlCommitment {
    authority_domain_id
    route_id
    suite_id
    operation_kind
    operation_digest
}
```

The delivery-log interface is not suite-specific:

```go
type DeliveryLog interface {
    AppendDelivery(context.Context, DeliveryCommitment) (DeliveryLogRecord, error)
    AppendTrust(context.Context, TrustDeliveryCommitment) (DeliveryLogRecord, error)
    AppendSuiteControl(context.Context, SuiteControlCommitment) (DeliveryLogRecord, error)
    Prove(context.Context, DeliveryLogCheckpoint, []DeliveryLogSelector) (DeliveryLogProof, error)
}

type DeliveryLogVerifier interface {
    Verify(
        context.Context,
        DeliveryLogCheckpoint,
        DeliveryLogProof,
    ) (VerifiedDeliveryLogView, DeliveryLogCheckpoint, *VerificationResult, error)
}
```

Common code owns inclusion, append-only consistency, stale checkpoint recovery,
acknowledgement, compaction watermarks, and retry behavior. A suite receives a
`VerifiedDeliveryLogView` and interprets only its route-namespaced control
records. TUF remains the authority for whether a trust update is valid; the
delivery log adds ordering, idempotency, and audit but does not replace TUF
metadata verification.

### 10.2 What this gains

- V3 keeps its exact rotation-cutoff semantics per route without owning a
  private delivery transport protocol.
- Both suites get the same established-agent rollback protection for delivery
  history and the same lost-acknowledgement/catch-up path.
- Retention, tiling, compaction, and checkpoint reporting are implemented once.
- A future key-transparency suite can bind control events into the same
  delivery ordering domain when useful.

### 10.3 What this does not gain

For the Sigstore suite, the delivery log does not replace RFC 3161 trusted time.
A resource-manager-operated Merkle log proves ordering relative to an agent's
locally retained checkpoint; it does not prove that a signature existed during
a certificate's wall-clock validity period to a cold verifier.

The delivery log also does not provide Sigstore/Rekor-style public auditability
or global fork detection. Witnessing or gossip can be added independently.

## 11. Delivery verification transactions

Trust and content share transport, delivery-log operations, durable workflow
conventions, and acknowledgements, but have different authorization and
transaction boundaries.

### 11.1 Trust delivery

1. Load the retained trust state and the updater pinned for the authority
   domain. Never select an updater from the event payload.
2. Recompute the update digest, check the previous-state/idempotency
   precondition, and verify any applicable common delivery-log commitment.
3. Stage the TUF refresh from retained roots and metadata. Verify root rotation,
   signatures, thresholds, versions, expiry, rollback/freeze protections, and
   target hashes.
4. Validate the resulting profile and route descriptors against the statically
   installed suite registry without loading code or weakening policy.
5. Atomically persist the candidate trust state, applicable delivery-log
   checkpoint, and trust-delivery workflow state, then acknowledge it.

If a later content delivery fails, this independently accepted trust state
remains committed. If the trust event fails, no candidate metadata or profile
becomes visible.

### 11.2 Content delivery

1. Load the authenticated provenance profile and current durable trust,
   delivery-log, route, target, and fulfillment states.
2. Match the outer tenant, provenance domain, and target to locally established
   identity. Compare `required_trust_state` with current authenticated state;
   if required state is missing, request/defer for a separate trust delivery.
3. Canonicalize `DeliveryContent`, `AttestationGraph`, and the exact set of
   independently signed items. Recompute every typed-evidence digest and the
   aggregate digest; ignore reconstructable indexes for authority.
4. Verify delivery-log consistency from the retained checkpoint, inclusion of
   the exact content and attestation-bundle digests, the authenticated profile
   digest, and all selected route-control records.
5. Resolve the expected root-authorization role through the authenticated
   profile and start that route's verification session. Reject missing,
   ambiguous, mismatched-media-type, or failed routes without fallback.
6. Verify the root authorization, then follow the `AttestationGraph`. As
   verified statements establish expected addon, placement, relation, or
   update roles, resolve each role to exactly one route and verify each signed
   item independently. Return normalized statements and principals; never
   infer authority from graph IDs, package route labels, or lookup indexes.
   Reject every signed item not consumed by a recognized role or explicit
   endorsement policy.
7. Reconstruct derived inputs with the common hybrid evaluator using the
   verified, purpose-typed statements selected by the graph. Suites do not
   implement update authorization or derivation policy.
8. Evaluate the verified root input against the one concrete action according
   to `docs/design/authentication.md`, including the owner-resource-to-
   Fulfillment association, removal fulfillment-ID equality, graph limits, and
   target-local generation fencing.
9. Atomically or crash-consistently persist:

- candidate common delivery-log checkpoint;
- candidate state for every route used by this delivery;
- common fulfillment generation/state and any required authorization-
  consumption record; and
- either completed apply state or enough pending work to guarantee retry.

10. Acknowledge according to the target delivery contract.

Failures before step 9 discard candidate route and content-delivery state, but
not trust state accepted by an earlier `TrustDelivery`. A protocol may define a
separate durable rejection transition—for example, v3 recording an invalid
principal event as a bounded exception so the delivery log cannot be wedged.
Such a transition may commit only explicitly defined delivery-log or route
rejection state;
it never advances fulfillment generation, applies content, or makes the failed
identity authoritative. The common rejection/skip protocol remains an open
question below.

## 12. Mapping the two initial suites

### 12.1 Summary

| Boundary                      | Continuity / trust-model-v3                                | Sigstore keyless                                           |
| ----------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- |
| Client key acquisition        | Load/create continuity/device key; create session key      | Create ephemeral P-256 key                                 |
| Identity ceremony             | Nonce-bound OIDC enrollment; later continuity              | OIDC-authenticated Fulcio issuance per signing session     |
| FleetShift control operations | Enrollment, rotation, recovery, tombstone                  | Normally none                                              |
| Signed evidence item          | Standard Sigstore Bundle v0.3 using a public-key hint      | Standard Sigstore Bundle v0.3 using a Fulcio certificate   |
| Aggregate attestation bundle  | Common FleetShift collection of independently signed items | Common FleetShift collection of independently signed items |
| Trusted wall-clock time       | Not required for user delivery                             | RFC 3161 token in current profile                          |
| Low-churn trust               | Separate TUF trust deliveries for v3 manifest and policy   | Separate TUF trust deliveries for TrustedRoot and policy   |
| High-churn identity state     | Principal history + authenticated head map                 | Short-lived certificate; optional Rekor/CT evidence        |
| Manager proof work            | Map/history/event proofs keyed to signed item digests      | Usually only courier each producer's existing bundle       |
| Shared delivery log           | Deliveries + rotation markers                              | Deliveries; no required control marker                     |
| Route checkpoint              | Map root + bounded exceptions                              | Empty initially; later native log/witness checkpoints      |
| Common checkpoint             | Per-domain TUF state + delivery-log root/size              | Per-domain TUF state + delivery-log root/size              |
| Verified authority facts      | OIDC issuer/sub bound through accepted enrollment/history  | Fulcio CA URI + certificate OIDC issuer/sub + timestamp    |
| Historical signer rule        | Adjacent history events and marker interval                | Cert chain + trusted timestamp + retained historical roots |

Each column describes one route instance, not an all-or-nothing delivery mode.
A profile may instantiate the same suite more than once with different tenant
or provider authority states, or use continuity for one role and keyless for
another within the same content delivery.

### 12.2 V3 signed items and suite verification material

The current v3 `ContentAttestation` should be replaced by the common
`delivery-authorization/v1` statement, carried as one independently signed
Sigstore Bundle. Other evidence kinds use the same standard per-item format
with their own in-toto predicates and producer signatures:

```text
Sigstore Bundle v0.3 {
    verification_material.public_key.hint = signing_state_digest_hint
    dsse_envelope {
        payload_type = application/vnd.in-toto+json
        payload = FleetShift in-toto statement
        signatures[0].keyid = signing_state_digest_hint
        signatures[0].sig = continuity signature
    }
}
```

The public-key hint and DSSE `keyid` are matching proof-selection hints. They
are not signed by DSSE and grant no identity or authority. The v3 verifier uses
them to find candidate proof material, verifies that material from its retained
checkpoint, obtains the authenticated continuity key and identity state, and
then requires that key to verify the DSSE signature. Substitution of a hint or
proof therefore either resolves to the same authenticated key state or fails
signature, history, or policy verification.

Per-route v3 verification material contains identity proofs keyed to the
exact independently signed items that need them. Delegation, map, history, and
rotation material belongs here rather than in a custom per-item wrapper:

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

Rotation records and their inclusion paths live in the common delivery-log
proof. The v3 suite cross-checks exact marker index/hash/package references
against the already verified delivery-log view.

The v3 route checkpoint is the existing map root and exceptional-event set.
The delivery-log checkpoint moves to common state:

```text
ContinuityRouteState {
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
is too broad: it mixes suite verification, delivery-log verification,
generation, application, and acknowledgement, which are exactly the
responsibilities this design separates.

### 12.3 Sigstore signed items and suite verification material

Each signed evidence item is one unmodified Sigstore Bundle already used by the
POC. Its DSSE payload uses the same FleetShift predicate schemas as the
continuity suite. Bundle/DSSE decoding, structural limits, media-type checks,
and statement parsing should be shared with the v3 implementation inside the
suite extension packages; their credential and proof verification diverges
after that common parsing. A delivery with a user input, addon manifests,
placement, fulfillment relation, and update history therefore carries several
independent Sigstore Bundles created by their respective producers.
FleetShift's aggregate attestation evidence bundle collects them but has no
aggregate DSSE signature.

Most per-item verification material remains inside each standard bundle:

- Fulcio leaf certificate;
- DSSE signature;
- RFC 3161 timestamp;
- optional future Rekor inclusion material.

Per-route verification material can initially be an empty, versioned object.
A prior TUF `TrustDelivery` installs the Sigstore `TrustedRoot`; the content
delivery carries only its authenticated-state requirement. The common
delivery-log proof commits the concrete delivery and exact aggregate set of
signed evidence items.

`verify_sigstore_bundle` maps naturally to `VerifyEvidenceItem` and runs once
for each Sigstore Bundle. Certificate and timestamp checks yield credential
facts. The current `_authenticate_identity` logic should move behind the common
`PrincipalPolicy`, retaining exact-one-match and evidence-kind separation.

The Sigstore route initially has no suite-specific checkpoint. Its persistent
TUF state is authority-domain state and its delivery-log checkpoint is common.
If Rekor, CT, or witness consistency becomes part of acceptance, their retained
checkpoints become route state without changing the common verifier interface.

### 12.4 Mapping the hybrid semantic model

The partially deprecated hybrid POC is not a third cryptographic suite. It is
the strongest existing reference for common attestation semantics. Its model
maps to the clarified abstractions as follows:

| Hybrid object or behavior                                      | Clarified common model                                                                                                                                                                 |
| -------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SignedInput`                                                  | One `delivery-authorization/v1` `SignedEvidence` item plus a `SignedInputRef` graph node                                                                                               |
| `DerivedInput`                                                 | One `DerivedInput` graph recipe retaining its checked prior content ID/type, prior input, and user authorization, plus typed signed derivation evidence for an external-addon strategy |
| Signed `spec_update` manifest envelope                         | The same addon assertion encoded as a purpose-specific derivation predicate; no additional signature or authorization semantics                                                        |
| Root `Attestation.input`                                       | `AttestationGraph.root_input_id`                                                                                                                                                       |
| Root `Attestation.output`                                      | The single `ContentDelivery.delivery.action`; it is not duplicated in the evidence bundle                                                                                              |
| `PutManifests.manifests`                                       | `DeliveryContent.action.PutManifests.manifests`                                                                                                                                        |
| `RemoveByDeploymentId`                                         | `DeliveryContent.action.RemoveByFulfillmentId`; the legacy hybrid name conflates its deployment-shaped input with the kernel delivery identity                                         |
| Optional addon manifest signature                              | A separate `manifest-set/v1` item signing the exact manifests; the user-signed strategy determines the permitted producer and whether an exact source binding is required              |
| `PlacementEvidence.deployment_id`                              | A `placement/v1` fulfillment binding after resolving the user-facing owner resource to its Fulfillment; outer `target_id` remains only routing data                                    |
| `RegisteredSelfTarget`                                         | A `fulfillment-relation/v1` signed evidence item, optionally found through a reconstructable lookup index                                                                              |
| `VerificationBundle.inputs` and nested attestations            | `AttestationGraph` recipes plus the common signed-item map                                                                                                                             |
| `VerificationBundle.fulfillment_relations` lookup              | Signed items plus an optional checked `(addon_id, resource_type)` index                                                                                                                |
| `TrustStore`, `KeyBinding`, and `OutputSignature` verification | Route-owned credential verification and authenticated principal policy, not common wire structs                                                                                        |
| `verify_attestation(input, output, bundle, target_identity)`   | Common verification of the root input against the one package action, evidence bundle, and locally authenticated target identity                                                       |

This preserves the hybrid model's important semantics: intent-to-output
constraint evaluation, opaque addon-output authentication, placement,
fulfillment relations, derived inputs, removals, and generation checks. It also
clarifies that the hybrid `Attestation` is a useful verification composition,
not necessarily a top-level serialized provenance object.

Except for the representation and interface deltas below, this design adopts
the hybrid verification semantics and defers their specification to
`docs/design/authentication.md` and the hybrid POC. In particular, using a
purpose-specific predicate for `spec_update` is purpose separation at the
DSSE/in-toto boundary; it does not change who authorizes an update, what an
addon signs, or how applicability and generation are evaluated.

The current hybrid Python objects are not wire-compatible without changes:

- raw canonical-JSON hashes and detached Ed25519 signatures need DSSE/in-toto
  type, purpose, tenant, and provenance-domain separation;
- signer-selected trust-anchor IDs move into authenticated profile routes and
  principal policy;
- key binding and signature verification move behind route suite interfaces;
- cryptographic verification is separated from common semantic evaluation;
- external manifest and derivation outputs become purpose-typed evidence; and
- embedded TUF refresh bytes, as used by the Sigstore parity POC, become prior
  `TrustDelivery` events rather than content verification material.

The first POC should therefore reuse the hybrid semantic tests and shapes, not
promise byte-for-byte compatibility with its current dataclasses. User output
signing is intentionally not a parity requirement; addon-signed manifests and
other independently produced evidence remain required where intent semantics
cannot validate an opaque result on their own.

### 12.5 Important guarantee differences remain visible

The common interface must not imply that both suites provide identical
security:

- v3 protects an established agent against continuity-history rollback but
  accepts local-view fork limitations and has no trusted wall-clock time;
- the current Sigstore profile without Rekor/CT has short-lived credentials and
  trusted timestamps but no public detection of Fulcio misissuance or
  equivocation;
- adding Rekor/CT/witnesses changes the Sigstore profile's guarantees;
- a common FleetShift delivery log adds local delivery-history continuity to both
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
  selector. The suite may reuse the Sigstore Bundle public-key form, as v3
  does, or register a different `SignedEvidence` media type without changing
  the aggregate or verifier interfaces.
- Prior TUF trust deliveries distribute the transparency service's bootstrap
  keys, policy, and witness configuration unless the provider provisions an
  equivalent anchor.
- `AssembleVerificationMaterial` obtains inclusion, consistency, and
  non-equivocation material for each signing identity.
- `BeginVerification` advances retained transparency checkpoints.
- `VerifyEvidenceItem` verifies each key binding and signature, then emits the
  same normalized issuer/subject/authority facts.
- Per-route `CandidateState` contains accepted log and witness checkpoints.

The independently signed statement model, profile routing, principal policy,
aggregate FleetShift evidence bundle, delivery log, apply path, and
acknowledgement contract do not change.

## 14. Security invariants

1. **Pinned selection:** provenance profile, authority domain, route, suite, and
   trust updater are selected from provisioned or authenticated local state,
   never from untrusted package data. Each route pins accepted signed-evidence
   media types and verification-material forms within a shared format.
2. **No fallback:** zero or ambiguous route matches fail, and parse, trust, or
   verification failure in the selected route is terminal for that attempt.
3. **Original signed items:** every exact user, workload, or addon signed item
   is stored and delivered. The manager does not translate or replace it.
4. **Independent authority:** no signature by one producer or by the resource
   manager is treated as a signature over the aggregate evidence bundle or as
   authority for another producer's statement. The common attestation evaluator,
   not a suite verifier or resource-manager invocation, determines how verified
   producer assertions satisfy an authorization.
5. **Authenticated relationships:** attestation-graph edges, outer indexes, and
   references locate or order work but never establish a relationship. Every
   accepted relationship is contained in or deterministically derived from
   verified signed statements.
6. **Purpose separation:** predicate type, DSSE payload type, signed-evidence
   media type, evidence kind, and suite protocol are checked together. The
   signed-evidence digest commits both media type and bytes, and any inner media
   type must match the outer typed value.
7. **Derived identity:** principal and anchor IDs come from verified evidence
   and accepted policy, never from an unauthenticated signer label.
8. **Exact-one policy match:** zero or ambiguous identity-policy matches fail.
9. **One concrete action:** `DeliveryContent` contains one tagged put/remove
   action. The evidence graph does not duplicate it; the root input is evaluated
   against that exact action. `RemoveByFulfillmentId.fulfillment_id` must equal
   the enclosing delivery's fulfillment ID; an owner-resource ID cannot
   substitute for it.
10. **Placement is evidence, not routing:** outer target fields must match local
    authenticated target identity but never substitute for a user-signed scope
    or delegation, a verified placement predicate/statement, or a fulfillment
    relation. Dynamic selectors are evaluated against target-local or otherwise
    authenticated facts, not resource-manager assertions.
11. **Manager is courier:** manager preflight and ordinary authorization do not
    substitute for target verification, and asking an addon to sign output does
    not authorize use of that output; the verified intent and target checks are
    still required.
12. **Trust/content separation:** a content delivery cannot install the trust
    update that makes itself acceptable. Trust deliveries authenticate and
    commit independently; route verification material remains untrusted until
    checked from retained checkpoints and roots.
13. **Transactional state:** within a content delivery, the delivery-log
    checkpoint, every used route, generation, authorization-consumption record,
    and durable apply state do not advance independently in a way that grants
    authority after failure.
    A previously accepted trust delivery is not rolled back by a later content
    failure. Explicit durable rejection/exception state may advance only when
    it cannot authorize or apply the rejected content.
14. **Rollback protection:** established agents never silently re-enter new-
    agent bootstrap after checkpoint loss, expiry, or compaction lag.
15. **Bounded verification:** graph size/depth, lookup-index size, proof sizes,
    signed-item count, statement sizes, and suite-specific work have explicit
    limits.
16. **Explainable failure:** errors identify profile/route selection, trust
    delivery, delivery-log, identity binding, signature, policy, attestation,
    constraint, or generation failure without exposing secret material.
17. **Controlled signer surface:** key handles are purpose-restricted and are
    not exposed to addon UI code or arbitrary byte-signing callers.
18. **Migration is authorization:** a profile or route change must chain from
    current trust or use explicit recovery; it is never a compatibility
    fallback.

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
from their signed content. The delivery log may commit the exact aggregate
for ordering and substitution resistance, but that commitment is not an
attestation signature. Multiple endorsements are several independently
verified items or a future explicit signed endorsement predicate—not an
unsigned reference and not a manager-generated aggregate signature.

### Duplicating the concrete delivery inside an attestation node

The hybrid POC's `Attestation { input, output }` expresses the arguments to a
verification operation. Once the delivery protocol already carries one typed
put/remove action, serializing another graph `output` creates two candidates
that must be reconciled and obscures which one is applied. The graph selects or
derives the root input; common code evaluates that input against the package's
single action.

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
downgrade and cross-protocol confusion. Local authenticated policy resolves an
expected role to exactly one route; package metadata only has to agree.

### Putting a TUF update in content verification material

TUF metadata is independently authenticated, versioned trust-delivery content,
not a proof about one attestation. Letting a content delivery carry and commit
the update that causes its own route or signer to become trusted conflates
authority transition with content authorization and makes failure semantics
unclear. Deliver the TUF refresh as a prior `TrustDelivery`, even when both
events share one ordered transport batch.

### Putting all mutable identity state in TUF

TUF is valuable for roots and policy, but per-signature identity churn would
turn it into a centralized high-frequency signing bottleneck and duplicate the
v3/key-transparency proof systems. Use it as the control plane, not the event
database.

### Treating the delivery log as a timestamp service

The resource manager can delay append. Delivery-log position gives protocol
order, not trustworthy signature creation time. Sigstore's short-lived
certificate profile still needs a trusted timestamp or an explicitly different
online-verification policy.

## 16. Conformance contract

Every suite implementation should run the same semantic and adversarial
contract, with mechanism-specific assertions where necessary.

Common cases include:

- exact hybrid semantic parity for intent input, derived input, addon manifest,
  placement, relation, put/remove, managed-resource, constraint, and generation
  tests; user output signing is not required;
- cross-tenant and wrong-target package rejection;
- tenant-user and provider-workload evidence verified through distinct routes,
  including different tenants with different user authorities;
- zero-route, ambiguous-route, wrong-authority-domain, wrong-principal-class,
  and attempted route-fallback rejection;
- signed/outer generation mismatch;
- purpose and predicate confusion;
- signed-evidence media-type relabeling;
- outer/inner media-type mismatch and typed-evidence digest mismatch;
- injection or omission of an independently signed evidence item;
- rejection of unreachable signed items unless an explicit endorsement rule
  consumes them;
- rejection of outer/action fulfillment-ID mismatch and substitution of a
  user-facing owner-resource ID for the kernel Fulfillment identity;
- root-input, derivation-edge, outer-reference, or lookup-index retargeting
  without a corresponding signed or deterministic relationship;
- adapter-level proof that purpose typing and graph assembly preserve the
  hybrid evaluator's authorization, applicability, and replay decisions;
- signer-label and anchor-label substitution;
- missing, ambiguous, and wrong-evidence-kind policy matches;
- manager modification of signed content or interpretation fields;
- trust-delivery rollback, freeze, expiry, root-rotation, previous-state, and
  target tamper;
- rejection of an embedded content trust update and proof that a valid prior
  trust delivery remains committed after later content failure;
- delivery-log leaf/root/inclusion/consistency tamper;
- stale manager checkpoint and lost acknowledgement;
- no authority-granting candidate state advances after any verification
  failure boundary; prior independently committed events and explicitly
  permitted non-authorizing rejection state remain intact;
- graph cycles, excessive graph depth, proof amplification, and oversized
  signed-evidence-item rejection;
- several single-signature Bundles over the same statement evaluated according
  to explicit endorsement/threshold policy rather than any-verifier fallback;
- migration/downgrade rejection.

V3-specific cases retain enrollment substitution, map/history proof tamper,
rotation cutoff, stale-agent, exception, historical-state coverage, public-key
hint/DSSE `keyid` mismatch, and rejection of certificate-based Bundle material.

Sigstore-specific cases retain Fulcio proof-of-possession, certificate path,
identity extension, RFC 3161, TrustedRoot, optional transparency evidence, and
rejection of a public-key-only Bundle under the keyless profile.

The existing Sigstore parity inventory is a useful precedent: test-name parity
alone is insufficient, so negative tests should pin the intended rejection
layer and forbid conversion into soft boolean assertions.

## 17. Suggested proof-of-concept sequence

1. Write common semantic fixtures and failing adversarial tests first. Define
   the tagged `PutManifests`/`RemoveByFulfillmentId` delivery action, minimal
   `AttestationGraph`, canonical aggregate digest, and purpose-specific in-toto
   predicates. Add adapter-level tests for purpose confusion, graph-reference
   retargeting, owner-resource versus Fulfillment-ID substitution, and
   delivery/output duplication.
2. Port the existing hybrid intent, derivation, manifest, placement,
   fulfillment-relation, removal, and generation tests to a suite-independent
   semantic evaluator. Keep its useful `verify(input, output, evidence)` model
   internally, but source `output` from the single `ContentDelivery` action and
   source signatures from `SignedEvidence` items. Changes to those attestation
   semantics belong in `docs/design/authentication.md`, not in the suite
   adapter.
3. Make both existing cryptographic implementations emit one standard Sigstore
   Bundle v0.3 per producer assertion. V3 uses the public-key form with matching
   hint/DSSE `keyid`; keyless uses its Fulcio certificate and timestamp. Keep
   the aggregate unsigned.
4. Introduce authenticated `ProvenanceProfile` routing and common principal
   policy. Exercise at least three configurations against the same semantic
   tests: all-continuity routes, all-keyless routes, and one composed delivery
   whose tenant-user and provider-workload evidence use different suites or
   independently configured authorities. Pin zero/ambiguous/fallback failures.
5. Introduce per-route `ManagerSuite` and `AgentSuite` sessions plus
   `DeliveryVerificationMaterial.routes`. Move v3 map/identity proof work behind
   its route and leave the keyless route material empty initially.
6. Extract the v3 delivery log into the common delivery-log component. Commit
   the exact `DeliveryContent`, graph, and signed-item set, and keep all cutoff,
   catch-up, stale-checkpoint, and lost-ack tests.
7. Add the stateful TUF updater and first-class `TrustDelivery`. Publish profile,
   authority-domain principal policy, v3 trust manifest, and Sigstore
   `TrustedRoot` as separate authenticated targets. Prove that content cannot
   embed its own update and that a committed trust delivery survives later
   content rejection.
8. Add crash injection around trust-delivery commit and, separately, candidate
   delivery-log, route, generation, authorization-consumption, and apply state
   before treating the interfaces as viable.

The smallest useful spike is steps 1-4 around the existing hybrid and Sigstore
parity tests. It directly tests the most uncertain seams—single-copy delivery,
verified derivation recipes, and multi-domain route composition—before
introducing production storage or fully extracting the v3 delivery log.

## 18. Provisional decisions

These are recommendations for the first POC, not final product commitments:

1. Use common in-toto statement predicates and DSSE semantics for both suites.
2. Make each user, workload, or addon assertion an independently signed
   evidence item. Do not sign the aggregate FleetShift evidence bundle.
3. Model intent signing as the primary user flow. Keep addon-signed opaque
   outputs as independent evidence, but do not make user output signing a core
   field or first-POC parity requirement.
4. Keep one concrete tagged `PutManifests`/`RemoveByFulfillmentId` action in
   `ContentDelivery`; use a minimal `AttestationGraph` only for root-input
   selection and deterministic derived-input recipes.
5. Require every graph edge and evidence relationship to be expressed by, or
   deterministically verified from, signed statement content; aggregate indexes
   are optional lookup aids only.
6. Represent every externally produced manifest or derivation as a purpose-
   typed signed item. Inline outputs remain deterministically checked from
   signed intent. The hybrid evaluator retains responsibility for authorization
   and applicability.
7. Require both initial suites to emit complete standard Sigstore Bundle v0.3
   items: certificate verification material for Sigstore keyless and public-key
   hints for v3. Keep `SignedEvidence` media-typed so a future suite may use a
   different standard without changing the common interfaces.
8. Use an authenticated profile to resolve every expected authority-domain,
   principal-class, and evidence-kind combination to exactly one route. Permit
   explicit tenant-user/provider-workload composition; forbid fallback.
9. Use TUF for profile, policy, and low-churn trust material in every initial
   installation, transported and committed as a separate `TrustDelivery`.
10. Use one common delivery log across every route selected for a target.
11. Normalize verified mechanism evidence into credential facts, then share
    exact-one principal/anchor mapping and constraint evaluation.
12. Stage all content-related delivery-log, route, generation, and apply state
    and let the common content-delivery transaction commit it. Do not roll back
    an independently committed trust delivery after later content failure.
13. Represent multiple endorsements as several single-signature evidence items
    grouped by canonical statement digest unless measured payload size
    justifies a purpose-specific signed digest-endorsement predicate.
14. Keep suite control protocols typed and opaque to the core beyond transport,
    authorization, idempotency, and receipts.

## 19. Open questions for iteration

### Profile ownership and route scope

What owns the final target provenance profile: a provider root, a tenant root,
or a provisioned higher-level authority that composes delegated fragments from
both? The data model allows independent authority domains, but profile-change
authorization must prevent either side from silently replacing the other's
route.

How expressive should route applicability be beyond authority domain,
principal class, and evidence kind? The initial matcher should remain finite
and exact. Tenant/workspace IDs, signer identities, or predicate fields should
be added only when a concrete use case cannot be expressed through principal
policy after route verification.

Which workload verifier should demonstrate composition first? Separate Fulcio
authorities exercise the current Bundle code with little novelty; SPIFFE/SPIRE
or Kubernetes workload identity would better test that the route interface is
not accidentally keyless-specific.

### Common delivery log

Is the operational cost of appending every durable delivery to the log
acceptable for providers that select Sigstore? The proposed answer is yes
because it also unifies retry, rollback, and audit semantics, but the first POC
should measure append and proof costs independently of the suite.

Should the common delivery log be per tenant, provider, target group, or should
lower-scope logs be anchored into a higher-scope log? V3 rotation needs one
ordering domain spanning every target a key can authorize, while provider
workload routes may cross many tenants.

### TUF ownership and granularity

Does each tenant and provider workload domain have an independent TUF root, or
does a provider/provisioning root delegate their namespaces? Per-domain roots
maximize isolation; delegation may be substantially easier to operate. Either
shape must prevent the resource manager from becoming the unilateral root
authority.

How are common principal-policy changes authorized relative to suite-specific
trust-manifest changes? TUF roles and delegations may make the v3 trust
manifest's separate update-policy machinery smaller than currently designed.

What exact `required_trust_state` comparison is useful without enabling stale
policy selection? The agent must always apply current revocations. The POC
should decide whether package requirements carry exact target digests, minimum
snapshot versions, a profile epoch, or only diagnostics for requesting a trust
sync.

### Signed evidence item details

The provisional v3 profile puts the same signing-state selector in the Sigstore
Bundle's `verificationMaterial.publicKey.hint` and the DSSE signature's
`keyid`, as required by the Bundle model. It remains only a lookup hint and must
resolve through authenticated history to the key that verifies the signature.
The POC must confirm that maintained Sigstore libraries accept and preserve
this standard public-key Bundle form without keyless-specific assumptions.

Which compatibility and registration rules allow a future suite to introduce
a new `SignedEvidence.media_type`? The core should require a pinned suite to
declare exact accepted media types and size limits, but it should not need a
new universal signature or verification-material schema.

Do request authorization statements need a stable signed provenance-domain ID
in addition to tenant ID and predicate type to prevent cross-instance replay?
The provisional answer is yes.

How is the owner-resource-to-Fulfillment association authenticated? A
user-facing `Deployment`, managed resource, or campaign owns the Fulfillment,
while concrete placement, generation, delivery, and removal use
`fulfillment_id`. If the Fulfillment ID is known at signing time it can be bound
directly; otherwise the verifier needs a signed relationship or a stable
deterministic derivation from signed owner identity. An outer resource-manager
mapping cannot establish this association by itself.

Should graph node IDs be canonical recipe digests or merely bounded local
selectors covered by the aggregate delivery-log commitment? Canonical digests
make deduplication and explanation easier, but do not replace verification of
every edge.

What exact canonical statement encoding and cross-language vectors will be
normative? The current v3 Go JSON encoding is explicitly not sufficient.

### Stateful verification

Can TUF metadata, route state, common delivery-log state, and target generation
be stored with the required per-event atomicity in the fleetlet? Trust delivery
and content delivery intentionally use separate transactions; within each one,
the durable ordering protocol and recovery states need to be specified before
production.

Can an ordered batch acknowledge a successful `TrustDelivery` while leaving a
following `ContentDelivery` paused or rejected without confusing manager retry
logic? The design says yes; the target-delivery protocol needs explicit
per-event receipts to make that behavior durable and observable.

How does a retained delivery-log checkpoint advance past a permanently
rejected trust or content commitment without treating that event as accepted
or blocking unrelated later work? The existing v3 exceptional-event mechanism
is a precedent, but common rejection/skip semantics and their scope need an
explicit protocol.

### Time, transparency, and history

Is RFC 3161 mandatory for the first Sigstore profile, or can a provider choose
online verification during certificate validity? Those are distinct profiles
with different historical-verification guarantees and should have different
suite protocol IDs or authenticated parameters.

Will the production Sigstore option require Rekor/CT, allow the current logless
profile, or expose both? The guarantee profile must clearly distinguish them.

How long must v3 event bodies, old Sigstore CA/TSA roots, bundles, and
delivery-log proof material remain available for current fulfillment
reconciliation?

### Migration and multiple signatures

What authorizes v3 -> Sigstore and Sigstore -> v3 migrations? Requiring one
valid signed evidence item under each suite over the same migration statement
is a strong starting point, but recovery and unavailable-old-suite cases need
explicit policy.

Which migration and high-risk operations require several endorsements over one
statement? The initial representation is several single-signature Bundles
grouped by canonical statement digest. Thresholds, required routes, and
principal diversity must be authenticated policy, not “any installed verifier
accepts.” A digest-endorsement predicate should be added only after measuring
large real statements and aggregate compression.

### Client UX

What does `AcquireSigner` return when user interaction or an external service
is unavailable: a synchronous error, a durable challenge, or a `PausedAuth`
continuation? The interface likely needs a challenge/result state before it is
made production-ready.

How narrowly can browser and addon signing APIs be scoped so the user sees the
tenant, target scope, action, and content identity at the actual signing
boundary for both suites?
