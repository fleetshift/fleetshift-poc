# Pluggable provenance suites

Status: design draft for discussion; interfaces remain provisional

This document proposes interfaces between FleetShift's stable attestation and
delivery semantics and an authenticated **provenance profile** containing one
or more configured **trust domains**. Each trust domain selects one provenance
suite and the trust and applicability constraints under which that suite may
accept evidence. The document maps two initial suite implementations:

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
call sites. The aim is to let providers accept signatures from explicitly
configured trust domains—for example, a tenant-user trust domain and a
provider-workload trust domain—while keeping FleetShift's authorization
language, evidence relationships, delivery contract, policy evaluation, and
apply path the same.

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

which configured trust domain and mechanism establish the signer's identity,
how trust reached a verifier, and how the resulting signature is verified
```

## 2. Goals

- Providers select an authenticated provenance profile without creating
  different fulfillment, managed-resource, placement, removal, or generation
  semantics. A profile may configure tenant-user and provider-workload trust
  domains with different suites without allowing evidence to supply trust or
  trigger verifier fallback.
- Every independently signed evidence item is retained and later delivered
  exactly as signed; the resource manager does not translate, combine, or
  re-sign those items.
- Clients use one high-level signing API even though acquiring a signer may
  mean loading a continuity key or performing OIDC + Fulcio issuance.
- Common resource-manager code assembles the FleetShift attestation evidence
  bundle, while each selected trust domain assembles its additional
  verification material; v3 must construct map/history proofs and Sigstore
  usually does not.
- The delivery agent has one common attestation-verifier boundary. It receives
  normalized federated identities and verified statements from selected suite
  instances instead of inspecting public keys, certificates, key-history
  proofs, or signer-selected identity labels itself. The selected trust domain
  remains verification context; it does not become part of the signer's
  identity.
- Trust configuration and ordered delivery reuse as much implementation as is
  defensible, while separately configured trust domains retain their own roots,
  constraints, and verifier state.
- The common provenance data model stays deliberately small: an aggregate of
  media-typed signed items plus additional typed verification material. Initial
  suites may reuse the same standard format inside that extension point without
  making the core depend permanently on it.
- Trust-domain-specific suite state advances transactionally with the content
  delivery that consumes it. Independently authenticated trust updates advance
  through their own trust-delivery transaction.
- A new suite can be added by implementing bounded client, manager, and agent
  interfaces plus a conformance test contract.

## 3. Non-goals

- Dynamically downloading verifier code from the resource manager.
- Unstructured mixing or evidence-controlled fallback among cryptographic
  components. Each trust-domain entry selects one cohesive suite configuration.
  An evidence item's trust-domain hint may select only that exact authenticated
  entry; it cannot supply a suite, root, or alternate verification policy.
- Reproducing the resource manager's complete tenant/workspace authorization
  policy at every delivery agent.
- Treating a management-plane preflight verification as delivery authority.
- Making TUF a high-churn database of every identity key or delivery.
- Making a content delivery carry the trust update that causes that same
  delivery to become acceptable. Trust synchronization is a separate
  authenticated delivery operation.
- Claiming that a local append-only delivery log provides wall-clock time, public
  transparency, or global fork detection.
- Preventing signed evidence from being presented to another management plane
  whose authenticated trust-domain configuration accepts the same federated
  identity and for which all signed semantic constraints also hold. Evidence is
  portable; a target's accepted profile, delivery-log checkpoint, trust-domain
  state, target identity, and Fulfillment state are not.
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
- **Delivery verification material:** Replaceable common and trust-domain-
  specific proof material needed to verify one content delivery, such as a
  delivery-log proof or v3 map/history proofs. It is neither signed content
  evidence nor a trust update.
- **Verifiable relationship:** A relationship among evidence items or delivery
  content whose identity-affecting fields are carried by a signed statement or
  are deterministically derived from signed statements. Outer references and
  indexes may locate evidence, but do not make a relationship authoritative.
- **Provenance profile:** Authenticated target configuration containing the
  trust-domain entries that the verifier may use. The profile is selected by
  local provisioning and authenticated trust state, never by a content
  delivery.
- **Trust domain ID:** A stable opaque identifier or URI intended to be globally
  unique. It names one FleetShift verification configuration and its suite
  state, and is compared exactly; URI shape does not imply discovery or network
  access. It is a profile lookup key, not a signer identity namespace or an
  alias for an external issuer.
- **Trust domain:** One profile entry selecting a cohesive suite protocol,
  accepted evidence representation, trust configuration, credential
  constraints, applicability, verification-material type, and suite state.
  Separate tenant-user and provider-workload trust domains may select the same
  or different suites.
- **Trust-domain hint:** The `trust_domain_id` carried with a signed evidence
  item. It is an untrusted exact lookup key into the authenticated profile. The
  selected suite must prove that the evidence satisfies that trust domain's
  configured trust, identity-authority, and credential constraints. The hint
  does not become part of the authenticated identity. Failure is terminal
  rather than a cue to try another trust domain.
- **Provenance suite:** A versioned implementation of signer acquisition,
  identity lifecycle, signed-evidence construction, identity/key binding,
  suite verification-material assembly, and target-side cryptographic
  verification. A suite is instantiated for a trust domain; it is not
  necessarily the sole suite used by a delivery.
- **Trust distribution:** The authenticated update mechanism for relatively
  low-churn profile, trust roots, and verification policy. The initial
  implementation is TUF, transported as a first-class trust delivery rather
  than embedded in content verification material.
- **Trust delivery:** A separately verified and acknowledged meta-delivery that
  advances the profile's trusted TUF state, including trust-domain
  configuration and trust material. TUF metadata signatures, thresholds,
  versions, expiry, and root rotation authenticate the update.
- **Delivery log:** FleetShift's cryptographic, scoped append-only sequence of
  content, trust, and suite-control commitments. Delivery agents retain local
  checkpoints. This is the v3 delivery log generalized into shared
  infrastructure; the exact tenant/provider/target-group scope remains open.
  It is distinct from the journal in the
  [target delivery protocol](architecture/target_delivery_contract.md#journaling),
  which records target-side work and recovery state.
- **Identity scheme:** An open, versioned, globally unique or registry-controlled
  name for the canonical semantics of an external identity, such as
  `oidc-sub/v1`, `spiffe-id/v1`, or `x509-anchor-spki/v1`. This is a
  domain-separation tag, not a closed enum; common code compares it exactly but
  does not branch on its cases. A suite may introduce a new scheme but may not
  redefine an existing scheme's canonical form.
- **Identity authority:** The scheme-canonical external namespace that qualifies
  a subject. Examples are an exact OIDC `iss`, a canonical SPIFFE trust domain,
  or—when generic X.509 provides no better namespace—a trust-anchor SPKI digest.
  It is distinct from both the FleetShift trust domain and the mechanism that
  issued or verified a credential.
- **Federated identity:** The exact canonical `(scheme, authority, subject)`
  tuple produced after the selected suite verifies a credential and the trust
  domain's constraints. FleetShift does not map this tuple into a local
  principal ID. Equal subjects under different schemes or authorities remain
  different identities; the same externally qualified identity may be verified
  through more than one trust domain or suite.
- **Credential facts:** Mechanism-level facts produced only after cryptographic
  verification, such as the credential authority, key/state identifier,
  certificate or enrollment properties, and trusted signing time. For example,
  a Fulcio CA is a credential authority while the certificate's authenticated
  OIDC issuer and subject form the federated identity.

## 5. Proposed architecture

```text
user request          addon output          placement/relation       later update
    |                     |                         |                      |
    | signs statement     | signs statement         | signs statement      | signs statement
    v                     v                         v                      v
signed evidence A    signed evidence B         signed evidence C     signed evidence D
 trust domain: users  trust domain: addons    trust domain: addons    selected trust domain
    \_____________________|_________________________/_____________________/
                              |
                              v
resource manager assembles, but does not sign as a whole:
    one concrete delivery action
    + FleetShift attestation evidence bundle {graph, A, B, C, D, ...}
    + delivery-log and per-trust-domain verification support
                              |
                              v
delivery agent:
    each item's trust-domain hint selects one exact entry in the authenticated profile
    that trust domain's suite instance verifies the signed item independently
    common code checks trust-domain applicability and the authenticated federated identity
    common verifier follows only relationships proven by signed content
        or deterministic policy over verified statements and authoritative facts
    constraints -> placement/removal -> generation -> durable apply
```

The suite boundary is deliberately below attestation semantics and above raw
cryptography. A suite does not decide whether a manifest matches an inline
strategy, whether removal is permitted, or whether a generation is current.
It decides whether each particular statement is authentically signed by a
federated identity recognized by the selected trust domain's accepted trust
state. The common verifier determines the expected tenant, producer class,
evidence kind,
and, where required by attestation semantics, exact signer. It then checks that the
hinted trust domain is permitted for that expectation. There is no assumption
that one producer saw, assembled, or signed the complete evidence set.

### 5.1 Common versus suite-owned responsibilities

| Concern                                              | Common FleetShift code          | Provenance suite                   |
| ---------------------------------------------------- | ------------------------------- | ---------------------------------- |
| Authorization statement schemas                      | Yes                             | No                                 |
| DSSE/in-toto semantic profile                        | Yes                             | Encodes each signed evidence item  |
| Evidence relationships and derivation                | Yes                             | No                                 |
| CEL and strategy-implied constraints                 | Yes                             | No                                 |
| Put/remove and generation fencing                    | Yes                             | No                                 |
| Normal API authorization                             | Yes                             | No                                 |
| Expected producer role and trust-domain applicability | Profile-driven                  | No fallback                        |
| Signer acquisition                                   | No                              | Yes                                |
| Enrollment/rotation/recovery protocol                | Transport only                  | Yes                                |
| Signed-evidence media type                           | Enforces trust-domain entry     | Yes                                |
| Cryptographic signature verification                 | No                              | Yes                                |
| Credential-to-external-identity proof                | No                              | Yes                                |
| Federated identity                                   | Checks expected signer          | Authenticates scheme/authority/subject |
| Low-churn trust delivery                             | Common interface; initially TUF | Defines/parses its targets         |
| Durable delivery commitment ordering                 | Yes                             | May add namespaced control events  |
| Per-trust-domain suite proof construction            | No                              | Yes                                |
| Per-trust-domain verifier checkpoint                 | Opaque storage and transport    | Defines and validates              |
| Verification explanations                            | Common result tree              | Adds suite-specific children       |
| Apply and acknowledgement                            | Yes                             | No                                 |

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
verify. Every statement binds its tenant, evidence kind, and semantic content
or relationship. The associated credential or suite proof authenticates the
signer's federated identity. The outer `SignedEvidence.trust_domain_id` is only
an untrusted lookup hint selecting the configured verifier that must accept
that identity.
The statement does not bind a resource-manager deployment or management-domain
identifier. Signed evidence is portable; whether a verifier accepts it depends
on that verifier's authenticated profile and retained delivery state.

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
    trust-domain hint + media type + immutable serialized representation
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
  rotation, and delivery proofs travel in per-trust-domain delivery verification
  material and may be shared by many items.

The authenticated trust-domain entry, not the Bundle, determines which form is
valid. The initial v3 trust domain rejects certificate-based Bundle
verification material; the initial Sigstore keyless trust domain rejects a
public-key-only Bundle. Sharing
the media type must not become an implicit fallback between trust mechanisms.

“Bundle” in the Sigstore type name does not mean that it contains the other
FleetShift attestations for the delivery. It contains one independently signed
item and the stable material or selector needed to begin verifying that item.

The common layer treats every representation as immutable typed bytes:

```text
SignedEvidence {
    trust_domain_id // untrusted exact lookup hint
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
layer stores, digests, bounds, and dispatches the typed bytes; only the selected
suite parses their format. The common verifier sees the resulting
`VerifiedStatement`, not certificates, public-key hints, or format-specific
proofs.

The resource manager stores each item by a domain-separated digest over
`trust_domain_id`, `media_type`, and the exact `bytes`, then later delivers the
same typed value. The trust-domain hint and media type are part of the item
identity because changing the selected verifier or parser must not preserve its
digest.
When a format also carries an internal media type, as Sigstore Bundle does, the
suite requires it to match the outer value. Common code passes the exact
trust-domain entry selected by the outer hint to the suite instance. Relabeling
`trust_domain_id` therefore selects another explicitly
configured verifier; it never changes the federated identity returned by
successful verification. If that selected trust domain does not accept the
credential and identity, verification fails without trying the original or
another trust domain. The manager may add more
independently signed items as fulfillment evolves, but it never translates an
existing signature or wraps the aggregate evidence set in a manager-generated
provenance signature.

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
identity-class or explicit trust-domain requirements; it never interprets “any
installed verifier accepts” as a quorum.

Repeating a small statement in several Bundles is acceptable for the initial
profile and compresses well at the aggregate transport layer. If large
statements make that material, a future signed endorsement predicate may refer
to the canonical statement digest, payload type, purpose, and tenant. An
unsigned pointer to another Bundle is insufficient, and a reference to the
entire Bundle digest is unnecessarily brittle because it also binds certificate,
timestamp, signature, trust-domain hint, and serialization details. A future
native multi-signature evidence media type remains possible without changing the
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
attacker from hiding unverifiable or malformed material inside a logged
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
`SignedEvidence` trust domain ID, media type, and bytes. The canonical aggregate
digest covers the graph and the sorted signed-item digests, trust domain IDs,
media types, and exact bytes.
Semantic IDs inside verified statements remain distinct from storage digests;
lookup indexes may map semantic IDs to candidate item digests, but are excluded
from the aggregate digest because they can be reconstructed and the verifier
checks every mapping against decoded verified statements.

Within the provenance portion of a delivery, there are two top-level payloads:

1. `AttestationEvidenceBundle`, the aggregate of independently signed items;
   and
2. `DeliveryVerificationMaterial`, the additional common and trust-domain-
   specific support needed to verify those items for this delivery.

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
        trust_domains {
            trust_domain_id -> required_state
        }
    }
    delivery_log_proof
    trust_domains {
        trust_domain_id -> TypedBytes
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
material that is delivery- or trust-domain-wide, shareable, replaceable, or
not representable in that per-item format. There is no generic `concrete_output`:
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
states the profile digest and per-trust-domain state against which the manager
assembled the delivery. The agent compares it to authenticated local state and
may require a prior trust delivery; it never rolls back or switches profiles to
satisfy package metadata. Trust-domain material is keyed by trust domain ID
only for exact lookup. The locally accepted trust-domain entry determines the
suite and required media type, and all material remains untrusted until that
suite verifies it.

A TUF update is intentionally absent. Trust updates are independently
authenticated meta-deliveries described in Section 9, rather than
`DeliveryVerificationMaterial` that a content delivery can use to establish
its own trust policy.

The delivery log commits the digest of `DeliveryContent` and the canonical
attestation-evidence digest covering the exact graph and signed items.
Reconstructable lookup indexes are excluded.
That commitment proves exact delivery ordering and prevents later substitution
relative to an agent's checkpoint, but it does not make the resource manager an
attestation signer. Reconstructable delivery-log and trust-domain proofs are likewise
not included because they may be refreshed without changing the delivery or
its signed evidence.

The common verifier must compare outer routing and delivery fields to the
cryptographically verified statements. An outer tenant, target, fulfillment,
action, generation, root selector, reference, or lookup index is never
authoritative on its own.

An evidence item's trust domain ID and the trust domain IDs that key
verification material are lookup hints, not trust decisions. The agent loads
its authenticated profile, requires each hinted trust domain to exist and be
applicable to the expected role, and uses only the suite and trust configuration
in that trust-domain entry. Unknown, inapplicable, or failed trust domains are
rejected without fallback.

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
    └── trust-domain material, if required
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

The target verifies `A` through its hinted tenant-user trust domain and `M` and
`P` through their hinted provider-workload trust domains. Those trust domains
may use different trust configurations or suites. Common code then verifies
that each trust domain is applicable and that the resulting federated
identities match the signers required by Alice's authorization and the
authenticated addon registrations. It also verifies that `M` exactly matches the one
`PutManifests` action and satisfies the input-specific strategy, `P` applies to
Fulfillment `fulfillment-cluster-01`, the locally established target is in `P`, and all
constraints and generation checks pass. The outer graph and indexes only
nominate candidates. Replacing a manifest, input reference, placement item, or
target either fails a required signed binding/deterministic check or selects
another output that the signed strategy deliberately authorizes.

## 7. Provenance profile and trust-domain selection

An authorized operator configures one authenticated provenance profile for a
target or target class. The profile is a finite registry of trust domains:

```text
ProvenanceProfile {
    version

    trust_domains {
        trust_domain_id -> TrustDomain {
            suite_id // versioned protocol identifier
            accepted_evidence_media_types[]
            verification_material_media_type
            trust_domain_state_media_type

            suite_target_paths[]
            credential_constraints: TypedBytes // includes accepted identity schemes/authorities

            applies_to[] {
                tenant_id       // exact value or explicit all-tenants marker
                identity_class
                evidence_kind
            }
        }
    }
}
```

The target's accepted profile is implicit in its provisioned trust state and
authenticated management connection. Content and trust deliveries do not name
an alternate profile or management domain. A profile digest may be committed
for consistency and diagnostics, but it is not a deployment identity that
signed evidence must carry.

Conceptually, the selection surface is only
`profile.trust_domains[trust_domain_id] -> TrustDomain`. Common delivery-log
and target-delivery configuration remains outside that map; it does not create
another provenance selector.

A `TrustDomainID` is a stable, globally unique name for one complete FleetShift
verification configuration. It selects a suite, trust roots, credential and
identity constraints, applicability, verification-material format, and opaque
suite state. It is not extracted from a credential and is not part of the
signer's federated identity. A Sigstore trust-domain entry may constrain a
Fulcio authority, accepted OIDC identity authorities, certificate extensions,
TSA, and transparency services together. A v3 entry instead constrains its
continuity namespace, enrollment identity authority, algorithms, map, and
recovery policy.

The suite authenticates an external identity using an open identity scheme. An
OIDC-backed v3 or Fulcio suite returns the exact canonical
`(oidc-sub/v1, iss, sub)` tuple. A SPIFFE suite returns
`(spiffe-id/v1, trust-domain, workload-path)`. Generic X.509 has no universally
stable issuer namespace: a bare X.509 issuer name is neither globally unique nor
safe across unrelated CAs. A profile without a stronger certificate-native
namespace can use a scheme whose authority is the trust-anchor SPKI digest, with
the explicit consequence that unlinked CA rotation changes identity authority.
Preserving identity across such a rotation requires authenticated authority
succession; FleetShift must not hide it behind a stable local identity alias.

Identity-scheme canonicalization is scheme-specific. Common code performs no
generic URL, distinguished-name, or Unicode normalization; it compares the
suite-produced canonical tuple exactly. The scheme name is open and versioned,
so a future suite can add an identity system without adding another branch to
the common verifier. The selected trust-domain entry pins which schemes and
authorities its suite may return.

For example, a profile may contain a tenant-controlled v3 or keyless trust
domain for user authorizations and a provider-controlled SPIFFE, keyless, or
future workload trust domain for manifest and placement addons. Different
trust domains may use the same suite implementation with different roots and
constraints. The same trust domain may apply to several exact semantic roles
when the profile says so.

This trust-domain entry deliberately absorbs the earlier ideas of a route and
an authorization domain. Use a second trust domain when there is a genuinely
distinct credential/trust population or when migration requires explicit
overlap. Use additional `applies_to` entries when one trust domain signs for
several scopes. Who may change each entry is trust-repository governance—
initially TUF roles and delegations—not another runtime verification object.

A profile should not create synonymous trust-domain entries merely to express
different permissions; put semantic scope differences in `applies_to`.
Nevertheless, several trust domains may intentionally authenticate the same
federated identity—for example, v3 and Fulcio configurations during migration.
That does not create two identities. Trust-domain diversity must not be
interpreted as signer-identity diversity; an endorsement policy must state
whether it requires distinct verification paths, distinct federated signers,
or distinct signing acts.

The FleetShift default profile instantiates a continuity/v3 trust domain for
each credential population its initial implementation supports. Selecting
keyless Sigstore or another workload mechanism requires an authenticated
profile change; merely presenting evidence with a new trust domain ID never
enables it.

The profile and every referenced trust target are authenticated from
provisioned state and later trust deliveries. The profile is not accepted
merely because it arrived in a content delivery. `applies_to` enumerates exact
`(tenant, identity_class, evidence_kind)` tuples rather than taking the
Cartesian product of independent allowlists. An explicit all-tenants entry is
permitted for a genuinely provider-wide workload trust domain; implicit
wildcards are not.

The suite ID names a protocol, not a code package or running installation. For
example:

```text
fleetshift.dev/provenance/continuity/v1
fleetshift.dev/provenance/sigstore-keyless/v1
```

All relevant binaries have a static registry keyed by suite ID. Providers may
compile or deploy only the suites they support. Unknown suites fail closed.
Code is never loaded from the couriered package.

Trust-domain selection uses the item's claim only as an exact index into this
pinned configuration:

1. The common engine determines the expected tenant, evidence kind, producer
   class, and any exact signer requirement from the delivery contract, root
   context, or an already verified parent statement.
2. It reads `SignedEvidence.trust_domain_id` as an untrusted hint and looks up
   that exact trust domain in the locally authenticated profile. A missing
   trust domain fails.
3. Common code requires one of that trust domain's `applies_to` entries to
   match the expected tenant, class, and evidence kind. Zero or ambiguous
   matches fail.
4. The trust-domain entry—not the evidence—selects the suite, accepted media
   type, trust targets, credential constraints, verification material, and
   current suite state.
5. The suite verifies the signature and credential under that configuration
   and returns the canonical federated identity `(scheme, authority, subject)`
   plus mechanism facts.
6. Common code records the selected trust domain as verification context and
   checks any exact federated-signer requirement from the attestation semantics.

For the root authorization, the outer tenant and evidence kind nominate the
expected role but remain untrusted until the verified statement repeats and
binds them. Once the root verifies, its signed strategies and any independently
authenticated addon registration establish the expected signer and evidence
kind for addon outputs, placement, relations, and updates. Such a signer is an
exact federated identity or an explicitly authorized set of those identities.
Equal subject strings under different identity schemes or authorities are never
implicitly equivalent; the same exact identity verified through different
trust domains remains the same signer.

A profile may intentionally allow more than one trust domain for a semantic
role.
The evidence item chooses which allowed entry it claims, but this is not
fallback: failure under the selected entry is terminal, and common code never
tries another trust domain. Policies that require endorsements from several
trust domains state that requirement explicitly and verify a separate single-
signature item under each required trust domain.

### 7.1 Migration

Changing a trust domain's suite, accepted identity authorities, or
applicability is a trust and policy transition, not an ordinary content edit.
Routine root or key rotation may remain within one trust domain when its suite
protocol defines
continuity and historical verification. A materially different credential
system or a staged overlap is represented by a second trust-domain entry.

An established delivery agent accepts either change only through authenticated
profile/trust evolution or explicit out-of-band recovery and reprovisioning.
The resource manager cannot turn failed verification under an old trust domain
into a retry under a new one. During a staged migration, policy may require
endorsements under both trust domain IDs over the same canonical statement
digest; the dual requirement is explicit policy, not fallback behavior.

## 8. Concrete interfaces

The interfaces below are Go-like pseudocode. They describe process boundaries
and ownership; they are not yet proposed production Go APIs.

### 8.1 Shared types

```go
type SuiteID string
type TrustDomainID string
type IdentityScheme string // open, versioned, globally unique/registered identifier

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
    TrustDomainID TrustDomainID // untrusted exact lookup hint
    MediaType     string
    Bytes         []byte
}

type CredentialFacts struct {
    KeyID       string
    SigningTime *time.Time
    Attributes  map[string]Value
}

type FederatedIdentity struct {
    Scheme    IdentityScheme
    Authority string
    Subject   string
}

// This is the suite output before common trust-domain, applicability, and
// expected-signer checks admit it as a VerifiedEndorsement.
type VerifiedCredentialStatement struct {
    PredicateType   string
    Statement       []byte
    StatementDigest Digest
    Identity        FederatedIdentity
    Facts           CredentialFacts
}

type VerifiedEndorsement struct {
    Identity                   FederatedIdentity
    VerifiedUnderTrustDomainID TrustDomainID // context, not identity
    Facts                      CredentialFacts
}

type VerifiedStatement struct {
    PredicateType   string
    Statement       []byte
    StatementDigest Digest
    Endorsements    []VerifiedEndorsement
}

type VerifierCheckpoint struct {
    ProfileDigest     Digest
    TrustState        TypedBytes
    DeliveryLog       DeliveryLogCheckpoint
    TrustDomainStates map[TrustDomainID]TypedBytes // opaque to common code
}
```

`CredentialFacts` are mechanism facts, not an open authorization surface.
Implementations register typed fact schemas per suite/version. Policy may
expose a bounded projection to CEL, but arbitrary suite-provided attributes
must not silently become trusted authorization claims.

The suite authenticates and canonicalizes all three fields of
`VerifiedCredentialStatement.Identity` under the selected trust domain's
configuration. Common code treats `Scheme` as an open, versioned
domain-separation tag and compares the complete `(scheme, authority, subject)`
tuple exactly; it does not switch on scheme-specific cases or normalize their
values. The expected identity class and selected trust domain remain
verification context rather than additional components of identity.

`StatementDigest` is distinct from the signed-evidence-item digest. It
domain-separately commits the verified DSSE payload type and exact canonical
statement bytes, so common policy can recognize several endorsements of the
same assertion without binding their certificates, timestamps, signatures, or
Bundle serialization.

An initial standard Bundle yields exactly one `VerifiedEndorsement`. Keeping
endorsements as a list lets a future registered media type carry several
cryptographically verified signatures over one statement without changing the
aggregate or verification-session interface. Common threshold policy groups
endorsements across items by `StatementDigest`. Signatures that require
different trust domains remain separate items because each
trust-domain entry owns its own suite and trust configuration. Common policy
compares federated identities independently from those verification contexts;
it never infers signer diversity merely from different trust-domain IDs.

`VerifierCheckpoint` is the common target report and durable-state envelope.
Common code validates its profile, trust, and delivery-log portions and passes
only the selected opaque trust-domain state to the corresponding suite instance.

### 8.2 Suite registry

The client, resource manager, and delivery agent do not necessarily ship in
one binary, so each role has its own factory registry rather than one giant
`ProvenanceSuite` object.

```go
type SuiteDescriptor struct {
    ID                           SuiteID
    AcceptedEvidenceMediaTypes   []string
    VerificationMaterialType     string
    TrustDomainStateType         string
}

type ClientSuiteFactory interface {
    Descriptor() SuiteDescriptor
    Open(ClientDependencies, TrustedDomain) (ClientSuite, error)
}

type ManagerSuiteFactory interface {
    Descriptor() SuiteDescriptor
    Open(ManagerDependencies, TrustedDomain) (ManagerSuite, error)
}

type AgentSuiteFactory interface {
    Descriptor() SuiteDescriptor
    Open(AgentDependencies, TrustedDomain) (AgentSuite, error)
}
```

Descriptor capabilities are useful for compatibility checks and UX. They are
not a substitute for authenticated policy and are not consulted to weaken a
failed security check. `TrustedDomain` is the already authenticated projection
of one `ProvenanceProfile.trust_domains` entry plus its accepted trust targets; it is
never constructed from content-delivery metadata.

### 8.3 Client signing interface

```go
type SignerRequest struct {
    TrustDomainID TrustDomainID
    TenantID      string
    IdentityClass string
    EvidenceKind  EvidenceKind
    Interactive   bool
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
    IdentityHint() IdentityHint
    Close() error
}

type IdentityControlClient interface {
    SubmitSuiteOperation(
        context.Context,
        SuiteControlOperation,
    ) (SuiteControlReceipt, error)
}

type SuiteControlOperation struct {
    TrustDomainID TrustDomainID
    Kind          string
    OperationID   string
    Payload       TypedBytes
}
```

`AcquireSigner` owns the entire key-acquisition ceremony. It receives narrow
OIDC, secure-key-store, WebAuthn, and user-presentation dependencies; it does
not return private-key bytes. Addon code and mutable UI plugins never receive
a general-purpose signing handle. The returned session is scoped to the
requested trust domain, tenant, identity class, and evidence kind. It rejects a
statement whose signed context does not match that scope and emits the selected
trust domain ID only as the outer `SignedEvidence` lookup hint. The eventual
credential or suite proof must authenticate a federated identity accepted by
that trust domain; the hint itself grants neither identity nor authority.
`IdentityHint` is only for UX and evidence lookup.

Common client code requires `TrustDomainID` to name an applicable entry in the
authenticated profile before opening the suite. Where policy permits several
trust domains, user or client configuration may choose among those entries. A
mutable request or signer cannot introduce an unknown trust domain or change
the selected trust domain's suite, trust, or applicability, even when several trust
domains use the same Bundle media type.

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
    ) (VerifiedCredentialStatement, *VerificationResult, error)

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
    TrustDomainID                   TrustDomainID
    TrustDomainVerificationMaterial TypedBytes
    DeliveryLogSelectors            []DeliveryLogSelector
}

type VerificationMaterialAssemblyRequest struct {
    TrustDomainID              TrustDomainID
    Delivery                   DeliveryContent
    DeliveryDigest             Digest
    AttestationBundleDigest    Digest
    Attestations               AttestationEvidenceBundle
    TrustDomainEvidenceDigests []Digest
    DeliveryLogRecord          DeliveryLogRecord
    CurrentTrustDomainState    TypedBytes
    TrustedDomain              TrustedDomain
}

type RequestVerificationRequest struct {
    TrustDomainID TrustDomainID
    Evidence      SignedEvidence
    EvidenceKind  EvidenceKind
    TenantID      string
    TrustedDomain TrustedDomain
}
```

Common resource-manager code performs ordinary caller authentication and
authorization around `VerifyRequest`, `PrepareControl`, and delivery
submission. For a signed API mutation, it independently verifies the request's
signed evidence item, compares the authenticated federated identity to the live
API caller and operation, performs normal tenant/workspace
authorization, and stores the original item. This result is authoritative for
the resource manager's own request handling, but it is not target delivery
authority; the delivery agent repeats verification from its independent
checkpoint and trust state.

`PrepareControl` validates suite continuity and returns any control commitment
that needs a common delivery-log serialization point. Common code appends it
and passes the exact assigned record to idempotent `Finalize`; v3 uses that
record in the key event that completes rotation. A crash after append but
before finalization leaves an inert marker and resumes the durable workflow on retry.
A suite cannot use successful cryptographic validation to bypass ordinary
FleetShift authorization.

`AssembleVerificationMaterial` is called once for each trust domain used by the
delivery. It is proof construction, not authorization or attestation
construction. Common code has already assembled the independently signed
evidence items, grouped them by trust-domain hint, and checked each trust
domain's common applicability. The suite may read authenticated-map nodes, key
histories, or native transparency services to build the additional proof
material needed to verify those exact items. It
returns trust-domain verification material plus the suite-control records that
common delivery-log code must disclose alongside the delivery commitment. This
material and its selectors are untrusted until the common delivery-log verifier
and selected trust domain's agent-suite instance cross-check them.

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
    TrustDomainID                   TrustDomainID
    Delivery                        DeliveryContent
    Attestations                    AttestationEvidenceBundle
    TrustDomainEvidenceDigests      []Digest
    TrustDomainVerificationMaterial TypedBytes
    TrustedDomain                   TrustedDomain
    DeliveryLogView                 VerifiedDeliveryLogView
    CurrentTrustDomainState         TypedBytes
}

type ExpectedEvidence struct {
    TenantID      string
    Kind          EvidenceKind
    IdentityClass string
}

type VerificationSession interface {
    VerifyEvidenceItem(
        context.Context,
        SignedEvidence,
        ExpectedEvidence,
    ) (VerifiedCredentialStatement, *VerificationResult, error)

    CandidateState() TypedBytes
    Close() error
}
```

`BeginVerification` performs trust-domain-wide work once. For v3 this includes
advancing the authenticated map, validating new key events or recording
exceptions in a candidate state, and indexing selectively supplied identity
proofs. For Sigstore it parses accepted roots/policy and prepares verification
of the Sigstore Bundles assigned to that trust domain.

The common attestation engine looks up the item's trust-domain hint and checks
that trust domain's applicability before it calls `VerifyEvidenceItem`. Each
suite call returns the same `VerifiedCredentialStatement` shape. Common code
supplies the selected trust-domain configuration and admits the returned
federated identity and facts into the `VerifiedStatement`, recording the
selected trust domain separately as verification context. The engine
follows the `AttestationGraph` and validates every graph edge against those verified
statements, and evaluates derivation, placement, output constraints, removal,
and generation without branching on suite ID. It additionally checks the
identity class and any exact expected signer; suite success alone is not
semantic authorization.

Each trust domain's `CandidateState` is never persisted merely because suite
cryptography passed. All candidate trust-domain states are committed only after
the entire common attestation verification succeeds and the content-delivery
contract has durably recorded apply or pending work. Trust-updater state is not
part of this candidate set; it advances through a separate `TrustDelivery`.

### 8.6 Trust domain, identity authority, and credential authority

There is no separate provenance principal-mapping layer. A suite returns the
canonical federated identity that its verified credential establishes:

```text
FederatedIdentity = (IdentityScheme, external authority, external subject)
```

The identity scheme defines canonicalization of both authority and subject. It
is an open, versioned identifier rather than a closed algebraic type: common
code compares the complete tuple exactly and does not need to know how to parse
OIDC, SPIFFE, X.509, or a future identity system. A suite must reject values
that are not canonical for the selected scheme. Common code must not apply
generic URL normalization or assume that identical subject strings imply
identical identities.

For the two initial user suites, both v3 enrollment and Fulcio verification
ultimately authenticate an OIDC identity and therefore return the same form:

```text
(oidc-sub/v1, exact OIDC iss, exact OIDC sub)
```

The mechanisms still have different credential authorities and facts. V3 uses
accepted enrollment and continuity state; keyless Sigstore uses a Fulcio CA,
certificate, and trusted timestamp. A Fulcio CA does not replace the OIDC
issuer as the user's identity authority.

For SPIFFE, the stable identity namespace is the SPIFFE trust domain, not an
individual X.509 CA in the rotating trust bundle:

```text
(spiffe-id/v1, canonical SPIFFE trust domain, canonical workload path)
```

Kubernetes service-account JWTs likewise normally use an OIDC/JWT identity
scheme. A generic X.509 client-certificate profile that provides no stable URI
or other certificate-native identity namespace may instead use a domain-
separated trust-anchor SPKI digest as its authority. That is unambiguous but
makes an unlinked CA rotation an identity-authority change. Preserving identity
through rotation requires an authenticated succession mechanism; a FleetShift
trust-domain ID must not disguise that change.

The trust domain remains authenticated verification context. It selects a
suite and constrains accepted identity schemes, identity authorities,
credential authorities, algorithms, timestamps, transparency services, and
subject patterns. It may be recorded on a `VerifiedEndorsement` for policy,
audit, and migration requirements, but it never qualifies or replaces the
federated identity. The same exact identity can therefore be verified through
v3 and Fulcio during an explicit migration without becoming two principals.

Ordinary API account mapping and authorization remain resource-manager
concerns. Where attestation semantics name a stable addon or producer, its
authenticated registration may authorize one or more exact federated
identities; the addon or producer resource ID is not the signer's identity.

The current Sigstore POC's `_authenticate_identity` and the hybrid POC's
`TrustAnchor.verify_signer` should therefore be split conceptually into
trust-domain credential and identity-authority checks followed by common
expected-signer checks. Neither suite accepts a signed item's trust-domain
hint, subject label, addon ID, or anchor label as authority without proving the
federated identity under the selected trust-domain configuration.

## 9. Shared trust distribution

### 9.1 Initial decision: TUF for low-churn trust configuration

The first implementation should use a stateful TUF updater for:

- the provenance profile and trust-domain registry;
- each trust domain's credential constraints, suite trust roots, and parameters;
- trust-root and policy rotation with rollback/freeze protection.

The provisioned bootstrap for the profile's trust repository is a complete TUF
root or a stronger out-of-band anchor. TUF delegations may give tenant and
provider operators control of separate trust-domain target namespaces without
adding another runtime ownership abstraction. A later `TrustDelivery` may
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

The receiving agent already has one locally pinned updater and accepted
profile. A trust event does not name an alternate profile or updater. The
verified repository metadata and resulting profile must chain from that
retained state.

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
established profile is itself pinned by authenticated state; it is not chosen
by the trust or content delivery.

### 9.2 Initial TUF targets

Profile target:

```text
fleetshift/profile.json
```

V3 suite targets:

```text
fleetshift/trust-domains/<trust-domain-key>/continuity-v1/trust-manifest.json
```

The trust manifest carries OIDC enrollment issuer/client, accepted algorithms,
map and delivery-log parameters, recovery constraints, and trust-update policy
that is not already represented by TUF roles.

Sigstore suite targets:

```text
fleetshift/trust-domains/<trust-domain-key>/sigstore-keyless-v1/trusted-root.json
```

This is the standard Sigstore `TrustedRoot` containing the applicable Fulcio,
TSA, Rekor, and CT authorities. Additional credential constraints may live in
the profile entry or a typed target under the same trust-domain namespace. A
profile repository may delegate tenant and provider trust-domain namespaces to
different TUF roles; verification does not need a second runtime object to
represent that ownership.

`<trust-domain-key>` is a specified path-safe encoding or digest of the exact
opaque trust domain ID, not the raw value interpolated into a filesystem path.
The profile authenticates the target paths and each target repeats or otherwise
commits its trust domain ID so alternate encodings and path normalization
cannot alias trust domains.

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
trust-domain states. An invalid content policy, signature, placement, or apply
must not roll back an independently valid trust update. Conversely, a content
delivery cannot smuggle a trust update inside its verification material and
make itself valid in one inseparable step.

`ContentDelivery.verification.required_trust_state` lets an agent report that
it needs a particular prior trust sync. It never authorizes use of stale state:
the locally current authenticated policy remains authoritative, and a newer
revocation or trust-domain change cannot be bypassed by asking for an older
version.

## 10. Shared append-only delivery log

### 10.1 Initial decision: use it for every provenance suite

The v3 delivery log should become common FleetShift delivery infrastructure.
Every durable mutation is committed before dispatch:

```text
DeliveryCommitment {
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
    previous_trust_state_digest
    update_digest
    resulting_trust_state_digest
}
```

Suites may define purpose-separated control records:

```text
SuiteControlCommitment {
    trust_domain_id
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
`VerifiedDeliveryLogView` and interprets only its trust-domain-namespaced control
records. TUF remains the authority for whether a trust update is valid; the
delivery log adds ordering, idempotency, and audit but does not replace TUF
metadata verification.

### 10.2 What this gains

- V3 keeps its exact rotation-cutoff semantics per trust domain without owning a
  private delivery transport protocol.
- Both suites get the same established-agent rollback protection for delivery
  history and the same lost-acknowledgement/catch-up path.
- Retention, tiling, compaction, and checkpoint reporting are implemented once.
- A future key-transparency trust domain can bind control events into the same
  delivery ordering scope when useful.

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

1. Load the retained trust state and the updater pinned for the locally
   accepted profile. Never select an updater from the event payload.
2. Recompute the update digest, check the previous-state/idempotency
   precondition, and verify any applicable common delivery-log commitment.
3. Stage the TUF refresh from retained roots and metadata. Verify root rotation,
   signatures, thresholds, versions, expiry, rollback/freeze protections, and
   target hashes.
4. Validate the resulting profile and trust-domain descriptors against the
   statically installed suite registry without loading code or weakening
   policy.
5. Atomically persist the candidate trust state, applicable delivery-log
   checkpoint, and trust-delivery workflow state, then acknowledge it.

If a later content delivery fails, this independently accepted trust state
remains committed. If the trust event fails, no candidate metadata or profile
becomes visible.

### 11.2 Content delivery

1. Load the authenticated provenance profile and current durable trust,
   delivery-log, trust-domain, target, and fulfillment states.
2. Match the outer tenant and target to locally established identity. Compare
   `required_trust_state` with current authenticated state; if required state
   is missing, request/defer for a separate trust delivery.
3. Canonicalize `DeliveryContent`, `AttestationGraph`, and the exact set of
   independently signed items. Recompute every typed-evidence digest and the
   aggregate digest; ignore reconstructable indexes for authority.
4. Verify delivery-log consistency from the retained checkpoint, inclusion of
   the exact content and attestation-bundle digests, the authenticated profile
   digest, and all relevant trust-domain control records.
5. Read the root `SignedEvidence.trust_domain_id` as an untrusted lookup hint.
   Require an exact trust-domain entry in the authenticated profile and require
   it to apply to the expected tenant, identity class, and evidence kind. Then
   start that trust domain's verification session. The trust-domain entry—not
   the evidence—pins the suite, trust material, accepted media types, and
   credential constraints.
   Reject an unknown or inapplicable trust domain, a mismatched media type, or
   any verification failure without trying another trust domain.
6. Verify the root authorization, then follow the `AttestationGraph`. As
   verified statements establish expected addon, placement, relation, or
   update roles, apply the same exact trust-domain lookup and applicability
   checks to each signed item. Verify each credential and signature under only
   the trust-domain entry selected by its outer lookup hint. Return normalized
   statements and federated identities; never infer
   authority from graph IDs, package labels, unverified JWT or certificate
   issuer fields, or lookup indexes. Reject every signed item not consumed by a
   recognized role or explicit endorsement policy.
7. Reconstruct derived inputs with the common hybrid evaluator using the
   verified, purpose-typed statements selected by the graph. Suites do not
   implement update authorization or derivation policy.
8. Evaluate the verified root input against the one concrete action according
   to `docs/design/authentication.md`, including the owner-resource-to-
   Fulfillment association, removal fulfillment-ID equality, graph limits, and
   target-local generation fencing.
9. Atomically or crash-consistently persist:

   - candidate common delivery-log checkpoint;
   - candidate state for every trust domain used by this delivery;
   - common fulfillment generation/state and any required authorization-
     consumption record; and
   - either completed apply state or enough pending work to guarantee retry.

10. Acknowledge according to the target delivery contract.

Failures before step 9 discard candidate trust-domain and content-delivery
state, but not trust state accepted by an earlier `TrustDelivery`. A protocol
may define a separate durable rejection transition—for example, v3 recording
an invalid identity event as a bounded exception so the delivery log cannot be
wedged.
Such a transition may commit only explicitly defined delivery-log or trust-
domain rejection state; it never advances fulfillment generation, applies
content, or makes the failed identity authoritative. The common rejection/skip
protocol remains an open question below.

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
| High-churn identity state     | Identity history + authenticated head map                  | Short-lived certificate; optional Rekor/CT evidence        |
| Manager proof work            | Map/history/event proofs keyed to signed item digests      | Usually only courier each producer's existing bundle       |
| Shared delivery log           | Deliveries + rotation markers                              | Deliveries; no required control marker                     |
| Trust-domain checkpoint       | Map root + bounded exceptions                              | Empty initially; later native log/witness checkpoints      |
| Common checkpoint             | Profile TUF state + delivery-log root/size                 | Profile TUF state + delivery-log root/size                 |
| Federated identity            | `oidc-sub/v1` issuer and subject from accepted enrollment   | `oidc-sub/v1` issuer and subject from verified certificate |
| Verified credential facts     | Continuity state and signing-key identifiers                | Fulcio CA, certificate properties, and timestamp           |
| Historical signer rule        | Adjacent history events and marker interval                | Cert chain + trusted timestamp + retained historical roots |

Each column describes one trust-domain instance, not an all-or-nothing delivery
mode. A profile may configure multiple trust domains that use the same suite
with distinct configuration and trust, or use continuity for one trust domain
and keyless for another within the same content delivery.
Applicability may differ as well, but it cannot be the only distinction between
trust domain IDs.

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

Per-trust-domain v3 verification material contains identity proofs keyed to the
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

The v3 trust-domain checkpoint is the existing map root and exceptional-event
set. The delivery-log checkpoint moves to common state:

```text
ContinuityTrustDomainState {
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

Per-trust-domain verification material can initially be an empty, versioned
object. A prior TUF `TrustDelivery` installs the Sigstore `TrustedRoot`; the
content delivery carries only its authenticated-state requirement. The common
delivery-log proof commits the concrete delivery and exact aggregate set of
signed evidence items.

`verify_sigstore_bundle` maps naturally to `VerifyEvidenceItem` and runs once
for each Sigstore Bundle. Certificate and timestamp checks yield credential
facts. The suite enforces the configured credential constraints; common code
requires the verified statement's tenant and evidence kind and the expected
identity class to match. From the verified Fulcio identity extensions, the
suite returns the canonical `oidc-sub/v1` authority and subject. The selected
trust domain is recorded separately as the configuration under which that
identity and signature were accepted.

The Sigstore trust domain initially has no suite-specific checkpoint. Its
persistent TUF state belongs to the accepted profile, and its delivery-log
checkpoint is common. If Rekor, CT, or witness consistency becomes part of
acceptance, their retained checkpoints become trust-domain state without
changing the common verifier interface.

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
| `TrustStore`, `KeyBinding`, and `OutputSignature` verification | Trust-domain-selected credential verification and constraints, not common wire structs                                                                                                 |
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
  type, purpose, and tenant separation;
- signer-selected trust-anchor IDs become untrusted trust-domain hints resolved
  through the authenticated profile;
- key binding and signature verification move behind trust-domain-selected suite
  interfaces;
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
  same normalized federated identity and credential facts.
- Per-trust-domain `CandidateState` contains accepted log and witness checkpoints.

The independently signed statement model, authenticated trust-domain selection,
federated identity, aggregate FleetShift evidence bundle, delivery log, apply
path, and acknowledgement contract do not change.

## 14. Security invariants

1. **Authenticated trust-domain selection:** the accepted profile and trust
   updater come from provisioned or authenticated local state. An evidence
   item's `trust_domain_id` is only an untrusted exact-key lookup into that
   profile. The matching trust-domain entry pins the suite, trust roots,
   accepted evidence and verification-material media types, applicability, and
   credential and identity-authority constraints; package data cannot choose
   any of them. The trust-domain ID remains selection context and is not
   manufactured from credential fields.
2. **No fallback:** an unknown or inapplicable trust domain, an ambiguous
   expected role, or any parse, trust, constraint, or verification failure is
   terminal for that item. Verification never tries another trust domain after
   the selected one fails.
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
7. **Federated identity:** a suite derives a canonical
   `(scheme, authority, subject)` only after verifying the configured credential
   mechanism. Common code compares that complete tuple exactly. Raw JWT,
   certificate, SPIFFE, key, signer, or anchor labels do not select trust, and
   equal subject strings cannot merge identities across schemes or authorities.
8. **Exact applicability:** the selected trust domain must explicitly apply to
   the expected tenant, identity class, and evidence kind. Every wildcard is
   explicit authenticated policy, not an implicit default.
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
    update that makes its trust domain acceptable. Trust deliveries authenticate
    and commit independently; trust-domain verification material remains
    untrusted until checked from retained checkpoints and roots.
13. **Transactional state:** within a content delivery, the delivery-log
    checkpoint, every used trust-domain state, generation, authorization-
    consumption record, and durable apply state do not advance independently in
    a way that grants authority after failure.
    A previously accepted trust delivery is not rolled back by a later content
    failure. Explicit durable rejection/exception state may advance only when
    it cannot authorize or apply the rejected content.
14. **Rollback protection:** established agents never silently re-enter new-
    agent bootstrap after checkpoint loss, expiry, or compaction lag.
15. **Bounded verification:** graph size/depth, lookup-index size, proof sizes,
    signed-item count, statement sizes, and suite-specific work have explicit
    limits.
16. **Explainable failure:** errors identify profile/trust-domain selection,
    trust delivery, delivery-log, credential, identity, signature, applicability,
    attestation, constraint, or generation failure without exposing secret
    material.
17. **Controlled signer surface:** key handles are purpose-restricted and are
    not exposed to addon UI code or arbitrary byte-signing callers.
18. **Migration is authorization:** a profile or trust-domain change must chain
    from current trust or use explicit recovery; dual acceptance is explicit
    policy, never a compatibility fallback.
19. **No deployment identity claim:** signed evidence is not intrinsically
    bound to a resource-manager URL, cluster, or management-domain identifier.
    Another authenticated management plane may accept it only if that plane
    has a trust-domain configuration that authenticates the same federated
    identity and finds that every signed semantic constraint and local
    trust-domain applicability check matches. Target identity,
    Fulfillment/generation state, accepted profile state, and retained
    checkpoints remain local and are not made portable by the signature.

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
and a canonical federated identity, not a universal input representation.

### An untyped issuer string or closed identity union

OIDC `iss`, a SPIFFE trust domain, an X.509 issuer name, and a CA-key digest do
not share canonicalization or lifecycle semantics. Putting their bare values in
one untagged string permits cross-mechanism collisions and tempts common code to
apply unsafe generic URL or name normalization. A closed union avoids those
collisions but requires the common API to change whenever a new identity system
appears.

The open, versioned `IdentityScheme` plus canonical authority and subject keeps
the values domain-separated while letting common code treat the tuple as an
opaque exact identity. Encoding the same scheme tag into a single URI-shaped
identifier could also be safe, but it would be the same discriminator hidden in
a harder-to-validate string.

### One interface method per v3 concept

Making `Enroll`, `RotateContinuityKey`, `ProveMapLeaf`, and
`VerifyRotationMarker` part of the common API would merely rename v3 as the
abstraction. Lifecycle payloads and suite evidence must remain typed and
suite-owned behind common transport/session boundaries.

### Letting the package select the verifier

Trying all installed suites or accepting a package-declared suite enables
downgrade and cross-protocol confusion. The package may carry a
`trust_domain_id` lookup hint, but only the authenticated profile entry for
that exact ID chooses the suite, trust material, accepted media types,
applicability, and credential constraints. An unknown, inapplicable, or failing
trust domain is rejected
without fallback. Successful verification returns the federated identity; it
does not reinterpret the trust-domain hint as an identity field.

### A signed management-domain or resource-manager URL

A resource-manager URL is an operational locator, not a stable security
identity. Signing it would add friction to endpoint, load-balancer, and cluster
migrations without proving which infrastructure the recipient controls. A
free-standing management-domain claim has the same problem: unless it is bound
through an independently authenticated channel, another controller can simply
claim the same value.

Evidence is therefore portable across management planes by design. Portability
does not grant control of the same infrastructure: a second plane must already
have equivalent trust-domain configuration, target authority, target-local
Fulfillment and generation state, and consistent retained proof state. If a
concrete threat later requires narrower replay scope, add a signed audience
whose expected value is derived from an authenticated channel. Do not introduce
a nominal management-domain object merely as a label.

### Putting a TUF update in content verification material

TUF metadata is independently authenticated, versioned trust-delivery content,
not a proof about one attestation. Letting a content delivery carry and commit
the update that causes its own trust domain or signer to become trusted
conflates authority transition with content authorization and makes failure
semantics unclear. Deliver the TUF refresh as a prior `TrustDelivery`, even when both
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
- tenant-user and provider-workload evidence verified through distinct trust
  domains, including different tenants with different configured trust domains;
- unknown trust domain, inapplicable trust domain, wrong identity class, wrong
  evidence kind, and attempted cross-trust-domain fallback rejection;
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
- trust-domain-hint, identity-scheme, authority, subject, signer-label, and
  anchor-label substitution;
- proof that relabeling a signed item selects only the newly hinted
  authenticated trust-domain entry and never causes fallback or changes the
  federated identity derived from the credential;
- proof that equal subjects under different identity schemes or authorities
  remain distinct, while the same exact federated identity remains equal when
  verified through different suites or trust domains;
- missing, ambiguous, and wrong-evidence-kind applicability matches;
- manager modification of signed content or interpretation fields;
- trust-delivery rollback, freeze, expiry, root-rotation, previous-state, and
  target tamper;
- rejection of an embedded content trust update and proof that a valid prior
  trust delivery remains committed after later content failure;
- delivery-log leaf/root/inclusion/consistency tamper;
- stale manager checkpoint and lost acknowledgement;
- no authority-granting candidate trust-domain state advances after any
  verification failure boundary; prior independently committed events and
  explicitly permitted non-authorizing rejection state remain intact;
- graph cycles, excessive graph depth, proof amplification, and oversized
  signed-evidence-item rejection;
- several single-signature Bundles over the same statement evaluated according
  to explicit endorsement/threshold policy—including required trust domain IDs
  when applicable—rather than any-verifier fallback;
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
4. Introduce authenticated `ProvenanceProfile.trust_domains`, exact trust-domain
   lookup, applicability constraints, and canonical federated identity.
   Exercise at least three configurations against the same semantic tests:
   all-continuity trust domains, all-keyless trust domains, and one composed
   delivery whose tenant-user and provider-workload evidence use different
   trust domains or suites. Pin unknown, inapplicable, ambiguous-role, and
   fallback failures.
5. Introduce per-trust-domain `ManagerSuite` and `AgentSuite` sessions plus
   `DeliveryVerificationMaterial.trust_domains`. Move v3 map/identity proof work
   behind its trust domain and leave keyless trust-domain material empty
   initially.
6. Extract the v3 delivery log into the common delivery-log component. Commit
   the exact `DeliveryContent`, graph, and signed-item set, and keep all cutoff,
   catch-up, stale-checkpoint, and lost-ack tests.
7. Add the stateful TUF updater and first-class `TrustDelivery`. Publish the
   profile, trust-domain applicability and credential constraints, v3 trust
   manifest, and Sigstore `TrustedRoot` as authenticated targets. Use TUF roles
   and delegations where different administrators must govern trust-domain
   namespaces; do not add a parallel runtime authorization-domain model. Prove
   that content cannot embed its own update and that a committed trust delivery
   survives later content rejection.
8. Add crash injection around trust-delivery commit and, separately, candidate
   delivery-log, trust-domain, generation, authorization-consumption, and apply
   state before treating the interfaces as viable.

The smallest useful spike is steps 1-4 around the existing hybrid and Sigstore
parity tests. It directly tests the most uncertain seams—single-copy delivery,
verified derivation recipes, and multi-trust-domain composition—before
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
8. Require every evidence item to name its trust domain using a stable,
   globally unique opaque or URI-shaped `trust_domain_id`. Treat the outer value
   only as an exact lookup hint into the authenticated profile. The trust-domain
   entry selects one suite and must apply to the expected tenant, identity
   class, and evidence kind. Verify only under that selected entry and forbid
   fallback; do not incorporate the trust-domain ID into signer identity.
9. Use TUF for the profile, trust-domain constraints, and low-churn trust
   material in every initial installation, transported and committed as a
   separate `TrustDelivery`. Use TUF roles and delegations for divided
   governance rather than adding a runtime authorization-domain abstraction.
10. Use one common delivery log for a target regardless of which trust domains
    occur in a delivery.
11. Normalize verified mechanism evidence into the exact federated identity
    `(identity_scheme, external_authority, external_subject)` plus bounded
    credential facts. Keep identity schemes open and versioned, compare their
    canonical tuples exactly, and do not add a global principal-remapping layer.
12. Stage all content-related delivery-log, trust-domain, generation, and apply
    state and let the common content-delivery transaction commit it. Do not roll
    back an independently committed trust delivery after later content failure.
13. Represent multiple endorsements as several single-signature evidence items
    grouped by canonical statement digest unless measured payload size
    justifies a purpose-specific signed digest-endorsement predicate. When
    trust-domain diversity matters, name the required trust domains explicitly.
14. Keep suite control protocols typed and opaque to the core beyond transport,
    authorization, idempotency, and receipts.

## 19. Open questions for iteration

### Profile ownership and trust-domain scope

What owns the final target provenance profile: a provider root, a tenant root,
or a provisioned higher-level root that delegates trust-domain namespaces to
both? The runtime model needs only one authenticated trust-domain registry, but
TUF delegations must prevent one administrator from silently replacing a trust
domain owned by another administrator.

How expressive should trust-domain applicability be beyond tenant, identity
class, and evidence kind? The initial matcher should remain finite and exact.
Workspace IDs or predicate-specific constraints should be added only when a
concrete use case cannot be expressed by the signed statement and the trust
domain's credential constraints.

Which workload verifier should demonstrate multi-trust-domain composition
first? Separate Fulcio authorities exercise the current Bundle code with little
novelty; SPIFFE/SPIRE or Kubernetes workload identity would better test that
the trust-domain-selected suite interface is not accidentally keyless-specific.

### Common delivery log

Is the operational cost of appending every durable delivery to the log
acceptable for providers that select Sigstore? The proposed answer is yes
because it also unifies retry, rollback, and audit semantics, but the first POC
should measure append and proof costs independently of the suite.

Should the common delivery log be per tenant, provider, target group, or should
lower-scope logs be anchored into a higher-scope log? V3 rotation needs one
ordering domain spanning every target a key can authorize, while provider
workload trust domains may apply across many tenants.

### TUF ownership and granularity

Should one provisioned TUF root delegate tenant- and provider-owned trust-domain
namespaces, or should some deployments accept several independently rooted
profile fragments and compose them? A single root plus delegation is simpler;
multiple roots may provide stronger administrative isolation. Either shape
must prevent the resource manager from becoming the unilateral root authority
and must produce one unambiguous effective entry for each `trust_domain_id`.

How are trust-domain applicability and credential-constraint changes authorized
relative to suite-specific trust-manifest changes? TUF roles and delegations may
make the v3 trust manifest's separate update-policy machinery smaller than
currently designed.

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

What registry, length limits, and cross-language test vectors should define
`IdentityScheme` values and their canonical authority/subject encodings? The
first POC needs at least `oidc-sub/v1` and should reserve the SPIFFE and generic
X.509 profiles until their canonical forms and rotation semantics are tested.
The common verifier should require a scheme declared by the selected suite and
trust-domain configuration but should never gain a switch over known schemes.

What registration and lifecycle rules should apply to `trust_domain_id` values?
They must be compared as opaque exact strings, remain stable across routine
trust rotation, and never be reassigned to an unrelated credential source. URI
shape may help administration but must not cause network discovery or semantic
URL comparison during verification.

Signed statements deliberately omit a management-domain ID and resource-
manager URL. What concrete future threat would justify a narrower signed
audience? If one is added, the verifier's expected value must come from an
authenticated channel, and endpoint changes must not silently change it.

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

Can TUF metadata, trust-domain state, common delivery-log state, and target
generation be stored with the required per-event atomicity in the fleetlet?
Trust delivery and content delivery intentionally use separate transactions;
within each one, the durable ordering protocol and recovery states need to be
specified before production.

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

When should a trust change remain within one trust domain, and when should it
introduce a second trust domain? Routine root/key rotation should normally
preserve the trust domain ID. A materially different credential source, or a
staged v3 → Sigstore migration with explicit dual acceptance, should normally
use a second trust domain. Recovery and unavailable-old-trust-domain cases
still need explicit policy.

Which migration and high-risk operations require several endorsements over one
statement? The initial representation is several single-signature Bundles
grouped by canonical statement digest. Thresholds, required trust domain IDs,
and identity diversity must be authenticated policy, not “any installed
verifier accepts.” A digest-endorsement predicate should be added only after
measuring large real statements and aggregate compression.

### Client UX

What does `AcquireSigner` return when user interaction or an external service
is unavailable: a synchronous error, a durable challenge, or a `PausedAuth`
continuation? The interface likely needs a challenge/result state before it is
made production-ready.

How narrowly can browser and addon signing APIs be scoped so the user sees the
tenant, target scope, action, and content identity at the actual signing
boundary for both suites?
