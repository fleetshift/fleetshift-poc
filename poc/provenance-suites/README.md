# Provenance suite APIs

This POC exercises the three-sided provenance contract from
[`docs/design/architecture/provenance.md`](../../docs/design/architecture/provenance.md)
without committing to Sigstore, TUF, or continuity/v3.

It asks:

> Can a producer, resource manager, and target share one profile contract —
> create evidence, store and assemble it, verify it against authenticated
> authority configuration — so that a later well-known profile can replace the
> naive implementation without changing common selection or
> `AuthenticatedEvidence`?

The tests demonstrate that the answer is yes for a deliberately naive
`direct-key/v1` profile.

## The three APIs

A provenance profile is a configured implementation of a common contract:

```text
ProducerAPI
  CreateEvidence(exact purpose-typed assertion) -> TypedEvidence

ResourceManagerAPI
  AssembleSupportMaterial(TypedEvidence) -> replaceable support material
  DecodeAssertion(TypedEvidence) -> untrusted inner statement
  CheckDelivery(TypedEvidence) -> tentative principal and predicate hints

TargetAPI
  ParseHints(TypedEvidence) -> tentative (scheme, authority, tenant, subject, predicate)
  Verify(SignedStatement, authenticated profile and authority config)
      -> AuthenticatedEvidence, authenticated inner assertion
  Owns(predicate) -> whether this profile applies that suite-owned event
  Apply(authenticated result, evidence-log index assigned at RM acceptance)
      -> update suite-owned retained state
```

A `SignedStatement` is one independently authenticated assertion:
immutable `TypedEvidence` plus replaceable `SupportMaterial` used to verify
that evidence. A delivery package couriers each statement as an `Item`,
which may also carry that statement's evidence-log inclusion. Root and
supporting items are the same kind of object. The inner statement lives in
the evidence bytes; `Verify` emits it. Profile selection and `Apply` take
the `SignedStatement`, not the enclosing `Item`.

Common resource-manager code never parses `TypedEvidence` bytes. It owns the
immutable evidence repository and assigns each accepted identity one
canonical evidence-log position. Content deliveries look up the installed
profile by provenance type, unwrap the inner statement with
`DecodeAssertion`, then read routing identity with `DecodeDeliveryScope`.
Suite submit APIs such as enrollment do not: they authorize, check, register
the evidence identity, and enqueue one dispatch per currently relevant
agent. That pairing is why statement encodings can stay common while
evidence encodings stay profile-owned.

Common target code runs the documented selection algorithm:
untrusted provenance type and hints locate `AuthorityConfig`, one
unambiguous delivery policy supplies an ordered `any-of` profile list, the
first complete success wins, then principal and tenant mapping are
re-checked. Implementations arrive through trusted software; unknown types
fail closed.

Lifecycle operations stay typed per mechanism at the resource manager.
Direct-key enrollment is not a generic `RegisterKey` and is not a method of
`DeliveryAgent`. After the RM accepts it, enrollment is ordinary accepted
evidence: the identity is registered once in the evidence log and one
outbox dispatch is created per currently registered agent. Authenticated
predicate type then selects apply: intent predicates use fulfillment apply,
predicates the selected profile `Owns` call `TargetAPI.Apply`, and
`trust-config-update/v1` is reserved on the agent. Unknown predicates fail
closed even if policy matched.

## Typing layers

These identifiers are not interchangeable:

- **Provenance type** (`direct-key/v1`) selects the installed verifier.
- **Encoded** is media type plus bytes. `TypedEvidence` embeds it with a
  provenance type. `SupportMaterial` and `TypedManifest` are that same form
  as distinct types: support is refreshable and implied by the evidence it
  accompanies; a manifest is a payload item inside an authenticated
  deployment. `TypedAssertion` is not Encoded — predicate type is purpose,
  not a media type.
- **Media type** is how those bytes are encoded (proof encoding such as the
  direct-key signature stand-in for Sigstore Bundle v0.3, or payload encoding
  such as `application/vnd.example.replicas+json`).
- **Predicate type** is the inner assertion purpose. Policy matches the
  authenticated value after `Verify`. `ParseHints` and `DecodeAssertion`
  may expose it earlier as an untrusted hint used only to locate policy or
  route a request. Root user predicates in this POC are `deployment/v1` and
  `managed-resource/v1`. `fulfillment-relation/v1` is supporting evidence
  for managed resources only. `direct-key/enrollment/v1` is a suite-owned
  predicate that `direct-key/v1` declares via `Owns` and applies through
  `TargetAPI.Apply`. `trust-config-update/v1` remains an agent-owned
  sibling root predicate.

A deployment assertion carries typed manifests. A managed-resource assertion
carries a resource spec and resource type (API identity, not a media type).
Both root authorizations sign a `DeliveryScope`: the AIP-122 full resource
name of the Deployment or ManagedResource (1-1 with its fulfillment; not an
RM-assigned fulfillment ID), a `TargetID` stand-in for static placement,
generation, and action. The agent applies a managed resource only after
verifying a couriered fulfillment relation that names the derived payload
media type. Unused supporting evidence is not authority: a relation couriered
with a deployment does not change apply.

## The naive profile

`direct-key/v1` is a stand-in, not one of the initial production profiles.

- The producer holds one Ed25519 key pair bound to a claimed `oidc-sub/v1`
  principal.
- Enrollment evidence directly shares the public key plus a proof of
  possession. `Verify` authenticates that proof without a retained key.
  `Apply` is the mapping transition: first bind is trust-on-first-use;
  later substitution of an established mapping is rejected. This profile
  does not verify an issuer assertion that the claimant is that subject.
- Delivery evidence carries the inner statement, the signature, and a user
  reference, not the public key. Support material is empty. Verification
  uses only the retained mapping. `Verify` emits the authenticated
  statement; `DecodeAssertion` unwraps it without authenticating.

That is weaker than continuity/v3 or Sigstore on purpose. A compromised
resource manager can win the first enrollment bind. It cannot later
substitute an established mapping, forge a delivery signature, or alter
signed content and have the target accept it.

## The three FleetShift roles

```text
controlled producer
  - identifies the allowed provenance type and principal authority
  - creates purpose-typed deployment and managed-resource authorizations
  - uses direct-key CreateEvidence and CreateEnrollment
               |
               | TypedEvidence
               v
resource manager
  - performs ordinary API authorization through an Authorizer hook
  - stores immutable TypedEvidence in a common repository
  - assigns one evidence-log position per accepted TypedEvidence identity
    (root, supporting, and lifecycle), independently of later deliveries
    or target fanout
  - enqueues separate delivery/outbox records; a log index is not a retry
    handle
  - couriers a package-wide evidence-log update (checkpoint transition and
    consistency from the agent's last ack) plus an Item for each stored
    identity (SignedStatement with that identity's inclusion), including
    optional supporting items such as fulfillment relations
  - submits typed direct-key enrollment the same way: register, enqueue,
    then Dispatch to every currently registered agent
  - assembles empty support material for this profile
  - has an explicit CompromisedManager attack harness
               |
               | untrusted package
               |   EvidenceLog  *EvidenceLogUpdate (checkpoint + consistency)
               |   Root         Item (SignedStatement + inclusion)
               |   Supporting  []Item
               v
delivery agent
  - is bootstrapped with authenticated AuthorityConfig
  - never returns to TOFU after initialization
  - verifies package-wide log consistency and inclusion of the root Item
    before profile selection. This POC does not yet verify supporting-item
    inclusion.
  - selects a profile from policy and verifies independently
  - dispatches on authenticated predicate type: intent apply, profile-owned
    suite Apply, or the reserved trust-config-update handler
```

Storage is in memory. The evidence log is an RFC 6962 Merkle tree. The honest
RM assigns each accepted `TypedEvidence` identity one canonical leaf at
acceptance, not per delivery or outbox entry. Retrying or reusing evidence
does not append it again. Each couriered package carries a shared checkpoint
transition plus consistency proof, and each `Item` discloses that statement's
accepted position. The verifier recomputes the evidence identity from the
adjacent statement; inclusion does not serialize a leaf digest. Unrelated
accepted-evidence leaves are skipped via consistency, not listed. This POC
still verifies inclusion only for the root Item. There is no attestation
graph, credential presentation, rotation, or historical cutoff yet. Those
belong to the hybrid attestation POC and the mature profiles. This suite
implements only `RegisteredSelfTarget` for fulfillment relations.

## Security cases pinned by tests

| Scenario | Result |
| --- | --- |
| Producer enrolls and signs a `deployment/v1` authorization | Target applies the typed manifests |
| Enrollment is logged and Delivered to every registered agent | One evidence-log leaf and two dispatches; both agents Apply the mapping; later content consistency-proves over the enrollment leaf |
| Root plus two supporting statements accepted together | Three new evidence-log leaves; stored delivery holds identities, not duplicate bytes |
| Same supporting evidence reused in a later delivery | First index retained; log grows only for new evidence |
| Same evidence resubmitted after intervening leaves | Canonical index does not move; no new leaf |
| Duplicate supporting evidence in one acceptance | One leaf; stored support lists the identity once |
| Root repeated in support | Omitted from stored support; one leaf |
| Accept without a registered route, then Dispatch | Evidence is registered; outbox stays pending until the route exists; Dispatch does not append |
| Acknowledged Dispatch retry | No-op; no new leaf and no repeated authorization |
| `deployment/v1` and `trust-config-update/v1` | Do not call suite `Apply`; trust-config-update fails closed until the agent handler exists |
| Policy-matched predicate the profile does not `Owns` | Fail closed without calling suite `Apply` |
| Producer signs a `managed-resource/v1` spec with an addon-signed fulfillment relation | Target applies the derived manifest of the relation's media type; supporting items carry inclusions at their canonical indexes |
| Managed resource with no relation, wrong resource type, or unenrolled relation signer | Rejected |
| Fulfillment relation couriered with a deployment | Ignored; deployment apply is unchanged |
| Unknown root predicate | Fail closed |
| Deployment item missing manifest media type | Rejected |
| Resource manager signs a delivery with an unenrolled key | Rejected |
| Resource manager changes assertion bytes after the user signs | Rejected by content digest binding |
| Resource manager wins first enrollment for a claimed subject | Accepted (TOFU limitation) |
| Resource manager substitutes the key after the mapping is retained | Rejected |
| Resource manager bypasses RBAC but forwards genuine evidence | Accepted by the agent |
| Unknown provenance type | Fail closed |
| Second bootstrap of an initialized verifier | Rejected |
| Lost acknowledgement then retry | Idempotent apply via DispatchID; manager cache catches up via stale-checkpoint recovery |
| Lost acknowledgement, other target advances the log, then retry | Agent reports a stale checkpoint; manager rebuilds proofs without appending evidence |
| Rejected delivery after a verified log update | Log checkpoint advances; retry recovers the manager cache without applying or growing the log |
| Delivery to B while A is idle, then delivery to A | A's consistency proof covers B's leaf without disclosing B's evidence |
| Root Item inclusion does not prove root evidence identity | Rejected |
| Forked or skip-ahead log proofs | Rejected as a log fork, not reported as stale |

## Run it

From this directory:

```sh
go test -count=1 -v ./...
```

No external identity provider, database, or transparency service is required.

## File guide

| Path | Purpose |
| --- | --- |
| `protocol/` | TypedEvidence, Item, Principal, AuthorityConfig, selection, evidence-log update and inclusion, and the three APIs |
| `internal/merklelog/` | In-memory RFC 6962 compact-range store copied from the v3 POC |
| `directkey/` | Naive profile: enrollment, signature encoding, retained mapping |
| `producer/` | Controlled-producer role |
| `resourcemanager/` | Authorization, common evidence repository, evidence log, delivery/outbox, last-ack cache, typed enrollment accept, compromise harness |
| `deliveryagent/` | Bootstrap, package-wide log consistency, root Item inclusion, selection, verification, predicate dispatch, apply |
| `provenance_test.go` | End-to-end guarantees and accepted TOFU limitation |

## Recommended next experiments

1. Implement continuity/v3, Sigstore, and TUF behind the same three APIs.
2. Stop at `AuthenticatedEvidence` and hand the result to the hybrid
   attestation graph instead of applying a delivery authorization here.
3. Implement `trust-config-update/v1` on the agent-owned handler, still
   through the same evidence log, selection, and delivery path.
4. Add claim-derived tenant mapping and a second authority in one package.
