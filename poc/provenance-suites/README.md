# Provenance suite APIs

This POC exercises the three-sided provenance contract from
[`docs/design/architecture/provenance.md`](../../docs/design/architecture/provenance.md)
without committing to Sigstore, TUF, or continuity/v3.

It asks:

> Can a client, resource manager, and target share one profile contract —
> create evidence, store and assemble it, verify it against authenticated
> authority configuration — so that a later well-known profile can replace the
> naive implementation without changing common selection or
> `AuthenticatedEvidence`?

The tests demonstrate that the answer is yes for a deliberately naive
`direct-key/v1` profile.

## The three APIs

A provenance profile is a configured implementation of a common contract:

```text
ClientAPI
  CreateEvidence(exact purpose-typed assertion) -> TypedEvidence

ResourceManagerAPI
  StoreEvidence(TypedEvidence)
  AssembleSupportMaterial(TypedEvidence) -> replaceable support material
  DecodeAssertion(TypedEvidence) -> untrusted inner statement
  CheckDelivery(TypedEvidence) -> tentative principal and predicate hints

TargetAPI
  ParseHints(TypedEvidence) -> tentative (scheme, authority, tenant, subject, predicate)
  Verify(SignedStatement, authenticated profile and authority config)
      -> AuthenticatedEvidence, authenticated inner assertion
  Owns(predicate) -> whether this profile applies that suite-owned event
  Apply(authenticated result, log index) -> update suite-owned retained state
```

A `SignedStatement` is one independently authenticated assertion as couriered:
immutable `TypedEvidence` plus replaceable `SupportMaterial` used to verify
that evidence. Root and supporting items in a delivery package are the same
type. The inner statement lives in the evidence bytes; `Verify` emits it.

Common resource-manager code never parses `TypedEvidence` bytes. Content
deliveries look up the installed profile by provenance type, unwrap the inner
statement with `DecodeAssertion`, then read routing identity with
`DecodeDeliveryScope`. Suite submit APIs such as enrollment do not: they
authorize, check, append the evidence identity, and deliver to every
registered agent. That pairing is why statement encodings can stay common
while evidence encodings stay profile-owned.

Common target code runs the documented selection algorithm:
untrusted provenance type and hints locate `AuthorityConfig`, one
unambiguous delivery policy supplies an ordered `any-of` profile list, the
first complete success wins, then principal and tenant mapping are
re-checked. Implementations arrive through trusted software; unknown types
fail closed.

Lifecycle operations stay typed per mechanism at the resource manager.
Direct-key enrollment is not a generic `RegisterKey` and is not a method of
`DeliveryAgent`. After the RM accepts it, enrollment is an ordinary logged
delivery: the evidence identity is appended and `Deliver` is called on every
registered agent. Authenticated predicate type then selects apply: intent
predicates use fulfillment apply, predicates the selected profile `Owns`
call `TargetAPI.Apply`, and `trust-config-update/v1` is reserved on the
agent. Unknown predicates fail closed even if policy matched.

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
The agent applies a managed resource only after verifying a couriered
fulfillment relation that names the derived payload media type. Unused
supporting evidence is not authority: a relation couriered with a deployment
does not change apply.

## The naive profile

`direct-key/v1` is a stand-in, not one of the initial production profiles.

- The client holds one Ed25519 key pair bound to a claimed `oidc-sub/v1`
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
controlled client
  - identifies the allowed provenance type and principal authority
  - creates purpose-typed deployment and managed-resource authorizations
  - uses direct-key CreateEvidence and CreateEnrollment
               |
               | TypedEvidence
               v
resource manager
  - performs ordinary API authorization through an Authorizer hook
  - stores immutable TypedEvidence
  - appends the root evidence identity to an RFC 6962 Merkle log
  - couriers a log update (consistency from the agent's last ack plus
    inclusion of this leaf) with each independently authenticated
    SignedStatement (root plus optional supporting statements such as
    fulfillment relations)
  - submits typed direct-key enrollment the same way: log then Deliver to
    every registered agent
  - assembles empty support material for this profile
  - has an explicit CompromisedManager attack harness
               |
               | untrusted package
               |   Log         LogUpdate
               |   Root        SignedStatement
               |   Supporting []SignedStatement
               v
delivery agent
  - is bootstrapped with authenticated AuthorityConfig
  - never returns to TOFU after initialization
  - verifies log consistency and inclusion before profile selection
  - selects a profile from policy and verifies independently
  - dispatches on authenticated predicate type: intent apply, profile-owned
    suite Apply, or the reserved trust-config-update handler
```

Storage is in memory. The delivery log is an RFC 6962 Merkle tree. Each
couriered package discloses one leaf — this root `TypedEvidence` identity —
and proves append-only growth from the agent's retained `(size, root)`
checkpoint. Unrelated leaves are skipped via consistency, not listed.
There is no attestation graph, credential presentation, rotation, or
historical cutoff yet. Those belong to the hybrid attestation POC and the
mature profiles. This suite implements only `RegisteredSelfTarget` for
fulfillment relations.

## Security cases pinned by tests

| Scenario | Result |
| --- | --- |
| Client enrolls and signs a `deployment/v1` authorization | Target applies the typed manifests |
| Enrollment is logged and Delivered to every registered agent | Both agents Apply the mapping; later content consistency-proves over the enrollment leaf |
| `deployment/v1` and `trust-config-update/v1` | Do not call suite `Apply`; trust-config-update fails closed until the agent handler exists |
| Policy-matched predicate the profile does not `Owns` | Fail closed without calling suite `Apply` |
| Client signs a `managed-resource/v1` spec with an addon-signed fulfillment relation | Target applies the derived manifest of the relation's media type |
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
| Lost acknowledgement then retry | Idempotent apply; manager cache catches up via stale-checkpoint recovery |
| Lost acknowledgement, other target advances the log, then retry | Agent reports a stale checkpoint; manager rebuilds proofs |
| Rejected delivery after a verified log update | Log checkpoint advances; retry recovers the manager cache without applying |
| Delivery to B while A is idle, then delivery to A | A's consistency proof covers B's leaf without disclosing B's evidence |
| Log leaf does not match root evidence identity | Rejected |
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
| `protocol/` | TypedEvidence, Principal, AuthorityConfig, selection, log update, and the three APIs |
| `internal/merklelog/` | In-memory RFC 6962 compact-range store copied from the v3 POC |
| `directkey/` | Naive profile: enrollment, signature encoding, retained mapping |
| `client/` | Controlled-client role |
| `resourcemanager/` | Authorization, storage, Merkle log, last-ack cache, typed enrollment submit, compromise harness |
| `deliveryagent/` | Bootstrap, log verification, selection, verification, predicate dispatch, apply |
| `provenance_test.go` | End-to-end guarantees and accepted TOFU limitation |

## Recommended next experiments

1. Implement continuity/v3, Sigstore, and TUF behind the same three APIs.
2. Stop at `AuthenticatedEvidence` and hand the result to the hybrid
   attestation graph instead of applying a delivery authorization here.
3. Implement `trust-config-update/v1` on the agent-owned handler, still
   through the same log, selection, and delivery path.
4. Add claim-derived tenant mapping and a second authority in one package.
