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
  RequiresEvidenceLog() -> whether this suite needs FleetShift log positions
  BeginVerification(SignedStatement, authenticated profile and authority config)
      -> single-use session: Prepare identifies timestamp bindings;
         Finish authenticates and returns constraints
  Owns(predicate) -> whether this profile applies that suite-owned event
  Apply(authenticated result, verified subject temporal facts)
      -> update suite-owned retained state
```

A `SignedStatement` is one independently authenticated assertion:
immutable `TypedEvidence` plus replaceable `SupportMaterial` used to verify
that evidence. A delivery package couriers each statement as an `Item`,
which may also carry that statement's evidence-log inclusion. Root and
supporting items are the same kind of object. The inner statement lives in
the evidence bytes; `Finish` emits it. `SelectAndVerify` takes the package
`Item` so it can verify the sibling inclusion; `VerifyRequest` and `Apply`
take the `SignedStatement`, not the enclosing `Item`.

Common resource-manager code never parses `TypedEvidence` bytes. It owns the
immutable evidence repository and assigns each log-required identity one
canonical evidence-log position. Content deliveries look up the installed
profile by provenance type, unwrap the inner statement with
`DecodeAssertion`, then read routing identity with `DecodeDeliveryScope`.
Suite submit APIs such as enrollment do not: they authorize, check, register
the evidence identity, and enqueue one dispatch per currently relevant
agent. That pairing is why statement encodings can stay common while
evidence encodings stay profile-owned.

Common target code runs the documented selection algorithm:
untrusted provenance type and hints locate `AuthorityConfig`, the first
matching delivery policy supplies an ordered `any-of` profile list,
each candidate runs `Prepare`, occurrence verification, and `Finish`,
the first complete success wins, then external principal, tenant identity, and
policy selection are rechecked. Implementations arrive through trusted software; unknown types
fail closed.

Each authority defines profile configurations once. Delivery policies refer to
those configurations by exact digest, and can match one external tenant
partition or default to all partitions. Policies are evaluated in configured
order, and the first match wins. Put tenant exceptions before the match-all
default; an earlier default shadows later exceptions. Policy order is bound
by the authority-configuration digest. Verification failure never falls back
to another policy. References are resolved before any attempt, and authenticated
identity must select the same policy entry as the hints. Configuration
validation checks even shadowed policies' structure and profile references.
Evidence cannot supply anchors or choose historical configuration by digest.

A tenant identity is `(scheme, authority, partition?)`, derived from the
profile-authenticated principal. Producers, signed delivery scopes, and agent
provisioning use that external identity. Internal tenant IDs exist only in RM
configuration and its permission hook. The agent needs no external-to-internal
mapping. Each assertion authenticates under its own source policy. Common
semantics then bind resource intent to its provisioned tenant and fulfillment
relations to the platform-wide provider reference. Each platform has one
provider tenant and zero or more consumers, sharing one API; the provider
reference is provisioned separately from authority/profile definitions.

Lifecycle operations stay typed per mechanism at the resource manager.
Direct-key enrollment is not a generic `RegisterKey` and is not a method of
`DeliveryAgent`. After the RM accepts it, enrollment is ordinary accepted
evidence: the identity is registered once in the evidence log and one
outbox dispatch is created per currently registered agent. Authenticated
predicate type then selects apply: intent predicates use fulfillment apply,
predicates the selected profile `Owns` call `TargetAPI.Apply`, and
`trust-config-update/v1` is reserved on the agent. Unknown predicates fail
closed even if policy matched.

## Value ownership

Evidence, package items, published trust configuration, temporal constraints,
and verification results are immutable within common code, including their
nested slices and pointers. Common lookups, selection, normalization, cache
reads, and semantic callbacks share those values directly. The Go types
explicitly document this contract.

External package ingestion and trust bootstrap take independent snapshots.
Copies at profile and time-adapter boundaries isolate their buffers from
common state. Cached results are shared internally, and suite-owned Apply
receives a detached request. Replace an immutable value with a new one when
its content or configuration changes.

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
  authenticated value after `Finish`. `ParseHints` and `DecodeAssertion`
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
  possession. `Finish` authenticates that proof without a retained key.
  `Apply` is the mapping transition: first bind is trust-on-first-use;
  later substitution of an established mapping is rejected. This profile
  does not verify an issuer assertion that the claimant is that subject.
- Delivery evidence carries the inner statement, the signature, and a user
  reference, not the public key. Support material is empty. Verification
  uses only the retained mapping. `Finish` emits the authenticated
  statement; `DecodeAssertion` unwraps it without authenticating.
- `RequiresEvidenceLog` is false and `Prepare` returns no timestamp
  bindings. Delivery policy decides whether inclusion is required; common
  selection also verifies any supplied optional inclusion. `Finish` returns
  no constraints, and normalization yields an unbounded window.

That is weaker than continuity/v3 or Sigstore on purpose. A compromised
resource manager can win the first enrollment bind. It cannot later
substitute an established mapping, forge a delivery signature, or alter
signed content and have the target accept it.

## Temporal comparison status

The common temporal helper compares time coordinates only. `ObservedBy` on a
boundary records which authority supplied that boundary; it does not require a
subject observation from the same authority. The helper assumes its subject
times have already been admitted by the trusted-time source policy and
endorsed for that subject by `Finish`. Its current demonstrated rule is that
any one such observation may satisfy the window, and the same observation must
satisfy both establishment and retirement.

The direct-key profile emits no timestamps, and this POC has no concrete TSA
verifier or source policy. Future source and threshold policy should be
authenticated with the authority/profile configuration, use an explicit set
of approved authorities, and reject unconfigured sources. The helper tests
demonstrate coordinate semantics; they do not implement TSA trust policy.

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
  - resolves each accepted item's source policy and unions its log requirement
    with the installed mechanism's requirement
  - assigns one position per log-required TypedEvidence identity (root,
    supporting, and lifecycle), independently of delivery or target fanout
  - enqueues separate delivery/outbox records; a log index is not a retry
    handle
  - couriers an Item for each stored identity, including fulfillment support;
    registered items carry inclusion under a shared checkpoint update with
    consistency from the agent's last acknowledgement
  - omits the update when no couriered item is registered
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
  - first bounds and snapshots the whole couriered package; it rejects
    duplicate evidence identities, bounds checkpoint-root encodings, and
    validates every supplied proof's encoding
  - verifies any supplied package log update and root inclusion through
    `temporal.Prepare` before profile selection; absent log material leaves
    retained state unchanged
  - uses one private `verificationSession` per delivery to select and verify
    the root and any semantically selected supporting items through
    `SelectAndVerify`; successful results are memoized by evidence identity
    under fixed session trust; semantic callbacks check tenant relationships
    independently on every use, including cache hits
  - parses supporting hints lazily through each item's installed provenance
    verifier; hints locate candidates but do not authorize them or create
    dependency edges
  - leaves unused supporting inclusions cryptographically unverified
  - projects the **root** `VerificationResult` into `ApplyRequest.Temporal`
    (used-supporting positions stay on that node's result, not in Apply)
  - dispatches on authenticated predicate type: intent apply, profile-owned
    suite Apply, or the reserved trust-config-update handler
```

Storage is in memory. The evidence log is an RFC 6962 Merkle tree. The honest
RM assigns each log-required `TypedEvidence` identity one canonical leaf at
registration, not per delivery or outbox entry. Retrying or reusing evidence
does not append it again. A package with registered items carries a shared
checkpoint transition and consistency proof, and each registered `Item`
discloses its position. A policy can require root logging, support logging,
both, or neither. Mechanism requirements cannot be disabled by policy. The
verifier recomputes the evidence identity from the adjacent statement; inclusion
does not serialize a leaf digest. Unrelated accepted-evidence leaves are skipped via consistency, not listed. The agent
accepts a supplied log update only when `From` equals its retained checkpoint;
an older `From` is stale even if Merkle consistency against the retained head
would succeed. This POC verifies supplied root inclusion in `Prepare`; an
unlogged root can accompany logged support. The verification session calls
`SelectAndVerify` for the root and any used supporting Item. Repeated
authentication of an identity reuses the successful result; each semantic use
still checks its tenant relationships. Assertion purpose selects provenance
policy regardless of package position; predicate dispatch determines whether the root can produce
an action or apply a suite-owned control event. Unused supporting inclusions
are structurally checked but not cryptographically verified. There is no
full attestation graph, credential presentation, rotation, or historical cutoff
yet. Those belong to the hybrid attestation POC and the mature profiles. This suite implements only
`RegisteredSelfTarget` for fulfillment relations.

## Security cases pinned by tests

| Scenario | Result |
| --- | --- |
| Producer enrolls and signs a `deployment/v1` authorization | Target applies the typed manifests |
| Enrollment is logged and Delivered to every registered agent | One evidence-log leaf and two dispatches; both agents Apply the mapping; later content consistency-proves over the enrollment leaf |
| Root plus two supporting statements under log-required policies | Three new evidence-log leaves; stored delivery holds identities, not duplicate bytes |
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
| Tampered unused supporting inclusion | Ignored; deployment still applies |
| Malformed proof digest on unused support | Rejected during structural catalog validation |
| Tampered used supporting inclusion | Rejected; checkpoint still advances |
| Missing used supporting inclusion under a log-required policy | Rejected; checkpoint still advances |
| Unknown root predicate | Fail closed |
| Authenticated fulfillment relation presented as the root | Rejected by predicate dispatch; no effects |
| Original intent reused as a dependency of a later delivery | Same assertion-purpose policy; verified once per identity in each delivery |
| Cached authentication used for different semantic tenant checks | Matching use succeeds, mismatched use fails; one profile attempt, no edge from the rejected use |
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
| Package constructed from an older `From` than the retained checkpoint | Stale, even when equal-size RFC 6962 consistency against the retained head would no-op, and even when the successor lags or forks that head |
| `From` newer than retained, or same-size different `From` root | Rejected as a log fork, not reported as stale |
| Delivery to B while A is idle, then delivery to A | A's consistency proof covers B's leaf without disclosing B's evidence |
| Root Item inclusion does not prove root evidence identity | Rejected |
| Forked or skip-ahead log proofs | Rejected as a log fork, not reported as stale |
| Duplicate root/supporting identity in one couriered package | Rejected before log preparation or apply |
| More supporting candidates than the POC work bound | Rejected without truncating the candidate set |
| Multiple hinted fulfillment relations (phase-6 migration rule) | Temporarily rejected as ambiguous before candidate provenance verification; phase 7 selects authenticated matches and skips candidate-local hint failures within the work bound |
| Shared issuer, distinct consumer/provider partitions with the same subject | Separate principals; relation uses provider policy and provisioned tenant |
| Consumer/provider policies independently require logging | All four combinations apply; only required identities register and retries add no leaves |
| Mechanism requires logging while delivery policy makes it optional | Inclusion remains required at RM and target |
| Tenant exception before default, or default before exception | First match controls logging at RM and target, including consumer/provider policies and retries |
| First matching policy fails while a later policy could verify | Rejected without policy fallback |
| Omitted tenant hint would select a different policy entry | Rejected after authentication |
| Missing or duplicate profile configuration references | Invalid configuration before profile attempts |
| Caller has the same issuer/subject but another tenant partition | RM rejects enrollment or delivery |
| Authenticated relation with a mismatched or missing platform provider reference | Rejected before dependency selection or apply |
| Signed delivery scope | Carries external tenant identity; internal routing ID stays in RM permission requests |

## Run it

From this directory:

```sh
go test -count=1 -v ./...
```

No external identity provider, database, or transparency service is required.

## File guide

| Path | Purpose |
| --- | --- |
| `protocol/` | TypedEvidence, Item, Principal, AuthorityConfig, selection coordinator (`SelectAndVerify` on Item), evidence-log update and inclusion, ordered-log and validity-window types, `VerifiedSubjectTemporalInfo` on `ApplyRequest`, and the three APIs |
| `internal/merklelog/` | In-memory RFC 6962 compact-range store copied from the v3 POC |
| `temporal/` | Ordered-log adapter: `Prepare`, memoizing `VerifyOccurrence` |
| `directkey/` | Naive profile: enrollment, signature encoding, retained mapping, trivial Prepare/Finish session |
| `producer/` | Controlled-producer role |
| `resourcemanager/` | Authorization, common evidence repository, evidence log, delivery/outbox, last-ack cache, typed enrollment accept, compromise harness |
| `deliveryagent/` | Bootstrap, bounded structural evidence catalog, per-delivery memoized verification session, `temporal.Prepare` for supplied package consistency and root inclusion, predicate dispatch, apply |
| `provenance_test.go`, `policy_integration_test.go` | End-to-end guarantees, cross-tenant policy/log combinations, and accepted TOFU limitation |

## Recommended next experiments

1. Implement continuity/v3, Sigstore, and TUF behind the same three APIs.
2. Stop at `AuthenticatedEvidence` and hand the result to the hybrid
   attestation graph instead of applying a delivery authorization here.
3. Implement `trust-config-update/v1` on the agent-owned handler, still
   through the same evidence log, selection, and delivery path.
4. Recover external tenant partitions from real credentials and exercise a
   second authority in the same package.
