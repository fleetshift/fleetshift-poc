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

TargetAPI
  ParseHints(TypedEvidence) -> tentative (scheme, authority, tenant, subject)
  Verify(evidence, support, authenticated profile and authority config)
      -> AuthenticatedEvidence
```

Common target code runs the documented selection algorithm:
untrusted provenance type and hints locate `AuthorityConfig`, one
unambiguous delivery policy supplies an ordered `any-of` profile list, the
first complete success wins, then principal and tenant mapping are
re-checked. Implementations arrive through trusted software; unknown types
fail closed.

Lifecycle operations stay typed per mechanism. Direct-key enrollment is not a
generic `RegisterKey`, is not on the common APIs, and is not a method of
`DeliveryAgent`. The resource manager couriers it through a separate
`DirectKeyEnroller`.

## The naive profile

`direct-key/v1` is a stand-in, not one of the initial production profiles.

- The client holds one Ed25519 key pair bound to a claimed `oidc-sub/v1`
  principal.
- Enrollment evidence directly shares the public key plus a proof of
  possession. First acceptance is trust-on-first-use: this profile does not
  verify an issuer assertion that the claimant is that subject.
- The verifier retains the public key and user mapping.
- Delivery evidence carries the signature and user reference, not the public
  key. Support material is empty. Verification uses only the retained
  mapping.

That is weaker than continuity/v3 or Sigstore on purpose. A compromised
resource manager can win the first enrollment bind. It cannot later
substitute an established mapping, forge a delivery signature, or alter
signed content and have the target accept it.

## The three FleetShift roles

```text
controlled client
  - identifies the allowed provenance type and principal authority
  - creates purpose-typed delivery authorizations
  - uses direct-key CreateEvidence and CreateEnrollment
               |
               | TypedEvidence
               v
resource manager
  - performs ordinary API authorization through an Authorizer hook
  - stores immutable TypedEvidence
  - appends an in-memory delivery commitment before dispatch
  - assembles empty support material for this profile
  - has an explicit CompromisedManager attack harness
               |
               | untrusted package
               v
delivery agent
  - is bootstrapped with authenticated AuthorityConfig
  - never returns to TOFU after initialization
  - selects a profile from policy and verifies independently
  - applies the authenticated delivery authorization
```

Storage is in memory. The delivery log is size-ordered without Merkle proofs.
There is no attestation graph, credential presentation, rotation, or
historical cutoff yet. Those belong to the hybrid attestation POC and the
mature profiles.

## Security cases pinned by tests

| Scenario | Result |
| --- | --- |
| Client enrolls and signs a delivery authorization | Target applies the payload |
| Resource manager signs a delivery with an unenrolled key | Rejected |
| Resource manager changes assertion bytes after the user signs | Rejected by content digest binding |
| Resource manager wins first enrollment for a claimed subject | Accepted (TOFU limitation) |
| Resource manager substitutes the key after the mapping is retained | Rejected |
| Resource manager bypasses RBAC but forwards genuine evidence | Accepted by the agent |
| Unknown provenance type | Fail closed |
| Second bootstrap of an initialized verifier | Rejected |
| Lost acknowledgement then retry | Idempotent apply |

## Run it

From this directory:

```sh
go test -count=1 -v ./...
```

No external identity provider, database, or transparency service is required.

## File guide

| Path | Purpose |
| --- | --- |
| `protocol/` | TypedEvidence, Principal, AuthorityConfig, selection, and the three APIs |
| `directkey/` | Naive profile: enrollment, signature encoding, retained mapping |
| `client/` | Controlled-client role |
| `resourcemanager/` | Authorization, storage, log, routing, typed direct-key enrollment courier, compromise harness |
| `deliveryagent/` | Bootstrap, selection, verification, apply |
| `provenance_test.go` | End-to-end guarantees and accepted TOFU limitation |

## Recommended next experiments

1. Implement continuity/v3, Sigstore, and TUF behind the same three APIs.
2. Add Merkle inclusion and consistency proofs to the common delivery log.
3. Stop at `AuthenticatedEvidence` and hand the result to the hybrid
   attestation graph instead of applying a delivery authorization here.
4. Add `trust-config-update/v1` through the same profile-selection path.
5. Add claim-derived tenant mapping and a second authority in one package.
