# V3 trust-distribution prototype

This POC exercises the authentication-provenance and key-continuity core from
[`docs/design/trust_model_v3.md`](../../docs/design/trust_model_v3.md) without
committing to production storage or the full FleetShift derivation model.

It asks:

> Can a delivery agent bind a standard OIDC login to a client-held continuity
> key, accept only content signed by that key, and enforce a locally observed
> key-rotation cutoff while the resource manager authorizes, orders, stores,
> and couriers requests without originating their cryptographic provenance?

The tests demonstrate that the answer is yes, subject to the deliberately
local history and controlled-client limitations in the design.

## The three FleetShift roles

```text
controlled client
  - runs authorization code + PKCE against an ordinary OIDC provider
  - generates and retains an Ed25519 continuity key
  - signs enrollment proof, rotations, and simple content attestations
               |
               | enrollment package / signed delivery / signed rotation
               v
resource manager
  - performs ordinary API authorization through an Authorizer hook
  - stores plain ordered trust and delivery records
  - routes the exact records and evidence to the target
  - has an explicit CompromisedManager attack harness in the tests
               |
               | untrusted records and evidence
               v
delivery agent
  - is provisioned with tenant, target, OIDC issuer, and enrollment client ID
  - independently fetches OIDC discovery and JWKS
  - verifies nonce binding, key proof of possession, signatures, continuity,
    delivery ordering, rotation cutoffs, target binding, and generation
  - retains local trust and delivery checkpoints
```

The local OIDC provider is supporting infrastructure, not a FleetShift role.
It serves discovery, authorization, token, and JWKS endpoints over TLS. The
authorization endpoint uses a test-only automatic login selected by
`login_hint`; the authentication UX is a test double, but the wire flow is a
real one-time authorization-code exchange with state, nonce, PKCE S256,
issuer, audience, expiry, and a signed ID token. The resource manager never
mints or rewrites that token.

## Deliberate simplifications

The prototype uses two in-memory append-only slices with linear SHA-256 hash
chains. A checkpoint is only `(record count, last record hash)`. Every agent
receives each intervening record rather than a compact consistency proof. A
Merkle log, Tessera, tiles, durable storage, and skip proofs can replace this
mechanism later without changing the signed enrollment, rotation, or delivery
objects.

Signatures and hashes currently use Go's deterministic encoding of fixed JSON
structs. That is sufficient to test this single-language model, but it is not
a cross-language canonical-JSON specification. Reusing DSSE/in-toto or defining
versioned canonical test vectors is required before treating the encoding as a
wire protocol.

The content model is also intentionally small. A user signs a
purpose-separated `ContentAttestation` binding:

- tenant and target;
- identity and continuity-state digest;
- fulfillment and generation;
- put or remove action; and
- the exact content digest.

This stands in for the authoritative input/derivation/output/removal model in
`poc/attestation/hybrid` and `poc/attestation/sigstore_tuf_bundle`. Device and
session delegation are collapsed into the continuity key. There is no trust
manifest rotation, TUF, DSSE/in-toto envelope, TSA, tombstone, recovery,
semantic exception set, apply loop, durable acknowledgement, or external fork
witness yet.

The resource manager's `Authorizer` is intentionally an interface boundary,
not another identity system in this POC. It makes the distinction between
ordinary platform permission and delivery-agent provenance explicit.

## Protocol exercised

### Enrollment

1. The client generates its continuity key and an `EnrollmentIntent`.
2. The OIDC nonce is the digest of that intent, including tenant, issuer,
   enrollment client, enrollment ID, and continuity-key digest.
3. The client completes authorization code + PKCE and verifies the returned ID
   token through discovery and JWKS.
4. The client proves possession of the continuity key by signing the intent.
5. The resource manager appends and forwards the package.
6. The delivery agent repeats ID-token validation, checks the nonce and proof
   of possession, derives `identity_id = H(tenant, iss, sub)`, and records the
   initial continuity state.

### Delivery

1. The client signs the simple content attestation.
2. The resource manager performs its normal authorization check and appends a
   delivery record.
3. The delivery agent pins the structural log record, verifies the content
   digest and signature under the claimed continuity state, applies the
   state's validity interval, and fences stale generations.

### Rotation

1. The client obtains the current delivery checkpoint as a rotation barrier.
2. The old continuity key signs a transition binding the predecessor state,
   new-key digest, new generation, and exact cutoff checkpoint.
3. The new key signs the same transition as proof of possession.
4. An agent accepts the transition only if the cutoff belongs to delivery
   history that agent has already observed.
5. The old state is valid below the cutoff; the new state is valid at and
   above it.

## Security cases pinned by tests

| Scenario | Result |
| --- | --- |
| Resource manager substitutes only the enrollment public key | Rejected by the nonce-bound key digest |
| Resource manager replaces the full key binding and supplies valid attacker proof of possession with the old ID token | Rejected by the ID-token nonce |
| Resource manager replays an already accepted enrollment | Rejected because the identity is already enrolled |
| Resource manager signs a delivery with its own key while claiming the user's identity | Rejected by continuity-key verification |
| Resource manager changes content or delivery metadata after the user signs | Rejected by the content digest or signature |
| Resource manager fabricates a rotation with attacker-controlled old and new signatures | Rejected because the current continuity key did not authorize it |
| Resource manager substitutes the new key in a genuine transition | Rejected by the signed new-key digest and proof of possession |
| A transition lacks proof of possession by the genuine new key | Rejected even though the old key authorized the transition |
| Retired key signs at or after a cutoff already accepted by an agent | Rejected by that agent |
| Resource manager presents a fork from an older delivery checkpoint | Rejected by an established agent that retained the newer checkpoint |
| Resource manager presents a fork from an older trust checkpoint | Rejected by an established agent that retained the newer checkpoint |
| Resource manager bypasses its own RBAC but forwards a genuine, otherwise valid user signature | Accepted by the agent; complete RBAC is intentionally not reproduced there |
| Resource manager withholds rotation from one agent, then uses a compromised old key | The rotated agent rejects; the stale agent accepts on its local pre-rotation view |

The last two rows are tests, not disclaimers hidden outside the executable
model. They establish the precise boundary of the additional guarantee.

## Run it

From this directory:

```sh
go test -count=1 -v ./...
```

No external identity provider, database, container runtime, or log service is
required.

## File guide

| Path | Purpose |
| --- | --- |
| `client/` | Controlled-client OIDC flow, continuity key, delivery signing, and rotation |
| `resourcemanager/` | Ordinary authorization, ordered storage, courier behavior, and explicit compromise harness |
| `deliveryagent/` | Stateful target-side OIDC, continuity, signature, cutoff, log, and generation verification |
| `protocol/` | Deterministically encoded purpose-separated messages, signatures, digests, and linear record chains |
| `internal/testoidc/` | Minimal TLS OIDC authorization-code provider |
| `trust_model_test.go` | End-to-end guarantees and accepted limitations across all three roles |

## Recommended next experiments

1. Persist delivery-agent checkpoints and pending work, then inject crashes at
   every write boundary to prove acknowledgement and retry invariants.
2. Add the trust-manifest update-policy profiles and role-separated TUF root or
   delegated metadata before adding storage scale optimizations.
3. Replace the simple content attestation's encoding with the existing
   Sigstore Bundle and in-toto/DSSE types while retaining these exact tests.
4. Add device and session delegation, identity tombstones, recovery, and the
   optional OIDC reanchor paths.
5. Add semantic exceptions for invalid trust events so one bad principal does
   not halt an agent's entire trust stream.
6. Replace linear chains with compact append proofs and selective delivery,
   then compare a small Merkle implementation with Tessera.
7. Add proactive checkpoint distribution and optional peer comparison to show
   how the stale-agent window changes without making gossip mandatory.
8. Model the trust and delivery state machines formally and fuzz canonical
   encodings, mutations, replay, and crash interleavings.
