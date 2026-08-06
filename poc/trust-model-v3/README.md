# V3 trust-distribution prototype

This POC exercises the authentication-provenance and key-continuity core from
[`docs/design/trust_model_v3.md`](../../docs/design/trust_model_v3.md) without
committing to production storage or the full FleetShift derivation model.

It asks:

> Can a delivery agent bind a standard OIDC login to a client-held continuity
> key, accept only content signed by that key, and enforce a locally observed
> log-serialized key-rotation boundary while the resource manager authorizes,
> orders, stores, and couriers requests without originating their cryptographic
> provenance?

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
  - serializes deliveries and rotation markers in one tenant log
  - derives direct sparse-map leaf-update proofs for key-history heads
  - routes the exact records and evidence to the target
  - has an explicit CompromisedManager attack harness in the tests
               |
               | untrusted records and evidence
               v
delivery agent
  - is provisioned with tenant, target, OIDC issuer, and enrollment client ID
  - independently fetches OIDC discovery and JWKS
  - verifies nonce binding, key proof of possession, signatures, key-history
    extension, map roots, marker ordering, target binding, and generation
  - retains its accepted map root and delivery-log checkpoint
```

The local OIDC provider is supporting infrastructure, not a FleetShift role.
It serves discovery, authorization, token, and JWKS endpoints over TLS. The
authorization endpoint uses a test-only automatic login selected by
`login_hint`; the authentication UX is a test double, but the wire flow is a
real one-time authorization-code exchange with state, nonce, PKCE S256,
issuer, audience, expiry, and a signed ID token. The resource manager never
mints or rewrites that token.

## Deliberate simplifications

The prototype uses one in-memory append-only delivery slice, an ordinary
catch-up sequence of authenticated-map updates, and one linear SHA-256
key-event chain per identity. The delivery checkpoint and history head are only
`(record count, last record hash)`. The authenticated map is a 256-level sparse
Merkle tree: each update proves the old leaf or its absence under the agent's
retained root, then reuses the same sibling path to compute the root with only
that leaf replaced. The manager rebuilds paths in memory and sends all 256
siblings rather than compressing default subtrees.

The map-update sequence is not an authenticated log. Its records are merely a
way to find the chain of individually verified root transitions needed by a
stale agent. Production Merkle logs, compressed sparse-map proofs, Tessera,
tiles, durable storage, and skip proofs can replace the remaining linear
mechanisms without changing the cutoff-free signed rotation authorization.

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
5. The resource manager creates the first per-identity key event and advances
   the authenticated map to its history head.
6. The delivery agent repeats ID-token validation, checks the nonce and proof
   of possession, derives `identity_id = H(tenant, iss, sub)`, verifies the map
   update, and records the initial continuity state and history head.

### Delivery

1. The client signs the simple content attestation.
2. The resource manager performs its normal authorization check and appends a
   delivery record.
3. The delivery agent proves the record is in an append-only extension of its
   retained log, verifies the content digest and signature under the claimed
   continuity state, applies the marker-bounded validity interval, and fences
   stale generations.

### Rotation

1. The old continuity key signs an authorization binding the predecessor
   state, new-key digest, and new generation; it signs no manager checkpoint.
2. The new key signs the same authorization as proof of possession.
3. Under one sequencer lock, the resource manager appends that package as a
   rotation marker in the tenant delivery log and creates the next per-user
   key event referencing the exact marker index and hash.
4. The authenticated map advances from the previous key-history head to the
   append-only successor head using a direct old-root-to-new-root leaf proof.
5. An agent may accept this structural map update before reaching the marker,
   but it cannot use either side of the affected key interval until the exact
   marker is proven in its accepted delivery-log history.
6. If the marker occupies index `C`, the old state is valid for delivery
   indexes below `C`; the new state is valid for indexes above `C`.

## Security cases pinned by tests

| Scenario | Result |
| --- | --- |
| Resource manager substitutes only the enrollment public key | Rejected by the nonce-bound key digest |
| Resource manager replaces the full key binding and supplies valid attacker proof of possession with the old ID token | Rejected by the ID-token nonce |
| Resource manager places a genuine enrollment under another authenticated-map identity key | Rejected because the map key must equal the identity derived from the nonce-bound ID token |
| Resource manager replays an already accepted enrollment | Rejected because the identity is already enrolled |
| Resource manager signs a delivery with its own key while claiming the user's identity | Rejected by continuity-key verification |
| Resource manager changes content or delivery metadata after the user signs | Rejected by the content digest or signature |
| Resource manager fabricates a rotation with attacker-controlled old and new signatures | Rejected because the current continuity key did not authorize it |
| Resource manager substitutes the new key in a genuine transition | Rejected by the signed new-key digest and proof of possession |
| A transition lacks proof of possession by the genuine new key | Rejected even though the old key authorized the transition |
| Resource manager reuses an ordinary delivery record as an early cutoff | Rejected because the key event must reference the exact matching rotation-marker leaf |
| Resource manager references a genuine marker carrying a different rotation package | Rejected because the marker leaf and per-user key event must commit the same authorization |
| Successive key events reuse or move backward to an earlier marker | Rejected because marker positions for an identity must strictly advance |
| Agent advances the authenticated map before it has observed the marker | Permitted structurally, but deliveries depending on that boundary fail closed until marker inclusion is proven |
| An old-key delivery is signed before rotation but appended after the marker | Rejected because log position, not unprovable signature creation time, determines validity |
| A successor-key delivery is appended before its rotation marker | Rejected because the successor state is valid only after the marker |
| A historical delivery is presented after rotation | Accepted when its key event, retiring marker, current history head, and log inclusion all verify |
| Retired key signs at or after a marker already accepted by an agent | Rejected by that agent |
| Resource manager presents a fork from an older delivery checkpoint | Rejected by an established agent that retained the newer checkpoint |
| Resource manager presents a map branch rooted before the agent's retained map root | Rejected by the established agent |
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
| `deliveryagent/` | Stateful target-side OIDC, key-history, signature, marker, log, and generation verification |
| `protocol/` | Purpose-separated messages, signatures, digests, sparse-map leaf-update proofs, and linear history/delivery chains |
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
5. Add semantic exceptions for invalid key events so one bad principal does
   not halt an agent's map-update batch.
6. Replace the linear history and delivery chains with compact Merkle proofs,
   compress default sparse-map siblings, and add selective delivery; then
   compare a small Merkle implementation with Tessera.
7. Add proactive checkpoint distribution and optional peer comparison to show
   how the stale-agent window changes without making gossip mandatory.
8. Model the trust and delivery state machines formally and fuzz canonical
   encodings, mutations, replay, and crash interleavings.
