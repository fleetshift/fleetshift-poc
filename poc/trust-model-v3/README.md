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
  - serializes deliveries and rotation markers in one RFC 6962 tenant log
  - maintains RFC 6962 per-identity key histories
  - derives direct CONIKS sparse-map leaf-update proofs for history heads
  - routes selected records and compact proofs to the target
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

## Merkle implementation choices

The prototype instantiates all three authenticated structures from the design:

- the tenant delivery log is an RFC 6962 Merkle log with inclusion and
  consistency proofs;
- each identity's key-event history is a separate RFC 6962 Merkle log with
  inclusion and consistency proofs; and
- the history-head map is a 256-level, tenant-separated CONIKS sparse Merkle
  tree with direct absence, membership, and leaf-replacement proofs.

The two logs use
[`transparency-dev/merkle`](https://github.com/transparency-dev/merkle) for RFC
6962 hashing, compact frontiers, proof construction, and proof verification.
FleetShift code supplies only an in-memory node store and the small adapter
needed to retrieve historical roots. Tests compare every retained test-vector
root and proof against the library's own verifier.

The map uses Trillian's
[`merkle/smt`](https://pkg.go.dev/github.com/google/trillian/merkle/smt) writer
and HStar3 reconstruction plus its
[`merkle/coniks`](https://pkg.go.dev/github.com/google/trillian/merkle/coniks)
position-bound hashing. FleetShift code supplies the in-memory node accessor,
proof serialization, and the small absent-leaf reconstruction needed because
the package does not expose a standalone current-tree proof verifier. The
implementation cross-checks every generated replacement root against the
Trillian writer, uses full SHA-256 tenant binding in sparse-map keys in
addition to the CONIKS tree ID, and rejects non-canonical or incorrectly sized
hashes and paths.

[`Tessera`](https://github.com/transparency-dev/tessera) is deliberately not
embedded here. It is a production transparency-log runtime and storage layer,
whereas this POC needs an immediate, in-memory append inside the resource
manager's rotation transaction. Tessera is the preferred candidate when this
experiment grows durable tiled storage, checkpoint publication, and witnesses;
the RFC 6962 roots and proofs exercised here keep that storage migration
conceptually aligned.

The full Trillian log service is also not used. That project is in maintenance
mode and recommends Tessera for new transparency logs; this POC imports only
the focused sparse-Merkle and CONIKS library packages.

## Deliberate simplifications

Merkle nodes and protocol records remain in memory. The manager rebuilds a
principal's small key-history tree when creating its next proof and sends all
256 sparse-map siblings rather than compressing canonical empty subtrees.
Delivery-log updates, by contrast, disclose only requested leaves: unrelated
intervening deliveries are represented solely by the logarithmic consistency
and inclusion paths.

The map-update sequence is not an authenticated log. Its records are merely a
way to find the chain of individually verified root transitions needed by a
stale agent. Persistent tiles, compressed sparse-map proofs, batched history
extensions, durable storage, and skip proofs can improve scale without
changing the cutoff-free signed rotation authorization.

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
semantic exception set, external apply loop, durable persistence, or external
fork witness yet. The in-process push and acknowledgement model exercises
retry semantics, but both manager and agent state still live only in memory.

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
3. The resource manager constructs an RFC 6962 consistency proof from its last
   checkpoint acknowledged by that target's agent. It includes only the
   delivery and the rotation-marker leaves bounding its signing state.
4. The resource manager pushes the record and proofs through an in-process
   delivery-agent interface.
5. The delivery agent verifies the proofs, content digest and signature,
   marker-bounded validity interval, and stale-generation fencing, then applies
   the POC's in-memory content state and acknowledges.
6. Only after that acknowledgement does the resource manager advance its
   per-agent checkpoint.

The agent can inject a lost acknowledgement after it has updated its local
state. On retry, the manager initially constructs a proof from its older cached
checkpoint. The agent responds with its newer retained checkpoint; the manager
validates that checkpoint against its delivery-log branch, updates its cache,
and retries with a proof from the corrected position. Reapplying the same
signed generation is idempotent.

The agent can also inject a transport failure before accepting a push. In that
case neither the agent nor the manager's acknowledged checkpoint advances, but
the normally authorized delivery remains committed in the manager's log for a
later retry. The catch-up test uses this failure mode for ordinary client
traffic, then retries one selected record and confirms that the manager sends
only that record with the logarithmic consistency and inclusion proofs.

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
| Delivery push fails before the agent accepts it | Both sides retain the prior acknowledged checkpoint; a later manager retry catches up with selective proofs |
| Agent accepts and applies a delivery but its acknowledgement is lost | Manager retains its older checkpoint; retry recovers the agent's newer checkpoint and idempotently succeeds |
| Resource manager presents a fork from an older delivery checkpoint | Rejected by an established agent that retained the newer checkpoint |
| Resource manager presents a map branch rooted before the agent's retained map root | Rejected by the established agent |
| Resource manager bypasses its own RBAC but forwards a genuine, otherwise valid user signature | Accepted by the agent; complete RBAC is intentionally not reproduced there |
| Resource manager withholds rotation from one agent, then uses a compromised old key | The rotated agent rejects; the stale agent accepts on its local pre-rotation view |
| Resource manager tampers with a log root, leaf, inclusion path, consistency path, map sibling, or history proof | Rejected by the corresponding retained-root proof verification |
| A busy delivery log contains unrelated intervening leaves | The agent advances with logarithmic proofs and receives only selected delivery and marker records |

The RBAC-bypass and stale-agent rows are tests, not disclaimers hidden outside
the executable model. They establish the precise boundary of the additional
guarantee.

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
| `resourcemanager/` | Ordinary authorization, ordered storage, in-process push routing, per-agent acknowledged checkpoints, and explicit compromise harness |
| `deliveryagent/` | Stateful target-side OIDC, key-history, signature, marker, log, generation verification, and delivery fault injection |
| `protocol/` | Purpose-separated messages, signatures, digests, RFC 6962 log proofs, and sparse-map leaf updates |
| `internal/merklelog/` | In-memory storage adapter around `transparency-dev/merkle` |
| `internal/sparsemap/` | In-memory storage and proof adapter around Trillian SMT and CONIKS primitives |
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
6. Move RFC 6962 node storage to persistent tiles, evaluate Tessera for the
   delivery-log runtime, compress default sparse-map siblings, and batch
   per-principal history extensions.
7. Add proactive checkpoint distribution and optional peer comparison to show
   how the stale-agent window changes without making gossip mandatory.
8. Model the trust and delivery state machines formally and fuzz canonical
   encodings, mutations, replay, and crash interleavings.
