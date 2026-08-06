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
  - assembles identity history/map proofs and selected log proofs for the target
  - has an explicit CompromisedManager attack harness in the tests
               |
               | untrusted records and evidence
               v
delivery agent
  - is provisioned with tenant, target, OIDC issuer, and enrollment client ID
  - independently fetches OIDC discovery and JWKS
  - reconstructs identity state ephemerally while verifying nonce binding, key
    possession, signatures, history, map roots, markers, target, and generation
  - durably retains only its accepted map root, delivery-log checkpoint, and a
    bounded set of exceptional event digests as cryptographic trust state
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

## Storage and proof profile

Merkle nodes and protocol records remain in memory, but the access and wire
shapes deliberately match the intended server design:

- a key-history append uses its retained compact frontier and changed Merkle
  nodes; it does not retrieve or replay earlier event bodies;
- the versioned sparse map reads and writes one 256-level path; it does not
  scan the identity-head table and retains old node versions so it can prove a
  leaf under an older root held by an agent;
- a sparse-map proof carries a fixed 32-byte presence bitmap plus only the
  non-empty sibling hashes, omitting position-derived canonical empty nodes;
- a rotation map update carries the new event, its append/consistency proof,
  one authenticated immediate-predecessor event, and the exact marker record;
- a delivery carries one authenticated signing event and, only for a
  historical signing state, its immediate successor event; and
- a delivery-log update discloses only the delivery and its zero, one, or two
  adjacent rotation markers. Unrelated leaves are represented by logarithmic
  consistency and inclusion paths.

The POC deliberately chooses the minimum-state verifier profile. The agent's
complete cryptographic trust state is the accepted map root, delivery-log
checkpoint, and a bounded set of rare structured exceptions containing
identity, event sequence, event digest, and resulting-state digest. It retains
no per-identity head, public key, continuity state, marker boundary, history
path, or observed-log-record database. Content and generation maps model
application effects and are not verifier trust state.

Acceptance of a root means every newly introduced event was semantically
checked at advancement time or was atomically recorded as an exception. Later
selective proofs reconstruct only the state needed for one delivery; they rely
on that locally retained root-plus-exceptions invariant rather than replaying
the complete chain or contacting the OIDC provider. An exception for an
identity makes its entire descendant chain unusable. The manager can assemble
membership proofs for either its latest root or an older root deliberately
retained by an agent.

The map-update sequence is not an authenticated log. Its records are merely a
way to find the chain of individually verified root transitions needed by a
stale agent. Persistent tiles, batched history extensions, durable storage,
compaction, and skip proofs can improve scale without changing the cutoff-free
signed rotation authorization.

### Portable contracts and bounds

The server port should preserve these proof contracts rather than translating
the POC's in-memory collections literally:

```text
VerifierCheckpoint {
    map_root
    delivery_log_checkpoint
    exceptions[] { identity_id, sequence, event_digest, state_digest }
}

MapAdvanceEvidence {
    previous_root, successor_root
    previous_head_or_absence
    sibling_bitmap, non_empty_sibling_hashes
    key_history_append { new_event, successor_head, inclusion, consistency }
    predecessor_event?  // exactly previous_head.size - 1 for rotation
    rotation_record?    // exact index/hash/package referenced by new_event
}

DeliveryIdentityEvidence {
    current_head_and_compressed_map_membership
    signing_event
    successor_event?    // exactly signing_event.sequence + 1
}
```

For an identity history of size `h`, a map update reads one old event body for
a rotation and writes one new body; its history hashes are `O(log h)`. A
delivery reads one event body for a current signer or two for a historical
signer, each with an `O(log h)` membership path. A sparse-map path always
performs the fixed 256-level reconstruction, but sends only a 32-byte bitmap
and `k` non-empty hashes (`0 <= k <= 256`). Batching or sparse multi-proofs can
share branches across several identities later; there is no further
single-proof omission available without changing the proof encoding or hash
topology.

The manager-side storage interface therefore needs direct indexes from map
root to map revision, identity and revision to history head, and state digest
to event sequence. Event bodies and Merkle nodes are separate reads. This is
what keeps proof construction bounded even though the manager still owns the
ordinary user/state database.

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
exception-resolution protocol, durable semantic anchor, external apply loop,
durable persistence, or external fork witness yet. The in-process push and
acknowledgement model exercises retry semantics, but both manager and agent
state still live only in memory.

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
   of possession, derives `identity_id = H(tenant, iss, sub)`, and verifies the
   map update. It then retains only the successor map root, or that root plus a
   structured exception if the authenticated event is invalid.

### Delivery

1. The client signs the simple content attestation.
2. The resource manager performs its normal authorization check and appends a
   delivery record.
3. The resource manager constructs an RFC 6962 consistency proof from its last
   checkpoint acknowledged by that target's agent. It includes only the
   delivery and the rotation-marker leaves bounding its signing state.
4. It also constructs a compressed sparse-map membership proof and supplies
   only the signing event plus its immediate successor when the signer is
   historical. Those events identify the adjacent marker records to prove.
5. The resource manager pushes the record and proofs through an in-process
   delivery-agent interface.
6. The delivery agent reconstructs the identity's states ephemerally, verifies
   both proof axes, the content signature, marker-bounded validity interval,
   and stale-generation fencing, then applies the POC's in-memory content state
   and acknowledges.
7. Only after that acknowledgement does the resource manager advance its
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
| A historical delivery is presented after rotation | Accepted when its signing event, immediate successor, adjacent markers, current history head, and log inclusions verify |
| Retired key signs at or after a marker already accepted by an agent | Rejected by that agent |
| Delivery push fails before the agent accepts it | Both sides retain the prior acknowledged checkpoint; a later manager retry catches up with selective proofs |
| Agent accepts and applies a delivery but its acknowledgement is lost | Manager retains its older checkpoint; retry recovers the agent's newer checkpoint and idempotently succeeds |
| Resource manager presents a fork from an older delivery checkpoint | Rejected by an established agent that retained the newer checkpoint |
| Resource manager presents a map branch rooted before the agent's retained map root | Rejected by the established agent |
| Resource manager bypasses its own RBAC but forwards a genuine, otherwise valid user signature | Accepted by the agent; complete RBAC is intentionally not reproduced there |
| Resource manager withholds rotation from one agent, then uses a compromised old key | The rotated agent rejects; the stale agent accepts on its local pre-rotation view |
| Resource manager tampers with a log root, leaf, inclusion path, consistency path, map sibling, or history proof | Rejected by the corresponding retained-root proof verification |
| A busy delivery log contains unrelated intervening leaves | The agent advances with logarithmic proofs and receives only selected delivery and marker records |
| A long identity history is used for a delivery | The manager pushes only the signing event and optional immediate successor, each with a logarithmic inclusion proof |
| A sparse map contains mostly empty branches | The proof sends a 32-byte bitmap and only non-empty siblings |
| Many users enroll successfully | Agent trust-state size remains constant; no per-user head or state is retained |
| One authenticated key event is semantically invalid | Its digest enters the bounded exception set and unrelated valid map updates can continue |
| More events descend from an already exceptional identity | The Agent may advance structurally without consuming another exception slot; the identity remains unusable |
| A new invalid event arrives when the exception set is full | The agent refuses that successor map root |

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
| `deliveryagent/` | Minimal trust checkpoints, ephemeral OIDC/history/signature verification, application-state modeling, and delivery fault injection |
| `protocol/` | Purpose-separated messages, signatures, digests, self-contained identity evidence, RFC 6962 proofs, and sparse-map updates/membership proofs |
| `internal/merklelog/` | In-memory storage adapter around `transparency-dev/merkle` |
| `internal/sparsemap/` | In-memory storage and proof adapter around Trillian SMT and CONIKS primitives |
| `internal/testoidc/` | Minimal TLS OIDC authorization-code provider |
| `trust_model_test.go` | End-to-end guarantees and accepted limitations across all three roles |

## Recommended next experiments

1. Persist delivery-agent checkpoints and pending work, then inject crashes at
   every write boundary to prove acknowledgement and retry invariants.
2. Add the trust-manifest update-policy profiles and role-separated TUF root or
   delegated metadata.
3. Replace the simple content attestation's encoding with the existing
   Sigstore Bundle and in-toto/DSSE types while retaining these exact tests.
4. Add device and session delegation, identity tombstones, recovery, and the
   optional OIDC reanchor paths.
5. Add explicit exception resolution/reanchor; keep any optional semantic-anchor
   or cache profile separate from this minimum-state baseline.
6. Port the versioned sparse nodes and RFC 6962 frontier/node interfaces to
   durable storage, evaluate Tessera for the delivery-log runtime, compact old
   map versions, and batch per-principal history extensions.
7. Add proactive checkpoint distribution and optional peer comparison to show
   how the stale-agent window changes without making gossip mandatory.
8. Model the trust and delivery state machines formally and fuzz canonical
   encodings, mutations, replay, and crash interleavings.
