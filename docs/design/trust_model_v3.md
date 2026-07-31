# Verifiable Infrastructure Delivery with a Minimally Trusted Resource Manager

## 1. Purpose

The system protects existing tenant infrastructure from compromise of the central resource manager.

The resource manager remains the normal client-facing service. It accepts requests, stores state, routes deliveries, and returns ordinary responses. However, it does not possess credentials that independently authorize changes to tenant infrastructure.

Infrastructure changes are accepted only when a delivery agent independently verifies authorization evidence produced by an end user or trusted workload.

The intended security guarantee is:

> Compromise of the resource manager and its stored state does not grant authority to modify or delete existing tenant infrastructure through delivery agents.

The design does not attempt to guarantee perfect availability, confidentiality, query-result integrity, or globally consistent views during a resource-manager compromise. The compromised resource manager may withhold requests, delay them, provide incorrect read results, or cause denial of service.

The priority is keeping the security improvement operationally unobtrusive.

## 2. System roles



### Controlled client

The client:

- authenticates the user through OpenID Connect;
- obtains authorization for itself through OAuth2;
- generates and protects user keys;
- signs delivery authorizations;
- signs requests;
- communicates only with the resource manager.

The client does not communicate directly with delivery agents and does not need to know which delivery agents exist.

### Resource manager

The resource manager acts as the courier and registry operator.

It:

- receives signed client requests;
- stores public keys, state objects, proofs, and delivery bundles;
- maintains tenant Merkle structures;
- appends durable delivery commitments;
- validates request signatures;
- routes each delivery to its target delivery agent;
- supplies Merkle proofs and trust evidence.

It is trusted for ordinary service behavior but is not trusted as a source of infrastructure authority.

### Delivery agent

A delivery agent is the verifier and enforcement point attached to existing infrastructure.

It:

- maintains a small amount of local trusted state;
- validates tenant configuration;
- validates user identity continuity;
- verifies delivery signatures;
- checks ordered-log inclusion;
- applies only authorized deliveries.

Delivery agents are trusted to enforce the protocol correctly. They do not need to agree instantly with all other delivery agents.

### Identity provider

The identity provider authenticates users during initial enrollment and possibly explicit account recovery.

It does not:

- store user public keys;
- understand the key-continuity protocol;
- bind ordinary tokens to user keys;
- participate in normal key rotation or delivery signing.
- Optional timestamp authority

A timestamp authority is not required for the core user-key and ordered-delivery architecture.

It may remain useful for short-lived workload credentials or other evidence that requires wall-clock validity.

## 3. Threat model

The resource manager may be compromised and may:

- alter or delete its stored objects;
- present incorrect public keys;
- reorder requests that have not yet reached an accepted log checkpoint;
- omit trust evidence;
- withhold deliveries;
- present different views to different delivery agents;
- provide stale state to a newly provisioned delivery agent.

It must not be able to:

- forge a user signature;
- replace an established user key without authorization;
- manufacture an authorized infrastructure delivery;
- rewrite history already pinned by an established delivery agent;
- make an old key valid beyond its cryptographically established cutoff.

The design accepts limited trust on first use when provisioning a new delivery agent. A delivery agent that retains its local checkpoint is protected against rollback relative to that checkpoint.

No external transparency witness is required. Delivery agents serve as local witnesses to the history they have already accepted. Global fork detection can be added later through gossip or independent witnesses, but it is not a baseline requirement.

## 4. Per-tenant structures

Each tenant has two logically distinct authenticated structures.

They may share implementation and storage, but they serve different purposes.

### 4.1 Trust-update log and authenticated current-state map

The low-volume trust-update stream contains events such as:

- initial user enrollment;
- continuity-key rotation;
- device authorization or revocation;
- recovery-policy change;
- account disablement;
- tenant trust-manifest update;
- workload-authority update.

Applying these updates derives the tenant’s current authenticated map:

`identity identifier -> digest of current identity state`

The authenticated map answers:

> What is the tenant’s currently committed state for this identity?

A sparse Merkle tree is a likely implementation. The identity hash determines the leaf location, allowing both membership and absence proofs.

The map stores compact state digests, not necessarily full public keys.

For example:

`identity_id -> H(UserContinuityState7)`

The resource manager stores the actual state object separately:

`H(UserContinuityState7) -> serialized state`

The delivery agent stores only the latest tenant map root and trust epoch.

### 4.2 Ordered durable-delivery log

The delivery log is an append-only Merkle log containing compact commitments to infrastructure deliveries.

A leaf may contain:

```
DeliveryCommitment {
    tenant_id
    delivery_id
    target_id
    signing_identity_id
    signing_state_digest
    bundle_digest
}
```

The log answers:

> Was this exact delivery commitment included before or after a key transition?

Only deliveries need durable log inclusion. Ordinary signed queries can be verified once and discarded.

The resource manager stores the ordered leaves and Merkle nodes. A delivery agent stores only a log root and log size.

A delivery agent does not consume every delivery in the tenant. When it receives its next targeted delivery, the resource manager supplies:

- that delivery’s inclusion proof;
- a consistency proof from the delivery agent’s previous log root to the newer root.

This lets a delivery agent skip all unrelated delivery leaves while still verifying append-only growth.

## 5. Delivery-agent state

A delivery agent stores a compact checkpoint per tenant:

```
TenantVerifierState {
    tenant_id

    trust_epoch
    trust_map_root

    delivery_log_size
    delivery_log_root
}
```

It may additionally cache:

`identity_id -> last semantically validated state digest`

That cache is an optimization, not an authority source.

The checkpoint does not need to be signed when it remains in trusted local storage. Signing does not prevent rollback to an older correctly signed checkpoint.

The important properties are:

- durable persistence;
- integrity from the resource manager;
- atomic advancement;
- protection against local rollback where practical.

The delivery agent should persist the new checkpoint before acknowledging or applying an irreversible delivery.

## 6. Tenant trust root and trust manifest

Before validating a user enrollment, a delivery agent must know which identity provider and enrollment configuration belong to the tenant.

This configuration is represented by a tenant trust manifest.

```
TenantTrustManifest {
    tenant_id
    version
    previous_manifest_digest

    oidc {
        issuer
        enrollment_client_id
        permitted_id_token_algorithms
        key_resolution_policy
    }

    user_key_policy {
        permitted_continuity_algorithms
        permitted_device_algorithms
        permitted_session_algorithms
    }

    map_parameters
    delivery_log_parameters
    recovery_policy_constraints
}
```

The first trust manifest is rooted in tenant provisioning or limited trust on first use.

Subsequent trust-manifest updates are authorized by the currently trusted tenant-configuration key. A configuration-key rotation should be authorized by the old key and should include proof of possession by the new key.

> NOTE: We may want to weaken this to simply any verifiable signature within that tenant. Or make that a matter of policy captured in the last accepted trust bundle.

This is conceptually similar to a root metadata chain:

```
Provisioned tenant root
    authorizes
Tenant trust manifest
    identifies
OIDC issuer and enrollment client
    establishes
Initial user continuity key
```

The resource manager transports this material but cannot authorize its replacement.

### Identity-provider signing keys

Two key-resolution models are possible.

In the connected model, the trust manifest pins the exact issuer, enrollment client, permitted algorithms, and discovery policy. The delivery agent retrieves current signing keys directly from the issuer’s published key endpoint.

In the offline model, a versioned identity-provider key set is included or committed by the tenant trust manifest. Key rotation then requires an authorized manifest update.

The connected model is operationally simpler. The offline model provides more deterministic, self-contained verification.

## 7. Initial user enrollment

Initial enrollment establishes the first continuity key for an identity.

This is the only normal point at which the identity provider participates in user-key establishment.

> NOTE: We may relax that to enable per-user proof compaction, for example re-enrolling with the ID token nonce-binding on rotation.



### 7.1 Enrollment intent

Before authentication, the client generates the continuity key and constructs:

```
EnrollmentIntent {
    protocol: "user-key-enrollment-v1"
    tenant_id
    expected_issuer
    enrollment_client_id    ; is this necessary? this could be shared in trust bundle
    continuity_key_digest
    enrollment_id
}
```

The `enrollment_id` is a random unique value.

The intent does not include `sub`, because the stable subject identifier is learned from the returned ID token.

The OpenID Connect nonce is:

`nonce = H(canonical EnrollmentIntent)`

The identity provider treats the nonce as opaque and returns it in the signed ID token.

### 7.2 Enrollment package

The client sends the resource manager:

```
EnrollmentPackage {
    enrollment_intent
    continuity_public_key
    proof_of_possession_signature
    id_token
}
```

The proof-of-possession signature covers the enrollment intent.

The resource manager includes this package in the tenant trust-update stream.

### 7.3 Enrollment verification

A delivery agent checks:

1. The ID token signature is valid under the configured identity provider.
2. The token issuer exactly matches the tenant trust manifest.
3. The token audience contains the dedicated enrollment client identifier.
4. The token is within its accepted validity window.
5. The token nonce equals the hash of the enrollment intent.
6. The public key hashes to the digest in the intent.
7. The proposed continuity key signed the enrollment intent.
8. The identity is currently unenrolled.

The delivery agent extracts `iss` and `sub` from the validated ID token and derives:

`identity_id = H(protocol, tenant_id, iss, sub)`

This binds the first continuity key to whoever authenticated in that specific OpenID Connect flow without requiring the identity provider to understand public keys.

The ID token is used only as enrollment evidence. It is not used as an ordinary bearer credential for infrastructure operations.

## 8. Enrollment and transition failures

One invalid or unverifiable trust event must not block the entire tenant.

The authenticated map represents the tenant’s structurally committed current state. A delivery agent separately records which portion of that state it has semantically validated.

A delivery agent maintains approximately:

```text
VerifierTenantTrustState {
    trust_log_size 
    trust_log_root
    current_identity_map_root

    semantic_watermark
    exceptional_principals
}
```

The `semantic_watermark` identifies the contiguous trust-log prefix the delivery agent has processed. The exception set is a simple set of principals which did not validate at that watermark.

> NOTE: The exception set could be another authenticated Merkle structure that identifies enrollment, recovery, or transition events within that prefix that it could not validate.

For example:

```text
Alice State4:
    anchor event covered by watermark
    no relevant exception
    trusted

Bob State9:
    anchor event after semantic watermark
    unknown

Carol State0:
    enrollment event covered by watermark
    principal exceptional
    untrusted
```

The delivery agent may continue advancing the structural trust log and authenticated map even when an event cannot be semantically accepted. It must atomically record the exception—or stop advancing its semantic watermark—before treating later events as processed.

The effective authorization rule is:

```text
trusted(state) =
    state is proven under the accepted map root
    AND
    its trust anchor is covered by the semantic watermark
    AND
    neither the anchor nor a required descendant transition is exceptional
```

This allows Alice’s and Bob’s unrelated trust state to continue advancing even if Carol’s enrollment is invalid.

A state descending from an exceptional enrollment or transition remains untrusted:

```text
exceptional Enrollment E10
    -> State0
    -> State1
    -> State2

State2 remains untrusted because it depends on E10
```

The delivery agent refuses only deliveries whose authorization depends on that unresolved chain. It can return an `AUTHORIZATION_ANCHOR_REQUIRED` (e.g. "PausedAuth") result so the client can perform a fresh nonce-bound OIDC reanchor and automatically resume the delivery.

A successful reanchor creates a new trusted anchor event:

```text
E10: original enrollment, exceptional
E900: fresh OIDC reanchor, validated

current State3 depends on E900
```

The authenticated map does not need to be rewritten. Its current leaf simply advances to a state that references the new anchor.

### Effect on past deliveries

A later invalid enrollment, transition, or current-state claim does not retroactively invalidate deliveries that a delivery agent previously accepted under a valid historical authorization chain.

Historical delivery validity is evaluated against:

- the delivery commitment’s log position;
- the key state valid at that position;
- the delivery agent’s previously accepted trust checkpoint.

Therefore, a malicious or invalid current-state update can:

- block future deliveries for the affected identity;
- prevent reconciliation of a retained delivery if its required evidence is unavailable;
- cause user-specific denial of service.

It cannot retroactively alter the payload, signature, log position, or authorization decision of an already accepted delivery.

Only an explicitly defined and valid revocation policy could assign retrospective meaning, and the baseline protocol does not do so.

### New delivery agents

A newly provisioned delivery agent may accept the tenant’s current trust manifest, trust-log checkpoint, and authenticated-map root through trust on first use.

That checkpoint establishes its initial semantic trust boundary. It does not replay every historical enrollment decision and may therefore disagree with an older delivery agent that previously rejected a historical enrollment.

After bootstrap, the delivery agent processes new trust events normally and maintains its own watermark and exception set.

### Storage failure

If a delivery agent cannot persist another exception safely, it must not advance its semantic watermark beyond that event.

It may still:

- advance structural Merkle state;
- continue authorizing identities anchored before the semantic watermark;
- require fresh interactive reauthentication for identities depending on later events.

This degrades authorization selectively rather than disrupting the whole tenant.

### Identity deletion

Identity deletion creates a permanent tombstone rather than returning the identity to an unseen state.

Re-enrollment must be an authorized transition from the tombstone or a separately defined recovery/reanchor operation. An old enrollment package cannot be replayed as a new first enrollment.

## 9. User key hierarchy

The preferred hierarchy has three levels:

```
Continuity key
    authorizes
Device key
    authorizes
Session key
    signs requests and deliveries
```



### 9.1 Continuity key

The continuity key is long-lived and rarely used.

It authorizes:

- new devices;
- device revocation;
- continuity-key rotation;
- recovery-policy changes.

It should be hardware-backed where practical and require explicit user authentication for use.

Its signer interface should accept narrowly defined key-management operations rather than arbitrary bytes.

### 9.2 Device key

Each controlled client installation has its own device key.

The continuity key signs a device authorization:

```
DeviceAuthorization {
    identity_id
    continuity_state_digest
    device_key_digest
    permissions
    generation
}
```

The device key manages sessions but cannot replace the continuity key unless the policy explicitly grants that ability.

A compromised device key can create sessions until revoked but does not automatically become permanent identity takeover.

### 9.3 Session key

A session key is ephemeral and can remain in process memory.

At session start:

1. The client generates a session key.
2. The user confirms through biometric or device authentication.
3. The device key signs:
  ```
   SessionAuthorization {
       tenant_id
       identity_id
       device_key_digest
       session_key_digest
       session_id
       audience
       permitted_actions
       validity_constraints
   }
  ```

> NOTE: Audience / permitted actions / validity constraints may be invented and not needed

The session key then signs ordinary requests without repeated biometric prompting.

The user friction is concentrated at meaningful boundaries rather than every operation.

### 9.4 Two-level profile

The three-level hierarchy is not fundamental.

A simpler profile can use:

```
Combined device/continuity key
    authorizes
Session key
```

The authenticated-map, delivery-log, resource-manager, and delivery-agent protocols remain unchanged.

The main loss is compromise containment: compromise of the combined key permits both malicious session creation and continuity-key replacement.

The wire protocol should support generic delegation chains rather than hard-coding one exact depth.

## 10. Signed requests and deliveries

Ordinary queries may be signed for live verification but do not require durable logging.

Deliveries use the ordered delivery log.

The client creates a delivery bundle:

```
UserDeliveryBundle {
    tenant_id
    target_id
    delivery_id
    payload or in-toto statement

    current signing context     ; what is this?
    session public key
    session authorization
    device public key
    device authorization

    delivery signature
}
```

The client sends this bundle only to the resource manager.

The client experience remains an ordinary request/response operation. No special cryptographic outbox or direct delivery-agent communication is required.

Normal application retry and idempotency behavior may still be used.

## 11. Resource-manager delivery flow

1. The client signs the delivery and sends it to the resource manager.
2. The resource manager computes a canonical delivery commitment.
3. The resource manager appends the commitment to the tenant delivery log. (NOTE: this requires serializing all deliveries within a tenant. Is this overhead acceptable? can we use a smaller scope e.g. workspace or fulfillment?)
4. The resource manager sends the complete delivery bundle only to the targeted delivery agent.
5. It attaches:
  - the delivery’s log index;
  - a Merkle inclusion proof;
  - the current delivery-log root and size;
  - a consistency proof from the delivery agent’s prior log root; (this implies we track or can otherwise recover the fulfillment target's prior ack'd log root)
  - current identity-state evidence;
  - an authenticated-map proof;
  - any trust updates needed to advance the delivery agent’s trust checkpoint.

The delivery agent verifies the evidence independently.

Other delivery agents do not receive the delivery.

## 12. Delivery-agent verification

For a user-signed delivery, the delivery agent:

1. Verifies that the newer delivery-log root is an append-only extension of its stored root.
2. Verifies that the exact delivery commitment appears at the claimed log index.
3. Recomputes the bundle digest and commitment.
4. Advances the tenant trust map through any necessary trust updates.
5. Verifies the authenticated-map proof for the user’s current claimed state.
6. Establishes a semantically valid continuity-state anchor.
7. Validates any continuity transitions needed for the historical signing state.
8. Verifies the continuity-key authorization of the device.
9. Verifies the device-key authorization of the session.
10. Verifies the session signature over the delivery.
11. Checks tenant, target, audience, purpose, and replay or idempotency constraints.
12. Confirms that the signing state was valid at the delivery-log index.
13. Persists the new checkpoint.
14. Acks the delivery.

The resource manager can withhold or corrupt evidence, causing failure. It cannot produce a valid authorization chain without the relevant private keys.

## 13. Key rotation

A continuity-key rotation creates a new state:

```
ContinuityState8 {
    identity_id
    generation: 8
    continuity_key_digest: H(C8)
    recovery_policy_digest

    predecessor_state_digest: H(State7)
    transition_digest: H(Transition7To8)
}
```

The transition includes:

```
ContinuityTransition {
    identity_id
    previous_state_digest: H(State7)
    new_state_digest: H(State8)

    ; How does the client get this information?
    delivery_log_cutoff_size: N
    delivery_log_root_at_cutoff: LN

    signature_by_old_key
    proof_of_possession_by_new_key
}
```

The old key authorizes its successor. The new key proves that the client controls the new private key.

The transition is included in the trust-update stream and updates the current authenticated-map leaf.

The delivery-log cutoff creates a logical validity boundary:

```
old state valid for delivery indexes < N
new state valid for delivery indexes >= N
```

The resource manager may delay a legitimate old-key delivery until after the cutoff, causing rejection. That is denial of service, not unauthorized access.

## 14. Historical verification after rotation

The current Merkle root does not reconstruct an old public key.

The old public key is supplied as evidence, either:

- directly in the historical delivery bundle;
- from a content-addressed evidence store operated by the resource manager;
- or both.

The delivery agent trusts the old key only after verifying its authorization history.

Suppose an old delivery was signed through:

```
Continuity key C7
    authorized
Device key D3
    authorized
Session key S12
    signed
Delivery X
```

The evidence includes:

- public key S12;
- its device-signed session authorization;
- public key D3;
- its continuity-signed device authorization;
- public key C7;
- historical continuity state State7;
- later state State8;
- transition State7 -> State8;
- current map proof;
- delivery-log inclusion proof.

The delivery agent checks:

```
H(C7) == State7.continuity_key_digest
H(State7) == State8.predecessor_state_digest
C7 authorized State8
```

For several rotations, the supplied state chain continues until it reaches either:

- a state the delivery agent previously validated;
- the initial nonce-bound enrollment;
- or another trusted semantic snapshot.

The ordered delivery log then establishes that the old delivery was committed before the old state’s cutoff.

This proves logical ordering without a timestamp authority.

## 15. Delivery-log retention, compaction, and evidence storage

The delivery log separates permanent cryptographic continuity from both temporary application history and authorization evidence.

### 15.1. Delivery-log retention and compaction

For each delivery, the resource manager initially retains:

- the payload and signature evidence;
- the delivery and fulfillment identifiers;
- the canonical commitment fields;
- the delivery’s log position;
- the Merkle leaf and supporting tree hashes.

These categories have independent lifetimes.

When a delivery is no longer valid desired state and must not be delivered again, its payload, signatures, authorization evidence, and commitment preimage may be deleted. If queryable history is still required, identifiers and status metadata may remain temporarily.

When delivery history expires, the delivery identifier, commitment fields, and delivery-to-log-position index may also be deleted. When the fulfillment itself is undeployed and garbage-collected, its remaining identifier and state are removed.

None of this semantic garbage collection affects Merkle-log correctness. Inclusion and consistency proofs operate on hashes, not on the original commitment fields.

Merkle proof state is retained only as far back as necessary to support:

1. the oldest checkpoint held by a currently supported verifier; and
2. the oldest delivery that may still be delivered or reconciled and therefore still requires an inclusion proof.

The lowest such position is the log’s compaction watermark.

Below the watermark, obsolete leaves and internal nodes may be folded into a compact prefix frontier or equivalent boundary tiles. Above the watermark, enough tiled hash state is retained to generate inclusion and consistency proofs for all supported verifier checkpoints and deliverable entries.

The watermark advances only when:

- no supported verifier remains behind it; and
- no still-deliverable fulfillment depends on an older delivery.

Consequently, compaction does not weaken correctness for any supported operation. A verifier whose checkpoint has expired past the watermark is treated as a fresh verifier and bootstraps through the existing trust-on-first-use process.

The retained state therefore has three different scaling behaviors:

- current fulfillment state scales with the number of live fulfillments;
- semantic delivery history is bounded by product-retention policy;
- Merkle hash state is bounded by the supported verifier-catch-up and deliverability window.

At the modeled platform rate, un-compacted tiled hashes would grow by approximately 0.42 TB per replica per year. A 30-day uncompacted window requires approximately 35 GB, while a 90-day window requires approximately 104 GB, plus negligible compact-prefix state.

Garbage-collecting a compact 128–256-byte semantic commitment record after its retention period avoids approximately 1.7–3.4 TB of indefinite annual growth per replica at the modeled delivery rate. Those records instead become a bounded working set.  
  
> TODO: update these numbers for our actual scale range

### 15.2. Content-addressed evidence storage and garbage collection

The authenticated map, delivery log, and content-addressed evidence store serve different roles.

The authenticated map commits to the current authoritative state of each identity:

```text
identity identifier -> current state digest
```

The delivery log commits to the ordering of deliveries and allows supported verifiers to advance from previously accepted checkpoints.

The content-addressed evidence store holds the semantic objects needed to validate current or retained state:

```text
state digest          -> state object
key digest            -> public key
transition digest     -> rotation or recovery object
authorization digest  -> device or session authorization
delivery digest       -> retained delivery bundle
```

Objects are addressed by digest so that they may be stored in untrusted storage and safely deduplicated. Their integrity is established by hashes and signatures rather than by trusting the storage service.

The evidence store is garbage-collected by reachability. Its roots include:

- current identity states referenced by the authenticated map;
- current tenant trust manifests;
- active device and recovery authorizations;
- current fulfillment payloads;
- delivery versions retained by product-history policy;
- and trust transitions not yet superseded by an independently usable anchor.

An old state, key, transition, authorization, or delivery bundle may be deleted when:

- it is not part of current authoritative state;
- no current or retained delivery references it;
- it is not needed to validate a supported transition;
- and no product-retention policy requires it.

Fresh OIDC binding during continuity-key rotation can make the new state independently anchorable. Once this has occurred, earlier enrollment and rotation evidence need remain only while referenced by retained deliveries.

The resulting storage behavior is:

- current identity and fulfillment storage scales with the number of live resources;
- retained delivery and transition evidence is bounded by explicit retention and compaction policies;
- deleted resources do not leave permanent semantic object graphs;
- delivery-log hash material is compacted below the verifier and deliverability watermark.

A small amount of aggregate cryptographic state may outlive individual resources, such as a compacted delivery-log frontier. This state contains no recoverable delivery or fulfillment semantics and exists only to preserve append-only continuity for supported verifiers.

A deleted identity may optionally retain a compact tombstone when required to prevent replay or unauthorized resurrection. This is current authorization state, not historical evidence, and is unnecessary when the product explicitly permits clean re-enrollment under a new generation.

## 16. Recovery

Recovery policy must be committed before the normal continuity key is lost.

A recovery policy may identify:

- one dedicated recovery key;
- several recovery keys with a required signature count;
- preauthorized peer identities;
- an IdP-assisted recovery path;
- activation delays;
- cancellation rules.

An offline recovery key is simply a private key kept away from routine client use, such as on a hardware token, encrypted backup, or secondary device.

A rule such as “two of three recovery keys” means that two ordinary signatures from the three named keys are required. Specialized threshold cryptography is not necessary.

Normal rotation uses the current continuity key.

Recovery uses the precommitted recovery policy when the continuity key is no longer available.

IdP-assisted recovery, if supported, should be treated as a distinct and weaker path. It can use another nonce-bound authentication flow but may require additional delay, notification, or approval because identity-provider compromise would then affect recovery.

## 17. Delivery-agent catch-up and scale

A very busy tenant may produce approximately 100,000 durable deliveries per month, averaging roughly two per minute.

This is a modest append rate for a per-tenant Merkle log.

A tenant may also have 10,000 delivery agents. The architecture avoids broadcasting every delivery to every delivery agent.

A delivery agent that has been idle receives:

- a consistency proof from its old delivery-log root to the current root; (compact at O(logN))
- an inclusion proof for the one targeted delivery;
- accumulated trust updates since its previous trust epoch.

It does not receive intervening unrelated delivery leaves.

Trust updates should be substantially less frequent than deliveries. They may be delivered:

- lazily with the next targeted delivery;
- periodically to active delivery agents;
- eagerly only when operationally useful.

Eager deliveries are particularly useful when evidence expires (e.g. ID token nonce binding).

Per-tenant logs and maps isolate churn among tenants and keep single-tenant delivery agents focused on only their tenant.

## 18. No dedicated transparency witnesses

The baseline design does not use Sigstore-style external witnesses.

Each delivery agent remembers the checkpoints it has accepted and refuses rollback relative to those checkpoints.

This guarantees:

> A resource manager cannot rewrite an established delivery agent’s previously observed history.

It does not guarantee:

> Every delivery agent immediately sees one globally consistent history.

A compromised resource manager may maintain divergent branches for different delivery agents, particularly during initial trust-on-first-use provisioning.

This is accepted as a practical limitation. Optional checkpoint gossip, peer comparison, or external witnesses can be added later without changing the core signing model.

## 19. Timestamp-authority role

A timestamp authority is not required for:

- initial user enrollment;
- user-key continuity;
- key rotation;
- durable ordering of user deliveries;
- historical user-signature verification.

The ordered delivery log supplies protocol ordering:

```
delivery commitment
    before or after
key-transition cutoff
```

It does not supply wall-clock time.

A timestamp authority may be useful where wall-clock validity matters, especially when integrating workload credentials that expire after a short period. It can also supplement the delivery log for verifiable timestamps.

## 20. Provisional workload integration

Workloads should not necessarily imitate user-managed continuity keys.

The stable trust object for a workload is generally the workload identity authority and authorization policy, while individual workload keys are short-lived.

The authenticated map may contain:

```
WorkloadAuthorityState {
    authority_id
    authority_type
    trust_material_digest
    policy_digest
    generation
    previous_state_digest
}
```

Examples include:

- a SPIFFE trust domain;
- a Kubernetes cluster identity authority;
- a cluster-local credential bridge;
- a cloud workload identity authority.

### SPIFFE

A workload obtains an X.509 SVID and private key.

It can use that key to authorize an ephemeral run key:

```
SPIFFE authority
    issues SVID
SVID key
    authorizes run key
run key
    signs workload attestations
```

The delivery bundle carries the SVID, chain, run-key authorization, and attestation signature.

Durable verification after certificate expiry may require a trusted timestamp or another mechanism that establishes wall-clock validity during the certificate’s lifetime.

### Kubernetes ServiceAccounts

A Kubernetes ServiceAccount token is a bearer token rather than a workload-held signing key.

A small cluster-local credential bridge (the Fleetlet?) can:

```
validate a short-lived, audience-restricted, Pod-bound ServiceAccount token;
validate possession of a generated run key;
sign a portable workload-key authorization.
```

The bridge authority and Kubernetes identity policy are committed in the tenant authenticated map.

This is a narrow credential-binding service, not a full Fulcio or transparency-log deployment.

### Common workload bundle

The final workload evidence can use a pluggable form:

```
WorkloadAttestationBundle {
    statement
    run_key_signature
    run_public_key
    run_key_authorization

    verification_material: oneof {
        SPIFFE evidence
        Kubernetes bridge evidence
        generic X.509 evidence
        cloud workload evidence
    }

    workload_authority_state
    authenticated_map_proof
}
```

The in-toto statement and Sigstore bundle formats may be reused where they fit, with additional objects for authenticated-map and continuity evidence.

I would update both sections, mainly to reflect bounded evidence retention, delivery-log compaction, fresh OIDC binding on rotation, and the distinction between structural map state and semantically validated authority.

## 21. Fundamental versus flexible elements

### Fundamental

The system requires:

* an initial tenant trust anchor and authenticated tenant trust manifest;
* a defensible initial binding between an external user identity and a user-controlled continuity key;
* cryptographic authorization for changes to trusted identity state;
* proof of possession for newly introduced keys;
* an authenticated current-state map for tenant identities and trust authorities;
* semantic validation of a user’s current or historical state before that state is used as authority;
* an append-only ordered commitment log for durable deliveries;
* inclusion of each newly accepted delivery commitment in that log;
* continuity from each bootstrapped delivery agent’s previously accepted log checkpoint;
* delivery-agent local checkpoints that are protected from rollback by the resource manager;
* cryptographic binding among the signed payload, tenant, target, fulfillment, delivery identity, and signing context;
* sufficient keys, delegations, and authorization evidence to validate every current or retained delivery;
* retention and compaction rules that do not remove evidence still required by a supported verifier or a deliverable fulfillment.

The delivery log itself does not require semantic authorization for every appended leaf. It establishes ordering and append-only continuity. Authority to modify infrastructure comes from the independently verified delivery signature and authorization chain.

Historical public keys and authorization objects are therefore not retained merely because they once existed. They remain available only while referenced by:

* current authoritative identity state;
* a live fulfillment;
* a delivery version retained by product policy;
* or a trust transition that has not yet been replaced by an independently usable anchor.

### Flexible

The system can vary:

* two-level versus three-level user key hierarchy;
* exact recovery policy;
* whether every continuity rotation receives a fresh OIDC identity binding;
* whether cold verifiers use a retained transition chain, a fresh per-user reanchor, or another trusted checkpoint;
* identity-provider signing-key resolution and archival policy;
* whether trust-map changes are validated eagerly or lazily per identity;
* how eagerly delivery agents receive trust updates;
* the verifier-support or lease policy that determines the delivery-log compaction watermark;
* delivery and query-history retention periods;
* physical storage and tiling layout for Merkle structures;
* tenant-global delivery logs versus project logs anchored into a tenant-global ordering structure;
* whether the trust map and delivery log share one storage service;
* whether checkpoint gossip or external witnesses are later added;
* which workload identity adapters are supported;
* whether a timestamp authority is enabled for workload evidence.

## 22. Current recommended profile

The preferred implementation is:

```text
Per tenant:
    provisioned tenant trust anchor
    authenticated tenant trust manifest
    low-volume trust-update log
    sparse authenticated current-state map
    tenant-wide append-only delivery log

Per user:
    continuity key
    per-device key
    ephemeral session key

Initial enrollment:
    OIDC ID token
    nonce bound to:
        tenant
        issuer and enrollment client
        enrollment identifier
        continuity-key digest
    continuity-key proof of possession

Continuity rotation:
    old continuity key authorizes the transition
    new continuity key proves possession
    fresh OIDC nonce binding anchors the new key to the same iss/sub
    rotation binds the previous state and delivery-log cutoff

Normal delivery:
    session-signed delivery bundle
    sent only to the resource manager
    commitment appended to the tenant delivery log
    bundle and required evidence sent only to the target delivery agent

Delivery-log verification:
    verifier retains previously accepted log size and root
    new delivery includes append-only consistency and inclusion evidence
    these may be encoded as one combined append proof
    no authorization is required for unrelated appended commitments

Trust-state verification:
    verifier retains tenant trust-map epoch and root
    user state is semantically validated before being used as authority
    validated user, device, and session evidence is cached by digest
    map proofs and transition evidence are supplied only on cold use,
        cache loss, or relevant trust-state change

Per delivery agent:
    checkpoints for every tenant it serves:
        trust epoch and map root
        delivery-log size and root
    optional per-identity, device, and session validation cache

Historical verification:
    old keys and authorization objects are content-addressed and deduplicated
    evidence is retained only while referenced by current or retained deliveries
    fresh OIDC-bound rotation independently anchors the current continuity key
    old delivery ordering is established by Merkle-log inclusion and key cutoffs
    unreferenced semantic history is garbage-collected

Delivery-log retention:
    payloads, signatures, commitment fields, and identifiers have independent
        product-defined retention periods
    generated Merkle proofs are not persisted
    tiled hashes are retained above the compaction watermark
    obsolete hashes below the watermark are folded into a compact prefix frontier
    the watermark cannot pass:
        a supported verifier checkpoint
        or a delivery that may still be reconciled
    verifiers older than the supported watermark rebootstrap through TOFU

Baseline infrastructure:
    no Fulcio
    no general certificate-transparency deployment
    no dedicated witness network
    no tenant administrator semantic-checkpoint authority
    no required timestamp authority for user deliveries
```

This profile keeps all state needed for current correctness and supported catch-up, while avoiding permanent semantic residue from deleted deliveries, fulfillments, users, and obsolete key histories.


The central resource manager remains operationally important but no longer possesses authority that can be converted into control of all tenant infrastructure.
