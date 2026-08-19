# Verifiable Infrastructure Delivery with a Minimally Trusted Resource Manager

## 1. Purpose

The system adds cryptographic protection for existing tenant infrastructure against compromise of the central resource manager.

The resource manager remains the normal client-facing service. It accepts requests, performs the platform's primary authorization checks, stores state, routes deliveries, and returns ordinary responses. However, it does not possess user or workload signing credentials with which it can forge end-to-end provenance.

Infrastructure changes on the attested delivery path are accepted only when a delivery agent independently verifies authentic provenance produced by an end user or trusted workload and validates the delivery using the existing FleetShift attestation model.

The intended additional security guarantee is:

> Compromise of the resource manager and its stored state does not allow the resource manager to forge an end-user or workload signature, substitute an established signing key relative to a verifier's accepted history, or make an unattested or altered delivery pass provenance verification.

This is not a claim that every delivery agent independently reproduces the platform's complete authorization policy. The resource manager remains the primary authority for tenant, workspace, and API authorization. A compromised resource manager can bypass its own checks and can route a genuinely signed request that ordinary platform policy would have rejected. The delivery agent verifies the weaker but cryptographically end-to-end property of **authentic user or workload provenance** and the constraints carried by that signed provenance. This is a substantial defense-in-depth improvement over a target that unconditionally trusts a central deputy, but it does not eliminate trust in resource-manager authorization.

The design does not attempt to guarantee perfect availability, confidentiality, query-result integrity, or globally consistent views during a resource-manager compromise. The compromised resource manager may withhold requests, delay them, provide incorrect read results, or cause denial of service.

The priority is keeping the security improvement operationally unobtrusive.

### Relationship to the existing attestation design

This proposal does not define a replacement attestation or delivery-
authorization language. It inherits the input, derivation, constraint,
placement, addon-evidence, generation, put, and removal semantics described in
[provenance.md](architecture/provenance.md#evidence-and-attestation-semantics)
and exercised by the
[hybrid attestation prototype](../../poc/attestation/hybrid/README.md). Where
those semantics overlap, that common design and its tests remain authoritative.

This proposal focuses on the previously unresolved trust-distribution problem:

- binding an ordinary OIDC identity to a user-controlled continuity key without custom identity-provider claims or a key-aware identity provider;
- distributing current and historical key state through an untrusted resource manager;
- ordering durable deliveries relative to key transitions; and
- retaining only the evidence needed for current operation and supported historical verification.

The initial continuity/v3 per-item encoding is Sigstore Bundle v0.3
(`application/vnd.dev.sigstore.bundle.v0.3+json`), with the Bundle public-key
identifier as an untrusted proof-selection hint. Authenticated history and
delivery-log proofs remain separate support material. That is a profile-owned
mapping in
[provenance.md](architecture/provenance.md#initial-evidence-encodings), not a
common-interface requirement, and it does not make Fulcio, a timestamp
authority, or a transparency service required infrastructure. The
[logless Sigstore bundle POC](../../poc/attestation/sigstore_tuf_bundle/README.md)
documents and enforces the same Bundle v0.3 item shape for the Sigstore
profile.

The surrounding enrollment, continuity, delivery-ordering, and local-cutoff model has an executable three-role prototype in [`poc/trust-model-v3`](../../poc/trust-model-v3/README.md). It uses RFC 6962 logs for tenant delivery ordering and per-principal key histories, a CONIKS sparse Merkle tree for the authenticated history-head map, and a simple signed-content attestation. Storage remains in memory so persistence and derivation concerns do not obscure the core guarantees.

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
- performs the platform's primary tenant, workspace, and API authorization checks;
- stores public keys, state objects, proofs, and delivery bundles;
- maintains tenant Merkle structures;
- appends durable delivery commitments and ordering-sensitive key-transition markers;
- validates request signatures;
- routes each delivery to its target delivery agent;
- supplies Merkle proofs and trust evidence.

It is trusted for ordinary service behavior and primary platform authorization. It is not trusted as a source of authentic user or workload provenance or as the sole enforcement point on the attested delivery path.

### Delivery agent

A delivery agent is the verifier and enforcement point attached to existing infrastructure.

It:

- maintains a small amount of local trusted state;
- validates tenant configuration;
- validates user identity continuity;
- verifies delivery signatures;
- validates the inherited FleetShift attestation and constraint model;
- checks ordered-log inclusion;
- applies only deliveries that pass its configured provenance-verification profile.

Delivery agents are trusted to enforce the protocol correctly. They do not need to agree instantly with all other delivery agents.

### Identity provider

The identity provider authenticates users during initial enrollment. It may also participate in explicit account recovery or an optional fresh identity binding during continuity-key rotation.

It does not:

- store user public keys;
- understand the key-continuity protocol;
- bind ordinary tokens to user keys;
- participate in delivery signing.

Normal continuity-key rotation does not require the identity provider. A profile may require or permit a fresh nonce-bound OIDC authentication as an additional factor or as an explicitly weaker reanchor path.

### Optional timestamp authority

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
- replace an established user key relative to a delivery agent's accepted trust history without satisfying that history's transition policy;
- make an unattested or altered infrastructure delivery pass provenance verification;
- rewrite history already pinned by an established delivery agent;
- make an old key valid at or beyond a cutoff that the same delivery agent has already accepted.

The design accepts limited trust on first use only when provisioning a genuinely new delivery agent. A delivery agent that has ever established and retained a local checkpoint is not made new again merely because it was offline, fell behind a retention watermark, lost contact with the resource manager, or needs software recovery. It remains protected against rollback relative to that checkpoint and fails closed if it cannot obtain a valid continuation. Resetting such an agent requires an explicit tenant-authorized or out-of-band recovery procedure.

No external transparency witness is required. Delivery agents serve as local witnesses to the history they have already accepted. The resulting guarantee is local: a resource manager can withhold a transition and keep another delivery agent on a branch where that transition has not yet been accepted. If a retired key is also compromised, that branch may continue accepting its signatures until it accepts the transition. Global fork detection can be added later through gossip or independent witnesses, but it is not a baseline requirement.

Publishing new delivery-log checkpoints, rotation markers, and important key-history heads proactively to more delivery agents can reduce the population and duration of stale branches. It also creates more independently retained checkpoints for later comparison. Without comparison or gossip, proactive publication alone does not prove that every agent received the same branch.

### Controlled-client security considerations

The controlled client and its signing interface are part of the trusted computing base. A private key that cannot be extracted still does not help if compromised client code can cause the user to approve an attacker-selected operation without an intelligible presentation of what is being signed.

For browser clients, the security-sensitive enrollment, canonicalization, display, and signing surface should be kept small and separated from resource-manager-served mutable content where practical. Strong options include an independently deployed origin, a sandboxed and isolated signing context with a narrow message protocol, or an external/native signer. Addon UI JavaScript must not receive direct access to key handles or a general-purpose signing API. A service worker may mediate a narrow signing protocol and reduce accidental exposure, but a same-origin service worker is defense in depth rather than an independent trust root against compromise of that origin.

The client should display the tenant, action, target scope, and signed content identity at the signing boundary. OIDC ceremonies and ID-token validation follow [OpenID Connect Core](https://openid.net/specs/openid-connect-core-1_0.html). The protocol intentionally requires only ordinary OIDC behavior, including nonce round-tripping; it does not require custom claims, Rich Authorization Requests, proof-of-possession access tokens, non-trivial scope mapping, or per-target audiences.

### Enforcement boundary and bypass paths

The guarantee applies to changes mediated by an attestation-verifying delivery agent. A protocol proxy to a target control plane, a direct cloud API credential, a break-glass channel, or any other mutation path that bypasses the verifier is outside that guarantee and relies on its own authorization controls.

Target and deployment profiles should make these bypasses explicit. Hardened profiles can structurally omit mutating proxy channels; less restrictive profiles can retain them subject to target-native authorization. Updates to the delivery agent, fleetlet, its trust configuration, or its channel profile are themselves security-sensitive operations and must not silently create an unattested path around the verifier.

## 4. Per-tenant structures

Each tenant has two tenant-wide authenticated structures:

- trust state, represented by an authenticated history-head map; and
- durable-delivery ordering, represented by an append-only event log.

Each map leaf commits an append-only history head for one principal or trust authority. Those histories are identity-local evidence committed by the map, not another tenant-wide sequencing structure.

Authenticated-map evolution is proven directly as a leaf replacement. For each update, the resource manager supplies the old leaf value—or an absence proof—and its sparse-map sibling path. The old value and siblings must reconstruct the delivery agent's retained map root. The changed principal's history proof must extend the old history head to the new head. Replacing only that leaf and reusing the exact sibling path must then reconstruct the claimed successor root. Because the sibling commitments are unchanged, the proof establishes that no other map leaf changed in that transition.

Conceptually, the evidence is:

```text
AuthenticatedMapLeafUpdate {
    previous_root
    identity_map_key
    previous_leaf_or_absence
    successor_leaf
    sibling_path
    principal_history_extension
    successor_root
}

reconstruct(previous_leaf_or_absence, sibling_path) == retained_root
verify_append_only(previous_leaf_or_empty_history_head, successor_leaf.history_head)
reconstruct(successor_leaf, same sibling_path) == successor_root
```

A delivery agent accepts a map update only when its `previous_root` exactly equals the root the agent already retains. A sequence of such self-verifying transitions provides version continuity and rollback protection without another authenticated log. The resource manager may retain and serve an ordinary ordered sequence of update proofs for catch-up, but that sequence is transport and storage metadata, not a third authenticated structure.

### 4.1 Per-principal histories and authenticated history-head map

Each user or workload has a low-volume append-only key-event history containing events such as:

- initial user enrollment;
- continuity-key rotation;
- device authorization or revocation;
- recovery-policy change;
- account disablement;
- identity tombstoning or reanchor.

Tenant-wide events remain outside a user's history, including:

- tenant trust-manifest update;
- workload-authority update.

The tenant authenticated map commits the latest head of each principal's history:

```text
identity identifier -> KeyHistoryHead {
    history_size
    history_root
    current_state_digest
}
```

The authenticated map answers:

> What append-only key history and current state does the tenant currently commit for this identity?

The history root commits every key event on that branch. The current-state digest is a compact lookup hint committed as the result of the latest event; it does not replace the history commitment. A map update for an existing identity must prove that the new history head is an append-only extension of the previously committed head. A sparse Merkle tree is a likely map implementation, while each low-volume per-principal history may use a Merkle log, hash chain, or another append-only accumulator with suitable membership and extension proofs.

“Committed” is deliberately not the same as “authorized.” The map and history proofs establish structural selection and append-only continuity. A state becomes usable as authority only after the delivery agent has semantically validated the enrollment or transition events on which it depends and the relevant delivery-log markers. A malicious resource manager may commit invalid state and thereby cause selective denial of service, but map membership alone must never make that state authoritative for delivery.

The identity hash determines the sparse-map leaf location, allowing both membership and absence proofs. The resource manager stores public keys, state objects, signed transition authorizations, and key-event bodies separately by digest. It also retains enough per-principal history hash material to prove append-only extension and membership while those proofs remain supported.

For example:

```text
identity_id -> {
    size: 8
    root: H(KeyEvent0..KeyEvent7)
    current_state_digest: H(UserContinuityState7)
}
```

The resource manager stores the actual state object separately:

`H(UserContinuityState7) -> serialized state`

To advance from one map root to the next, a delivery agent verifies both the changed principal's append-only history extension and the direct sparse-map leaf-update proof described above. For catch-up across several changes, it verifies a chain in which each successor root is the next proof's `previous_root`. An arbitrary new map root supplied by the resource manager is never sufficient by itself.

In the recommended streaming-validation profile, one history extension does not carry the principal's complete history. It carries:

- the new event and its membership proof under the successor history head;
- an append-only consistency proof from the previous history head;
- for a rotation, one membership proof selecting the immediate predecessor event under the previous head; and
- the exact delivery-log marker record referenced by the new event. Marker inclusion in the accepted delivery-log branch may be proven then or deferred until a delivery depends on it.

The immediate predecessor is sufficient to reconstruct the currently accepted state, verify the old-key authorization, verify new-key possession, and enforce generation and marker monotonicity. The accepted map root plus the exception index records the result of that semantic validation, so earlier event bodies do not need to be replayed or stored by the delivery agent.

Sparse-map paths should omit canonical empty subtrees. For a 256-level SHA-256 map, the portable encoding is a 256-bit (32-byte) sibling-presence bitmap followed by only the selected non-empty 32-byte hashes in leaf-to-root order. Empty hashes are position-derived during reconstruction. This removes their bandwidth and storage cost while preserving the fixed 256-level verification topology.

The resource manager should retain proof-oriented indexes rather than rebuild either authenticated structure from application rows. A practical storage boundary provides versioned sparse-map node reads by `(map revision, node position)`, one-path leaf replacement, a per-principal compact history frontier, indexed event-body reads by sequence or state digest, and Merkle-node reads for inclusion and consistency proofs. Map advancement then touches one sparse path and `O(log h)` history nodes; delivery proof assembly reads one or two event bodies rather than the whole identity history.

### 4.2 Ordered durable-delivery log

The delivery log is an append-only Merkle event log containing compact commitments to infrastructure deliveries and key-rotation markers.

A leaf may contain:

```
DeliveryCommitment {
    tenant_id
    delivery_id
    fulfillment_id
    target_id
    generation
    action
    signing_identity_id
    signing_state_digest
    delivery_package_digest
}

KeyRotationMarker {
    tenant_id
    identity_id
    rotation_authorization_digest
}
```

The log answers:

> Was this exact delivery commitment included before or after the exact marker for a key transition?

Only durable deliveries and transitions whose key-validity semantics depend on delivery ordering need log inclusion. Ordinary signed queries can be verified once and discarded.

The marker contains the signed rotation package or commits its digest. The resulting key-history event references the exact marker leaf by position and leaf hash, not merely by a manager-provided checkpoint or integer. This binds the authenticated-map update to one ordering fact on one delivery-log branch.

The resource manager stores the ordered leaves and Merkle nodes. A delivery agent stores only a log root and log size.

A delivery agent does not consume every delivery in the tenant. When it receives its next targeted delivery, the resource manager supplies:

- that delivery’s inclusion proof;
- a consistency proof from the delivery agent’s previous log root to the newer root.

This lets a delivery agent skip all unrelated delivery and marker leaves while still verifying append-only growth.

## 5. Delivery-agent state

A delivery agent stores a compact checkpoint per tenant:

```
TenantVerifierState {
    tenant_id

    trust_map_root
    exceptional_events[] {
        identity_id
        event_sequence
        event_digest
        resulting_state_digest
    }

    delivery_log_size
    delivery_log_root
}
```

The recommended minimum-state profile gives the retained map root a precise local semantic meaning: every map transition leading to that root was structurally verified and its newly introduced event was semantically validated, except for identities listed in `exceptional_events`. Recording a new exception and advancing to its successor root must be crash-consistent. An exception blocks the affected event and all descendants for that identity; later descendant updates need not consume additional exception entries.

This invariant makes the root plus rare exception index the semantic checkpoint. The agent does not need a user database of heads, states, public keys, boundaries, history paths, or previously disclosed log records. When checking a delivery, it authenticates the selectively supplied event bodies under the retained root and reconstructs the needed state ephemerally. It does not repeat time-sensitive OIDC checks that already succeeded while advancing the map, but it does recheck deterministic bindings, hashes, key possession, and the delivery signature.

The exception fields are deliberately enough to diagnose the exact failure and reject the principal's descendants without retrieving its history. If the bounded exception set has no room for a newly invalid principal event, the agent must refuse that successor root. Valid updates and descendants of an already recorded exceptional identity do not require new entries.

An implementation may additionally cache `identity_id -> head/state/boundaries` for performance, but that cache is neither required nor authoritative. A different profile may retain durable semantic anchors to support trusted bootstrap snapshots or evidence garbage collection without replaying streamed updates. Those anchors are an optional storage/retention trade-off, not part of the minimum-state online profile.

The checkpoint does not need to be signed when it remains in trusted local storage. Signing does not prevent rollback to an older correctly signed checkpoint.

The important properties are:

- durable persistence;
- integrity against resource-manager tampering;
- crash-consistent advancement;
- protection against local rollback where practical.

The delivery agent must not acknowledge a delivery until it has durably persisted enough information to recover or safely retry after a crash. This includes the verifier checkpoint and either the completed apply result or the delivery data or pending-work record needed to resume idempotently.

The protocol does not require all of this state to be written in one database transaction. An implementation may use a transaction, ordered idempotent writes, compare-and-swap, a target-native generation fence, a lease, or another controller concurrency mechanism. The required property is that a crash at any point before acknowledgement leads to safe retry, no partially persisted semantic state is treated as authority, and concurrent controller execution cannot apply an older or unverified delivery over a newer accepted one.

This follows the [target delivery contract](architecture/target_delivery_contract.md#guarantees): acknowledgement may precede final external convergence when a durable work record is sufficient to guarantee progress, or it may follow a completed apply. It must not follow verification that exists only in memory.

## 6. Tenant trust root and trust manifest

Before validating a user enrollment, a delivery agent must know which identity provider and enrollment configuration belong to the tenant.

This configuration is represented by a tenant trust manifest.

```
TenantTrustManifest {
    tenant_id
    version
    previous_manifest_digest

    trust_update_policy

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

The first trust manifest is rooted in tenant provisioning or limited trust on first use for a genuinely new verifier.

Subsequent trust-manifest updates follow the `trust_update_policy` in the currently accepted manifest. Useful profiles include:

- **Dedicated configuration authority:** one or more tenant-configuration keys authorize updates. Thresholds may be required. A configuration-key rotation is authorized under both the previous policy and the successor policy and includes proof of possession for newly introduced keys. This is the preferred strict profile.
- **TUF-shaped authority:** standard TUF root and targets metadata carry trust material and policy, reusing established threshold, version, rollback, and rotation behavior.
- **Any currently verifiable tenant signer:** any identity already valid under the tenant's accepted trust state may authorize a manifest update through a signer class allowed by the manifest. This is a deliberately weaker but operationally simple profile. Delivery agents ordinarily accept updates only over an authenticated controller channel from their configured resource manager, and resource-manager authorization restricts who may submit them; an ordinary enrolled user cannot deliver an update directly. The signature prevents the resource manager from inventing an update by itself. This profile does not protect against a compromised resource manager or controller channel acting with a malicious or compromised enrolled signer that the policy accepts.

The update policy is itself part of the previously accepted manifest, so the resource manager cannot silently choose the weaker profile. Tenant deployments can choose the profile appropriate to their risk and operational constraints rather than making the strictest ceremony mandatory everywhere.

In every profile, the approving signature covers a purpose-separated manifest-update object containing at least the tenant, previous manifest digest and version, successor manifest digest and version, and transition purpose. An unrelated signed request cannot be reinterpreted as a manifest update.

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

The resource manager transports this material but cannot cryptographically authorize its replacement by itself.

More precisely, the resource manager cannot satisfy the configured cryptographic update policy by itself. It remains the ordinary controller authorization boundary and may withhold, delay, or selectively route an otherwise valid update.

### Identity-provider signing keys

Two key-resolution models are possible.

In the connected model, the trust manifest pins the exact issuer, enrollment client, permitted algorithms, and discovery policy. The delivery agent retrieves current signing keys directly from the issuer’s published key endpoint and validates ID tokens according to OpenID Connect Core.

In the offline model, a versioned identity-provider key set is included or committed by the tenant trust manifest. Key rotation then requires an authorized manifest update.

The connected model is operationally simpler. The offline model provides more deterministic, self-contained verification.

## 7. Initial user enrollment

Initial enrollment establishes the first continuity key for an identity.

This is the baseline point at which the identity provider participates in user-key establishment. Normal continuity-key rotation can rely only on the established key chain. A profile may additionally use fresh nonce-bound OIDC authentication during rotation, recovery, or reanchoring, with the security trade-offs described below.

### 7.1 Enrollment intent

Before authentication, the client generates the continuity key and constructs:

```
EnrollmentIntent {
    protocol: "user-key-enrollment-v1"
    tenant_id
    expected_issuer
    enrollment_client_id
    continuity_key_digest
    enrollment_id
}
```

The `enrollment_id` is a random unique value.

The issuer and enrollment client are repeated in the signed intent even though they also appear in the trust manifest. This deliberate context binding makes the intended OIDC ceremony unambiguous and prevents the same signed intent from being reinterpreted under another issuer or client configuration.

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

The resource manager creates the first event in the identity's key history from this package and produces a direct authenticated-map leaf-update proof from absence to that history head.

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

It also checks that the intent's tenant, expected issuer, enrollment client, protocol version, and algorithms match the accepted tenant trust manifest. The standard ID-token checks are defined by OpenID Connect Core rather than repeated exhaustively here.

The delivery agent extracts `iss` and `sub` from the validated ID token and derives:

`identity_id = H(protocol, tenant_id, iss, sub)`

This binds the first continuity key to whoever authenticated in that specific OpenID Connect flow without requiring the identity provider to understand public keys.

The ID token is used only as enrollment evidence. It is not used as an ordinary bearer credential for infrastructure operations.

## 8. Enrollment and transition failures

One invalid or unverifiable trust event must not block the entire tenant.

The authenticated map represents the tenant’s structurally committed key-history heads. A delivery agent separately records which history events and validity boundaries it has semantically validated.

A delivery agent maintains the semantic portion of `TenantVerifierState` approximately as:

```text
SemanticTrustState {
    exceptional_events[] {
        identity_id
        event_sequence
        event_digest
        resulting_state_digest
    }
}
```

There is no tenant-wide semantic watermark because there is no global trust-event log. In the recommended online profile, an event becomes part of the accepted semantic baseline when the agent validates it while advancing the authenticated map. The exception set identifies enrollment, recovery, or transition events that were structurally committed but did not validate. Each exception includes a principal index as well as event and state digests, so the agent can reject every descendant without loading that principal's history. An authenticated exception structure is possible if the rare bounded set ever becomes large enough to justify one.

For example:

```text
Alice State4:
    introducing transitions validated while advancing the retained map root
    trusted

Carol State0:
    enrollment event exceptional
    untrusted
```

The delivery agent may continue advancing its authenticated-map root even when one event cannot be semantically accepted, so an invalid principal does not block unrelated principals. Before retaining that successor root in a form that could later treat the affected chain as authority, it must crash-consistently record the exception. This may be implemented transactionally or with an ordered durable protocol that cannot expose the new root without the corresponding semantic status.

The effective signing-state trust rule is:

```text
usable_signing_state(state) =
    a current key-history head is proven under the accepted map root
    AND
    the state's key event is proven in that append-only history
    AND
    the accepted root was reached by semantically validating each introducing
        event or atomically recording an exception
    AND
    the identity has no unresolved exceptional ancestor
    AND
    every delivery-log marker needed to bound the delivery position is proven
```

This allows Alice’s unrelated trust state to continue advancing even if Carol’s enrollment is invalid.

A state descending from an exceptional enrollment or transition remains untrusted:

```text
exceptional Enrollment E10
    -> State0
    -> State1
    -> State2

State2 remains untrusted because it depends on E10
```

The delivery agent refuses only deliveries whose provenance verification depends on that unresolved chain. If tenant policy permits the explicitly weaker independent-reanchor path, it can return an `AUTHORIZATION_ANCHOR_REQUIRED` (e.g. "PausedAuth") result so the client can perform a fresh nonce-bound OIDC reanchor and automatically resume the delivery. Otherwise, the unresolved chain requires another configured recovery path.

A successful reanchor creates a new trusted anchor event:

```text
E10: original enrollment, exceptional
E900: fresh OIDC reanchor, validated

current State3 depends on E900
```

The identity does not need to move to another map key. Its current leaf advances to the history head containing the new anchor event.

### Effect on past deliveries

A later invalid enrollment, transition, or history-head claim does not retroactively invalidate deliveries that a delivery agent previously accepted under a valid historical signer and attestation chain.

Historical delivery validity is evaluated against:

- the delivery commitment’s log position;
- the key event and adjacent rotation markers that make its state valid at that position;
- the delivery agent’s previously accepted map root, exception index, and delivery-log checkpoint.

Therefore, a malicious or invalid current-state update can:

- block future deliveries for the affected identity;
- prevent reconciliation of a retained delivery if its required evidence is unavailable;
- cause user-specific denial of service.

It cannot retroactively alter the payload, signature, log position, or acceptance decision of an already accepted delivery.

Only an explicitly defined and valid revocation policy could assign retrospective meaning, and the baseline protocol does not do so.

### New delivery agents

A genuinely newly provisioned delivery agent may make one explicit bootstrap trust decision over a tenant checkpoint that binds the current trust manifest, authenticated-map root, delivery-log position and root, initial semantic anchors, and any bootstrap exceptions. Trust on first use may establish that checkpoint when no stronger provisioning anchor is available.

That bootstrap decision treats the states committed by the checkpoint, excluding any recorded exceptions, as the agent's initial semantic baseline. Merely receiving a map root outside this bootstrap decision would still establish only structural state. The agent does not replay every historical enrollment decision and may therefore disagree with an older delivery agent that previously rejected a historical enrollment.

After bootstrap, the delivery agent accepts only map updates rooted in that retained map root and maintains its own semantic anchors and exception set.

An established delivery agent that retains any prior checkpoint is not eligible for this path. If retained proof material can no longer extend that checkpoint, the agent fails closed and requires an explicit tenant-authorized or out-of-band recovery. Retention policy may end automatic catch-up support, but it must not silently reset an established target's trust.

### Storage failure

If a delivery agent cannot persist another exception safely, it must not durably accept the successor map root in a way that could make the affected chain authoritative.

It may still:

- reject that map update and keep its prior root;
- continue accepting independently proven signing states that do not depend on the failed event;
- require fresh interactive reauthentication for identities depending on later events.

This degrades authorization selectively rather than disrupting the whole tenant.

### Identity deletion

Identity deletion creates a permanent compact tombstone rather than returning the identity to an unseen state. The tombstone records only the identity identifier, its terminal generation or predecessor digest, the deletion event, and the policy for any future re-enrollment. Historical tokens, public keys, and transition objects can still be garbage-collected when no retained delivery needs them.

Re-enrollment is an authorized transition from the tombstone to a new generation or a separately defined recovery/reanchor operation. It is never represented by making the leaf absent again. This prevents an old enrollment package from being replayed as a new first enrollment while preserving an explicit product choice to permit a clean user experience for re-enrollment.

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

These `permissions` constrain what the device may do within the user's signing authority. They do not grant tenant or workspace permissions and do not replace the resource manager's primary authorization checks.

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

The exact audience, action, and validity fields are profile-dependent rather than fundamental to trust distribution. If present, they are internal signature constraints, not custom OIDC claims or per-target OIDC audience mappings, and must be enforced by the verifier. Wall-clock validity has the limitations described in the timestamp-authority section; profiles that do not need it can use generation or log-position constraints instead.

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

The existing FleetShift attestation package remains the authoritative description of what was signed and why a concrete delivery is allowed. The user may sign an input while addons sign derived updates, placement, or opaque output; output-signing profiles may instead bind the exact delivered artifacts. Put and removal actions, expected generation, constraints, and evidence-kind separation retain the semantics and validation rules already exercised by the hybrid and Sigstore/TUF POCs.

This proposal adds continuity evidence for the user-signing portion of that package. Where the existing verifier expects a key binding, certificate, or trust-anchor lookup, it may instead validate:

- the signing identity and continuity-state digest;
- the session and device public keys and their signed delegations, when the selected hierarchy uses them;
- an authenticated-map proof for the identity's current key-history head;
- a membership proof for the event that produced the signing state;
- only when that state is historical, a membership proof for its immediate successor event;
- the continuity states and public keys reconstructed from those one or two events; and
- delivery-log inclusion and consistency evidence for the delivery and every adjacent rotation marker needed to bound its position.

For the baseline continuity-key profile, delivery evidence therefore contains at most two identity event bodies regardless of history length. Device or session delegation adds the independently required delegation objects; it does not require replay of unrelated continuity events.

The complete delivery package cryptographically binds the tenant, target, fulfillment, delivery identity, generation, action, authoritative attestation root, and signing context. That aggregate package and attestation graph remain common and profile-neutral. Each independently authenticated continuity/v3 item inside them uses Sigstore Bundle v0.3 over a purpose-typed in-toto statement; the Bundle is one graph item, not the aggregate. History and delivery-log proofs travel as support material rather than as a custom per-item wrapper.

The client sends its signed request or input only to the resource manager. The resource manager assembles the eventual delivery package from that signed material and any independently signed derivation, placement, or output evidence required by the authoritative attestation model.

The client experience remains an ordinary request/response operation. No special cryptographic outbox or direct delivery-agent communication is required.

Normal application retry and idempotency behavior may still be used.

## 11. Resource-manager delivery flow

1. The client signs the request or input and sends it to the resource manager.
2. The resource manager derives the concrete action under the existing attestation model, collects any required signed evidence, and assembles the delivery package.
3. The resource manager computes a canonical delivery commitment and appends it to the tenant delivery log.
4. The resource manager sends the complete delivery bundle only to the targeted delivery agent.
5. It attaches:
  - the delivery’s log index;
  - a Merkle inclusion proof;
  - the current delivery-log root and size;
  - a consistency proof from the delivery agent’s prior log root;
  - the chain of direct sparse-map leaf-update proofs from the agent's retained map root, when the map must advance;
  - the identity's current key-history head and compressed authenticated-map membership proof;
  - the signing event and its per-identity history membership proof;
  - the immediate successor event and membership proof only when the signing event is not current; and
  - inclusion evidence for the zero, one, or two rotation markers referenced by those adjacent events.

The manager finds the signing event through a state-digest-to-sequence index, reads at most two event bodies, and constructs their proofs from retained history nodes. It reads one versioned sparse-map path for the agent's exact retained root. Neither operation scans all events for the identity or all identities in the tenant.

The delivery agent verifies the evidence independently.

Other delivery agents do not receive the delivery.

The delivery agent's reported map root and delivery-log checkpoint are authoritative for selecting the map-update chain and log consistency proof. The resource manager may cache the last acknowledged state for efficiency, but loss or corruption of that cache affects availability only: the agent can report its retained roots again.

The normal delivery acknowledgement advances the resource manager's cached checkpoint for that agent to the exact log root and size accepted by the request. If the agent durably accepts a delivery but its acknowledgement is lost, the manager's cache remains behind. A retry may therefore carry a consistency proof rooted at an older checkpoint than the agent currently retains. The agent responds with its newer retained checkpoint; the manager authenticates that checkpoint against its delivery-log branch, updates the cache, and reconstructs the request from the corrected position. The delivery itself remains idempotent, so the retry is safe whether the first attempt stopped after durable acceptance or after completed application.

### Delivery-log scope

The recommended profile uses one ordered log per tenant. This serializes commitment assignment within a tenant, but it gives every key transition one ordering domain across all targets that key may authorize. A workspace, project, or fulfillment log is possible only if its checkpoints are anchored into a tenant-wide ordering structure or the transition protocol places and proves a marker in every applicable log. The performance and storage trade-offs remain to be validated against the actual scale range.

## 12. Delivery-agent verification

For a user-signed delivery, the delivery agent:

1. Verifies that the newer delivery-log root is an append-only extension of its stored root.
2. Verifies that the exact delivery commitment appears at the claimed log index.
3. Recomputes the delivery-package digest and commitment.
4. Advances the tenant trust map through direct leaf-update proofs rooted at its retained map root.
5. Verifies the authenticated-map proof for the user’s current key-history head.
6. Verifies membership of the signing event and, for a historical state, its exact immediate successor under that head.
7. Rejects an identity with an unresolved exception and reconstructs the one or two supplied states, relying on the retained root-plus-exceptions invariant for semantic checks performed during map advancement.
8. Verifies inclusion of the adjacent rotation markers needed to establish the state's validity interval at the delivery index.
9. Verifies the continuity-key authorization of the device.
10. Verifies the device-key authorization of the session.
11. Verifies the user signature over the signed input or output represented by the authoritative attestation package.
12. Runs the existing attestation verifier for derivation, addon signatures, placement, put or removal constraints, generation, tenant, target, purpose, and replay protection.
13. Persists the new verifier checkpoint and enough delivery or controller state to recover or safely retry.
14. Begins, resumes, or records completed application using the controller's concurrency and idempotency mechanisms.
15. Acknowledges only once the target delivery contract's durable-progress requirement is satisfied.

The resource manager can bypass its own authorization checks, withhold or corrupt evidence, or route signed material in ways ordinary policy would reject. It cannot forge the provenance chain or make altered material satisfy the inherited attestation constraints without the relevant private keys.

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

The client signs a cutoff-free rotation authorization:

```
RotationAuthorization {
    identity_id
    previous_state_digest: H(State7)
    new_state_digest: H(State8)
    signature_by_old_key
    proof_of_possession_by_new_key
}
```

The old key authorizes its successor. The new key proves that the client controls the new private key. Neither signature covers a resource-manager-selected checkpoint or cutoff.

After authorizing the ordinary API operation, the resource manager submits a rotation marker containing that package, or its cryptographic commitment, to the same per-tenant log sequencer used for deliveries. Suppose the marker is assigned index `C`. The manager then creates the next identity-local key event, which contains the rotation package, the resulting state digest, and an exact reference to the marker leaf:

```text
KeyEvent7To8 {
    previous_key_event_root
    rotation_authorization
    resulting_state_digest: H(State8)
    marker: { index: C, leaf_hash: HC }
}
```

That event extends the user's append-only key history. The authenticated-map leaf advances from the old history head to the new head by reusing the old leaf's sibling path; the same update carries the per-user history-extension proof.

To validate this transition without retaining the user's previous state, the update also selects the immediate predecessor event with a membership proof under the old history head. That one event reconstructs the accepted old public key, generation, state digest, and preceding marker. The agent verifies the old-key signature, new-key proof of possession, successor digest, next generation, and strictly increasing marker, then discards both event bodies after advancing its root. No earlier key event is needed for this map transition.

The marker creates the logical validity boundary:

```
old state valid for delivery indexes < C
rotation marker occupies index C
new state valid for delivery indexes > C
```

The log append operation is the serialization point. An old-key delivery signed before rotation but appended after `C` is invalid, while an old-key delivery appended before `C` remains valid even if delivered to the target later. The protocol intentionally proves append order, not signature creation time. The resource manager may therefore delay an authentic old-key request until after the marker and cause its rejection. That is denial of service, not unauthorized access. A client that wants clean availability semantics should wait for important in-flight operations to be durably logged before submitting rotation.

The resource manager cannot choose an already-pinned early cutoff: it cannot insert the signed marker into the accepted prefix of an append-only log. It can append the marker immediately after receiving authorization, delay or omit it, append inert duplicates, or present a fork to an agent that has not pinned the conflicting history. Those are the existing denial-of-service and local-view limitations. A key event names one exact marker, and successive accepted marker positions for an identity must strictly increase.

The marker append and authenticated-map update do not need to be one indivisible cryptographic operation. A crash after the marker but before the map update leaves an inert marker; it does not rotate the key. The map update becomes meaningful only when it references that exact included marker. Implementations should nevertheless use a transaction or ordered durable workflow so retries, receipts, and client activation are unambiguous.

Concurrent authorizations from the same predecessor are competing branches, not two sequential rotations. Clients should serialize rotation and activate a successor only after receiving a durable commit receipt. A delivery agent accepts at most one append-only continuation from its retained history head. Preventing or detecting equivocation across agents still requires gossip, witnesses, or another global consistency mechanism.

### 13.1 Deferred marker validation

A delivery agent may receive the authenticated-map update before its delivery-log checkpoint reaches `C`. It may verify the old-key authorization, new-key proof of possession, per-user history extension, direct map leaf-update proof, and the marker record's exact hash and package without yet proving that marker under its accepted delivery-log checkpoint. This advances trust state without making the boundary usable for a delivery. The minimum-state profile need not retain the marker reference separately: the accepted history root commits it, and the manager re-supplies the selectively proven adjacent event and marker inclusion when a delivery depends on that boundary.

If the agent's accepted delivery-log checkpoint is already beyond `C`, it should validate the marker eagerly and reject a mismatched leaf, package, or branch. Otherwise it may defer the inclusion check. Before accepting any delivery whose signing-state interval depends on that rotation—whether before or after `C`—it must prove the exact marker's inclusion in an append-only extension of its retained delivery-log checkpoint. A rejected delivery can be retried after the marker proof arrives.

The cutoff becomes effective for a delivery agent only when that agent accepts the key-history transition and proves its marker. An agent kept on an older map branch has not yet established that cutoff even if it happens to observe an otherwise inert marker. Proactive transition distribution narrows this window but does not turn the local guarantee into global consistency.

## 14. Historical verification after rotation

The current map root and key-history head do not reconstruct an old public key or event body.

The old public key is supplied as evidence, either:

- directly in the historical delivery bundle;
- from a content-addressed evidence store operated by the resource manager;
- or both.

The delivery agent trusts the old key only after verifying its producing event under the current authenticated head, confirming that the identity has no unresolved exception, and applying the semantic meaning of its locally accepted root.

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

The baseline evidence includes:

- public key S12 and its device-signed session authorization;
- public key D3 and its continuity-signed device authorization;
- the current key-history head and compressed authenticated-map proof;
- the one key event that produced State7 and its history membership proof;
- because State7 is historical, the immediate successor event that produced State8 and its history membership proof; and
- delivery-log inclusion for Delivery X and the markers referenced by those two events.

The delivery agent checks:

```
SigningEvent.resulting_state_digest == H(State7)
SigningEvent is included under the current history head
SuccessorEvent.sequence == SigningEvent.sequence + 1
SuccessorEvent.rotation.previous_state_digest == H(State7)
SuccessorEvent is included under the same current history head
MarkerIndex(establishing State7) < DeliveryIndex(X)
DeliveryIndex(X) < MarkerIndex(retiring State7)
```

The successor event is sufficient to identify the first transition that retires the signing state; no later transition is relevant to its interval. The signing event itself identifies the marker that established a non-initial state. Consequently a middle-generation key needs two event bodies, the latest key needs one, and the initial key plus its successor also needs two. History length does not change this bound. Each event membership path remains `O(log h)`.

This selective rule depends on the streaming-validation invariant: the agent semantically checked every introducing event while advancing to its retained root and recorded exceptions durably. A cold verifier bootstrapped directly to a snapshot must receive an equivalently trusted semantic baseline; a bare map root does not create this invariant. An optional anchored profile may use a durable semantic snapshot instead, but does not change the adjacent-event delivery shape once that baseline is established.

The minimum-state verifier does not retain marker records, so the manager resends the zero, one, or two adjacent marker inclusions on each delivery. An optional cache may omit already proven markers without changing authority.

The per-user history establishes which events bound the key's validity interval. The ordered delivery log establishes that the delivery commitment falls inside that interval.

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

For each selected rotation marker, the resource manager likewise retains the signed rotation package or committed preimage, the marker position, the corresponding key-event reference, and enough tree hashes to prove inclusion while any supported current or historical delivery depends on that boundary.

When a delivery is no longer valid desired state and must not be delivered again, its payload, signatures, authorization evidence, and commitment preimage may be deleted. If queryable history is still required, identifiers and status metadata may remain temporarily.

When delivery history expires, the delivery identifier, commitment fields, and delivery-to-log-position index may also be deleted. When the fulfillment itself is undeployed and garbage-collected, its remaining identifier and state are removed.

None of this semantic garbage collection affects Merkle-log correctness. Inclusion and consistency proofs operate on hashes, not on the original commitment fields.

Merkle proof state is retained only as far back as necessary to support:

1. the oldest checkpoint held by a currently supported verifier; and
2. the oldest delivery that may still be delivered or reconciled and therefore still requires an inclusion proof; and
3. the oldest rotation marker still needed to validate a supported signing-state interval and not replaced by an adequate durable verifier anchor.

The lowest such position is the log’s compaction watermark.

Below the watermark, obsolete leaves and internal nodes may be folded into a compact prefix frontier or equivalent boundary tiles. Above the watermark, enough tiled hash state is retained to generate inclusion and consistency proofs for all supported verifier checkpoints and deliverable entries.

The watermark advances only when:

- no supported verifier remains behind it; and
- no still-deliverable fulfillment depends on an older delivery or unanchored marker.

Consequently, compaction does not weaken correctness for any supported operation. An established verifier whose checkpoint falls behind the supported watermark is not treated as fresh and does not use trust on first use. It fails closed for automatic catch-up and requires an explicit tenant-authorized or out-of-band recovery that preserves or deliberately replaces its prior trust anchor. Only a genuinely new verifier may use the initial TOFU path.

The retained state therefore has three different scaling behaviors:

- current fulfillment state scales with the number of live fulfillments;
- semantic delivery history is bounded by product-retention policy;
- Merkle hash state is bounded by the supported verifier-catch-up and deliverability window.

At the modeled platform rate, un-compacted tiled hashes would grow by approximately 0.42 TB per replica per year. A 30-day uncompacted window requires approximately 35 GB, while a 90-day window requires approximately 104 GB, plus negligible compact-prefix state.

Garbage-collecting a compact 128–256-byte semantic commitment record after its retention period avoids approximately 1.7–3.4 TB of indefinite annual growth per replica at the modeled delivery rate. Those records instead become a bounded working set.

> TODO: update these numbers for our actual scale range

### 15.2. Content-addressed evidence storage and garbage collection

The authenticated map, delivery log, and content-addressed evidence store serve different roles.

The authenticated map commits to the current structurally selected key-history head of each identity:

```text
identity identifier -> { history size, history root, current state digest }
```

The map conclusively identifies what the accepted branch currently commits to, but it does not grant delivery authority by itself. A history event and resulting state become usable for provenance verification only after successful semantic validation or because the verifier retained an appropriate durable semantic anchor.

The delivery log commits to the ordering of deliveries and rotation markers and allows supported verifiers to advance from previously accepted checkpoints.

The content-addressed evidence store holds the semantic objects needed to validate current or retained state:

```text
state digest          -> state object
key digest            -> public key
key-event digest      -> enrollment, rotation, recovery, or tombstone event
transition digest     -> signed rotation or recovery authorization
authorization digest  -> device or session authorization
delivery digest       -> retained delivery bundle
```

Objects are addressed by digest so that they may be stored in untrusted storage and safely deduplicated. Their integrity is established by hashes and signatures rather than by trusting the storage service.

The evidence store is garbage-collected by reachability. Its roots include:

- current identity states and key events needed to validate authenticated history heads;
- current tenant trust manifests;
- active device and recovery authorizations;
- durable semantic anchors on which evidence compaction relies;
- current fulfillment payloads;
- delivery versions retained by product-history policy;
- and trust transitions not yet superseded by an independently usable anchor.

The history root continues to commit an event after its semantic body is deleted. As with the delivery log, implementations retain or compact enough history hash material to prove supported membership and extension queries. An old state, key, event body, transition, authorization, or delivery bundle may be deleted when:

- it is not needed to validate current committed state from a supported anchor;
- no current or retained delivery references it;
- it is not needed to validate a supported transition;
- and no product-retention policy requires it.

Fresh OIDC binding during continuity-key rotation has two distinct uses:

- **Additional factor:** the transition still requires the old continuity key and proof of possession by the new key. OIDC adds freshness but does not replace continuity evidence.
- **Independent reanchor:** an explicit policy allows a cold verifier to accept the new key from a fresh nonce-bound OIDC ceremony without replaying the predecessor chain. This permits older enrollment and rotation evidence to be deleted once retained deliveries no longer reference it, but weakens continuity for that verifier: compromise of the IdP or user account can establish an attacker-chosen replacement key without the old continuity key.

There is no such weakening when a verifier already validated the old-key-authorized transition and its marker and durably retained the resulting history head, event root, or state digest as a semantic anchor. In that case, the verifier's local checkpoint records that the two-factor transition and validity boundary were previously accepted. A cold verifier or one that lost this durable anchor must either receive the predecessor evidence, use an explicitly weaker independent reanchor policy, or fail closed.

The resulting storage behavior is:

- current identity and fulfillment storage scales with the number of live resources;
- retained delivery, key-event, and transition evidence is bounded by explicit retention and compaction policies;
- deleted resources do not leave permanent semantic object graphs beyond compact current-state tombstones where required;
- delivery-log hash material is compacted below the verifier and deliverability watermark.

A small amount of aggregate cryptographic state may outlive individual resources, such as compacted delivery-log and per-user-history frontiers. This state contains no recoverable delivery or fulfillment semantics and exists only to preserve append-only continuity for supported verifiers.

A deleted identity retains a compact tombstone to prevent replay or unauthorized resurrection. This is current state, not historical evidence. A product may offer a clean re-enrollment experience, but the protocol still represents it as an authorized transition from the tombstone to a new generation rather than erasing the identity back to an unseen leaf.

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

- a consistency proof from its old delivery-log root to the current root, compact at `O(log n)`;
- an inclusion proof for the one targeted delivery;
- inclusion proofs for the zero, one, or two rotation markers bounding the delivery's signing state;
- a chain of direct authenticated-map leaf updates starting at its retained map root; and
- one compressed current-head map proof, one signing-event membership proof, and an immediate-successor membership proof only for a historical signer.

It does not receive intervening unrelated log leaves, unrelated identities, or the rest of the signing identity's event history.

Authenticated-map updates should be substantially less frequent than deliveries. They may be delivered:

- lazily with the next targeted delivery;
- periodically to active delivery agents;
- eagerly only when operationally useful.

Eager deliveries are particularly useful when evidence expires (e.g. ID token nonce binding).

Important key-history heads, rotation markers, and delivery-log checkpoints may also be published proactively to a broader set of delivery agents even when they have no targeted delivery. This reduces how long agents remain unaware of a rotation and increases the number of locally retained checkpoints. It costs fan-out bandwidth and does not by itself prevent a compromised resource manager from presenting different branches; comparison or gossip is required for fork detection.

Per-tenant logs and maps isolate churn among tenants and keep single-tenant delivery agents focused on only their tenant.

## 18. No dedicated transparency witnesses

The baseline design does not use Sigstore-style external witnesses.

Each delivery agent remembers the map roots and delivery-log checkpoints it has accepted and refuses rollback relative to them.

This guarantees:

> A resource manager cannot rewrite an established delivery agent’s previously observed history.

It does not guarantee:

> Every delivery agent immediately sees one globally consistent history.

A compromised resource manager may maintain divergent branches for different delivery agents, particularly during initial trust-on-first-use provisioning or by withholding later transitions from selected agents.

This is accepted as a practical limitation. The old-key cutoff guarantee begins independently at each agent when that agent accepts the key-history transition and proves its exact delivery-log marker. Optional proactive publication, checkpoint gossip, peer comparison, or external witnesses can be added later without changing the core signing model.

## 19. Timestamp-authority role

A timestamp authority is not required for:

- initial user enrollment;
- user-key continuity;
- key rotation;
- durable ordering of user deliveries;
- historical user-signature verification under logical cutoff or generation-based validity.

The ordered delivery log supplies protocol ordering:

```
delivery commitment
    before or after
key-transition marker
```

It does not supply wall-clock time.

A timestamp authority may be useful where wall-clock validity matters, especially when integrating workload credentials that expire after a short period. It can also supplement the delivery log for verifiable timestamps, delayed delivery across an expiry boundary, or cold verification that must prove a signature existed during a wall-clock validity window.

Without trusted time, the baseline can still verify signature integrity and logical ordering indefinitely while the required keys and transitions are retained. It cannot prove to a cold verifier that a newly presented signature was created before a wall-clock expiration. Such profiles either require acceptance before expiry, use log-position or generation validity instead of wall-clock signing time, fail closed when the distinction matters, or opt into a timestamp authority. The timestamp authority remains an optional robustness feature rather than a core dependency.

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

A small cluster-local credential bridge, potentially implemented as a narrowly scoped fleetlet capability, can:

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

When this workload evidence enters the attestation graph under continuity/v3, the outer item is Sigstore Bundle v0.3. The in-toto statement remains the inner authenticated assertion. SPIFFE, Kubernetes-bridge, or other verification material and authenticated-map proofs travel as support material.

## 21. Fundamental versus flexible elements

### Fundamental

The system requires:

* an initial tenant trust anchor and authenticated tenant trust manifest;
* a trust-manifest update policy selected by the previously accepted manifest;
* a defensible initial binding between an external user identity and a user-controlled continuity key;
* cryptographic authorization for changes to trusted identity state;
* proof of possession for newly introduced keys;
* an authenticated history-head map for tenant identities and trust authorities;
* append-only per-principal key-event histories, or an equivalent authenticated history commitment;
* semantic validation of a user’s current or historical key events, state, and ordering boundaries before that state is used as authority;
* an append-only ordered event log for durable deliveries and key-rotation markers;
* inclusion of each newly accepted delivery commitment in that log;
* inclusion of the exact marker referenced by every accepted ordering-sensitive key transition;
* direct authenticated-map leaf-update proofs rooted in each delivery agent's previously accepted map root;
* continuity from each bootstrapped delivery agent’s previously accepted delivery-log checkpoint;
* delivery-agent local checkpoints that are protected from rollback by the resource manager;
* cryptographic binding among the signed payload, tenant, target, fulfillment, delivery identity, and signing context;
* sufficient keys, delegations, and authorization evidence to validate every current or retained delivery;
* retention and compaction rules that do not remove evidence still required by a supported verifier or a deliverable fulfillment.

The delivery log itself does not require semantic authorization for every
appended leaf. It establishes ordering and append-only continuity. A rotation
marker has no authority unless an authenticated per-principal key event
references it and the transition signatures validate. Authentic provenance
comes from the independently verified signature and key-continuity chain;
permission to act and consistency between authenticated input and concrete
output retain the resource-manager authorization and common attestation
semantics defined in [provenance.md](architecture/provenance.md).

Historical public keys and authorization objects are therefore not retained merely because they once existed. They remain available only while referenced by:

* current committed identity history and state;
* a live fulfillment;
* a delivery version retained by product policy;
* a durable semantic anchor;
* or a trust transition that has not yet been replaced by an independently usable anchor.

### Flexible

The system can vary:

* two-level versus three-level user key hierarchy;
* exact recovery policy;
* the trust-manifest update policy, from a dedicated threshold authority to any currently verifiable tenant signer;
* whether continuity rotation receives a fresh OIDC identity binding as an additional factor;
* whether cold verifiers require a retained transition chain or durable semantic anchor, or may use an explicitly weaker fresh per-user OIDC reanchor;
* identity-provider signing-key resolution and archival policy;
* whether trust-map changes are validated eagerly or lazily per identity;
* whether marker inclusion is validated with the map update or deferred until a delivery depends on it;
* how eagerly delivery agents receive authenticated-map updates;
* the verifier-support or lease policy that determines the delivery-log compaction watermark;
* delivery and query-history retention periods;
* physical storage and tiling layout for Merkle structures;
* tenant-global delivery logs versus project logs anchored into a tenant-global ordering structure;
* whether the trust map and delivery log share one storage service;
* whether checkpoint gossip or external witnesses are later added;
* which workload identity adapters are supported;
* whether a timestamp authority is enabled for user or workload evidence that needs trusted wall-clock ordering.

## 22. Current recommended profile

The preferred implementation is:

```text
Per tenant:
    provisioned tenant trust anchor
    authenticated tenant trust manifest
    manifest-selected trust-manifest update policy
    sparse authenticated key-history-head map
    tenant-wide append-only delivery-and-rotation log

Per user:
    append-only key-event history committed by the map leaf
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
    signed authorization binds the predecessor and successor, not a cutoff
    resource manager appends that package as a delivery-log marker
    marker position is the cutoff serialization point
    next per-user key event references the exact marker leaf
    authenticated map advances to the resulting key-history head
    fresh OIDC binding is optional:
        as an additional factor without weakening continuity
        or as an explicitly weaker independent reanchor

Normal delivery:
    existing signed-input/output/removal attestation model remains authoritative
    V3 continuity evidence replaces or supplements signer key-binding evidence
    client sends signed input or output only to the resource manager
    resource manager assembles the complete delivery package
    commitment appended to the tenant delivery log
    package and required evidence sent only to the target delivery agent

Delivery-log verification:
    verifier retains previously accepted log size and root
    new delivery includes append-only consistency and inclusion evidence
    relevant rotation markers include exact-leaf inclusion evidence
    these may be encoded as one combined append proof
    an unreferenced or invalid rotation marker is inert
    no authorization is required for unrelated appended events

Trust-state verification:
    verifier retains its previously accepted trust-map root
    each update proves the old leaf under that root and computes the successor
        root by replacing only that leaf and reusing the sibling path
    sparse path uses a 32-byte presence bitmap and only non-empty siblings
    structural map membership alone does not grant authority
    map leaf commits current per-user key-history head and state digest
    each new event has append-only inclusion and consistency evidence
    rotation update additionally proves only its immediate predecessor event
    verifier semantically validates each introducing event during map advance
    invalid event is atomically indexed as a structured principal exception
    accepted root plus exception index is the semantic checkpoint
    event bodies, states, keys, heads, and marker boundaries are discarded

Resource-manager proof storage:
    versioned sparse nodes answer one path at an agent's retained map root
    sparse leaf replacement reads and writes one 256-level path
    per-user compact frontier appends without replaying old event bodies
    state-digest index locates a signing event directly
    event membership reads one body plus O(log h) Merkle nodes
    generated wire proofs are assembled on demand and need not be persisted

Per delivery agent:
    checkpoints for every tenant it serves:
        trust-map root
        bounded structured semantic exceptions
        delivery-log size and root
    no required per-user verifier database
    optional per-identity, device, and session validation cache

Historical verification:
    old keys and authorization objects are content-addressed and deduplicated
    evidence is retained while referenced by current or retained deliveries,
        or while needed to validate a supported transition
    independent OIDC reanchor is optional and explicitly weakens cold-verifier
        continuity to current IdP/account control
    current map proof authenticates the latest per-user history head
    signing-event proof authenticates the historical key
    immediate-successor proof identifies its retirement boundary when needed
    at most two continuity event bodies are sent regardless of history length
    delivery and marker inclusion prove the delivery falls in the key interval
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
    established verifiers older than the supported watermark fail closed and
        require explicit tenant-authorized or out-of-band recovery
    TOFU is reserved for genuinely new verifier provisioning

Baseline infrastructure:
    no required Fulcio
    no general certificate-transparency deployment
    no dedicated witness network
    no tenant administrator semantic-checkpoint authority
    no required timestamp authority for user deliveries
```

This profile keeps all state needed for current correctness and supported catch-up, while avoiding permanent semantic residue from deleted deliveries, fulfillments, and obsolete key histories. Deleted identities leave only compact current-state tombstones.

The central resource manager remains operationally important and remains the primary platform authorization authority. This design prevents it from converting compromise into forged end-user or workload provenance on the attested delivery path, while explicitly leaving non-attested mutation paths and complete policy authorization to their own controls.
