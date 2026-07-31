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

This proposal does not define a replacement signed-input or delivery-authorization language. It inherits the signed input, derived input, output constraint, placement, addon-signature, generation, put, and removal semantics described in [authentication.md](authentication.md#provenance-attestation-protocol-and-validation) and exercised by the [hybrid attestation prototype](../../poc/attestation/hybrid/README.md). Where those semantics overlap, that existing design and its tests remain authoritative.

This proposal focuses on the previously unresolved trust-distribution problem:

- binding an ordinary OIDC identity to a user-controlled continuity key without custom identity-provider claims or a key-aware identity provider;
- distributing current and historical key state through an untrusted resource manager;
- ordering durable deliveries relative to key transitions; and
- retaining only the evidence needed for current operation and supported historical verification.

The wire representation may extend or reuse the standard Sigstore bundle, in-toto/DSSE, and TUF-shaped trust material explored by the [logless Sigstore bundle POC](../../poc/attestation/sigstore_tuf_bundle/README.md), provided doing so preserves this protocol's continuity and ordering properties. Reusing those formats does not imply that Fulcio, a timestamp authority, or a transparency service becomes required infrastructure.

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
- appends durable delivery commitments;
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

Publishing new delivery-log checkpoints and important trust transitions proactively to more delivery agents can reduce the population and duration of stale branches. It also creates more independently retained checkpoints for later comparison. Without comparison or gossip, proactive publication alone does not prove that every agent received the same branch.

### Controlled-client security considerations

The controlled client and its signing interface are part of the trusted computing base. A private key that cannot be extracted still does not help if compromised client code can cause the user to approve an attacker-selected operation without an intelligible presentation of what is being signed.

For browser clients, the security-sensitive enrollment, canonicalization, display, and signing surface should be kept small and separated from resource-manager-served mutable content where practical. Strong options include an independently deployed origin, a sandboxed and isolated signing context with a narrow message protocol, or an external/native signer. Addon UI JavaScript must not receive direct access to key handles or a general-purpose signing API. A service worker may mediate a narrow signing protocol and reduce accidental exposure, but a same-origin service worker is defense in depth rather than an independent trust root against compromise of that origin.

The client should display the tenant, action, target scope, and signed content identity at the signing boundary. OIDC ceremonies and ID-token validation follow [OpenID Connect Core](https://openid.net/specs/openid-connect-core-1_0.html). The protocol intentionally requires only ordinary OIDC behavior, including nonce round-tripping; it does not require custom claims, Rich Authorization Requests, proof-of-possession access tokens, non-trivial scope mapping, or per-target audiences.

### Enforcement boundary and bypass paths

The guarantee applies to changes mediated by an attestation-verifying delivery agent. A protocol proxy to a target control plane, a direct cloud API credential, a break-glass channel, or any other mutation path that bypasses the verifier is outside that guarantee and relies on its own authorization controls.

Target and deployment profiles should make these bypasses explicit. Hardened profiles can structurally omit mutating proxy channels; less restrictive profiles can retain them subject to target-native authorization. Updates to the delivery agent, fleetlet, its trust configuration, or its channel profile are themselves security-sensitive operations and must not silently create an unattested path around the verifier.

## 4. Per-tenant structures

Each tenant has two logical authenticated subsystems: trust state, represented by an append-only update log and its derived current-state map, and durable-delivery ordering, represented by a separate append-only log.

The three structures may share implementation and storage, but their verification roles remain distinct.

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

“Committed” is deliberately not the same as “authorized.” The map proves which state digest is present under an accepted structural checkpoint. A state becomes usable as authority only after the delivery agent has semantically validated the enrollment or transition chain on which it depends. A malicious resource manager may commit invalid state and thereby cause selective denial of service, but map membership alone must never make that state authoritative for delivery.

A sparse Merkle tree is a likely implementation. The identity hash determines the leaf location, allowing both membership and absence proofs.

The map stores compact state digests, not necessarily full public keys.

For example:

`identity_id -> H(UserContinuityState7)`

The resource manager stores the actual state object separately:

`H(UserContinuityState7) -> serialized state`

The trust-update log is append-only. An accepted trust checkpoint binds its log size and root to the derived map epoch and root. To advance, a delivery agent verifies continuation of the trust log, validates the relevant events, and verifies or recomputes the corresponding map update. The exact sparse-map encoding may vary, but an arbitrary new map root supplied by the resource manager is never sufficient by itself.

### 4.2 Ordered durable-delivery log

The delivery log is an append-only Merkle log containing compact commitments to infrastructure deliveries.

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

    trust_log_size
    trust_log_root
    trust_epoch
    trust_map_root
    semantic_watermark
    exceptional_event_digests
    durable_semantic_anchors

    delivery_log_size
    delivery_log_root
}
```

The structural fields identify the history and current map accepted by this verifier. The semantic fields identify which parts of that structurally committed state have actually been validated and may be used as authority. `durable_semantic_anchors` contains only state digests whose required transition policy the verifier has already validated and on which evidence-retention decisions depend.

The delivery agent may additionally cache:

`identity_id -> last semantically validated state digest`

That cache is an optimization, not an authority source: losing it causes revalidation from retained evidence. If an implementation intends to delete predecessor evidence because a state was already validated, the validated state digest is no longer merely a cache entry; it must first be promoted into `durable_semantic_anchors` and protected with the rest of the checkpoint.

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

It also checks that the intent's tenant, expected issuer, enrollment client, protocol version, and algorithms match the accepted tenant trust manifest. The standard ID-token checks are defined by OpenID Connect Core rather than repeated exhaustively here.

The delivery agent extracts `iss` and `sub` from the validated ID token and derives:

`identity_id = H(protocol, tenant_id, iss, sub)`

This binds the first continuity key to whoever authenticated in that specific OpenID Connect flow without requiring the identity provider to understand public keys.

The ID token is used only as enrollment evidence. It is not used as an ordinary bearer credential for infrastructure operations.

## 8. Enrollment and transition failures

One invalid or unverifiable trust event must not block the entire tenant.

The authenticated map represents the tenant’s structurally committed current state. A delivery agent separately records which portion of that state it has semantically validated.

A delivery agent maintains the semantic portion of `TenantVerifierState` approximately as:

```text
SemanticTrustState {
    semantic_watermark
    exceptional_event_digests
    durable_semantic_anchors
}
```

The `semantic_watermark` identifies the contiguous trust-log prefix the delivery agent has processed. The exception set identifies enrollment, recovery, or transition events that did not validate within that prefix. Keying exceptions by event or state digest preserves the exact dependency that failed; an implementation may additionally maintain a principal index or an authenticated exception structure for scale.

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
    enrollment event exceptional
    untrusted
```

The delivery agent may continue advancing the structural trust log and authenticated map even when an event cannot be semantically accepted. It must crash-consistently record the exception—or stop advancing its semantic watermark—before treating later events as processed. This may be implemented transactionally or with an ordered durable protocol that cannot expose a partially advanced semantic state.

The effective signing-state trust rule is:

```text
usable_signing_state(state) =
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

The delivery agent refuses only deliveries whose provenance verification depends on that unresolved chain. If tenant policy permits the explicitly weaker independent-reanchor path, it can return an `AUTHORIZATION_ANCHOR_REQUIRED` (e.g. "PausedAuth") result so the client can perform a fresh nonce-bound OIDC reanchor and automatically resume the delivery. Otherwise, the unresolved chain requires another configured recovery path.

A successful reanchor creates a new trusted anchor event:

```text
E10: original enrollment, exceptional
E900: fresh OIDC reanchor, validated

current State3 depends on E900
```

The authenticated map does not need to be rewritten. Its current leaf simply advances to a state that references the new anchor.

### Effect on past deliveries

A later invalid enrollment, transition, or current-state claim does not retroactively invalidate deliveries that a delivery agent previously accepted under a valid historical signer and attestation chain.

Historical delivery validity is evaluated against:

- the delivery commitment’s log position;
- the key state valid at that position;
- the delivery agent’s previously accepted trust checkpoint.

Therefore, a malicious or invalid current-state update can:

- block future deliveries for the affected identity;
- prevent reconciliation of a retained delivery if its required evidence is unavailable;
- cause user-specific denial of service.

It cannot retroactively alter the payload, signature, log position, or acceptance decision of an already accepted delivery.

Only an explicitly defined and valid revocation policy could assign retrospective meaning, and the baseline protocol does not do so.

### New delivery agents

A genuinely newly provisioned delivery agent may make one explicit bootstrap trust decision over a tenant checkpoint that binds the current trust manifest, trust-log position and root, authenticated-map epoch and root, and any bootstrap exceptions. Trust on first use may establish that checkpoint when no stronger provisioning anchor is available.

That bootstrap decision treats the states committed by the checkpoint, excluding any recorded exceptions, as the agent's initial semantic baseline. Merely receiving a map root outside this bootstrap decision would still establish only structural state. The agent does not replay every historical enrollment decision and may therefore disagree with an older delivery agent that previously rejected a historical enrollment.

After bootstrap, the delivery agent processes new trust events normally and maintains its own watermark and exception set.

An established delivery agent that retains any prior checkpoint is not eligible for this path. If retained proof material can no longer extend that checkpoint, the agent fails closed and requires an explicit tenant-authorized or out-of-band recovery. Retention policy may end automatic catch-up support, but it must not silently reset an established target's trust.

### Storage failure

If a delivery agent cannot persist another exception safely, it must not advance its semantic watermark beyond that event.

It may still:

- advance structural Merkle state;
- continue accepting signing states anchored before the semantic watermark;
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
- the continuity state, enrollment or reanchor evidence, and required transitions;
- an authenticated-map proof under the verifier's accepted trust checkpoint; and
- delivery-log ordering evidence needed to evaluate cutoffs.

The complete delivery package cryptographically binds the tenant, target, fulfillment, delivery identity, generation, action, authoritative attestation root, and signing context. The exact encoding may reuse the `DeliveryPackage`, Sigstore Bundle, in-toto Statement, and DSSE structures from the existing POC rather than creating a parallel FleetShift-only envelope.

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
  - the current trust-log root and size and a consistency proof from the agent's prior trust-log root;
  - the resulting trust-map epoch and root and the proof needed to verify the relevant map update;
  - current identity-state evidence;
  - an authenticated-map proof;
  - any trust updates needed to advance the delivery agent’s trust checkpoint.

The delivery agent verifies the evidence independently.

Other delivery agents do not receive the delivery.

The delivery agent's reported or requested checkpoint is authoritative for selecting the consistency proof. The resource manager may cache the last acknowledged checkpoint for efficiency, but loss or corruption of that cache affects availability only: the agent can send its retained log size and root again.

### Delivery-log scope

The recommended profile uses one ordered log per tenant. This serializes commitment assignment within a tenant, but it gives every key transition one ordering domain across all targets that key may authorize. A workspace, project, or fulfillment log is possible only if its checkpoints are anchored into a tenant-wide ordering structure or the transition protocol otherwise establishes a cutoff in every applicable log. The performance and storage trade-offs remain to be validated against the actual scale range.

## 12. Delivery-agent verification

For a user-signed delivery, the delivery agent:

1. Verifies that the newer delivery-log root is an append-only extension of its stored root.
2. Verifies that the exact delivery commitment appears at the claimed log index.
3. Recomputes the delivery-package digest and commitment.
4. Advances the tenant trust map through any necessary trust updates.
5. Verifies the authenticated-map proof for the user’s current claimed state.
6. Establishes a semantically valid continuity-state anchor.
7. Validates any continuity transitions needed for the historical signing state.
8. Verifies the continuity-key authorization of the device.
9. Verifies the device-key authorization of the session.
10. Verifies the user signature over the signed input or output represented by the authoritative attestation package.
11. Runs the existing attestation verifier for derivation, addon signatures, placement, put or removal constraints, generation, tenant, target, purpose, and replay protection.
12. Confirms that the user signing state was valid at the delivery-log index.
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

The transition includes:

```
ContinuityTransition {
    identity_id
    previous_state_digest: H(State7)
    new_state_digest: H(State8)

    delivery_log_cutoff_size: N
    delivery_log_root_at_cutoff: LN

    signature_by_old_key
    proof_of_possession_by_new_key
}
```

The old key authorizes its successor. The new key proves that the client controls the new private key.

The client obtains the cutoff through a rotation-barrier request. The resource manager returns a proposed current delivery-log size and root. The client binds that exact pair into the signed transition; entries already represented by the root occupy indexes below `N`, while anything appended afterward occupies an index at or above `N`. The resource manager does not need to stop appending while the transition is signed. It may fork, withhold, or delay the barrier, but those behaviors have the local-view and denial-of-service limitations already accepted by the threat model.

The transition is included in the trust-update stream and updates the current authenticated-map leaf.

The delivery-log cutoff creates a logical validity boundary:

```
old state valid for delivery indexes < N
new state valid for delivery indexes >= N
```

The resource manager may delay a legitimate old-key delivery until after the cutoff, causing rejection. That is denial of service, not unauthorized access.

The cutoff becomes effective for a delivery agent only when that agent accepts the transition. An agent kept on an older branch has not yet established that cutoff; proactive transition distribution narrows this window but does not turn the local guarantee into global consistency.

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

The authenticated map commits to the current structurally selected state of each identity:

```text
identity identifier -> current state digest
```

The map conclusively identifies what the accepted branch currently commits to, but it does not grant delivery authority by itself. A state becomes usable for provenance verification only after successful semantic validation or because the verifier retained it as a durable semantic anchor.

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
- durable semantic anchors on which evidence compaction relies;
- current fulfillment payloads;
- delivery versions retained by product-history policy;
- and trust transitions not yet superseded by an independently usable anchor.

An old state, key, transition, authorization, or delivery bundle may be deleted when:

- it is not part of current committed state;
- no current or retained delivery references it;
- it is not needed to validate a supported transition;
- and no product-retention policy requires it.

Fresh OIDC binding during continuity-key rotation has two distinct uses:

- **Additional factor:** the transition still requires the old continuity key and proof of possession by the new key. OIDC adds freshness but does not replace continuity evidence.
- **Independent reanchor:** an explicit policy allows a cold verifier to accept the new key from a fresh nonce-bound OIDC ceremony without replaying the predecessor chain. This permits older enrollment and rotation evidence to be deleted once retained deliveries no longer reference it, but weakens continuity for that verifier: compromise of the IdP or user account can establish an attacker-chosen replacement key without the old continuity key.

There is no such weakening when a verifier already validated the old-key-authorized transition and durably retained the resulting state digest as a semantic anchor. In that case, the verifier's local checkpoint records that the two-factor transition was previously accepted. A cold verifier or one that lost this durable anchor must either receive the predecessor evidence, use an explicitly weaker independent reanchor policy, or fail closed.

The resulting storage behavior is:

- current identity and fulfillment storage scales with the number of live resources;
- retained delivery and transition evidence is bounded by explicit retention and compaction policies;
- deleted resources do not leave permanent semantic object graphs beyond compact current-state tombstones where required;
- delivery-log hash material is compacted below the verifier and deliverability watermark.

A small amount of aggregate cryptographic state may outlive individual resources, such as a compacted delivery-log frontier. This state contains no recoverable delivery or fulfillment semantics and exists only to preserve append-only continuity for supported verifiers.

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
- accumulated trust updates since its previous trust epoch.

It does not receive intervening unrelated delivery leaves.

Trust updates should be substantially less frequent than deliveries. They may be delivered:

- lazily with the next targeted delivery;
- periodically to active delivery agents;
- eagerly only when operationally useful.

Eager deliveries are particularly useful when evidence expires (e.g. ID token nonce binding).

Important trust transitions and delivery-log checkpoints may also be published proactively to a broader set of delivery agents even when they have no targeted delivery. This reduces how long agents remain unaware of a rotation and increases the number of locally retained checkpoints. It costs fan-out bandwidth and does not by itself prevent a compromised resource manager from presenting different branches; comparison or gossip is required for fork detection.

Per-tenant logs and maps isolate churn among tenants and keep single-tenant delivery agents focused on only their tenant.

## 18. No dedicated transparency witnesses

The baseline design does not use Sigstore-style external witnesses.

Each delivery agent remembers the checkpoints it has accepted and refuses rollback relative to those checkpoints.

This guarantees:

> A resource manager cannot rewrite an established delivery agent’s previously observed history.

It does not guarantee:

> Every delivery agent immediately sees one globally consistent history.

A compromised resource manager may maintain divergent branches for different delivery agents, particularly during initial trust-on-first-use provisioning or by withholding later transitions from selected agents.

This is accepted as a practical limitation. The old-key cutoff guarantee begins independently at each agent when that agent accepts the transition. Optional proactive publication, checkpoint gossip, peer comparison, or external witnesses can be added later without changing the core signing model.

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
key-transition cutoff
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

The in-toto statement and Sigstore bundle formats may be reused where they fit, with additional objects for authenticated-map and continuity evidence.

## 21. Fundamental versus flexible elements

### Fundamental

The system requires:

* an initial tenant trust anchor and authenticated tenant trust manifest;
* a trust-manifest update policy selected by the previously accepted manifest;
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

The delivery log itself does not require semantic authorization for every appended leaf. It establishes ordering and append-only continuity. Authentic provenance comes from the independently verified signature and key-continuity chain; permission to act and consistency between signed input and concrete output retain the resource-manager authorization and attestation semantics inherited from the broader authentication design.

Historical public keys and authorization objects are therefore not retained merely because they once existed. They remain available only while referenced by:

* current committed identity state;
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
* how eagerly delivery agents receive trust updates;
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
    manifest-selected trust update policy
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
    rotation binds the previous state and delivery-log cutoff
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
    these may be encoded as one combined append proof
    no authorization is required for unrelated appended commitments

Trust-state verification:
    verifier retains trust-log size and root, and trust-map epoch and root
    structural map membership alone does not grant authority
    user state is semantically validated before being used as authority
    validated user, device, and session evidence is cached by digest
    states relied on for evidence compaction become durable semantic anchors
    map proofs and transition evidence are supplied only on cold use,
        cache loss, or relevant trust-state change

Per delivery agent:
    checkpoints for every tenant it serves:
        trust-log size and root
        trust epoch and map root
        semantic watermark, exceptions, and durable anchors
        delivery-log size and root
    optional per-identity, device, and session validation cache

Historical verification:
    old keys and authorization objects are content-addressed and deduplicated
    evidence is retained while referenced by current or retained deliveries,
        or while needed to validate a supported transition
    a verifier may compact predecessor evidence after durably recording a
        semantically validated successor anchor
    independent OIDC reanchor is optional and explicitly weakens cold-verifier
        continuity to current IdP/account control
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
