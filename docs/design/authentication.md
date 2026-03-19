# Authentication model

This document describes a novel approach to authentication for a management plane, under the overall governing principle that a compromise to the customer-facing management platform MUST NOT compromise an entire multi-tenant provider estate.

This leads to a few key constraints:

- No highly privileged service accounts
- End to end user authentication & auditing
- No or limited storage of customer credentials (e.g. if we store refresh tokens, they are scoped, rotated, and sender-constrained)
- Trust anchors all external to the core platform (customer's own tenant IdP, a separate platform addon, key registry, ...)

## The delivery problem

Implementing a useful management platform within these constraints is difficult. The platform frequently acts as an intermediary between a user and a target where the user isn't making the API call directly. This is a problem in both time and space:

- **Time**: long-running rollouts outlive a user's credential presentation (e.g. JWT). The authorization must persist beyond the token's validity window.
- **Space**: in provider delivery, the authorization must cross a trust boundary the user doesn't span directly. The user is behind the curtain with no direct authority at the factory cluster. See provider_consumer_model.md for the full provider/consumer/factory topology.

Both require the platform to carry proof of the user's authorization to a place or moment where the user can't present it themselves. The design resolves this with two orthogonal concerns:

- **Provenance** — cryptographic proof of who authorized the operation and what they authorized. Every delivery carries a user signature. The delivery agent verifies this before applying (if it's capable of doing so).
- **Credential presentation** — whose credential actually applies the resources at the target: the user's own token (run-as-me), a workload/delegation SA (run-as-workload), or the delivery agent's SA (run-as-platform).

These are independent: any provenance strategy works with any credential-presentation mode. The trust model, design approach, and authorization details below address how both concerns are realized.

## Trust model

The platform is never a trust root. Anything that verifies credentials or signatures has to have a trust anchor, and those anchors must be external to the platform. The platform consumes trust — it does not establish it. Updating a trust anchor must itself require credentials that chain back to the current anchor.

### Trust anchors

When you can't trust the platform, where do you look?

- **User-level.** A user with a signing key (an IdP-authenticated principal on a device) directly signs operations. Every delivery carries provenance traceable to a specific person and device. This is the strongest level because compromise requires both the user's IdP identity and their signing key.
- **Tenant-level.** The tenant's IdP provides identity and authorization claims. Operations carry tenant-level identity (e.g., a user's ID token), but without a user signature the platform is trusted to faithfully represent the user's intent. This is the baseline for credential presentation.
- **Addon-level.** Addons — scoring services, external placement authorities, capacity controllers — sign decisions with their own keys. The delivery agent trusts the addon's signing authority directly, not via the platform. This requires sufficient controls: key management, scope limits, and auditable enrollment of addon signing keys. See [Placement enforcement dimension 3](#placement-enforcement-and-removal-protection) for how this works in practice.

### Why these anchors work

- It's not new trust. The tenant already trusts their IdP for everything else. Users and user agents can maintain per device keys, on devices that are already managed. We're building on an existing relationship, not introducing a new one.
- It's tenant-controlled. The tenant manages user lifecycle, MFA policy, group membership, key rotation, and endpoint devices. A platform-operated root (like a signing CA) would be new trust the tenant has to accept from the platform operator, or deal with the cost and risk of running themselves.
- Compromise is tenant- or user-scoped. If tenant T's IdP is compromised, only tenant T is affected. A platform-level root (CA, signing service) has cross-tenant blast radius.

Addon-level trust is parallel. We can delegate trust to addons, not unlike delegating trust to target cluster delivery agents. Addons can have their own signing keys, managed within the addon's trust boundary. It can take a different position in the network than the directly user-facing platform API.

### Residual risk

A compromised platform cannot subvert IdP trust on existing targets, so long as the trust is already established and the platform has no write access to it. The target requires a tie to an existing trust anchor to change them. Only new targets during a compromise window are at risk, and only if the platform is in the trust establishment path for those targets.

## Design approach

- Platform-side work (inventory, search, local operations) is the easy case — the platform can authorize and audit those locally. The delivery problem above is where the design must be careful.
- Every delivery carries a user signature: a user with a key (an IdP-authenticated principal on a device) signs the operation, and the delivery agent verifies the signature before applying. Key enrollment is trivially automated by tooling. Without a delivery agent capable of verification, there is no target-side provenance — the credential-presentation mode still applies, and PausedAuth handles credential gaps. The platform's audit trail records provenance regardless.
- Signing is independent of whose credential applies the resources. The deployment chooses a credential-presentation mode — the user's own token, a workload/delegation SA, or the platform's delivery agent SA — and this is orthogonal to provenance. The delivery agent verifies the user's signature in all cases.
- Transport (how the instruction reaches the delivery agent) is the other orthogonal dimension. Any credential-presentation mode works over any transport.
- Bootstrap-time privilege is sometimes unavoidable, but it must not become steady-state trust authority. In particular, the platform must not be able to rewrite target trust configuration or otherwise turn temporary operational access into permanent identity authority.

The platform's delivery authority is contingent on target delivery authorizing a credential and validating provenance. It is only a courier.

Credentials and provenance are often time and scope bound. Tokens expire. Keys rotate. Permissions change. When the platform isn't a super authority–when it's just a courier–it can reach a scenario when what it's trying to be delivered is refused. This design treats this as an expected and recoverable state: e.g. "PausedAuth". **PausedAuth** is the universal fallback. Whenever credentials or attestation is missing, expired, or no longer sufficient, the deployment transitions to `PausedAuth` instead of failing. An authorized user can resume it with fresh approval or fresh credentials. CIBA (Client-Initiated Backchannel Authentication) composes naturally with this: `PausedAuth` is the state ("we need credentials"), and CIBA is one way to obtain them ("prompt the user on another device").

## Credential presentation

Whose credential applies the resources at the target. This is the user-facing choice when creating a deployment.

### Token passthrough (run as me)

The simplest model: the user's bearer token is passed through to the target. Full end-to-end user identity. Works while the token lives. Prefer keeping it in memory only; if replay/recovery requires persistence, treat it as a short-lived credential and handle it accordingly.

When the token expires mid-rollout, or on workflow replay, the deployment transitions to `PausedAuth` and waits for an authorized user to resume it with a fresh token. Any authorized user can resume; this gives approval-gate semantics for free.

#### Refresh tokens (credential durability for run as me)

Refresh tokens can be used to make "run-as-me" durable. It preserves end-to-end user identity at the target (the refreshed token IS the user's token), but, to secure it properly, requires advanced IdP features.

Ideally you would:

- Sender constrain them (DPoP, RFC 9449). This makes the platform privileged but only via its protected private key. Leaked credentials are not a problem. Sender constrained refresh tokens have some support. It would require the backend to be a confidential client and not the frontend. That can complicate CLI integration. Maybe you only approve these long lived flows through the browser, though. It's a few-time operation.
- Scope them. This can be hard because it requires more IdP configuration e.g. client per cluster which could be awful without automation. And automating that is itself difficult to set up (dynamic client registration / aud configuration). Plus you'd want token exchange of some kind or the original aud needs to include every cluster. There are some archived notes around Rich Authorization Requests (RAR) which could help.

Refresh tokens shine when: (a) the IdP supports sender constraints, flexible token exchange, and RAR (rare in practice), and (b) the targets work well with proper OAuth (access tokens, transaction tokens). Outside of that users should use them with caution.

### Delegation service accounts (run as workload)

When something is long running, the user creates a service account dedicated to run on their behalf, with a scoped subset of their permissions.

The provisioning flow is synchronous (while the user is present):

1. User creates a deployment targeting cluster X
2. The platform, using the user's own token, creates a ServiceAccount + Role + RoleBinding in the target cluster
3. K8s prevents privilege escalation: the RBAC API rejects RoleBinding creation if the user doesn't hold the permissions being bound. The user can only delegate authority they actually have.
4. User's token is discarded after provisioning. Never stored.

The platform then impersonates the service account using its service account identity. This is a small improvement over `TokenRequest`:

- Impersonation is auditable; token request looks indistinguishable from any other actor with the service account
- There is no additional token that can be used for anything else; that needs to expire, etc.

Ideally:

- Something expires these over time
- When the user's permissions shrink below those of their shadow service accounts, the delegated service accounts are automatically restricted too

You could also "just" create specific service accounts to run workloads that you wanted long-running, with strict permissions. If they ever tried to escape that, the deployment pauses for approval.

Trade-offs:

- The target sees the service account identity, not the user. User identity is in the platform's audit log, correlatable via SA naming/annotations. With user signing (the default provenance model), user provenance is still cryptographically bound — the delivery agent verifies the user's signature even when the SA is the apply credential. If the target lacks a delivery agent capable of verification, this binding is only correlatable, not cryptographic.
- Permission drift: if the creating user loses access, the SA retains its grants until explicitly reconciled. We may be able to eagerly cascade permission changes done by the platform to SAs associated with the user.
- K8s-specific pattern. Other targets need equivalents (IAM AssumeRole for AWS, Managed Identity for Azure, etc).

### None (run as platform)

This is the most novel model. It relies on provenance, transport authentication, and the "fleetlet" delivery agent design to secure, in order to avoid giving the platform server trust, while supporting both time & space separation for authorized deployments. Provenance provides prove of original authorization, and the fleetlet has isolated authority for its target behind network separation that decouples it from the platform server.

## Provenance

When supported, a delivery agent independently verifies that a real user authorized the operation. This composes with all credential presentations. It tightens the scope of what a compromised platform can do, especially in the "run as platform" case.

1. A request intent (and/or derived decisions like manifests or placement) is accompanied by signed proof material (attestation envelope)
2. The platform delivers the envelope to the target
3. The target-side delivery agent validates the envelope and applies only if validation succeeds. See the attestation protocol section below for the concrete envelope and validation sequence.

Users sign directly with their own key, per device per user agent — no stored tokens per deployment, no IdP-level RAR dependency, works with any standard OIDC provider. Without a delivery agent capable of verification, provenance is not verified at the target — the platform's audit trail is the only record. An earlier design considered embedding a JWT in the delivery envelope as a lighter-weight provenance mechanism, but any target capable of running that protocol is equally capable of verifying user signatures, which is strictly better. See the Reference section for historical context.

The trust chain has three independent links:

- JWT proves identity (from the tenant IdP, establishing a trust chain from the user's configured public key)
- User signature proves authorization (the user signed THIS specific content — intent or output)
- Platform connection auth proves transport integrity (for standard transport); platform signature for buffer transport

These are independent — compromising one doesn't break the others. The per-request JWT needn't be stored. The user authenticates (JWT verified at request time), signs the content (cryptographic proof of what they authorized + their current claims), then the per-request JWT can be discarded. The user's signature carries everything the delivery agent needs. No stored per-request tokens, no token-reuse-window concern.

Note: a JWT may still be stored as part of the key binding bundle (see below), but that is per-enrollment with a TTL — not per-request.

### Verification level: intent signing vs. output signing

The user always signs. The question is *what* they sign — their intent (the input) or the platform's output (rendered manifests, placement decisions). This is a verification-level dial that applies to any state-changing operation: deployments, placement changes, label updates, pool membership.

**Intent signing (default)**: user signs their request — a deployment spec, a label change, a placement override. The platform derives consequences (renders manifests, computes placement deltas) and the delivery agent verifies that those consequences are consistent with the signed intent. On manifest invalidation (e.g., config rotation), the intent hasn't changed — the signature stays valid and no re-signing is needed. The tradeoff: the platform is in the derivation trust chain. The target must trust that the platform faithfully derived the outputs from the signed intent. Field mapping rules (below) constrain what the platform can derive.

**Output signing (opt-in, higher security)**: user signs the derived outputs — rendered manifests, specific placement deltas (e.g., "add to cluster A, remove from cluster B"). The signed artifact IS the applied artifact — zero derivation trust. But every change to the outputs requires the user to re-sign — the deployment enters PausedAuth until they do. Worth it for high-stakes / low-churn operations where derivation trust is unacceptable and toil is manageable.

Both levels compose with any credential-presentation mode (run-as-me, run-as-workload, run-as-platform). The delivery agent verifies whichever level was used.

### Signing surfaces

All three follow the same model: a per-user key pair is generated during enrollment, the private key stays with the user, the public key is registered via an OIDC-authenticated ceremony.

- **Web UI — Passkeys (WebAuthn):** The frontend computes `hash(intent)` and uses `navigator.credentials.get()` with the hash as the challenge. User approves via biometric. Zero new key management — passkeys are generated during enrollment, stored in the device's secure enclave, synced via passkey providers. Phishing-resistant, cross-platform.
- **CLI — Generated signing key:** `fleetshift auth enroll-signing` generates a dedicated ECDSA key pair. Private key stored in the OS keychain (macOS Keychain / Secure Enclave, Linux secret-service, Windows Credential Manager) — hardware-backed where available. On `fleetshift deploy`, the CLI signs `hash(intent)` with the stored key; user sees a keychain prompt (biometric on macOS Touch ID, PIN elsewhere). SSH keys supported as an opt-in alternative.
- **GitOps — Signing in git:** The user (or tooling like `fleetshift gitops sign`, a pre-commit hook, or a CI step) signs the content hash with their signing key and stores the signature in the git repo alongside the source content. The platform reads the signature from the repo, renders manifests if needed, and delivers everything in the same attestation envelope as web/CLI. The delivery agent's verification is identical to the other surfaces — no git metadata forwarding, no special GitOps verification path. Git is just the transport/storage medium for the user's signature. The same signing key used for `fleetshift deploy` works for `fleetshift gitops sign`. Standard git commit signing (`git commit -S`) is orthogonal: it provides git-level integrity (defense in depth) but is not the FleetShift verification mechanism.

### Key distribution and binding

The delivery agent must verify that a public key genuinely belongs to a given user. If the platform controls the key registry, a compromised platform can swap a user's key and forge intents. Key-to-user binding must be anchored externally — not derived from the platform itself. Two distribution models are available (which can vary by tenant or user):

1. **Platform distribution through a "JWT binding bundle."** This binds a public key entry to a user's own JWT, self-signed, tracing trust back to an IdP rather than the platform, while retaining the platform as the distribution mechanism.
2. **External key source** such as GitHub or GitLab APIs (which both offer unauthenticated key endpoints for retrieving public key material per user). As in IdP trust / JWKS, these external sources must be configured with the appropriate authority and validated at the delivery agent.

**Key binding bundle (model 1):** At registration time, the user creates a self-certifying bundle:

1. User authenticates to their IdP → gets a JWT
2. User generates key pair (or uses an existing one)
3. User signs `{public_key, jwt.sub, jwt.iss, timestamp}` with the new private key (proof of possession)
4. The bundle `{key_binding_doc, key_binding_signature, jwt}` is stored on the platform

The delivery agent verifies the bundle independently:

- JWT signature valid against tenant IdP JWKS → the IdP vouched for this user's identity at registration time
- `jwt.sub` matches the claimed user identity → the key is for this user
- Key binding is signed by the corresponding private key → proof of possession
- The intent is signed by the same key → continuity

A compromised platform can't swap the key because it would need a JWT with `sub=alice@tenant.com` to create a valid binding for Alice — and it can't get that without authenticating as Alice to the tenant's IdP. The IdP is the trust anchor for key binding.

**Key binding TTL and renewal:** Key bindings have a TTL (e.g., 30-90 days). Before expiry, the client auto-renews by creating a fully fresh bundle: authenticate to IdP → get new JWT (signed with current IdP key) → re-sign the key binding doc with updated timestamp (fresh proof of possession) → replace the old bundle. Everything in the renewed bundle is from the same point in time. Automatable if the client has a session or refresh token. Provides a natural revocation boundary: even if a key is compromised, the binding expires within the TTL.

**GitOps key registry:** With the unified signing model, GitOps uses the same key binding bundle as web/CLI — the primary verification mechanism is identical across all surfaces. The git hosting platform's public key endpoints (GitHub `GET /users/:username/ssh_signing_keys`, `GET /users/:username/gpg_keys`; GitLab `GET /users/:id/keys`) remain available as an additional or fallback verification source, fetched directly by the delivery agent (not through the platform). These endpoints are unauthenticated and function like JWKS endpoints.

TODO: Can different key registries be pluggable? The high level API is the same (validate this signature for this user) but the implementations are quite different.

**IdP key rotation:** In the case of platform-stored keys (with key binding bundle), there is a security vs availability tension. When JWKS changes, all keys need to be resigned. Keeping a JWKS history (caching old IdP signing keys) however undermines, to some extent, the purpose of key rotation — you rotate keys to limit the blast radius of compromise, and keeping old keys trusted defeats that. On the other hand, routine rotation becomes regular toil for those authorizing deployments, when a lack of authorization means provisioning is unavailable.

We should consider independent TTLs for the key bundles/JWKS so we can reduce the risk that routine rotation causes mass toil or availability issues. Key binding bundles must be renewed before the signing key leaves the history of stored JWKSs.

**Emergency rotation (compromise response):** IdP immediately removes old key. All key bindings signed with that key become unverifiable unless we keep history. Affected deployments enter PausedAuth. This is correct behavior — if the IdP rotated due to compromise, old key bindings should be invalidated. JWKS history  undermines this, in order to reduce toil as part of regular rotation. If we do keep history, an administrative "do not trust this kid" would be required.

**UX softening for rotation events:** The platform watches the IdP's JWKS. When it detects a rotation, it proactively notifies users whose key bindings were signed with the rotating key. If CIBA is available, the platform can push out-of-band re-authentication requests. An interactive user agent (web UI, CLI) can do an automatic renewal with the user already present. Inactive users whose key bindings expire enter PausedAuth on their next interaction — standard flow.

TODO: Look at how this interacts with "root" accounts

### Multi-signature for high availability and audit

A deployment can carry multiple signatures over the same content from different authorized users. The delivery agent accepts the intent if at least one signature is verifiable against a valid key binding. Benefits:

- **High availability:** if one signer's key binding expires (inactive user, missed renewal), the deployment continues as long as another signer's binding is still valid. Reduces PausedAuth events for critical deployments.
- **Audit:** multiple signatures cryptographically demonstrate review and acceptance from multiple humans in the loop — useful for compliance and change management audit trails.

Reactive re-signing (any authorized user re-signs the current content when a key binding is approaching expiry) already falls out of the existing "updates by different users" model. Multi-signature adds proactive redundancy on top: signatures are collected at creation/update time, not just when someone re-signs later.

### Updates and anti-replay

Deployments can be updated by different authorized users. Each update requires the editor to provide a new signature. The delivery agent verifies the new signature.

A compromised platform can't forge a user signature (no user signing key). Residual attacks:

- **Replay:** present an old signature. Defense: monotonic sequence number or nonce in the signed content. Delivery agent rejects operations with sequence <= current.
- **Withholding:** refuse to deliver a validly signed operation (DoS). Observable — the user sees their deployment isn't progressing.
- **Misdirection:** deliver a legitimately signed operation to the wrong target. Defense: the signed content includes target scope; the delivery agent checks consistency.

### Placement enforcement and removal protection

If a compromised platform can trigger removal of all resources (by manipulating placement or sending unsigned deletions), the signing model hasn't bought much. The delivery agent must be able to independently verify that any placement or removal action is legitimate. The same core principle of external trust anchors applies here along several dimensions:

**1. Removal allowance (deployment/addon property).** Whether a deployment can be removed from a cluster via placement change, or only via explicit signed deletion. This is a knob the deployment or addon declares. Pool-based placements (e.g., hosted control planes on management clusters) would typically disallow removal by placement change — once placed, the resource stays absent explicit lifecycle action. Consumer workload deployments may allow it. This prevents a blanket "remove everything" attack for sticky deployments.

**2. Change provenance for placement-affecting state.** When the platform delivers a removal or rescheduling decision, it should carry the provenance of the triggering state change — not just "remove this" but "remove this because user X removed label L from you; here's the signed request." The triggering state change (label removal, pool membership update, etc.) is itself a signed action — the same signing model applies to placement-affecting operations as to deployment intents. The delivery agent verifies: the state change was signed by an authorized user, the change means the placement constraints no longer match, therefore removal is legitimate. This extends to any platform-known state that affects placement (labels, pool membership, etc.). The cluster agent doesn't just trust "the platform says remove" — it verifies the *reason* for removal, backed by a user signature on the triggering change.

**3. External placement authority.** When placement decisions come from outside the platform (scoring addons, external capacity services), they carry their own signing authority. The delivery agent can verify these independently. A cluster-side component scoring itself is one case; an external service in a separate trust boundary signing placement decisions with its own keys is another. The platform consumes these decisions but doesn't control them at the enforcement point.

**4. Platform-generated artifacts bound to signed intent.** The pool membership set, the resolved target list — these are platform-generated. The signed intent includes placement constraints (label selectors, pool reference, target scope). The delivery agent independently confirms it matches: for label-based placement, the agent checks its own locally-trusted labels against the intent's constraints; for pool-based, pool membership is derived from cluster labels (self-assessable) or explicitly assigned with admin-signed provenance. The pool definition itself (membership criteria) chains back to a signed action — the delivery agent can verify that the pool's criteria were legitimately established.

Examples of how these compose:

- *Pool-based HCP placement:* removal disallowed by placement change (dimension 1). Consumer signed intent says "place in pool X." Agent verifies it's in pool X via its own labels (dimension 4). Deletion requires consumer signed deletion intent. Provider draining a management cluster is an elevated lifecycle operation with provider-level authorization.
- *Label-based workload placement:* removal allowed by placement change (dimension 1). Platform says "remove because label changed." Agent verifies the signed label change request (dimension 2) — who signed it, was it in scope. If verified, agent accepts the removal. With output signing, the user can additionally sign the placement delta itself, giving the agent proof of the specific removal decision.
- *Score-based rebalancing:* scoring addon signs its placement decisions independently (dimension 3). Agent trusts the addon's authority. Platform routes the decision but can't forge it.

### Provenance attestation protocol and validation

The delivery agent needs to verify that what's being applied matches what the user authorized. The verification protocol is uniform regardless of credential presentation or whether the separation is in time, space, or both.

The delivery agent performs four checks:

1. **Signature verification** — the user signed this specific content (cryptographic). With intent signing (default), the signed content is the intent; with output signing, the signed content is the rendered output. This simultaneously proves identity and authorization.
2. **Key binding verification** — the signing key belongs to this user, anchored to the tenant IdP via the key binding bundle. The delivery agent validates the bundle's JWT against the tenant's JWKS, confirms the subject matches the claimed user, and verifies proof of possession.
3. **Derivation consistency** (intent signing only) — the generated manifests are structurally consistent with the signed intent. If the intent says `tenant=acme, namespace=production`, no manifest can target a different tenant or namespace. With output signing this check doesn't apply — the signed content IS the applied content.
4. **Temporal validity** — the operation hasn't expired (`now() <= valid_until`).

Any check failure transitions the deployment to `PausedAuth`.

This is compatible with manifest invalidation. When manifests are re-generated (e.g., config rotation triggers `InvalidateManifests`), the intent hasn't changed — the same signature is still valid. New manifests must still satisfy the same field mapping rules. See "Verification level: intent signing vs. output signing" above for the tradeoffs between signing the intent vs. the output.

**Envelope:**

```
create_attestation(intent, user_signature, key_binding):
    {
        intent: intent,
        intent_hash: hash(intent),
        user_signature: user_signature,
        key_binding: key_binding,
        created_at: now(),
        valid_until: user_specified_expiry or default,
        platform_signature: sign_platform_key(...)  // optional, for buffer transport
    }
```

**Validation:**

```
validate_attestation(attestation, manifest):
    assert user_signature_valid(attestation.intent, attestation.user_signature, attestation.key_binding.public_key)
    assert key_binding_jwt_valid(attestation.key_binding.jwt, tenant_idp_jwks)
    assert key_binding_possession_valid(attestation.key_binding)
    assert attestation.key_binding.jwt.sub == claimed_user
    assert now() <= attestation.valid_until
    assert manifests_consistent_with_intent(manifest, attestation.intent)  // intent signing only
    // platform signature checked only for buffer transport
    // any assertion failure → PausedAuth
```

The platform signature is optional for standard (gRPC) transport — the user's signature provides the integrity guarantee. For buffer transport, the platform signature remains valuable since there is no connection-level auth.

#### Field mapping rules

Derivation consistency (check 3) requires mapping rules: which intent fields correspond to which manifest fields. These rules are the enforcement mechanism — they determine what "consistent with the signed intent" actually means structurally.

Start with hardcoded rules in the delivery agent for well-known fields (TBD, but e.g. `namespace`, `tenant` label, resource types). This is code, auditable, and as trustworthy as the delivery agent binary itself. An attacker would need to replace the binary, not just modify a config.

If configurable rules are needed later (dynamic resource types, addons), updates to those rules must be secured by a trust anchor external to the platform — e.g., a token from the platform administrator's IdP, validated the same way any trust configuration change is validated. The platform must not be able to unilaterally loosen the mapping rules on a target. The same principle as IdP trust configuration applies: the platform is a courier for rule updates, not the authority.

### Open questions

- RAR (RFC 9396) adoption is still early. The architecture should degrade gracefully when the IdP only supports scopes or audiences. What's the minimum binding level we're willing to accept before falling back to PausedAuth / re-approval?
- For the CIBA gitops flow: how does CI authenticate to initiate the CIBA flow? It needs its own client credentials with the IdP, which is itself a stored secret. This is a narrow, well-scoped secret (can only initiate approval requests, can't issue tokens without user consent), but it exists.
- Key lifecycle for user signing: rotation, revocation, lost keys. What happens when a user loses their device? The key binding TTL provides a natural expiry, but active revocation (before TTL) may need a mechanism.
- Claims freshness with user signing: the signed intent embeds claims from signing time. If the user's permissions change after signing, the claims are stale. How should the system handle this — re-check via SCIM/CAEP, key binding TTL as a natural bound, or require re-signing on permission changes?
- Anti-replay mechanism details: monotonic sequence numbers vs. nonces — which is simpler to implement correctly for the delivery agent?
- Secure bootstrap of cluster-side label/identity state for placement enforcement. How does a cluster initially receive its labels through a non-platform authority?
- Trust model for scoring addons and external scoring services. How does a delivery agent know to trust a particular scoring addon's signatures?
- Whether placement constraints in signed intents are mandatory or opt-in.
- Multi-signature policy: when to require vs. allow multiple signers, quorum rules for critical deployments.
- Can different key registries be pluggable? The high-level API is the same (validate this signature for this user) but implementations differ (key binding bundles, git hosting platform endpoints, etc.).

## Delivery architecture

How the provenance and credential presentation models are enforced at the target.

### Cluster-side delivery architecture (K8s)

Architectural constraints for the cluster-side delivery agent (part of the fleetlet):

- The delivery agent combines verification, apply, and status/drift reporting in one component. It watches managed resources and reports status and drift back to the platform. If drift is detected, the platform re-delivers through the appropriate delivery path.
- There is no separate broadly privileged reconciler acting on platform-originated data. The delivery agent combines verification and apply in one step — no intermediate CRD, no separate controller with broad RBAC consuming unchecked platform data.
- The delivery agent's in-cluster SA credential never leaves the cluster. The platform sends delivery instructions over the fleetlet connection, and the delivery agent uses its local SA. No cluster credentials travel to the platform.

### Transport as a security knob

The attestation contract (envelope in → validate → apply) is the same regardless of how the envelope reaches the delivery agent. Transport is a configuration choice per target profile:

- **Standard**: attestation envelopes delivered over the fleetlet gRPC connection. Simple, low latency. The platform has a live connection to the delivery agent process.
- **Hardened**: attestation envelopes written to a buffer (S3, Kafka, NATS). The delivery agent reads from the buffer, validates, applies. No direct connection between the platform and the privileged component. The buffer is the airgap. See provider_consumer_model.md for the full buffer mode discussion.
- **CRD-based (not preferred)**: SignedOperation CRDs as a K8s-native transport. The delivery agent watches the API server for SignedOperation resources instead of reading from gRPC or a buffer. Adds standard K8s semantics (watch, list, kubectl visibility) without changing the validation contract. However, this introduces artificial intermediate resources on the cluster that aren't part of the actual workload — the cluster sees SignedOperation CRs alongside its real resources. The standard and hardened options are preferred because the end cluster only sees the real manifests that were delivered, which is more transparent.

The delivery agent's code is identical across transports. Dialing up the security knob (from standard to hardened to CRD-based) requires no changes to the validation logic or the attestation format — only a transport configuration change.

Note on platform signatures and transport: for the standard (gRPC) transport, the fleetlet connection is already authenticated (mTLS or workload identity). The delivery agent knows the message came from the real platform via connection auth, so a platform signature on the envelope is redundant — the user's signature (against the tenant IdP) is the meaningful check. The platform signature becomes valuable for buffered transport, where there is no connection-level auth and anyone with write access to the buffer could inject messages. The signature can be deferred until buffer transport is needed without changing the envelope format — it's an additive field.

## GitOps

GitOps workflows introduce a specific challenge: the git repo is the source of truth, and the platform applies from there. The signing and attestation models above apply to GitOps — the verification contract is the same regardless of whether the intent originates from an interactive session or a git commit.

### Long-lived authority

This is the GitOps version of "run as the user over time." It assumes the platform stores something per user like a scoped refresh token and later applies with that user's own identity. With a sufficiently advanced IdP and configuration, this is technically securable, but the hard problem is still secure storage and lifecycle of long-lived user credentials.

Open questions remain: which user's authority controls apply when multiple users edit over time, and how unauthorized git changes feed back into the desired state. A CI check that runs platform-side authorization ahead of time could catch a lot.

The bigger challenge is securely storing longer lived credentials. See the refresh tokens section above.

### Intent-bound tokens for GitOps

With user-level signing, the GitOps verification model is unified with web/CLI: tooling signs the content hash and stores the signature in the git repo alongside the source content. The delivery agent's verification path is identical across all surfaces — no git metadata forwarding, no special GitOps verification path. Git commit signing (`git commit -S`) provides orthogonal git-level integrity (defense in depth, recommended but not required by FleetShift). The git hosting platform's public key endpoints remain available as a fallback/additional verification source for the delivery agent.

The token-based flows below are relevant as a complement to signing, particularly for GitOps-specific authorization patterns:

The tighter the binding between token and content, the safer it is to include a token alongside manifests in git. A token with no meaningful scoping beyond identity is risky because it can authorize too much during its validity window. A RAR-scoped access token with `manifest_hash` is the strongest form — it can only authorize the exact manifest it's bound to, and it expires.

Two flows:

**Token before commit (user-driven):** The user's CLI computes `hash(manifest)`, requests an access token from the IdP with `authorization_details` containing the manifest hash (via RAR), and commits the manifest + token together. The gitops controller validates the token against the tenant's IdP JWKS, checks that `authorization_details.manifest_hash` matches the actual manifest, and delivers if valid.

**Approval after commit (CIBA):** The user commits the manifest without a token. CI detects the change, computes the manifest hash, and initiates a CIBA (Client-Initiated Backchannel Authentication, an OIDC extension) flow. The user receives an approval prompt on a separate device showing what they're approving (via CIBA's `binding_message` parameter). On approval, CI receives a RAR-scoped token and attaches it for the gitops controller.

CIBA separates the commit from the approval — natural for gitops where you commit, review in PR, and approve after merge as a separate step. The user doesn't need a token at commit time.

When a token in git expires before the manifest is applied, the controller triggers re-approval (new CIBA flow or equivalent). This is `PausedAuth` semantics: expired credentials pause rather than fail.

Without full RAR support, audience scoping plus standard scopes still provide a weaker but meaningful form of binding (for example, a token scoped to the GitOps platform by `aud`, plus `scope=deploy:cluster-x:namespace-production`). This is not 1:1 content-bound, but it can still be better than giving the GitOps platform its own standing god credential: the token is still tied to a user, expires, and preserves end-to-end identity at apply time.

In that weaker-binding model, the remaining question is whether the residual scope is acceptable for the target environment. If it is, the token can still chain naturally into `PausedAuth`, re-approval, or refresh-token-based durability as needed. If it is not, prefer re-approval at apply time or a signing/attestation path that does not rely on a repo-stored bearer credential.

A useful refinement is to wrap repo-stored authorization material in a JWE encrypted for the target GitOps delivery platform. That reduces exposure in the repository while preserving a user-linked token at apply time. It does not strengthen authorization semantics on its own, so the enclosed token still needs acceptable scope, but paired with platform audience scoping and reasonable deploy scopes it can be a decent model in practice.

## Operational concerns

### IdP trust management

**Discovery.** From a single issuer URL, everything else is derivable via standard OIDC discovery: JWKS (signing keys), endpoints, key rotation — all automatic, no platform involvement. Verifiers poll JWKS on their own schedule. The platform is not in this path.

**Changing trust configuration.** Admin operations that affect trust (new verifiers, audience changes, etc.) are standard OIDC-authenticated actions. The admin authenticates via their tenant IdP, gets a standard ID token, and the system verifies that token before applying any change. The admin's ID token IS the proof that chains back to the current trust anchor. No custom token types or non-standard IdP features needed. The platform can transport trust configuration changes (it's a courier) but cannot author them. Every change requires a credential from the tenant's IdP. The platform's own credentials are never sufficient to modify trust configuration.

**Establishment at the target.** For cloud-managed clusters (EKS, GKE, AKS): IdP trust is configured via the cloud provider's API, protected by the tenant's cloud IAM. The platform should not have IAM permissions to modify cluster authentication settings — this is naturally separable from deployment-level permissions. For self-managed clusters: how IdP trust reaches the target is TBD. The key constraint is that the platform must not be the authority for IdP trust configuration on the target — however provisioning works, it must chain back to the tenant's trust root independently of the platform.

### Bootstrapping targets

When targets (e.g. clusters) are bootstrapped, some elevated privilege is unavoidable (for example, a kubeconfig or a privileged user). That privilege can bootstrap RBAC syncing and related setup, whether those are their own deployments or part of the cluster deployment itself.

For the delegation SA model, bootstrapping also provisions the platform's own identity in the cluster. Its service account may get narrow impersonation permissions (to impersonate delegate SAs). This places trust in the platform, but it is scoped and auditable. We may not need this model.

Critically, bootstrapping must not give the platform **ongoing** authority over IdP trust configuration at the target. Elevated privileges during bootstrap are acceptable because they are time-bounded and observable. But if the platform retains the ability to reconfigure which IdP the target trusts, then a platform compromise can redirect trust and forge identity — defeating all downstream verification. The platform's runtime credentials at a target should be scoped to workload operations, not authentication configuration. Any bootstrap or proxy path that relies on a privileged kubeconfig should be retired, rotated, or otherwise narrowed as soon as possible, and must not become the authority over target trust configuration.

### IdP orchestration

In various scenarios, we could benefit from specific IdP configuration:

- Per cluster client IDs (audiences)
- Permission-level scoping (assuming you have an authorizer which takes this into account)
- If an IdP can handle the refresh token route... setup for that
- Token exchange (RFC 8693) for audience swapping without per-cluster client IDs
- CAEP/Shared Signals Framework (SSF) for real-time session revocation and permission change events

### Open challenges

- Git ops – GitOps has a platform level indirection: the git repo is the authority, and the platform applies from there. Some tools may support tenant-specific service accounts or impersonation.
- Audience scoping – if we want to scope tokens to particular clusters, we need separate audiences for those. More IdP configuration to do. Hard to make dynamic. Token Exchange (RFC 8693) can address this: exchange a platform-audience token for a target-audience token at the IdP. The IdP controls policy (which exchanges are allowed, for which audiences). This avoids per-cluster client IDs but requires IdP support (Keycloak, Dex have it; Auth0/Okta partial).
- Reconciliation – this is similar to the gitops challenge.
- Permission tracking – when a delegation service account's RBAC should track the creating user's permissions over time.
- Root user – there should be some non-IdP issued credential or out of band channel for configuring IdP trust. If your IdP is down or compromised or you messed up the configuration and you need to reconfigure, you need some escape hatch.
- Trust anchor distribution – this might be solved but it is tricky to think through end to end. If you are trying to avoid privileged service accounts, then you also need to be very careful about how trust is established to tenant-level roots itself. If a compromise can reconfigure all of those, then all of the end to end verification is not helping there.
- BMC credentials are unavoidable – maybe they can only be retrieved with a user token

## Practical architecture summary

For K8s targets, the layered model:

Every delivery carries a user signature (provenance). The table below shows how credential presentation and transport compose. Signing accompanies all rows — it is not a separate mode.


| Credential presentation | Apply credential         | User identity at target                             | User presence needed          |
| ----------------------- | ------------------------ | --------------------------------------------------- | ----------------------------- |
| Run as me               | User's live token        | Full (IdP-verified)                                 | During operation (or refresh) |
| Run as workload         | Delegation / workload SA | SA identity (provenance cryptographically bound)    | At creation only              |
| Run as platform         | Delivery agent SA        | Delivery agent (provenance cryptographically bound) | At signing only               |
| Any credential failure  | N/A (paused)             | N/A (paused)                                        | To resume (or CIBA-prompted)  |


Signing verification level can be dialed up per deployment: intent signing (default, user signs the intent) or output signing (user signs rendered manifests and/or placement deltas, zero derivation trust).

**Without a delivery agent capable of verification**, the credential-presentation table still applies but provenance is platform-audit-only — no target-side cryptographic verification. The credential modes work unchanged; only the provenance column weakens.

Delivery transport is configurable per target profile: standard (fleetlet gRPC), hardened (buffered via S3/Kafka/NATS), or future CRD-based. The attestation format and validation logic are identical across transports.

### Target credential presentation

The delivery agent declares what credential presentation it needs; the platform should not hard-code one token type for every target.

Typical contracts:

- K8s API apply/proxy: pass through the user's token when the target directly trusts the tenant IdP.
- AWS: ask for an ID token or SAML assertion, then `AssumeRoleWith*Identity` -> SigV4.
- GCP: ask for an ID token, then token exchange -> GCP token.
- Other targets: ask for "an access token for X" and let the delivery agent perform whatever target-specific exchange is needed.

If the durability model is delegation SAs, the delivery agent derives or requests the delegated credential from user-linked identity/provenance rather than a platform-global secret.

If we control the target's auth stack (for example, a Kubernetes distribution we customize), it is even better to validate access tokens and scopes/resource indicators directly rather than relying only on ID tokens. Vault-backed service-account credentials are a last resort; prefer credentials derived from the end user.

### Deployment options (UX)

Every deployment carries a user signature (provenance). The user-facing choice is about **credential presentation** — whose credential applies the resources:

1. **Run as me** — the delivery agent uses the user's token for apply. The target sees the user's real identity. Requires a live token (or refresh capability). Best when the user has direct authority at the target.
2. **Run as workload identity** — a dedicated service account applies the resources. The user signed the operation when they authorized the workload. The target sees the SA identity, but provenance is cryptographically bound to the user via the user's signature.
3. **Run as platform** — the delivery agent's own SA applies the resources. The user signed the operation when they gave it to the platform. The target sees the delivery agent, but provenance is cryptographically bound to the user.

All three carry the user's signature. The delivery agent verifies the signature in every case.

Optionally, **output signing** can be layered on any credential-presentation mode: the user signs the platform's derived outputs (rendered manifests, placement deltas) rather than just the intent, eliminating derivation trust at the cost of requiring the user to re-sign on every output change. This is a verification-level dial, not a fourth credential option.

Without a delivery agent capable of verification, credential presentation still works — the user chooses run-as-me, run-as-workload, or run-as-platform as above. Provenance is not verified at the target; it's recorded in the platform's audit trail only.

## Appendix: Archived content

### Constrained impersonation (not recommended)

This is conceptually similar to the above, but means the delivery agent directly impersonates the user. The fundamental problem: K8s impersonation lets the impersonator assert group membership, and K8s has no way to verify those assertions. Even with constrained impersonation (limiting which users can be impersonated via resourceNames), the impersonator can claim arbitrary groups for that user. If the platform can impersonate group "admins", it can put any user in that group regardless of their actual membership. These are unverifiable claims about a user.

With token passthrough, the IdP is the authority on claims – groups are in the token, cryptographically signed by the IdP. With impersonation, the platform is the authority. This is a fundamentally weaker trust model for any environment where group-based authorization matters. At most it should be a compatibility fallback, not the preferred steady-state model.

#### JWT-embedded provenance chain (considered, not pursued)

This model was considered as a lighter-weight provenance mechanism but is not pursued. The key insight: any target capable of running this protocol (parsing attestation envelopes, extracting JWTs, validating against IdP JWKS, doing structural field matching) is equally capable of verifying user signatures, which is strictly better — no stored tokens, no expiry window, no platform-can-forge-while-holding-JWT risk. User-level signing supersedes this entirely.

The approach uses only standard OIDC + a platform integrity key. Two-factor: the tenant's JWT provides identity/authorization, a platform-owned key provides integrity. Neither alone is sufficient. The user's JWT is embedded in an attestation envelope alongside the intent, signed by a platform-owned key.

Trust model:

- Compromised platform key alone: can sign manifests but can't produce a valid tenant JWT. Rejected at validation.
- Stolen JWT alone: can present identity but can't sign manifests. Rejected at validation.
- Compromised platform (has key + user's JWT in transit): can create attestations paired with the user's JWT while the JWT is live. Same exposure window as token passthrough, bounded by JWT lifetime.

Compared to Fulcio: a compromised Fulcio CA can forge signatures for any user indefinitely. This model limits forgery to users whose JWTs the platform currently holds, within JWT lifetime. The blast radius is smaller by orders of magnitude.

The residual risk (platform can pair a valid JWT with arbitrary manifest content while the JWT is live) is inherent to any model where the platform holds the user's token. It's the same as token passthrough but with better auditability — the signed manifest input is a persistent, inspectable artifact rather than an ephemeral API call. Unauthorized manifest inputs are detectable after the fact. User-level signing resolves this risk: the user signs directly, the per-request JWT is discarded after request-time verification, and the platform can't pair it with other operations.

The platform key is not a god key — it can only assert integrity, not identity. Its compromise alone cannot authorize anything. It could be scoped per-tenant to further limit blast radius.

This is a bounded short-lived credential retention model, not a long-lived secret model like refresh tokens. The platform may retain a JWT for replay/audit purposes, but the credential expires quickly and is only one factor of two.

Persisting user JWTs to a database (rather than just validating them in-memory per-request) is a deliberate architectural choice. The security question is what happens when the store is compromised. Here, the blast radius is: one user per token, only that user's authorized operations, only within the token's remaining lifetime, and only as one factor of two (the platform signature is also required). JWTs should be encrypted at rest and purged after expiry or operation completion. With user-level signing, this per-request JWT persistence becomes unnecessary — the user's signature replaces the stored JWT as the durable proof.

**JWT-embedded attestation envelope:**

```
create_attestation(user_jwt, intent):
    sign_platform_key({
        jwt: user_jwt,
        intent: intent,
        intent_hash: hash(intent),
        jwt_hash: hash(user_jwt),
        created_at: now(),
        valid_until: user_specified_expiry or default,
    })
```

**Validation:**

```
validate_attestation(attestation, manifest):
    assert platform_signature_valid(attestation)
    assert jwt_signature_valid(attestation.jwt, tenant_idp_jwks)
    assert attestation.created_at <= attestation.jwt.exp
    assert now() <= attestation.valid_until
    assert hash(manifest) == attestation.intent_hash  // if content-bound
    assert user_authorized(attestation.jwt, manifest)  // best-effort
    // any assertion failure → PausedAuth
```

#### Credential durability for JWT-embedded model

Had this model been pursued, the stale JWT after expiry would have been the only provenance. The system could supplement it with ongoing checks:

- Honor a user-specified validity bound ("this deployment is valid for N hours").
- Re-check permissions when invalidation or other signals arrive — against synced RBAC or the IdP, not the expired JWT.
- Track user status and permission changes over time (via SCIM/CAEP/SSF) and react accordingly — restricting, pausing, or revoking the operation.

With user signing, these ongoing checks may still be useful for claims freshness (the signed intent embeds claims from signing time), but they are not a provenance concern — the signature itself is the durable proof.

#### IdP support for RAR

RAR is a published RFC (May 2023). IdP support is growing but not yet universal (Keycloak has partial support via custom protocol mappers, full RAR is in progress). The architecture should accommodate the tightest binding the IdP supports and degrade gracefully: check `authorization_details` fields if present, fall back to `scope`-level checks, reject or require re-approval if no binding is present.

The binding-level spectrum for token-based provenance:


| Binding level   | Standard                               | What it constrains                                                                       |
| --------------- | -------------------------------------- | ---------------------------------------------------------------------------------------- |
| Identity only   | OIDC core (ID token)                   | Who the user is                                                                          |
| Action category | OAuth scopes                           | Kind of action (e.g. `deploy`, `deploy:production`)                                      |
| Target          | RFC 8707 (Resource Indicators)         | Which resource server / cluster accepts the token                                        |
| Intent details  | RFC 9396 (Rich Authorization Requests) | Structured authorization of the user's intent: tenant, target, namespace, resource types |
| Exact intent    | RFC 9396 + intent hash                 | Token bound to a specific intent (deployment spec) via content hash — 1:1 binding        |


Rich Authorization Requests (RFC 9396) is the key standard for structured intent binding. The `authorization_details` parameter carries structured JSON describing what the user authorized — at the intent level, not the manifest level:

```json
{
  "type": "fleetshift_deploy",
  "tenant": "acme",
  "target": "cluster-x",
  "namespace": "production",
  "intent_hash": "sha256:e3b0c44298fc..."
}
```

#### JWT + RAR as an alternative to user signing

JWT with RAR containing an intent hash achieves perfect 1:1 intent-to-user binding — the token is cryptographically tied to exactly one intent. User signing achieves the same binding strength. The difference is operational, not cryptographic:

- **JWT + RAR (with intent hash):** perfect binding, but requires storing a unique token per deployment, centralizing signing in the IdP, and requiring RAR support and configuration at the IdP (not universally available).
- **User signing:** perfect binding, with distributed private keys per user per device (already common with Git commit signing), no stored tokens per deployment, and no RAR dependency — works with any standard OIDC provider.

User signing is preferred for the same reasons it supersedes JWT-embedded provenance: distributed keys, no token storage, no IdP feature dependencies.

### Considered and not recommended

- **Web Crypto API (SubtleCrypto):** Fallback for environments without passkey support. Weaker: no hardware protection (keys are JS-accessible, vulnerable to XSS), browser/origin-specific, no biometric UX.
- **HTTP Message Signatures (RFC 9421):** Signs components of an HTTP *request* (method, path, headers, body digest) for hop-by-hop transport integrity. The signature is bound to the HTTP request lifecycle and doesn't survive beyond it. For FleetShift, the signature must travel from the user to the delivery agent through intermediaries (platform, rendering, transport) — a content signature over the intent hash, not a transport signature over one HTTP hop.
- **Git commit signing as the verification mechanism:** Git commit signatures are bound to the git object model (tree hash, parent, author). Using them for delivery verification requires either forwarding git metadata to the delivery agent (fragile, doesn't survive rendering) or having the agent pull from git directly (heavy). Instead, GitOps uses the same signing model as web/CLI: tooling signs the content and stores the signature in git. Git commit signing is orthogonal git-level integrity.