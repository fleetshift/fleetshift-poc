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

## Summary of deployment options

When deploying something, a user is presented with options based on:

- Who they want to see in the end target (e.g. cluster)
- What authority they have there
- Their security allowance.
  - Level 1: Low trust. Trusts the platform with durable, scoped user credentials. (This can still be an improvement over common management platform approaches.)
  - Level 2: Brief trust. Trusts the platform with temporary scoped user credentials.
  - Level 3: Near-zero trust. Trusts the platform with tightly-bound signed intent.
  - Level 4: Zero trust. Does not trust the platform at all. Pure courier of end to end signed manifests.
- Their availability constraints.

**In all cases**, the platform itself still enforces its own authorization rules, which it tries to make coherent with targets through syncing. Because of this, **deployments are not tightly coupled to individual users, even if their auth is used.** If they are unavailable, any other authorized user can approve and "take over" the deployment. Additional approvers can be added preemptively.

### Credential presentation

| Run as (identity at target) | Use when                                                                                                                                                  | Authority       | Base security (before provenance) | Availability | Commentary                                                                                                 |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | -------- | ------------ | ----------------------------------------------------------------------------------------------------------------- |
| Me                          | <ol><li>The delivery target does not support platform verification (e.g. AWS, GCP, ...)<li>Operations are short-lived or few<li>You have permissions</ol> | Target cluster  | Level 2  | Low          | Simple & secure for when it fits.                                                                                 |
| Me (+ refresh tokens)       | <ol><li>The delivery target does not support platform verification<li>Operations are long-lived<li>You have advanced IdP features</ol>                    | Target cluster  | Level 1* | Medium       | Makes sense if you want to realy track the user.<p>*Depends on IdP features.                               |
| Delegate service account    | <ol><li>The delivery target does not support platform verification<li>Operations are long-lived</ol>                                                      | Target cluster* | Level 1  | High         | Depends on some advanced orchestration for the service account<p>*RBAC orchestrated by platform (itself w/ E2E auth) |
| Standalone service account  | <ol><li>The delivery target does not support platform verification<li>Operations are long-lived<li>You do not have direct permissions</ol>                | Target cluster  | Level 1  | High         | Exists mainly as a fallback.                                                                                      |
| Platform                    | <ol><li>The delivery target supports platform verification<li>You do not want to, or cannot, authorize the end user as the target</ol>                    | Platform        | N/A*     | Medium*      | Probably the best balance but *requires target provenance verification support.<p>Especially attractive to multi-tenant service providers.               |

### Provenance

Provenance adds a high degree of security and enables a zero-trust platform architecture. It comes at the cost of some availability: signing keys must be made available. Tenants need to set up external key registries, or users need to ensure they periodically login to refresh their key during IdP key rotation intervals.

| Provenance        | Security            | Availability                                        | Commentary |
|-------------------|---------------------|-----------------------------------------------------| ---------- |
| None              | Credential baseline | Credential baseline                                 | The fallback for non platform targets (e.g. third party APIs) |
| Signed intent     | Level 3             | Credential baseline * public key availability       | A good default. |
| Signed manifests  | Level 4             | Low - user interaction required for all deployments | EXTREME security for rare, extremely sensitive deployments. |

## Trust model

The platform is never a trust root. Anything that verifies credentials or signatures has to have a trust anchor, and those anchors must be external to the platform. The platform consumes trust — it does not establish it. Updating a trust anchor must itself require credentials that chain back to the current anchor.

### Trust anchors

When you can't trust the platform, where do you look?

- **User-level.** A user with a signing key (an IdP-authenticated principal on a device) directly signs operations. Every delivery carries provenance traceable to a specific person and device. This is the strongest level because compromise requires both the user's IdP identity and their signing key.
- **Tenant-level.** The tenant's IdP provides identity and authorization claims. Operations carry tenant-level identity (e.g., a user's ID token), but without a user signature the platform is trusted to faithfully represent the user's intent. This is the baseline for credential presentation.
- **Addon-level.** Addons — scoring services, external placement authorities, capacity controllers, manifest generators — sign decisions and outputs with their own keys. The delivery agent trusts the addon's signing authority directly, not via the platform. This requires sufficient controls: key management, scope limits, and auditable enrollment of addon signing keys. See [Placement enforcement and removal protection](#placement-enforcement-and-removal-protection) for placement decisions and [Addon-generated manifests and the co-signing model](#addon-generated-manifests-and-the-co-signing-model) for manifest signing.

### Why these anchors work

- It's not new trust. The tenant already trusts their IdP for everything else. Users and user agents can maintain per device keys, on devices that are already managed. We're building on an existing relationship, not introducing a new one.
- It's tenant-controlled. The tenant manages user lifecycle, MFA policy, group membership, key rotation, and endpoint devices. A platform-operated root (like a signing CA) would be new trust the tenant has to accept from the platform operator, or deal with the cost and risk of running themselves.
- Compromise is tenant- or user-scoped. If tenant T's IdP is compromised, only tenant T is affected. A platform-level root (CA, signing service) has cross-tenant blast radius.

Addon-level trust is parallel. We can delegate trust to addons, not unlike delegating trust to target cluster delivery agents. Addons can have their own signing keys, managed within the addon's trust boundary. It can take a different position in the network than the directly user-facing platform API.

#### Trust anchor constraints

Trust anchors are not just key registries — they can carry constraints that scope what signers can authorize through them. These are CEL predicates evaluated when a signer is verified against the anchor. The predicate receives the anchor's attributes and the authenticated subject (including the content being authorized).

This enables multi-tenant isolation at the verification layer. For example, a tenant-scoped trust anchor can carry a constraint like `subject.content.deployment_id startsWith anchor.attributes.tenant_prefix`, ensuring that a signer recognized by tenant A's anchor cannot authorize deployments for tenant B, even if the platform is compromised.

Trust anchor constraints are identity boundaries: a user anchor and an addon anchor are independent policy enforcement points. A signer recognized by a user anchor cannot satisfy a constraint that requires an addon anchor, and vice versa. This prevents cross-anchor identity confusion. See `TrustAnchor` and `TrustAnchorConstraint` in `poc/attestation/hybrid/model.py` for the concrete model, and the tenant-scoping tests in `poc/attestation/hybrid/test_hybrid.py`.

### Residual risk

A compromised platform cannot subvert IdP trust on existing targets, so long as the trust is already established and the platform has no write access to it. The target requires a tie to an existing trust anchor to change them. Only new targets during a compromise window are at risk, and only if the platform is in the trust establishment path for those targets.

## Design approach

- Platform-side work (inventory, search, local operations) is the easy case — the platform can authorize and audit those locally. The delivery problem above is where the design must be careful.
- Every delivery carries a user signature: a user with a key (an IdP-authenticated principal on a device) signs the operation, and the delivery agent verifies the signature before applying. Key enrollment is trivially automated by tooling. Without a delivery agent capable of verification, there is no target-side provenance — the credential-presentation mode still applies, and PausedAuth (discussed below) handles credential gaps. The platform's audit trail records provenance regardless.
- Signing is independent of whose credential applies the resources. The deployment chooses a credential-presentation mode — the user's own token, a workload/delegation SA, or the platform's delivery agent SA — and this is orthogonal to provenance. The delivery agent verifies the user's signature in all cases.
- Transport (how the instruction reaches the delivery agent) is the other orthogonal dimension. Any credential-presentation mode works over any transport.
- Bootstrap-time privilege is sometimes unavoidable, but it must not become steady-state trust authority. In particular, the platform must not be able to rewrite target trust configuration or otherwise turn temporary operational access into permanent identity authority.

The platform's delivery authority is contingent on target delivery authorizing a credential and validating provenance. It is only a courier.

Credentials and provenance are often time and scope bound. Tokens expire. Keys rotate. Permissions change. When the platform isn't a super authority–when it's just a courier–it can reach a scenario when what it's trying to be delivered is refused. This design treats this as an expected and recoverable state: e.g. "PausedAuth". **PausedAuth** is the universal fallback. Whenever credentials or attestation is missing, expired, or no longer sufficient, the fulfillment transitions to `PausedAuth` instead of failing. An authorized user can resume it with fresh approval or fresh credentials. CIBA (Client-Initiated Backchannel Authentication) composes naturally with this: `PausedAuth` is the state ("we need credentials"), and CIBA is one way to obtain them ("prompt the user on another device").

## Credential presentation

Whose credential applies the resources at the target. This is the user-facing choice when creating a deployment.

### Token passthrough (run as me)

The simplest model: the user's bearer token is passed through to the target. Full end-to-end user identity. Works while the token lives. Prefer keeping it in memory only; if replay/recovery requires persistence, treat it as a short-lived credential and handle it accordingly.

When the token expires mid-rollout, or on workflow replay, the fulfillment transitions to `PausedAuth` and waits for an authorized user to resume it with a fresh token. Any authorized user can resume; this gives approval-gate semantics for free.

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

You could also "just" create specific service accounts to run workloads that you wanted long-running, with strict permissions. If they ever tried to escape that, the fulfillment pauses for approval.

Trade-offs:

- The target sees the service account identity, not the user. User identity is in the platform's audit log, correlatable via SA naming/annotations. With user signing (the default provenance model), user provenance is still cryptographically bound — the delivery agent verifies the user's signature even when the SA is the apply credential. If the target lacks a delivery agent capable of verification, this binding is only correlatable, not cryptographic.
- Permission drift: if the creating user loses access, the SA retains its grants until explicitly reconciled. We may be able to eagerly cascade permission changes done by the platform to SAs associated with the user.
- K8s-specific pattern. Other targets need equivalents (IAM AssumeRole for AWS, Managed Identity for Azure, etc).

### None (run as platform)

This is the most novel model. It relies on provenance, transport authentication, and the "fleetlet" delivery agent design to secure, in order to avoid giving the platform server trust, while supporting both time & space separation for authorized deployments. Provenance provides prove of original authorization, and the fleetlet has isolated authority for its target behind network separation that decouples it from the platform server.

## Provenance

> The provenance model described in this section has a working prototype in `poc/attestation/hybrid/`. The prototype isolates the target-side verification core — the attestation data model, constraint evaluation, strategy-implied policy, update chains, and trust anchor verification. See `poc/attestation/hybrid/README.md` for a guide to the prototype and its mapping to this design. Where this section and the prototype cover the same ground, the prototype's concrete model is authoritative.

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

**Intent signing (default)**: user signs their request — a deployment spec, a label change, a placement override. The platform derives consequences (renders manifests, computes placement deltas) and the delivery agent verifies that those consequences satisfy the constraints implied by the signed intent. On manifest invalidation (e.g., config rotation), the intent hasn't changed — the signature stays valid and no re-signing is needed. The tradeoff: the platform is in the derivation trust chain. The target must trust that the platform faithfully derived the outputs from the signed intent. Strategy-implied constraints and explicit output constraints (below) bound what the platform can derive.

**Output signing (opt-in, higher security)**: user signs the derived outputs — rendered manifests, specific placement deltas (e.g., "add to cluster A, remove from cluster B"). The signed artifact IS the applied artifact — zero derivation trust. But every change to the outputs requires the user to re-sign — the deployment enters PausedAuth until they do. Worth it for high-stakes / low-churn operations where derivation trust is unacceptable and toil is manageable.

Both levels compose with any credential-presentation mode (run-as-me, run-as-workload, run-as-platform). The delivery agent verifies whichever level was used.

### Signing surfaces

All three follow the same model: a per-user key pair is generated during enrollment, the private key stays with the user, the public key is registered via an OIDC-authenticated ceremony.

- **Web UI — Passkeys (WebAuthn):** The frontend computes `hash(intent)` and uses `navigator.credentials.get()` with the hash as the challenge. User approves via biometric. Zero new key management — passkeys are generated during enrollment, stored in the device's secure enclave, synced via passkey providers. Phishing-resistant, cross-platform.
- **CLI — Generated signing key:** `fleetshift auth enroll-signing` generates a dedicated ECDSA key pair. Private key stored in the OS keychain (macOS Keychain / Secure Enclave, Linux secret-service, Windows Credential Manager) — hardware-backed where available. On `fleetshift deploy`, the CLI signs `hash(intent)` with the stored key; user sees a keychain prompt (biometric on macOS Touch ID, PIN elsewhere). SSH keys supported as an opt-in alternative.
- **GitOps — Signing in git:** The user (or tooling like `fleetshift gitops sign`, a pre-commit hook, or a CI step) signs the content hash with their signing key and stores the signature in the git repo alongside the source content. The platform reads the signature from the repo, renders manifests if needed, and delivers everything in the same attestation envelope as web/CLI. The delivery agent's verification is identical to the other surfaces — no git metadata forwarding, no special GitOps verification path. Git is just the transport/storage medium for the user's signature. The same signing key used for `fleetshift deploy` works for `fleetshift gitops sign`. Standard git commit signing (`git commit -S`) is orthogonal: it provides git-level integrity (defense in depth) but is not the FleetShift verification mechanism.

### Key distribution and binding

The delivery agent must verify that a public key genuinely belongs to a given user. If the platform controls the key registry, a compromised platform can swap a user's key and forge intents. Key-to-user binding must be anchored externally — not derived from the platform itself. Two distribution models are available (which can vary by tenant or user):

1. **Platform distribution through a "JWT binding bundle."** This binds a public key entry to a user's own JWT, self-signed, tracing trust back to an IdP rather than the platform, while retaining the platform as the distribution mechanism. IMPORTANT: This requires IdP support to bind the token to the user's key, such as through proof of posession and the cnf claim, through RaR authorization_details, or some other mechanism.
2. **External key source** such as GitHub or GitLab APIs (which both offer unauthenticated key endpoints for retrieving public key material per user). As in IdP trust / JWKS, these external sources must be configured with the appropriate authority and validated at the delivery agent.

TODO: Can different key registries be pluggable? The high level API is the same (validate this signature for this user) but the implementations, at least in these two cases, are quite different.

#### Platform distribution

At "registration time" (login, or at auto-detected intervals by the user agent), the user creates a self-certifying bundle:

1. User generates key pair (or uses an existing one)
2. User authenticates to their IdP with their key (whether through PoP or resource authorization) → gets an access JWT with a claim referring back to their key
    - This is what requires advanced IdP support, although more and more IdPs are supporting PoP.
    - This should ideally be a separate audience than normal management API calls
3. User signs `{public_key, jwt.sub, jwt.iss, timestamp}` with the new private key (proof of possession)
4. The bundle `{key_binding_doc, key_binding_signature, jwt}` is stored on the platform

The delivery agent verifies the bundle independently:

- JWT signature valid against tenant IdP JWKS → the IdP vouched for this user's identity at registration time
- `jwt.sub` matches the claimed user identity → the key is for this user
- The JWT's key reference matches the key being used → proof of possession for the bearer token
- Key binding is signed by the corresponding private key → proof of possession for the bundle
- The intent is signed by the same key → continuity

A compromised platform can't swap the key because it would need a JWT with _both_ `sub=alice@tenant.com` and a binding to _that_ key, to create a valid binding for Alice. It can't get that without authenticating as Alice to the tenant's IdP. The IdP is the trust anchor for key binding.

**Key binding TTL and renewal:** Key bindings have a TTL (e.g., 30-90 days). Before expiry, the client auto-renews by creating a fully fresh bundle: authenticate to IdP → get new JWT (signed with current IdP key) → re-sign the key binding doc with updated timestamp (fresh proof of possession) → replace the old bundle. Everything in the renewed bundle is from the same point in time. Automatable if the client has a session or refresh token. Provides a natural revocation boundary: even if a key is compromised, the binding expires within the TTL.

**IdP key rotation:** In the case of platform-stored keys (with key binding bundle), there is a security vs availability tension. When JWKS changes, all keys need to be resigned. Keeping a JWKS history (caching old IdP signing keys) however undermines, to some extent, the purpose of key rotation — you rotate keys to limit the blast radius of compromise, and keeping old keys trusted defeats that. On the other hand, routine rotation becomes regular toil for those authorizing deployments, when a lack of authorization means provisioning is unavailable.

We should consider independent TTLs for the key bundles/JWKS so we can reduce the risk that routine rotation causes mass toil or availability issues. Key binding bundles must be renewed before the signing key leaves the history of stored JWKSs.

Another option is to consider a "Transparency log / Rekor-style append-only log" which can be used for a trusted source of history.

Because of this, the right choice **may** be to use an external key registry. TBD.

**Emergency rotation (compromise response):** IdP immediately removes old key. All key bindings signed with that key become unverifiable unless we keep history. Affected deployments enter PausedAuth. This is correct behavior — if the IdP rotated due to compromise, old key bindings should be invalidated. JWKS history  undermines this, in order to reduce toil as part of regular rotation. If we do keep history, an administrative "do not trust this kid" would be required.

**UX softening for rotation events:** The platform watches the IdP's JWKS. When it detects a rotation, it proactively notifies users whose key bindings were signed with the rotating key. If CIBA is available, the platform can push out-of-band re-authentication requests. An interactive user agent (web UI, CLI) can do an automatic renewal with the user already present. Inactive users whose key bindings expire enter PausedAuth on their next interaction — standard flow.

TODO: Look at how this interacts with "root" accounts

#### External distribution

**GitOps key registry:** With the unified signing model, GitOps uses the same key binding bundle as web/CLI — the primary verification mechanism is identical across all surfaces. The git hosting platform's public key endpoints (GitHub `GET /users/:username/ssh_signing_keys`, `GET /users/:username/gpg_keys`; GitLab `GET /users/:id/keys`) remain available as an additional or fallback verification source, fetched directly by the delivery agent (not through the platform). These endpoints are unauthenticated and function like JWKS endpoints.

If the external key registry cannot be reached directly, we could consider a proxy deployed in secure network (not reachable externally, unlike the management server) that bridges verifiers to these registries.

### Multi-signature for high availability and audit

A deployment can carry multiple signatures over the same content from different authorized users. The delivery agent accepts the intent if at least one signature is verifiable against a valid key binding. Benefits:

- **High availability:** if one signer's key binding expires (inactive user, missed renewal), the deployment continues as long as another signer's binding is still valid. Reduces PausedAuth events for critical deployments.
- **Audit:** multiple signatures cryptographically demonstrate review and acceptance from multiple humans in the loop — useful for compliance and change management audit trails.

Reactive re-signing (any authorized user re-signs the current content when a key binding is approaching expiry) already falls out of the existing "updates by different users" model. Multi-signature adds proactive redundancy on top: signatures are collected at creation/update time, not just when someone re-signs later.

### Updates, derivation chains, and anti-replay

Deployments can be updated in two ways:

**Direct re-signing (the common case).** An authorized user modifies the deployment and signs the new state directly as a fresh `SignedInput`. The delivery agent verifies the new signature. This is simple and sufficient when the updating user can sign the full deployment content.

**Derivation chains (when direct re-signing is impractical).** When updates are produced by automated processes — upgrade planners, fleet-wide patch addons — the updater may not be the original signer, and the update may apply to many deployments at once. In this case, the update is modeled as a `DerivedInput`: a new deployment version reconstructed from a prior input and a verified update attestation. See `DerivedInput` in `poc/attestation/hybrid/model.py` and `poc/attestation/hybrid/mutation.py` for the derivation and constraint inheritance logic; the fleet upgrade scenario in `poc/attestation/hybrid/test_delivery.py` is the most representative end-to-end test.

A `DerivedInput` references:

- A prior input (the previous version — itself a `SignedInput` or another `DerivedInput`)
- An update attestation whose verified output carries a signed `spec_update`

The `spec_update` contains:

- A `derive_input_expression` (CEL) that transforms the prior input's content into the new version
- Optional `preconditions` (CEL) that must hold on the prior content for the update to apply
- Optional additional `output_constraints` layered on top of inherited constraints

The delivery agent verifies the entire chain recursively:

1. Verify the prior input (back to the root `SignedInput`)
2. Verify the update attestation (including its own signer, trust anchor, and constraints)
3. Check preconditions against the prior content
4. Apply the derivation expression to produce the new deployment content
5. Confirm the derivation didn't rewrite deployment identity

**Constraint scope.** Explicit output constraints on an input bind that input's immediate output — they are not accumulated across the chain. Each attestation in the chain is independently verified: the prior's constraints govern the prior's output, the update's constraints govern the update's output (the spec_update), and the final delivery's constraints come from strategy-implied constraints (derived from the final computed content) plus any explicit constraints the update adds for the final layer. A top-level constraint might not have any idea what the bottom of the chain looks like, and that's fine — trust flows through the chain because each link is independently verified. Strategy-implied constraints are always derived late from the final computed content, so if the update changes the strategy, the new strategy's constraints apply.

> **Note:** The current POC (`poc/attestation/hybrid/mutation.py:derive_constraints`) accumulates prior constraints forward unconditionally. This is a known divergence from the intended per-layer model and should be revisited.

**Update attestations are themselves deployments.** An update attestation has its own input, strategies, and signer. Its placement strategy naturally gates which deployments the update applies to — the update is verified with the target deployment as the placement target. This means an upgrade planner addon can sign which deployment IDs a patch is allowed to touch.

**Anti-replay.** A compromised platform can't forge a user signature (no user signing key). Residual attacks:

- **Replay:** present an old signature. Defense: `expected_generation` in the signed content. The delivery agent tracks a monotonic generation per deployment and rejects operations where `expected_generation != local_generation + 1`. Generation numbers increment through derivation chains.
- **Withholding:** refuse to deliver a validly signed operation (DoS). Observable — the user sees their deployment isn't progressing.
- **Misdirection:** deliver a legitimately signed operation to the wrong target. Defense: the signed content includes target scope (deployment_id, placement strategy); the delivery agent checks consistency.

### Placement enforcement and removal protection

If a compromised platform can trigger removal of all resources (by manipulating placement or sending unsigned deletions), the signing model hasn't bought much. The delivery agent must be able to independently verify that any placement or removal action is legitimate.

Removal is a first-class delivery action (`RemoveByDeploymentId` in `poc/attestation/hybrid/model.py`), verified through the same constraint system as puts. The placement and removal scenarios are tested in `poc/attestation/hybrid/test_delivery.py`. The signed input's placement strategy determines when removal is allowed — this is an emergent property of the strategy, not a separate flag:

- **Predicate placement**: the strategy-implied constraint allows removal only when the predicate does *not* match the target. If the target still matches, removal is rejected. This means a deployment "sticks" to targets that match its predicate — the platform cannot remove it by simply asserting removal.
- **Addon placement**: the strategy-implied constraint allows removal only when the target is *not* in the signed placement evidence's target list. The placement evidence must be signed by the named addon, and its `deployment_id` must match the attested deployment, preventing cross-deployment replay.

In both cases, the `deployment_id` in the removal action must match the signed input's `deployment_id`, preventing confused-deputy attacks.

The same core principle of external trust anchors applies here along several dimensions:

**1. Self-assessed placement (predicate strategy).** The signed intent includes a CEL predicate (e.g., label selectors). The delivery agent evaluates the predicate against its own locally-trusted identity. No platform involvement — the agent decides for itself whether it matches. For pool-based placement, pool membership can be derived from cluster labels (self-assessable) or explicitly assigned with admin-signed provenance.

**2. External placement authority (addon strategy).** When placement decisions come from outside the platform (scoring addons, external capacity services), they carry their own signing authority as `PlacementEvidence`. The delivery agent verifies the evidence independently: the signature is valid against the named trust anchor, the `deployment_id` binds the evidence to this specific deployment, and the current target is consistent with the signed target list. The platform consumes these decisions but doesn't control them at the enforcement point.

**3. Change provenance for placement-affecting state.** When the platform delivers a removal or rescheduling decision, the triggering state change (label removal, pool membership update, etc.) is itself a signed action — the same signing model applies to placement-affecting operations as to deployment intents. The delivery agent verifies: the state change was signed by an authorized user, the change means the placement constraints no longer match, therefore removal is legitimate.

**4. Sticky deployments.** Pool-based placements (e.g., hosted control planes on management clusters) can use placement strategies that inherently resist removal. With predicate placement, the deployment stays as long as the target matches. With addon placement, the deployment stays as long as the signed evidence includes this target. Explicit signed deletion requires a separate signed attestation. Provider draining a management cluster is an elevated lifecycle operation with provider-level authorization.

Examples of how these compose:

- *Pool-based HCP placement:* predicate strategy with pool match expression. Agent verifies it matches via its own labels. Removal requires the predicate to stop matching (e.g., the pool label is changed via a signed action) or an explicit signed deletion.
- *Label-based workload placement:* predicate strategy with label selector. When the target's labels change (via a signed label change), the predicate may stop matching, and the strategy-implied constraint then allows removal.
- *Score-based rebalancing:* addon placement strategy. Scoring addon signs placement decisions with updated target lists. Agent verifies the addon's signature independently. Platform routes the decision but can't forge it.

### Provenance attestation protocol and validation

The delivery agent needs to verify that what's being applied matches what the user authorized. The verification protocol is uniform regardless of credential presentation or whether the separation is in time, space, or both. The core data model and verification logic are implemented in `poc/attestation/hybrid/model.py`, with trust store and bundle types in `poc/attestation/hybrid/verify.py`.

Verification proceeds in three phases:

**Phase 1: Input verification.** The delivery agent verifies the signed input — the user's authorization of the deployment.

1. **Signer consistency** — the signature's signer matches the key binding's signer.
2. **Trust anchor verification** — the signer's key is recognized by the claimed trust anchor, and the anchor's constraints (if any) are satisfied. See [Trust anchor constraints](#trust-anchor-constraints).
3. **Key binding verification** — the signing key belongs to this user, anchored to the tenant IdP via the key binding bundle. The delivery agent validates the bundle against the tenant's JWKS, confirms the subject matches the claimed user, and verifies proof of possession.
4. **Signature verification** — the user signed this specific content (cryptographic). The signed envelope includes the deployment content, output constraints, validity bound, and optional expected generation. This simultaneously proves identity and authorization.
5. **Temporal validity** — the operation hasn't expired (`now() <= valid_until`).

For derived inputs (updates), the delivery agent recursively verifies the entire chain back to the root signed input. See [Updates, derivation chains, and anti-replay](#updates-derivation-chains-and-anti-replay).

**Phase 2: Output constraint evaluation.** The delivery agent verifies that the concrete delivery action (put manifests or remove) satisfies all constraints. Constraints come from two sources:

- **Strategy-implied constraints** derived from the signed input's strategy declarations. The signer's choice of strategy determines what verification looks like. See [Strategy-implied constraints](#strategy-implied-constraints).
- **Explicit output constraints** signed by the user as part of the input envelope. These are CEL expressions evaluated at verification time over a context containing `{input, output, target, action, placement}`.

Both sources are combined; all constraints must pass. This replaces a simple "derivation consistency" check with a more general model: the signed input is not just data — it is a compact declaration of how verification must work.

**Phase 3: Generation check.** If the signed input includes an `expected_generation`, the delivery agent checks it against its local fulfillment state for replay protection.

Any check failure transitions the fulfillment to `PausedAuth`.

This is compatible with manifest invalidation. When manifests are re-generated (e.g., config rotation triggers `InvalidateManifests`), the intent hasn't changed — the same signature is still valid. New manifests must still satisfy the same constraints. See "Verification level: intent signing vs. output signing" above for the tradeoffs between signing the intent vs. the output.

**Attestation structure:**

An attestation pairs a signed input (the authorization) with a concrete delivery action (the output):

```
Attestation:
    attestation_id: string
    input: SignedInput | DerivedInput
    output: PutManifests | RemoveByDeploymentId

SignedInput:
    content: InputContent             // polymorphic: DeploymentContent, ManagedResourceContent, etc.
    signature: Signature              // signer_id, public_key, content_hash, signature_bytes
    key_binding: KeyBinding           // signer_id, public_key, trust_anchor_id, binding_proof
    valid_until: timestamp
    output_constraints: [Constraint]  // signed CEL predicates over {input, output, target, action, placement}
    expected_generation: int?         // optional replay protection

PutManifests:
    manifests: [ManifestEnvelope]     // ordered typed payloads (resource_type, content)
    signature: OutputSignature?       // optional addon signature over manifests
    placement: PlacementEvidence?     // optional signed placement decision

RemoveByDeploymentId:
    deployment_id: string             // must match input's deployment_id
    placement: PlacementEvidence?
```

The signed envelope (what the user actually signs) includes the deployment content, output constraints, validity bound, and optional expected generation. This means the signer authorizes not just the deployment spec but also the rules the eventual output must satisfy.

**Validation:**

```
verify_attestation(attestation, target_identity):
    // Phase 1: input verification
    verified_input = verify_input(attestation.input)

    // Phase 2: output constraint evaluation
    strategy_constraints = derive_strategy_constraints(verified_input.content)
    all_constraints = strategy_constraints + verified_input.output_constraints
    cel_context = {input, output, target: target_identity, action, placement}
    for constraint in all_constraints:
        assert constraint.evaluate(cel_context)

    // Phase 3: generation check
    if verified_input.expected_generation != null:
        assert verified_input.expected_generation == local_state.generation + 1

    // any assertion failure → PausedAuth

verify_input(signed_input: SignedInput):
    assert signed_input.signature.signer_id == signed_input.key_binding.signer_id
    anchor = trust_store.get(signed_input.key_binding.trust_anchor_id)
    assert anchor.verify_signer(signer_id, public_key, subject)
    assert key_binding_possession_valid(signed_input.key_binding)
    assert signed_input.signature.public_key == signed_input.key_binding.public_key
    envelope = {content, output_constraints, valid_until, expected_generation}
    assert signed_input.signature.content_hash == hash(envelope)
    assert signature_valid(signed_input.signature, hash(envelope))
    assert now() <= signed_input.valid_until

verify_input(derived_input: DerivedInput):
    // recursive — see "Updates, derivation chains, and anti-replay"
    verified_prior = verify_input(prior_input)
    verified_update = verify_attestation(update_attestation, target={id: deployment_id})
    derived_content = apply_update(verified_prior.content, verified_update.spec_update)
    derived_constraints = update.output_constraints  // per-layer: update governs the final output
```

The platform signature (not shown above) is optional for standard (gRPC) transport — the user's signature provides the integrity guarantee. For buffer transport, the platform signature remains valuable since there is no connection-level auth.

#### Strategy-implied constraints

The signed input declares strategy types for manifests and placement (e.g., `inline`, `addon`, `predicate`). The delivery agent derives verification constraints from these strategy declarations. This is the mechanism that determines what "consistent with the signed intent" actually means — the signer's choice of strategy controls what verification looks like.

The delivery agent has code for each known strategy type that produces constraints. This code is auditable and as trustworthy as the delivery agent binary itself. An attacker would need to replace the binary, not just modify a config. Unknown strategy types fail closed (verification rejects). See `poc/attestation/hybrid/policy.py` for the constraint derivation logic.

**Manifest strategies:**

| Strategy | Implied constraint |
|---|---|
| `inline` | Output manifests must exactly match the manifests in the signed input. |
| `addon` | Output manifests must be signed by the named addon via the named trust anchor. |

**Placement strategies:**

| Strategy | Implied constraints |
|---|---|
| `predicate` | Put is allowed only when the CEL predicate matches the target. Removal is allowed only when the predicate does *not* match. |
| `addon` | Placement evidence must be signed by the named addon. The action (put/remove) must be consistent with the signed target list. |

Strategy-implied constraints and explicit output constraints (signed CEL expressions) are combined and all must pass. This means the signer can layer additional restrictions on top of the strategy-implied ones (e.g., namespace restrictions, batch size limits on placement targets).

#### Addon-generated manifests and the co-signing model

For addon-driven manifest strategies, the derivation is opaque: the user signs "I want observability," and the addon produces Prometheus agents, Thanos components, ConfigMaps, and Secrets. The delivery agent has no structural basis for matching output to input because the intent is structurally divorced from the output.

The solution is the **addon strategy type**: the signed input names the addon authorized to generate manifests and the trust anchor that validates the addon's identity. The strategy-implied constraint then requires:

1. The user authorized this addon for this deployment (user signature over the strategy spec, unchanged).
2. This addon produced these specific manifests (addon signature over the output).
3. The addon identity matches what the user authorized (signer ID and trust anchor ID in the output signature must match the strategy declaration).

Without the intent naming the addon, a rogue addon could sign manifests and attach them to any deployment. The user's intent is what ties authorization to the addon's output.

**Structural schema as defense-in-depth.** Addon registrations can include expected structural properties of the output: allowed GVKs, namespace patterns, label requirements. The delivery agent checks these as a second gate on top of the addon signature. This catches compromised addons (key theft) producing valid signatures on malicious manifests. The schema need not be exhaustive — just enough to catch obviously wrong output (e.g., "observability manifests should not contain ClusterRoleBindings granting `cluster-admin`"). Structural schemas are optional; some addons with highly variable output may omit them.

##### Addon key lifecycle

The addon needs a signing key pair. In practice, X.509 workload identity is the most deployable model for addons: the issuer binds addon identity to the public key, the private key stays local to the addon, and the delivery agent verifies signatures against a trusted certificate chain. The platform may courier signed output and leaf certificates, but it is not the identity authority.

Four models are relevant:

**SPIFFE/SPIRE (preferred when available).** The addon gets an X.509 SVID from its local SPIRE agent. It signs manifests directly with the SVID's private key. The delivery agent verifies the signature against the SPIFFE trust bundle for the addon's trust domain. The X.509 certificate IS the identity-to-key binding — no separate key binding ceremony needed. SPIRE handles rotation automatically (short-lived SVIDs, auto-renewed). Trust bundle rotation is a first-class SPIRE concern. The admin-signed addon registration includes the SPIFFE trust domain and trust bundle endpoint. Each addon gets its own SPIFFE ID (e.g., `spiffe://fleet-addons/mco-observability`), and the delivery agent enforces that only the expected identity signed the manifests.

**Kubernetes CSR API / local workload CA.** The addon generates a key pair locally and submits a `CertificateSigningRequest` to a signer trusted for addon identities. The signer issues a short-lived X.509 certificate whose identity (typically a URI SAN) names the addon. The addon signs manifests with the same private key, and the delivery agent verifies the signature against the certificate and the trusted CA bundle. The CSR API is the plumbing, not the trust policy: the signer/approver policy defines what identities may be claimed. Public key trust is rooted in the issuer's CA bundle or root fingerprint, not in the CSR object itself. For FleetShift verifiers, the practical default is to pin that CA material by value in addon registration; leaf certificates can then rotate freely under the same CA. If automatic trust-anchor updates are required, the same CA bundle can instead be published at a verifier-reachable location by reference.

**cert-manager workload certificates (practical Kubernetes option without SPIRE).** cert-manager can implement the CSR pattern without requiring SPIRE. An addon-local controller or CSI driver generates the private key locally and creates a `CertificateRequest` to a cert-manager `Issuer` or `ClusterIssuer`. The resulting certificate is used directly for manifest signing. If multiple addon identities share an issuer or CA, issuance should be constrained by an approver or equivalent CA partitioning so one addon cannot mint another addon's identity. If the addon cluster/CA itself is the trusted addon authority, such constraints are defense in depth rather than the primary trust boundary. Public key trust follows the issuer CA rather than each addon's leaf key: cert-manager handles issuance only. As with the raw CSR API, the practical default is to pin the issuer CA bundle or root fingerprint by value in addon registration. If automatic trust-anchor updates are required, the CA bundle can instead be published at a verifier-reachable location by reference. Tools like trust-manager help publish that CA in-cluster, but they do not by themselves solve external trust distribution.

**JWT key binding bundle (completeness; likely impractical in practice).** In principle the addon could bind its signing key to a workload identity JWT, similar to the user key binding bundle. In practice this requires the workload identity issuer to mint a token that is itself bound to the addon's signing key (for example `cnf`, `jkt`, or an equivalent key confirmation claim). Mainstream Kubernetes/cloud workload identity systems generally bind tokens to the workload subject, audience, or role, but not to an arbitrary application signing key. Without that key confirmation, a platform that can read the stored JWT can replace the bundle with a different key. This model is included for completeness, but is unlikely to be the default recommendation.

**Admin-provisioned key (fallback).** When the addon runs in an environment without workload identity or local certificate issuance (bare metal, air-gapped), the admin registers the addon's public key or CA as part of a signed administrative action. Rotation is manual unless the registration later authorizes a stronger identity source (SPIFFE trust domain, local CA, etc.).

##### Addon key distribution to the delivery agent

For certificate-based addon identities, the delivery agent needs the addon's leaf certificate plus a trusted CA bundle or trust domain. The raw workload identity token does not need to leave the addon cluster. External trust distribution is therefore a separate concern from local certificate issuance.

Two approaches:

**By value (pinned CA bundle or root fingerprint).** The addon registration includes the CA bundle, intermediate set, or root fingerprint directly. The delivery agent caches or pins it. This is the simplest practical model because it requires no custom external endpoint: leaf certificates can rotate freely under the pinned CA, and only CA rotation requires an overlap window or administrative re-registration.

**By reference (trust domain or CA bundle publication point).** The addon registration includes a SPIFFE trust domain, static HTTPS bundle URL, object storage location, git-hosted file, OCI artifact, or other verifier-reachable trust source. The delivery agent resolves the current trust bundle at verification time. This works when the publication point is reachable from the delivery agent and supports automatic trust-anchor updates. A custom endpoint is optional, not required.

For by-value distribution, rotation is handled through overlap or re-registration: old and new CA material must both be trusted during the rotation window, or an admin must explicitly update the pinned trust bundle. If the overlap window is missed, deployments enter PausedAuth until trust is re-established — the standard fail-safe degradation. In-cluster tools such as trust-manager can help publish the CA bundle to local consumers, but FleetShift verifiers outside the addon cluster still need either by-value pinning or a separate external publication point.

##### Network topology reinforcement

When addons run behind the provider/consumer network curtain (on factory clusters with restricted fleetlet profiles), the network topology reinforces the cryptographic model. The platform cannot reach the addon's cluster — the fleetlet connection is outbound-only, the protocol channel is structurally absent. The platform cannot request tokens on the addon's behalf, cannot access the addon's signing key, and cannot interact with the local SPIRE agent, certificate issuer, or K8s token infrastructure. The signing key stays in the addon's security domain by network enforcement. The only thing that crosses the curtain is the addon's signed output, carried through the fleetlet delivery channel.

This means a compromised platform can invoke the addon (it can send messages through the delivery channel) but cannot impersonate it. The curtain makes the channel unidirectional for trust: signed artifacts flow out, but identity material cannot be injected in.

### Open questions

- For the CIBA flow: how does CI authenticate to initiate the CIBA flow? It needs its own client credentials with the IdP, which is itself a stored secret. This is a narrow, well-scoped secret (can only initiate approval requests, can't issue tokens without user consent), but it exists.
- Claims freshness with user signing: the signed intent embeds claims from signing time. If the user's permissions change after signing, the claims are stale. How should the system handle this — re-check via SCIM/CAEP, key binding TTL as a natural bound, or require re-signing on permission changes?
- Anti-replay mechanism details: the POC implements `expected_generation` as a monotonic counter. How does the delivery agent handle gaps (e.g., a skipped generation due to a failed delivery)?
- Secure bootstrap of cluster-side label/identity state for placement enforcement. How does a cluster initially receive its labels through a non-platform authority?
- Trust model for scoring addons and external scoring services. How does a delivery agent know to trust a particular scoring addon's signatures? (Partially addressed by the addon key lifecycle section above — the same signing and registration model applies to scoring addons, placement addons, and manifest-generating addons.)
- Addon signing: for addons with highly dynamic output (e.g., per-target customization that varies with target state), what is the right granularity for structural schemas? Per-addon-version? Per-deployment? Or purely optional?
- Trust bundle rotation: what is the acceptable overlap window for addon CA rotation? Should the platform enforce a minimum overlap duration as a prerequisite for addon registration with by-value trust bundles?
- Multi-signature policy: when to require vs. allow multiple signers, quorum rules for critical deployments.
- Can different key registries be pluggable? The high-level API is the same (validate this signature for this user) but implementations differ (key binding bundles, git hosting platform endpoints, etc.).
- Derivation chain depth: should there be a maximum chain depth to bound verification cost? What happens when a long chain makes verification expensive?
- Real OIDC key binding: the POC models key binding as a simple proof-of-possession over a raw public key. The real model needs the JWT binding bundle described above. What are the implications for the verification bundle size and delivery agent complexity?

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

- Audience scoping – if we want to scope tokens to particular clusters, we need separate audiences for those. More IdP configuration to do. Hard to make dynamic. Token Exchange (RFC 8693) can address this: exchange a platform-audience token for a target-audience token at the IdP. The IdP controls policy (which exchanges are allowed, for which audiences). This avoids per-cluster client IDs but requires IdP support (Keycloak, Dex have it; Auth0/Okta partial).
- Root user – there should be some non-IdP issued credential or out of band channel for configuring IdP trust. If your IdP is down or compromised or you messed up the configuration and you need to reconfigure, you need some escape hatch.
- Trust anchor distribution – this might be solved but it is tricky to think through end to end. If you are trying to avoid privileged service accounts, then you also need to be very careful about how trust is established to tenant-level roots itself. If a compromise can reconfigure all of those, then all of the end to end verification is not helping there.
- BMC credentials are unavoidable – maybe they can only be retrieved with a user token
- Key rotation (as discussed) is difficult to balance security, availability, and UX (toil). The right choice may be an external public key store.

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

Delivery transport is configurable per target profile: standard (fleetlet gRPC) or hardened (buffered via S3/Kafka/NATS). The attestation format and validation logic are identical across transports.

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

