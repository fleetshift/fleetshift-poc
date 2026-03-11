# Security model

Principles:

- Minimize built-in trust of platform components. Some trust is unavoidable (bootstrapping, token vending), but it should follow an auditable least-privilege model. No god-mode service accounts or keys.
- End to end user identity everywhere – auditable, no confused deputy
- The tenant's IdP is the root trust anchor. The platform is a consumer of tenant trust, never an authority over it. Compromising the platform must not be sufficient to forge identity or redirect trust.

## Target credential model

The delivery target plugin gets a say in what credential presentation it should get. So if its a k8s agent, it needs the user's ID token. etc.

How does this work for the service account delegation discussed later? The delivery agent in this case knows to call the token request. Its input is only identity information about the user.

Other reasonable contracts could be "give me the access token" or "I need an access token for X" (so we try to token exchange if we can, for example).

For APIs that leverage federation, the delivery agent handles:

- AWS: Ask for ID token or SAML assertion. AssumeRoleWith*Identity -> sigv4
- GCP: Ask for ID token, token exchange -> GCP token
- etc...

As a fall back these agents could get vault credentials for a service account perhaps, but we want to work off the end user.

## Doable

- We can definitely use the ID token end to end, assuming a common IdP trust and reused client IDs across clusters and the platform. This works for synchronous / short run operations, limited by token lifespan.
- We can definitely query inventory and do platform-side operations securely
- We can sync RBAC on the platform side to kubernetes, assuming we use the user's identity to establish RBAC in the managed cluster. This requires we bootstrap new clusters with the right privileges.
- We can run deployments for as long as we have a token. We can pause deployments waiting for reapproval.

## Things you could do if you can customize the target (e.g. kube distro)

- Validate access tokens instead of ID tokens
- Take into account access token scope (or beyond, resource identifiers, etc)

## Challenges

- Git ops – GitOps has a platform level indirection: the git repo is the authority, and the platform applies from there. Some tools may support tenant-specific service accounts or impersonation.
- Audience scoping – if we want to scope tokens to particular clusters, we need separate audiences for those. More IdP configuration to do. Hard to make dynamic. Token Exchange (RFC 8693) can address this: exchange a platform-audience token for a target-audience token at the IdP. The IdP controls policy (which exchanges are allowed, for which audiences). This avoids per-cluster client IDs but requires IdP support (Keycloak, Dex have it; Auth0/Okta partial).
- Reconciliation – this is similar to the gitops challenge.
- Permission tracking – when a delegation service account's RBAC should track the creating user's permissions over time.
- Root user – there should be some non-IdP issued credential or out of band channel for configuring IdP trust. If your IdP is down or compromised or you messed up the configuration and you need to reconfigure, you need some escape hatch.
- Trust anchor distribution – this might be solved but it is tricky to think through end to end. If you are trying to avoid privileged service accounts, then you also need to be very careful about how trust is established to tenant-level roots itself. If a compromise can reconfigure all of those, then all of the end to end verification is not helping there.
- BMC credentials are unavoidable – maybe they can only be retrieved with a user token

## Bootstrapping targets

When targets (e.g. clusters) are bootstrapped we may necessarily have elevated privileges at that point for that target (e.g. a kubeconfig or a privileged user). Under that identity we assume we can bootstrap other configs like RBAC syncing. These could perhaps be their own deployments or part of the cluster deployment itself.

For the delegation SA model, bootstrapping also provisions the platform's own identity in the cluster. Its service account may get tight impersonation permissions (to impersonate delegate SAs). This is the one piece of unavoidable platform trust, but it's scoped and auditable.

Critically, bootstrapping must not give the platform **ongoing** authority over IdP trust configuration at the target. Elevated privileges during bootstrap are acceptable because they're time-bounded and observable. But if the platform retains the ability to reconfigure which IdP the target trusts, then a platform compromise can redirect trust and forge identity — defeating all downstream verification. The platform's runtime credentials at a target should be scoped to workload operations, not authentication configuration.

## Distributing trust anchors

Anything that verifies credentials has to have a trust root. Configuring this trust root over time must itself require something that ties back to that trust root. So if you are using OIDC, you can only change the issuer if you have OIDC credentials from the current issuer.

### Why the tenant IdP is the right root

The tenant's IdP issuer URL is the irreducible trust anchor. Every other trust relationship (signing keys, delegation SAs, platform credentials) derives from it. It's the right root because:

- It's not new trust. The tenant already trusts their IdP for everything else. We're building on an existing relationship, not introducing a new one.
- It's tenant-controlled. The tenant manages user lifecycle, MFA policy, group membership, key rotation. A platform-operated root (like a signing CA) would be new trust the tenant has to accept from the platform operator.
- Compromise is tenant-scoped. If tenant T's IdP is compromised, only tenant T is affected. A platform-level root (CA, signing service) has cross-tenant blast radius.

### OIDC discovery as the distribution mechanism

From a single issuer URL, everything else is derivable via standard OIDC discovery: JWKS (signing keys), endpoints, key rotation — all automatic, no platform involvement. Verifiers poll JWKS on their own schedule. The platform is not in this path.

### Changing trust configuration

Admin operations that affect trust (new verifiers, audience changes, etc.) are standard OIDC-authenticated actions. The admin authenticates via their tenant IdP, gets a standard ID token, and the system verifies that token before applying any change. The admin's ID token IS the proof that chains back to the current trust anchor. No custom token types or non-standard IdP features needed.

The platform can transport trust configuration changes (it's a courier) but cannot author them. Every change requires a credential from the tenant's IdP. The platform's own credentials are never sufficient to modify trust configuration.

### Trust establishment at the target

For cloud-managed clusters (EKS, GKE, AKS): IdP trust is configured via the cloud provider's API, protected by the tenant's cloud IAM. The platform should not have IAM permissions to modify cluster authentication settings — this is naturally separable from deployment-level permissions.

For self-managed clusters: how IdP trust reaches the target is TBD. The key constraint is that the platform must not be the authority for IdP trust configuration on the target — however provisioning works, it must chain back to the tenant's trust root independently of the platform.

### Residual risk

A compromised platform cannot subvert IdP trust on existing targets — the trust is already established and the platform has no write access to it. Only new targets during a compromise window are at risk, and only if the platform is in the trust establishment path for those targets. For cloud-managed clusters this risk is eliminated by IAM separation.

## Durable user authorization

The platform frequently acts as an intermediary between a user and a target where the user isn't making the API call directly. This is a problem in both time and space:

- **Time**: long-running rollouts outlive the user's token. The authorization must persist beyond the token's validity window.
- **Space**: in provider delivery, the authorization must cross a trust boundary the user doesn't span directly. The user is behind the curtain with no direct authority at the factory cluster. See provider_consumer_model.md for the full provider/consumer/factory topology.

Both require the platform to carry proof of the user's intent to a place or moment where the user can't present it themselves. The mechanisms below apply to both — though some (token passthrough, delegation SAs) only work when the user has direct authority at the target (no separation or intermediary in "space"), while the JWT-embedded provenance chain and signed intent models work across both dimensions.

### Token passthrough (synchronous baseline)

The simplest model: the user's bearer token is passed through to the target. Full end-to-end user identity. Works while the token lives. Not sure if we can avoid storing this or if we can use workflow affinity to try and just use a token in memory.

When the token expires mid-rollout, or on workflow replay, the deployment transitions to PausedAuth and waits for an authorized user to resume it with a fresh token. Any authorized user can resume – this is approval-gate semantics for free.

PausedAuth is the universal fallback for all credential models: whenever credentials are insufficient, the deployment pauses rather than failing. CIBA (Client-Initiated Backchannel Authentication) composes naturally with PausedAuth: instead of passively waiting for a user to show up, the system actively prompts the user for re-approval on a separate device. PausedAuth is the state ("we need credentials"), CIBA is the mechanism ("reach out to the user").

### Accepted initial authorization with ongoing checks

The JWT from the initial request establishes who authorized the operation and when. For long-running operations, rather than requiring a live token throughout, the system can accept this initial authorization and supplement it with ongoing checks:

- Honor a user-specified validity bound in the initial request ("this deployment is valid for N hours").
- Re-check permissions when invalidation or other signals arrive — against synced RBAC or the IdP, not the expired JWT.
- Track user status and permission changes over time (via SCIM/CAEP/SSF) and react accordingly — restricting, pausing, or revoking the operation.

This is the weakest credential model (the JWT is stale), but it's practical for operations where the user is known, the permissions are checkable independently, and the risk of a stale authorization is bounded by the validity limit. Falls back to PausedAuth/CIBA when a check fails.

This is the same fundamental tradeoff Kubernetes users already accept: `kubectl apply` authorizes at request time, the CR lives on, and the controller reconciles it with a service account — no ongoing verification of the original user's permissions. FleetShift's model is strictly better: the operation carries cryptographic proof of who authorized it (the embedded JWT), there is no god-mode controller service account, and the platform can still do ongoing permission checks and react to changes. We're not asking users to accept a new tradeoff — we're giving them one they already accept, minus the worst parts, with no stored secrets.

### Service accounts specifically for delegation

When something is long running, the user creates a service account dedicated to run on their behalf, with a scoped subset of their permissions.

The provisioning flow is synchronous (while the user is present):

1. User creates a deployment targeting cluster X
2. The platform, using the user's own token, creates a ServiceAccount + Role + RoleBinding in the target cluster
3. K8s prevents privilege escalation: the RBAC API rejects RoleBinding creation if the user doesn't hold the permissions being bound. The user can only delegate authority they actually have.
4. User's token is discarded after provisioning. Never stored.

The platform then impersonates the service account using its service account identity. This is a small improvement over TokenRequest:

- Impersonation is auditable; token request looks indistinguishable from any other actor with the service account
- There is no additional token that can be used for anything else; that needs to expire, etc.

Ideally: 

- Something expires these over time
- When the user's permissions restricts to less than their shadow service accounts, it automatically restricts the permissions of those service accounts

You could also "just" create specific service accounts to run workloads that you wanted long-running, with strict permissions. If they ever tried to escape that, the deployment pauses for approval.

Trade-offs:

- The target sees the service account identity, not the user. User identity is in the platform's audit log, correlatable via SA naming/annotations but not cryptographically bound.
- Permission drift: if the creating user loses access, the SA retains its grants until explicitly reconciled. We may be able to eagerly cascade permission changes done by the platform to SAs associated with the user.
- K8s-specific pattern. Other targets need equivalents (IAM AssumeRole for AWS, Managed Identity for Azure, etc).

### Refresh tokens

These are credentials and tough to store. This is an alternative path to delegation SAs, not a complement. It preserves end-to-end user identity at the target (the refreshed token IS the user's token), but requires advanced IdP features.

Ideally you'd:

- Sender constrain them (DPoP, RFC 9449). This makes the platform privileged but only its protected private key. Leaked credentials are not a problem. Sender constrained refresh tokens have some support. It would require the backend to be a confidential client and not the frontend. That can complicate CLI integration. Maybe you only approve these long lived flows through the browser, though. It's a few-time operation.
- Scope them. This can be hard because it requires more IdP configuration e.g. client per cluster which could be awful without automation. And automating that is itself difficult to set up (dynamic client registration / aud configuration). Plus you'd want token exchange of some kind or the original aud needs to include every cluster.

Refresh tokens shine when: (a) the IdP supports sender constraints and flexible token exchange (rare in practice), and (b) the targets work well with proper OAuth (access tokens, transaction tokens). For K8s with OIDC auth, delegation SAs are simpler and avoid the stored-secret problem entirely.

#### Run-as-you vs. run-as-platform

Refresh tokens and accepted initial authorization may not be a strict upgrade path (one replacing the other). They could be coexisting credential modes with different trust/convenience tradeoffs:

- **Run-as-you (refresh token):** The platform holds a refresh token and gets fresh tokens as the user. Operations at the target see the user's live identity. Naturally tracks IdP policy over time (user disabled → refresh fails, client permissions revoked → response reflects it). Requires a stored secret. Stronger ongoing guarantees, but higher trust in the platform.
- **Run-as-platform (initial authorization):** The platform acts under its own identity, carrying the user's initial JWT as proof of authorization. No stored secret beyond the short-lived JWT. Weaker ongoing guarantees (stale authorization), but no long-lived credential exposure.

The choice could be user-driven ("I want this deployment to run as me") or policy-driven — the platform evaluates what the user is allowed to do (which resources, which contexts) and determines the available modes. Which modes a user can select is itself an authorization decision at the platform layer.

TODO: The exact policy model is uncertain. The key insight is that these modes have different security properties and operational tradeoffs, and giving users (or administrators) a choice — gated by privilege — may be better than picking one model for all cases.

### Constrained impersonation

This is conceptually similar to the above, but means the platform directly impersonates the user. The fundamental problem: K8s impersonation lets the impersonator assert group membership, and K8s has no way to verify those assertions. Even with constrained impersonation (limiting which users can be impersonated via `resourceNames`), the impersonator can claim arbitrary groups for that user. If the platform can impersonate group "admins", it can put any user in that group regardless of their actual membership. These are unverifiable claims about a user.

With token passthrough, the IdP is the authority on claims – groups are in the token, cryptographically signed by the IdP. With impersonation, the platform is the authority. This is a fundamentally weaker trust model for any environment where group-based authorization matters.

## IdP orchestration

In various scenarios, we could benefit from specific IdP configuration:

- Per cluster client IDs (audiences)
- Permission-level scoping (assuming you have an authorizer which takes this into account)
- If an IdP can handle the refresh token route... setup for that
- Token exchange (RFC 8693) for audience swapping without per-cluster client IDs
- CAEP/Shared Signals Framework (SSF) for real-time session revocation and permission change events

## Git ops models

### Long lived authority

This assumes we can store something per user like a scoped refresh token. There are many challenges along this path but technically securable with a sufficiently advanced IdP and configuration.

1. Signed commit establishes authn for a change
2. User authorizes server to run changes on their behalf w/ scoped token with particular session limits
3. Applying change runs with user's own identity & applies with their own token

This could have a few models:
- apply runs under an authorized user for the deployment, but the user's identity is used to authorized a change to the deployment
- apply runs under the authorized user of the change, regardless of who originally created the deployment
- apply runs under an authorized user for the deployment, and whether or not the user can edit it is up to git repo <- this is broken

If a change in git is not authorized, what's the feedback loop for that? how do we get back in sync?

One thing that could help matters is if there was a CI check that ran authorization through on the platform level – this could probably catch a lot.

The bigger challenge is securely storing longer lived credentials. See "Refresh tokens" above.

### Signed intent

A more promising model: something cluster-side that validates "signed intents" before applying.

1. A manifest in git is accompanied by a signature and a revision (ideally w/ provenance via hash)
2. The platform delivers attested manifests to the cluster
3. The cluster-side delivery agent validates the attestation (proof material, user identity, authorization), and applies the constituent manifests only if validation succeeds. See the attestation-based delivery section below for the concrete protocol.

The original design considered keyless signing (Fulcio/cosign model), where the user proves OIDC identity to a CA and gets a short-lived certificate binding their identity to an ephemeral signing key. This has a central CA problem (see below). The JWT-embedded provenance chain replaces it.

NOTE: We should revisit this for the case the customer _already has a trusted Fulcio CA_.

The platform's delivery authority is contingent on valid attestations. A compromised platform can deliver attestation envelopes, but without a valid tenant JWT embedded in the envelope, the delivery agent rejects them. The platform can't apply unattested manifests.

#### Signed intent beyond GitOps

Could the deployment itself be the "durable tightly scoped approval" via signing? Two models:

**Eager signing**: generate all manifests upfront, user signs the rendered output, deliver signed artifacts. No provenance chain needed – the signed artifact IS the applied artifact. Clean. But every invalidation requires re-generation, re-review, and re-signing. The user must be present for every invalidation, which is operationally equivalent to PausedAuth. The benefit over PausedAuth is the trust model: cryptographic proof of intent at the target, not just "the platform had a valid token."

**Lazy signing**: user signs the deployment spec, platform generates manifests just-in-time. Invalidation can proceed without the user. But now the platform is in the rendering trust chain – the target must trust that the platform faithfully translated the signed spec into these specific manifests. This requires a provenance chain (spec signature + rendering attestation) and reintroduces platform trust for correctness, though not for identity.

Eager signing is the simpler and more honest model but converges to PausedAuth UX for invalidation. Lazy signing avoids the UX problem but reintroduces trust. Neither is strictly better than delegation SAs for the invalidation case.

Signed intent is most compelling for GitOps (manifests are already in git, already reviewed, signing is natural) and as a trust-model upgrade for environments where cryptographic proof of user intent matters. For interactive long-running deployments, delegation SAs + PausedAuth is the pragmatic choice.

#### Certificate authority problem

The Fulcio/keyless signing model introduces a central CA whose root key, if compromised, can forge certificates for any user for any intent. The transparency log (Rekor) provides detection after the fact but not prevention. This violates the "no god-mode keys" principle — the CA root key is exactly such a key.

We want signing authority to derive from the tenant's own trust infrastructure (their IdP), not from a platform-operated CA. Ideally this requires only standard OIDC support from the IdP.

#### JWT-embedded provenance chain

An alternative to the Fulcio model that uses only standard OIDC + a platform integrity key. Two-factor: the tenant's JWT provides identity/authorization, a platform-owned key provides integrity. Neither alone is sufficient.

The user's JWT is embedded in an attestation envelope alongside the intent, signed by a platform-owned key. The concrete envelope format and validation sequence are defined in the attestation-based delivery section below.

Trust model:

- Compromised platform key alone: can sign manifests but can't produce a valid tenant JWT. Rejected at validation.
- Stolen JWT alone: can present identity but can't sign manifests. Rejected at validation.
- Compromised platform (has key + user's JWT in transit): can create attestations paired with the user's JWT while the JWT is live. Same exposure window as token passthrough, bounded by JWT lifetime.

Compared to Fulcio: a compromised Fulcio CA can forge signatures for any user indefinitely. This model limits forgery to users whose JWTs the platform currently holds, within JWT lifetime. The blast radius is smaller by orders of magnitude.

The residual risk (platform can pair a valid JWT with arbitrary manifest content while the JWT is live) is inherent to any model where the platform sees the user's token. It's the same as token passthrough but with better auditability — the signed manifest input is a persistent, inspectable artifact rather than an ephemeral API call. Unauthorized manifest inputs are detectable after the fact.

The platform key is not a god key — it can only assert integrity, not identity. Its compromise alone cannot authorize anything. It could be scoped per-tenant to further limit blast radius.

Persisting user JWTs to a database (rather than just validating them in-memory per-request) is a deliberate architectural choice. The security question is what happens when the store is compromised. Here, the blast radius is: one user per token, only that user's authorized operations, only within the token's remaining lifetime, and only as one factor of two (the platform signature is also required). Compare to a god-mode service account: any user, any operation, indefinitely, single factor. JWTs should be encrypted at rest and purged after expiry or operation completion.

#### Tightening intent-token binding

The JWT-embedded provenance chain's main gap is that the JWT doesn't bind to specific manifest content — a compromised platform can pair a valid JWT with any manifest while the JWT is live. OAuth standards offer a spectrum of binding tightness:

| Binding level | Standard | What it constrains |
|---|---|---|
| Identity only | OIDC core (ID token) | Who the user is |
| Action category | OAuth scopes | Kind of action (e.g. `deploy`, `deploy:production`) |
| Target | RFC 8707 (Resource Indicators) | Which resource server / cluster accepts the token |
| Intent details | RFC 9396 (Rich Authorization Requests) | Structured authorization details: target, namespace, action type |
| Exact content | RFC 9396 + content hash | Token bound to a specific manifest hash — 1:1 binding |

Rich Authorization Requests (RFC 9396) is the key standard. The `authorization_details` parameter carries structured JSON describing what the token authorizes:

```json
{
  "type": "fleetshift_deploy",
  "target": "cluster-x",
  "namespace": "production",
  "manifest_hash": "sha256:e3b0c44298fc..."
}
```

With `manifest_hash` in `authorization_details`, the token is only valid for this exact manifest. Any change to the manifest invalidates the token. This closes the content-binding gap entirely — the platform can't pair the token with a different manifest because the hash won't match.

RAR is a published RFC (May 2023). IdP support is growing but not yet universal (Keycloak has partial support via custom protocol mappers, full RAR is in progress). The architecture should accommodate the tightest binding the IdP supports and degrade gracefully: check manifest hash if present in `authorization_details`, fall back to scope-level checks, reject or require re-approval if no binding is present.

#### Credential durability for long-running operations

The JWT-embedded provenance chain proves "user X authorized this at time T," but the JWT expires shortly after. For long-running operations, the full set of credential durability mechanisms applies — see the long-running rollouts section above. The JWT-embedded model layers cleanly with any of them: accepted initial authorization with ongoing checks as the default, PausedAuth + CIBA as the fallback, refresh tokens or delegation SAs where appropriate.

#### Intent-bound tokens for GitOps

The tighter the binding between token and content, the safer it is to include a token alongside manifests in git. An unscoped ID token in git is dangerous — it can authorize anything during its validity window. A RAR-scoped access token with `manifest_hash` is safe — it can only authorize the exact manifest it's bound to, and it expires.

Two flows:

**Token before commit (user-driven):** The user's CLI computes `hash(manifest)`, requests an access token from the IdP with `authorization_details` containing the manifest hash (via RAR), and commits the manifest + token together. The gitops controller validates the token against the tenant's IdP JWKS, checks that `authorization_details.manifest_hash` matches the actual manifest, and delivers if valid.

**Approval after commit (CIBA):** The user commits the manifest without a token. CI detects the change, computes the manifest hash, and initiates a CIBA (Client-Initiated Backchannel Authentication, an OIDC extension) flow. The user receives an approval prompt on a separate device showing what they're approving (via CIBA's `binding_message` parameter). On approval, CI receives a RAR-scoped token and attaches it for the gitops controller.

CIBA separates the commit from the approval — natural for gitops where you commit, review in PR, and approve after merge as a separate step. The user doesn't need a token at commit time.

When a token in git expires before the manifest is applied, the controller triggers re-approval (new CIBA flow or equivalent). This is PausedAuth semantics: expired credentials pause rather than fail.

Without full RAR support, standard scopes provide weaker but still useful binding (e.g. `scope=deploy:cluster-x:namespace-production`). Universally supported, much tighter than an unscoped token, but not 1:1 content-bound.

#### Open questions

- Signed intent is viable for K8s (admission webhooks are a natural fit). For other targets, it's a lot to ask – probably K8s-specific.
- TODO: Could the JWT-embedded provenance model extend to the "signed intent beyond GitOps" use case (lazy signing)? The hash chain from generated manifest → manifest input → JWT is essentially the provenance chain that lazy signing requires.
- SubjectAccessReview in the webhook needs the user's groups. ID tokens typically carry `sub` and `iss`, not always groups. The webhook may need to query the IdP for group membership or rely on a synced group mapping.
- RAR (RFC 9396) adoption is still early. The architecture should degrade gracefully when the IdP only supports scopes or audiences. What's the minimum binding level we're willing to accept before falling back to PausedAuth / re-approval?
- For the CIBA gitops flow: how does CI authenticate to initiate the CIBA flow? It needs its own client credentials with the IdP, which is itself a stored secret. This is a narrow, well-scoped secret (can only initiate approval requests, can't issue tokens without user consent), but it exists.

## Attestation-based delivery

The deployment specifies its authorization mode as part of the deployment strategy. Two modes:

- **Token passthrough**: the user's token is used directly as the caller credential. No attestation envelope. Works while the token is live, PausedAuth when it expires. Only viable when the user has direct authority at the target (no space separation).
- **Attestation**: an attestation envelope carries the user's JWT alongside the intent, signed by the platform. Required when there's space separation (the user has no direct authority at the target). Optional but valuable when there's only time separation (provides cryptographic proof of who authorized the operation, survives token expiry as an audit artifact).

Which mode a deployment uses is a property of that deployment, not a global setting. The platform evaluates what modes are available and applicable based on the authorization context.

### Attestation protocol

Within attestation mode, the protocol is uniform regardless of whether the separation is in time, space, or both. The envelope and validation sequence are the same in every case.

**Envelope:**

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

**Validation (same steps, always):**

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

Dimensions affect validation strength, not protocol shape: a fresh JWT makes the temporal check strong; a direct target makes the authz check strong (live SubjectAccessReview); RAR-scoped tokens make the content binding tight. The protocol doesn't branch — it degrades gracefully.

### Cluster-side delivery architecture (K8s)

The delivery agent (cluster-side, part of the fleetlet) handles both authorization modes:

- **Token passthrough**: the delivery agent uses the caller's token to apply manifests directly via Server-Side Apply. The target's API server validates the token and evaluates RBAC against the user's real identity.
- **Attestation mode**: the delivery agent receives attestation envelopes, validates the proof material internally (JWT against IdP JWKS, platform signature, bindings, validity bounds), and applies real resources using its own in-cluster ServiceAccount. It has broad RBAC but is the validation gate — nothing gets applied without passing the full validation sequence.

A separate read-only status agent watches managed resources and reports status and drift back to the platform. It has no write RBAC. If drift is detected, the platform re-delivers through the appropriate delivery path.

In attestation mode there is no broadly privileged reconciler. The delivery agent combines validation and apply in one step. No intermediate CRD, no separate controller with broad RBAC acting on platform-originated data. The delivery agent's in-cluster SA credential never leaves the cluster — the platform sends delivery instructions over the fleetlet connection, and the delivery agent uses its local SA. No cluster credentials travel to the platform.

### Transport as a security knob

The attestation contract (envelope in → validate → apply) is the same regardless of how the envelope reaches the delivery agent. Transport is a configuration choice per target profile:

- **Standard**: attestation envelopes delivered over the fleetlet gRPC connection. Simple, low latency. The platform has a live connection to the delivery agent process.
- **Hardened**: attestation envelopes written to a buffer (S3, Kafka, NATS). The delivery agent reads from the buffer, validates, applies. No direct connection between the platform and the privileged component. The buffer is the airgap. See provider_consumer_model.md for the full buffer mode discussion.
- **Future option**: SignedIntent CRDs as a K8s-native transport. The delivery agent watches the API server for SignedIntent resources instead of reading from gRPC or a buffer. Adds standard K8s semantics (watch, list, kubectl visibility) without changing the validation contract.

The delivery agent's code is identical across transports. Dialing up the security knob (from standard to hardened to CRD-based) requires no changes to the validation logic or the attestation format — only a transport configuration change.

## Practical architecture summary

For K8s targets, the layered model:

| Scenario | Mechanism | User identity at target | User presence needed |
|----------|-----------|------------------------|---------------------|
| Synchronous / short-lived ops | Token passthrough | Full (IdP-verified) | During operation |
| Long-running (run-as-platform) | Accepted initial auth + JWT-embedded provenance | Proof of initial user (JWT in signed artifact) | At creation only |
| Long-running (run-as-you) | Refresh tokens (when IdP supports it) | Full (IdP-verified, refreshed) | At creation only |
| Long-running (K8s-specific) | Delegation SAs | SA identity (correlatable) | At creation only |
| Any credential failure | PausedAuth + CIBA | N/A (paused) | To resume (or CIBA-prompted) |
| GitOps | Signed intent (JWT-embedded provenance) | Proof of initial user (JWT in signed artifact) | At signing only |
| GitOps (with RAR) | Intent-bound token | Full (IdP-verified, content-bound) | At signing only |

Delivery transport is configurable per target profile: standard (fleetlet gRPC), hardened (buffered via S3/Kafka/NATS), or future CRD-based. The attestation format and validation logic are identical across transports.

For non-K8s targets, the delivery agent declares what credential type it needs and handles the target-specific mechanics (AssumeRole, token exchange, etc). The platform provides the user's identity information and any stored credential references.
