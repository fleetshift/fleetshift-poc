# Authentication And Credential Presentation

## What this doc covers

- authentication of callers to the resource manager;
- whose credential applies resources at a target;
- credential lifetime, storage, exchange, and delegation;
- request signing as sender-constrained credential presentation;
- IdP orchestration that those credential modes depend on; and
- `PausedAuth` when credential or provenance requirements can be repaired.

## When to read this

Read this when designing an API authentication flow, choosing whose identity
applies resources at a target, handling credential expiry during a rollout, or
resuming a fulfillment that lacks acceptable authorization material.

## What is intentionally elsewhere

- The governing threat model, time/space delivery problem, security choices,
  and trust-anchor model: [security.md](security.md)
- Provenance profiles, attestation semantics, authenticated trust
  configuration, updates, and history:
  [architecture/provenance.md](architecture/provenance.md)
- Target apply, acknowledgement, generation, journaling, and recovery:
  [architecture/target_delivery_contract.md](architecture/target_delivery_contract.md)
- Resource-manager tenant, workspace, and resource authorization:
  [architecture/tenancy_and_permissions.md](architecture/tenancy_and_permissions.md)
- The superseded raw-signature and JWT key-binding provenance design:
  [archive/jwt_and_raw_key_provenance.md](archive/jwt_and_raw_key_provenance.md)

## Related docs

- [architecture.md](architecture.md)
- [security.md](security.md)
- [architecture/core_model.md](architecture/core_model.md)
- [architecture/provenance.md](architecture/provenance.md)
- [architecture/target_delivery_contract.md](architecture/target_delivery_contract.md)

## Overview

FleetShift separates two concerns in delivery authorization:

- **Provenance** is durable cryptographic proof of who authenticated an exact
  typed intent or supporting assertion.
- **Credential presentation** determines whose credential, if any, performs
  the concrete operation at the target: the user's own token (run as me), a
  workload or delegation identity (run as workload), the delivery agent's
  local identity (run as platform), or a pre-provisioned standalone service
  identity.

For the root request, the same canonical identity may be established through a
live credential and through provenance. The context and verification mechanism
differ; this does not make them different kinds of identity. A separate apply
identity is involved when a workload or delivery agent acts on the user's
behalf, and supporting evidence may come from its own addon or workload
identity.

The two axes are independent. Delivery policy may require a credential,
provenance, or both, and any accepted provenance profile can compose with any
compatible credential-presentation mode. The resource manager still performs
primary stateful authorization. Targets independently verify only the
credentials, provenance, constraints, and local state included in their
delivery contract.

**In all cases**, the resource manager still enforces its own authorization
rules, which it tries to make coherent with targets through syncing. Because
of this, **fulfillments are not tightly coupled to individual users, even if
their auth is used.** If they are unavailable, any other RM-authorized user
can approve and take over the fulfillment. Additional approvers can be added
preemptively. Takeover cannot bypass a policy requiring a particular identity,
provenance, or tenant; replacement material must still verify under current
delivery policy.

See [Delivery Security Model](security.md) for why the design separates these
axes, what an RM compromise can and cannot do, and how the available choices
trade security for availability. See
[Provenance And Trust Configuration](architecture/provenance.md) for the
durable provenance mechanisms themselves.

## Credential Verification

A credential method is a well-known verifier selected through authenticated
authority configuration. A credential may expose tentative scheme, authority,
subject, or tenant claims for lookup, but those values become authoritative
only after the selected method verifies them.

For an OIDC credential, verification normally includes:

- exact issuer and configured discovery/JWKS authority;
- cryptographic signature and supported algorithm;
- audience or resource indicator;
- expiry, not-before, and other freshness checks;
- subject and any claims consumed by policy constraints; and
- verified tenant-partition mapping.

An X.509 or another credential method performs its corresponding chain,
purpose, name, revocation, and freshness checks before yielding the same
canonical identity shape. A session cookie or internal access token may
represent a principal the RM already authenticated, but it does not become
portable target evidence merely because the RM issued it.

The RM performs credential verification for its own API decision. A target
independently verifies any credential that delivery policy requires it to
trust; an RM-authenticated session assertion alone does not substitute for that
verification.

When a live credential and provenance both authenticate the root request, they
must resolve to the same canonical identity and tenant partition. This does not
require the user's credential to match a separate addon or workload identity
that authenticated supporting evidence.

## Credential Presentation

Credential presentation answers whose credential applies the resources at the
target. This is the user-facing choice when creating a fulfillment.

When deploying something, a user is presented with options based on who they
want to see in the end target, what authority they have there, their security
allowance, and their availability constraints. Security allowances compose
with provenance and live in [security.md](security.md); this table is the
credential-presentation choice.

Typical use-when conditions below are the common fit, not exclusive
requirements. A provenance-capable target may still allow credential-only
delivery, and a target that cannot verify provenance may still use a delegated
or local apply identity.

| Run as | Use when | Apply authority source | User presence | Apply-credential availability | Commentary |
| --- | --- | --- | --- | --- | --- |
| Me | <ol><li>Operations are short-lived or few<li>You have permissions at the target<li>Typical when the delivery target does not independently verify platform provenance (e.g. AWS, GCP)</ol> | Target | During operation (or refresh) | Low | Simple and secure for when it fits. |
| Me (+ refresh tokens) | <ol><li>Operations are long-lived<li>You have permissions at the target<li>You have advanced IdP features<li>Typical when the delivery target does not independently verify platform provenance</ol> | Target | At initial authorization; unattended afterward while the refresh credential remains valid | Medium | Makes sense if you want to really track the user. Depends on IdP features. |
| Delegate service account | <ol><li>Operations are long-lived<li>You have permissions to delegate<li>Typical when the delivery target does not independently verify platform provenance</ol> | Target* | At creation only | High | Depends on some advanced orchestration for the service account. *RBAC orchestrated by the platform (itself with end-to-end auth). |
| Standalone service account | <ol><li>Operations are long-lived<li>You do not have direct permissions, or the target cannot accept end-user federation, bounded delegation, or local verified delivery</ol> | Target | Not required at apply | High | Exists mainly as a fallback. Concentrates standing authority, often platform-held. |
| Platform | <ol><li>You do not want to, or cannot, authorize the end user at the target<li>Especially attractive to multi-tenant service providers</ol> | Platform | Not required at apply. Required when creating or refreshing any required provenance. | High | The local apply credential is highly available. Complete authorization-path availability still depends on required provenance, proof material, and trust state. |

Apply-credential availability is whose live credential can perform the apply.
It is not the availability of the complete authorization path. Required
provenance, couriered proof material, and trust-state catch-up can still pause
a High-apply-credential delivery. See [`PausedAuth`](#pausedauth).

If credentials are missing, expired, or insufficient, user presence is needed
to resume, or the user may be CIBA-prompted.

### Token passthrough (run as me)

The simplest model: the user's bearer token is passed through to the target.
Full end-to-end user identity. Works while the token lives. Prefer keeping it
in memory only; if replay/recovery requires persistence, treat it as a
short-lived credential and handle it accordingly.

Prefer an access token or target-specific assertion with the correct audience,
resource indicator, and scope. An ID token is appropriate only where the
receiving system explicitly accepts one for authentication.

When the token expires mid-rollout, or on workflow replay, the fulfillment
transitions to `PausedAuth` and waits for an authorized user to resume it with
a fresh token. Any authorized user can resume; this gives approval-gate
semantics for free. Takeover still cannot bypass a policy requiring a
particular identity, provenance, or tenant; the target evaluates the
replacement credential under current delivery policy.

#### Refresh tokens (credential durability for run as me)

Refresh tokens can be used to make "run-as-me" durable. It preserves
end-to-end user identity at the target (the refreshed token IS the user's
token), but, to secure it properly, requires advanced IdP features.

Ideally you would:

- Sender constrain them (DPoP, RFC 9449). This makes the platform privileged
  but only via its protected private key. Leaked credentials are not a
  problem. Sender constrained refresh tokens have some support. It would
  require the backend to be a confidential client and not the frontend. That
  can complicate CLI integration. Maybe you only approve these long lived
  flows through the browser, though. It's a few-time operation.
- Scope them. This can be hard because it requires more IdP configuration
  e.g. client per cluster which could be awful without automation. And
  automating that is itself difficult to set up (dynamic client registration /
  aud configuration). Plus you'd want token exchange of some kind or the
  original aud needs to include every cluster. Rich Authorization Requests
  (RAR) could help.
- Rotate, revoke, and keep them short-lived where supported, encrypted at
  rest, and deleted when no longer needed.

Refresh tokens shine when: (a) the IdP supports sender constraints, flexible
token exchange, and RAR (rare in practice), and (b) the targets work well with
proper OAuth (access tokens, transaction tokens). Outside of that users should
use them with caution. Durable provenance plus a delegated or target-local
apply identity often avoids retaining user credentials entirely.

### Delegation service accounts (run as workload)

When something is long running, the user creates a service account dedicated
to run on their behalf, with a scoped subset of their permissions.

The provisioning flow is synchronous (while the user is present):

1. User creates a fulfillment targeting cluster X
2. The platform, using the user's own token, creates a ServiceAccount + Role +
   RoleBinding in the target cluster
3. K8s prevents privilege escalation: the RBAC API rejects RoleBinding
   creation if the user doesn't hold the permissions being bound. The user can
   only delegate authority they actually have.
4. User's token is discarded after provisioning. Never stored.

Later delivery uses the service account identity. On Kubernetes, that is a
target-specific implementation choice between narrowly impersonating the
already-created delegated ServiceAccount and minting a `TokenRequest`
credential for it. Impersonating the delegated SA is a small improvement over
`TokenRequest`:

- Impersonation is auditable; token request looks indistinguishable from any
  other actor with the service account
- There is no additional token that can be used for anything else; that needs
  to expire, etc.

This is not end-user impersonation. The impersonated identity is the delegated
ServiceAccount the user just created, and the impersonator is a
platform-controlled apply component (typically the delivery agent). Bootstrap
may grant that component only narrow impersonation rights over delegate SAs.
That places trust in the platform, but it is scoped and auditable. We may not
need this model.

Ideally:

- Something expires these over time
- When the user's permissions shrink below those of their shadow service
  accounts, the delegated service accounts are automatically restricted too

You could also "just" create specific service accounts to run workloads that
you wanted long-running, with strict permissions. If they ever tried to escape
that, the fulfillment pauses for approval.

Trade-offs:

- The target sees the service account identity, not the user. User identity is
  in the platform's audit log, correlatable via SA naming/annotations.
  Provenance can still cryptographically bind the durable intent to the user.
  If the target lacks a delivery agent capable of verification, this binding
  is only correlatable, not cryptographic.
- Permission drift: if the creating user loses access, the SA retains its
  grants until explicitly reconciled. We may be able to eagerly cascade
  permission changes done by the platform to SAs associated with the user.
- K8s-specific pattern. Other targets need equivalents (IAM AssumeRole for
  AWS, Managed Identity for Azure, etc).

### None (run as platform)

This model relies on provenance, transport authentication, and the "fleetlet"
delivery agent design to reduce the authority concentrated in the platform
server, while supporting both time and space separation for deliveries.
Provenance proves what an authentic user or workload signed, and the fleetlet
has isolated authority for its target behind network separation that decouples
it from the platform server. The delivery agent applies with a credential that
never leaves the target, such as its in-cluster ServiceAccount. No user or
cluster credential has to travel through the RM.

The local identity should have no more permission than the delivery contract
requires. When policy requires provenance, the agent uses its local authority
only after verifying the delivered assertions and constraints. The resource
manager remains the primary tenant, workspace, and resource authorization
boundary; target verification narrows what the agent will apply rather than
reproducing all RM policy.

The local apply credential is highly available. Complete authorization still
depends on whatever credential, provenance, proof material, and trust state
delivery policy requires. If a target accepts RM-delivered commands without
independently checking an applicable credential or provenance, compromise of
the RM effectively gains the delivery agent's apply scope.

### Standalone service account

A pre-provisioned service identity is a fallback when the user does not have
direct permissions, or when the target cannot accept end-user federation,
bounded delegation, or local verified delivery. Its credential is durable
platform-held authority and should be narrowly scoped, isolated per target or
purpose, rotated, and audited. It offers no inherent end-user attribution at
the target.

### Direct Kubernetes impersonation of end users is not preferred

Having a platform-controlled component impersonate an arbitrary end user is
weaker than presenting the user's authenticated token. That concern applies
whether the component is an RM-side proxy or a target-side delivery agent. It
is separate from narrowly impersonating an already-created delegated
ServiceAccount, which can be a useful workload mechanism.

The fundamental problem: K8s impersonation lets the impersonator assert group
membership, and K8s has no way to verify those assertions. Even with
constrained impersonation (limiting which users can be impersonated via
resourceNames), the impersonator can claim arbitrary groups for that user. If
the platform can impersonate group "admins", it can put any user in that group
regardless of their actual membership. These are unverifiable claims about a
user.

With token passthrough, the IdP is the authority on claims – groups are in the
token, cryptographically signed by the IdP. With end-user impersonation, the
platform is the authority. This is a fundamentally weaker trust model for any
environment where group-based authorization matters.

Prefer direct user credentials, a bounded delegated ServiceAccount, or a
target-local delivery identity constrained by verified delivery authorization.
Direct end-user impersonation remains a compatibility option only where its
effective user and group scope can be independently bounded and audited. At
most it should be a compatibility fallback, not the preferred steady-state
model.

## Request Signing

Request signing is credential presentation, not provenance. It
sender-constrains a live credential or authenticated key binding by covering
the relevant request components, destination or audience, body digest,
freshness value, and replay protection.

This is useful when an IdP issues only bearer tokens. Theft of the token alone
is then insufficient to reproduce the signed FleetShift request. It can also
protect operations that have no delivery or durable provenance, including
read-only queries.

The signature authenticates only the live root request. It does not become
historical evidence, authenticate supporting attestation-graph assertions, or
replace a provenance profile. If the request creates a durable mutation, any
required provenance is produced and evaluated separately.

Request signing is owned by a typed credential method. Enrollment and renewal
are credential-method operations under authenticated authority configuration.
An implementation may share key or verification machinery with a provenance
profile, but the provenance profile does not become the protocol owner merely
because machinery is shared. The exact HTTP or RPC signature format and
freshness mechanism remain open design work.

## Target Credential Presentation

The delivery agent declares what credential presentation it needs; the
platform should not hard-code one token type for every target.

Typical contracts:

- K8s API apply/proxy: pass through the user's token when the target directly
  trusts the tenant IdP. If we control the target's auth stack (for example, a
  Kubernetes distribution we customize), it is even better to validate access
  tokens and scopes/resource indicators directly rather than relying only on
  ID tokens. Run as platform uses the local delivery-agent ServiceAccount
  instead.
- AWS: ask for an ID token or SAML assertion, then `AssumeRoleWith*Identity`
  -> SigV4.
- GCP: ask for an ID token, then token exchange -> GCP token.
- Other targets: ask for "an access token for X" and let the delivery agent
  perform whatever target-specific exchange is needed.
- A target-native broker may derive a short-lived credential from an approved
  workload delegation.

If the durability model is delegation SAs, the delivery agent derives or
requests the delegated credential from user-linked identity/provenance rather
than a platform-global secret.

Vault-backed service-account credentials are a last resort; prefer credentials
derived from the end user.

Token exchange is preferable to issuing one credential with every possible
target audience. The issuer remains responsible for deciding which source
identity may exchange into which target audience and scope.

Credential acquisition must not silently change the target's provenance trust
configuration. Establishing or updating accepted authorities follows
bootstrap or an authenticated trust-update delivery, not an ordinary apply
credential.

## PausedAuth

Credentials and provenance are often time- and scope-bound. Tokens expire,
keys and trust rotate, and permissions change. `PausedAuth` is the recoverable
fulfillment state for a delivery that cannot currently satisfy its configured
authorization requirements.

Typical causes include:

- an absent, expired, wrong-audience, or insufficient target credential;
- policy requiring live user presence or reapproval;
- missing provenance or couriered historical proof material;
- a verifier that must catch up through authenticated trust updates; or
- an allowed external verification service or refreshable proof that is
  temporarily unavailable.

Another RM-authorized user may resume or take over with a fresh credential,
request signature, approval, provenance item, historical object, or
consecutive trust update, as appropriate. Additional approvers can be added
preemptively. Takeover cannot bypass a policy requiring a particular identity,
provenance, or tenant.

CIBA composes naturally with this: `PausedAuth` is the state ("we need
credentials"), and CIBA is one way to obtain them ("prompt the user on another
device"). For a CIBA flow initiated from CI, CI authenticates to the IdP with
its own client credentials. That is itself a stored secret. It is a narrow,
well-scoped secret (can only initiate approval requests, can't issue tokens
without user consent), but it exists.

`PausedAuth` is not a weaker verification mode. Resume does not override target
policy, fall back to TOFU or an unrelated authority, or allow stale work to
overtake newer generations. Replacement material must verify under current
policy and target-local ordering state.

Malformed, contradictory, wrong-target, or cryptographically invalid material
is rejected for that delivery attempt. If the desired fulfillment can be
repaired, it may remain in or enter `PausedAuth`; the invalid package itself
never becomes pending authority. Irrecoverable semantic or apply failures
belong to their own failure states.

## GitOps And Other Non-Interactive Flows

Provenance is agnostic of transport by design. Git as a source of truth is
therefore largely reusable as another transport for the same signed
assertions, with apply authority left intact. It does not need a
GitOps-specific token-binding or authorization model. Ordinary git commit
signing is independent unless a configured provenance profile explicitly
defines how it authenticates FleetShift content.

The credential-presentation modes above still apply. Details of GitOps as a
FleetShift transport are left for later work.

## Operational Concerns

### IdP orchestration

In various scenarios, we could benefit from specific IdP configuration:

- Per cluster client IDs (audiences)
- Permission-level scoping (assuming you have an authorizer which takes this
  into account)
- If an IdP can handle the refresh token route, setup for that
- Token exchange (RFC 8693) for audience swapping without per-cluster client
  IDs
- CAEP/Shared Signals Framework (SSF) for real-time session revocation and
  permission change events

Audience scoping is the recurring operational cost. If we want to scope
tokens to particular clusters, we need separate audiences for those. More IdP
configuration to do. Hard to make dynamic. Token Exchange (RFC 8693) can
address this: exchange a platform-audience token for a target-audience token
at the IdP. The IdP controls policy (which exchanges are allowed, for which
audiences). This avoids per-cluster client IDs but requires IdP support
(Keycloak, Dex have it; Auth0/Okta partial).

## Open Questions

- What request-signature representation and replay cache should the first
  credential method standardize?
- Which target classes justify durable refresh credentials rather than
  provenance plus delegated or target-local apply identity?
- How should user-permission changes reconcile existing delegated workload
  identities across target types?
- Which CIBA, token-exchange, and DPoP capabilities can FleetShift rely on in
  the first supported IdP integrations?
- Which `PausedAuth` causes and requested repairs should be represented in the
  public API without exposing security-sensitive detail?
