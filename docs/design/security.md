# Delivery Security Model

## What this doc covers

The security model shared by FleetShift credential presentation and delivery
provenance:

- the governing principle and resource-manager threat model;
- why delivery separates live credentials from durable provenance;
- the security and availability choices exposed by a fulfillment;
- where request signing fits; and
- how trust anchors and authenticated trust distribution support both axes.

## When to read this

Read this first when deciding what a FleetShift delivery protects, what remains
trusted, or which combination of credentials and provenance fits a target. The
credential and provenance documents define the corresponding mechanisms in
detail.

## What is intentionally elsewhere

- Credential modes, request signing, token exchange, and `PausedAuth`:
  [authentication.md](authentication.md)
- Provenance profiles, attestation semantics, authority configuration, trust
  updates, and historical verification:
  [architecture/provenance.md](architecture/provenance.md)
- Resource-manager tenant, workspace, and resource authorization:
  [architecture/tenancy_and_permissions.md](architecture/tenancy_and_permissions.md)
- Target apply, ordering, acknowledgement, and recovery:
  [architecture/target_delivery_contract.md](architecture/target_delivery_contract.md)
- The superseded JWT and raw-key provenance model:
  [archive/jwt_and_raw_key_provenance.md](archive/jwt_and_raw_key_provenance.md)

## Related docs

- [architecture.md](architecture.md)
- [authentication.md](authentication.md)
- [architecture/provenance.md](architecture/provenance.md)
- [provider_consumer_model.md](provider_consumer_model.md)

## Governing Principle And Threat Model

FleetShift is designed so that compromise of the customer-facing resource
manager does not automatically compromise an entire multi-tenant provider
estate.

That is a **minimized-trust** goal, not a claim that the platform is irrelevant
or strictly "zero trust". The resource manager remains the primary API and
authorization engine. It coordinates addons and decides which work to attempt.
Targets do not and cannot reproduce all of that state or policy.

This is nevertheless a substantial departure from the usual management-plane
trust boundary. Fleet managers and GitOps reconcilers commonly retain target
credentials or operate a privileged target-side agent; cloud service control
planes are generally themselves the authoritative API. These systems can
narrow that authority with RBAC, tenant boundaries, admission policy, and
credential isolation, but compromise of their accepted control input and apply
path usually leaves the target unable to distinguish legitimate intent from a
command fabricated by the management plane.

FleetShift instead lets the target independently verify selected facts about
identity, exact intent and supporting assertions, tenant and target scope,
ordering, and trust updates. For those facts, the RM is demoted from a
multitenant security oracle to a constrained deputy and provenance courier.
This is a qualitatively stronger containment boundary for deputy confusion and
management-plane compromise than is common today, while remaining short of
literal "zero" trust because the RM is still the primary stateful authorizer.

The boundary composes established mechanisms: end-to-end token passthrough
where targets accept user identity; proof of possession or request signatures
to sender-constrain live credentials; content-bound signatures and attestations
for durable provenance; and authenticated trust history—through retained
roots, transparency logs, or verifier checkpoints—so the courier cannot
silently rewrite identity or trust over time.

The resulting design favors:

- no platform-wide service account whose compromise grants an entire estate;
- end-to-end user or workload identity and auditing where the target can
  verify it;
- no or limited storage of customer credentials, with durable credentials
  unnecessary where possible and otherwise scoped, rotated, and
  sender-constrained;
- trust anchors outside the ordinary RM database and authenticated updates
  from already accepted trust; and
- target-local apply authority that cannot rewrite its own trust policy merely
  because it can mutate workloads.

A compromised RM may still bypass its own permissions, misuse authentic
authority within the scope target policy leaves to it, omit or delay work,
withhold proof material, and cause denial of service. New targets are also
exposed to compromise of whatever bootstrap or TOFU path first establishes
their trust. The stronger guarantees begin only after a verifier has retained
authenticated trust state.

With a more operationally intensive profile and target policy—using
independent authorities, trusted time, witnessed transparency, and
monitoring—the RM could approach an untrusted courier even for delivery
authorization; it would still control availability and remain trusted for any
stateful policy not reproduced at the target.

## The Delivery Problem: Time And Space

The platform frequently mediates between a user and a target where the user is
not making the target API call directly. Authorization has to cross two kinds
of separation:

- **Time:** a long-running rollout may outlive the user's live JWT, session, or
  interactive presence.
- **Space:** provider delivery may cross into a factory or target trust boundary
  where the user has no direct account or network path. See
  [provider_consumer_model.md](provider_consumer_model.md) for that topology.

FleetShift addresses these with two orthogonal axes:

- **Credential presentation** determines which credential, if any, is live at
  the target when the concrete operation is performed. It may be the user's
  credential, a delegated workload credential, or the delivery agent's local
  identity.
- **Provenance** is durable cryptographic evidence of who authenticated an
  exact typed intent or supporting assertion. It can remain verifiable after
  the original credential or interactive session expires.

These axes often describe the same canonical user identity through different
mechanisms and at different times. They do not imply separate kinds of person.
The identity used for the final target API call may genuinely differ when a
workload or local delivery agent applies on the user's behalf.

Request signing remains on the credential axis. It sender-constrains a live
credential and can protect requests, including read-only requests, that have no
durable delivery provenance. It covers the live root request rather than the
independent supporting assertions in an attestation graph.

Transport is another independent choice. Fleetlet streaming, buffered
delivery, and another authenticated transport do not change whose credential
is presented or what provenance must verify.

## Fulfillment Security And Availability Choices

A fulfillment chooses mechanisms based on who should appear at the target,
what authority is available there, how long work may continue unattended, and
how much trust may remain in the RM.

The useful security allowances are not rigid levels, because credential and
provenance choices compose. They describe progressively less authority left
with the RM:

- **Durable credential trust:** the RM retains a scoped user or service
  credential for unattended operation.
- **Brief credential trust:** the RM handles a short-lived credential and
  pauses when it needs renewed authority.
- **Intent-constrained trust:** provenance binds the root intent and supporting
  assertions, while authenticated constraints bound what the RM may derive or
  route from them.
- **Minimized RM trust:** the target verifies the root and every
  security-relevant derived assertion under independently retained trust. The
  RM remains the primary stateful authorizer, so this is not literally zero
  trust.

### Credential presentation

Availability here is **apply-credential** availability: whose live credential
can perform the apply. It is not the availability of the complete
authorization path. Required provenance, proof material, and trust state can
still pause a High-apply-credential delivery. Use-when, apply authority
source, and user presence live in [authentication.md](authentication.md).

| Apply mode                  | Typical use                                                                                                         | Apply-credential availability                                                                     | Security tradeoff                                                                                                                    |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Run as me                   | The user has direct target authority and work is short-lived or can pause for a fresh token                         | Low                                                                                               | Preserves target-native user attribution; the live bearer is tied to token lifetime                                                  |
| Run as me with renewal      | Long-lived work where the user should remain the apply identity and the IdP supports durable credentials            | Medium                                                                                            | Durable attribution requires protecting powerful long-lived credential material                                                      |
| Run as workload             | Long-running work can use a narrowly delegated target identity                                                      | High                                                                                              | Avoids retaining the user's credential, but delegated authority can drift or outlive the user                                        |
| Run as platform             | You do not want to, or cannot, authorize the end user at the target API                                             | High for the local apply credential; complete path depends on required provenance and trust state | Keeps target credentials local and works across the time/space gap; the target sees the agent rather than the user at its native API |
| Standalone service identity | The user lacks direct permissions, or the target cannot accept better federation, delegation, or local verification | High                                                                                              | Concentrates durable standing authority and provides weaker end-user attribution                                                     |

### Provenance and live authorization

| Delivery authorization    | Main affordance                                                                                               | Main limitation                                                                                                    |
| ------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Credential only           | Simple, target-native authorization for short-lived work or targets without provenance verification           | Availability follows credential lifetime; a bearer token remains usable by whoever can present it within its scope |
| Request-signed credential | Sender-constrains the live root operation and also works for non-delivery APIs                                | Still temporal and does not authenticate supporting graph evidence                                                 |
| Provenance only           | Durable exact-content authorization can cross time and trust boundaries while another identity performs apply | Requires supported signing/publication and historical-verification machinery                                       |
| Credential and provenance | Combines live presence or current target authority with durable content authentication                        | Both mechanisms must be available and agree where they authenticate the same root principal                        |

Provenance is expected to become broadly available rather than a rare
"high-security" mode. Continuity/v3 is the no-new-infrastructure baseline: it
provides verifier-local identity and key continuity, accepted-history rollback
resistance, and logical ordering, but not trusted wall-clock time or global
fork detection. Sigstore, TUF, and future well-known profiles share the
contract and may trade additional infrastructure for different or stronger
time, transparency, history, availability, and compromise guarantees. The
common attestation graph lets addons and placement authorities authenticate
their own exact assertions without asking one root signer to sign aggregate
output.

Consequently, raw output signing is not a separate fulfillment security level.
The normal model authenticates a purpose-typed root intent plus independently
authenticated derived assertions and constraints. A content producer may
authenticate its own exact output, but the platform does not ask an end user to
re-sign every rendered aggregate merely to obtain the strongest supported
mode.

The RM remains responsible for primary authorization in every row. Provenance
and target constraints reduce the authority that survives RM compromise; they
do not make the RM's stateful authorization responsibilities disappear. If
required credentials, provenance, or trust history are temporarily
unavailable, the fulfillment can enter `PausedAuth` and resume only after the
missing material verifies under current policy.

## Trust Anchors And Distribution

Credentials and provenance both begin from authenticated authority
configuration retained by the verifier. A trust anchor is the material and
policy from which a mechanism establishes a canonical principal or repository
authority—for example an OIDC issuer and JWKS, an X.509 CA, a Fulcio/TSA trust
root, a TUF root, or a continuity/v3 checkpoint.

Anchors are not merely keys. Their authenticated configuration can constrain
tenant mapping, accepted credential methods or provenance profiles, content
types, purposes, identities, target relationships, freshness, and historical
cutoffs. A shared multi-tenant IdP or CA may use a verified tenant claim or
certificate field to isolate principals without duplicating identical anchor
configuration per FleetShift tenant.

The ordinary RM database is not authoritative for target trust. Bootstrap or
TOFU installs the exact initial trust configuration on an uninitialized
verifier. Every later change is itself an authenticated trust-update delivery
accepted under predecessor policy. An initialized verifier never silently
returns to TOFU.

The RM may store and courier current configuration, past profile objects, and
proof material, but retained verifier state decides what is accepted. Current
configuration either retains the anchors and cutoffs needed for history, or
retained historical profile entries continue to do so. Profile-specific state,
such as a TUF root or continuity checkpoint, follows the same rule. Missing
objects cause a pause or denial of service, never fallback to unrelated trust.

This distribution model does not require every authority to be customer-run.
A provider may operate a multi-tenant IdP, CA, repository, or workload
authority. Its compromise has the blast radius granted to it by authenticated
configuration, so shared authorities are conscious provider and tenant trust
choices rather than implicit FleetShift roots.
