# Archived JWT And Raw-Key Provenance Design

> **Status: historical, non-normative.** This document preserves the provenance
> design that preceded authenticated authority configuration and pluggable
> provenance profiles. Current design is defined by
> [provenance.md](../architecture/provenance.md) and
> [authentication.md](../authentication.md). The hybrid attestation POC still
> implements parts of the representation described here.

## What this doc preserves

This design explored two related ways to carry user identity and intent through
an untrusted resource manager:

1. a user signs content with a persistent device key whose relationship to an
   OIDC identity is carried in a JWT key-binding bundle; and
2. an earlier alternative embeds a short-lived user JWT beside the content and
   relies on a platform signature for package integrity.

The first model informed FleetShift's independently authenticated evidence,
attestation graph, and courier threat model. It was partially modeled by
`poc/attestation/hybrid/`. It is archived because its raw signature,
`trust_anchor_id`, key-binding, rotation, and history mechanisms became one
hard-coded provenance suite rather than a general contract, and it lacked
the TSA or append-only logs to properly support historical verification.

## When to read this

Read this when interpreting the hybrid POC, reviewing why FleetShift moved away
from a universal raw-signature interface, or recovering design rationale about
OIDC key binding and the earlier JWT-embedded alternative.

## What is intentionally elsewhere

- Current provenance, attestation, trust update, and history design:
  [provenance.md](../architecture/provenance.md)
- Current credential presentation, request signing, and `PausedAuth`:
  [authentication.md](../authentication.md)
- Detailed continuity/v3 protocol: [trust_model_v3.md](../trust_model_v3.md)

## Related docs

- [Hybrid attestation prototype](../../../poc/attestation/hybrid/README.md)
- [Historical Sigstore/OCI/TUF alternative](../sigstore_oci_alternative.md)

## Terminology

The JWT key-binding bundle was sometimes informally called a "self-signed ID
token bundle." The ID token was not self-signed: the IdP signed the JWT. The
user-controlled key signed a separate binding document, and an IdP key-
confirmation claim was expected to connect that key to the JWT.

## Direct User-Signing Model

The client created one signing key per user agent or device. The private key
remained with the user, while the RM stored the public key, its identity
binding, and signatures over FleetShift content. A target independently
verified:

- the externally anchored relationship between the signing key and user;
- proof that the signer possessed the corresponding private key;
- the signature over the exact intent or output;
- any validity and generation bounds; and
- constraints relating that authenticated input to the concrete delivery.

The model deliberately separated this durable signature from the credential
used to apply resources. A user's token, a delegated workload credential, or a
target-local delivery-agent credential could perform the apply while the user
signature supplied durable provenance.

### Signing surfaces considered

- A web client could use a WebAuthn credential or passkey-like key operation.
- A CLI could generate a dedicated key held in the operating-system keychain.
- GitOps tooling could store a FleetShift content signature beside source
  content. Git commit signing remained independent defense in depth.

The design assumed these surfaces could all produce the same raw signature
form. The current profile design retains purpose-specific evidence creation but
does not require each mechanism to expose an identical key ceremony.

## JWT Key-Binding Bundle

The proposed platform-distributed bundle was conceptually:

```text
KeyBindingBundle {
    binding_document {
        public_key
        oidc_issuer
        oidc_subject
        issued_at
    }
    binding_document_signature
    oidc_jwt
}
```

Enrollment required an OIDC token cryptographically bound to the user's
signing key, for example through `cnf`, `jkt`, DPoP-style proof of possession,
or an equivalent IdP-assisted mechanism. Merely placing an arbitrary public key
and an ordinary bearer token in the same RM-stored object was insufficient: a
compromised RM holding that token could replace the key.

The target was expected to verify:

1. the JWT signature against configured issuer keys;
2. issuer, audience, subject, expiry, and applicable tenant claims;
3. the JWT's key-confirmation relationship to the public key;
4. the user's signature over the binding document; and
5. the same key's signature over the FleetShift assertion.

A compromised RM could courier or omit the bundle but could not substitute a
different key without obtaining a matching IdP assertion and private-key proof.

### Renewal and history tension

Bindings were assigned a finite lifetime and periodically renewed with a fresh
JWT and binding signature. This bounded compromised-key use but created a
historical-verification problem:

- immediately distrusting retired IdP signing keys could make older evidence
  unverifiable;
- retaining old JWKS indefinitely weakened emergency key rotation; and
- requiring every inactive user to renew around routine IdP rotation harmed
  availability.

Possible answers included retained JWKS history, explicit distrust lists,
short binding lifetimes, external key registries, or an append-only history.
Continuity/v3, Sigstore trusted-time validation, and retained historical profile
configuration now address these tradeoffs through explicit profile semantics.

### External key sources

GitHub, GitLab, or a dedicated user-key registry were considered as alternate
sources for public keys. Targets would fetch keys from a configured external
authority rather than trust an RM database entry. This simplified RM storage
but introduced availability, identity-mapping, key-history, and provider-
account-compromise questions.

The current design treats any such mechanism as a well-known provenance type or
credential method selected by authenticated authority configuration, rather
than a universal fallback key registry.

## Old Universal Evidence Representation

The hybrid POC modeled raw signatures and key bindings directly in the common
attestation shape:

```text
SignedInput {
    content
    signature {
        signer_id
        public_key
        content_hash
        signature_bytes
    }
    key_binding {
        signer_id
        public_key
        trust_anchor_id
        binding_proof
    }
    valid_until
    output_constraints[]
    expected_generation?
}
```

Verification looked up the claimed `trust_anchor_id`, checked the binding and
raw signature, and then evaluated common graph constraints. Addon output and
placement evidence used parallel raw-signature structures.

This representation was useful for proving the attestation graph, derivation,
placement, removal, and constraint algorithms. It was not a suitable universal
provenance interface:

- Sigstore evidence is a certificate-and-Bundle protocol, not merely a raw key
  signature plus a generic binding;
- continuity/v3 needs authenticated identity history and ordered rotation
  proofs;
- TUF authenticates a repository target and role rather than a user-held key;
  and
- an RM-maintained `trust_anchor_id` is the wrong authority-selection primitive.

Current common code therefore consumes the authenticated principal and exact
content binding produced by a selected profile. It does not prescribe this old
wire representation.

## Workload And Addon Variants Considered

The earlier design considered several ways for addons to authenticate generated
manifests or placement decisions:

- SPIFFE/SPIRE X.509 SVIDs;
- certificates issued through the Kubernetes CSR API or cert-manager;
- a pinned administrator-provisioned public key or CA;
- externally published CA bundles; and
- a JWT key-binding bundle for a workload signing key.

The X.509 forms remain plausible inputs to provenance profiles. The workload
JWT form was considered weak unless the token issuer bound the token to the
actual evidence-signing key. An ordinary Kubernetes service-account bearer
token does not establish that relationship.

These mechanisms are now configured under canonical principal authorities and
typed profiles. They are no longer embedded as branches of one common
`KeyBinding` object.

## Earlier JWT-Embedded Variant

An earlier design embedded a short-lived IdP JWT with the requested intent and
used a platform-held key to sign the package:

```text
PlatformSignedEnvelope {
    user_jwt
    intent
    intent_digest
    jwt_digest
    created_at
    valid_until
}
```

The target verified both the IdP signature and platform signature. The platform
key alone could not invent a user identity, and a stolen JWT alone could not
change the package. A compromised platform possessing both factors while the
JWT remained valid could nevertheless pair that credential with unauthorized
content. Persisting the JWT also enlarged the credential-theft surface.

Rich Authorization Requests or another IdP mechanism could bind a JWT to an
exact intent digest, but that required non-universal IdP support and one token
per intent. Direct user signing avoided token persistence but led to the key-
binding and history problems above.

The current design keeps credential-only authorization as an explicit policy
choice and supports request signing as sender-constrained credential
presentation. Neither is treated as durable provenance unless a configured
provenance profile authenticates the exact assertion.

## What The Current Design Retains

The following conclusions survived this design:

- credential presentation and durable provenance are independent;
- the RM may store and courier evidence but cannot make it authentic;
- evidence must bind exact, purpose-typed content;
- each user or workload authenticates its own assertion;
- common attestation relationships and constraints are profile-neutral;
- missing historical material is a recoverable availability problem, not a
  reason to trust an unrelated key; and
- target-side verification is required for guarantees against RM compromise.

The current design generalizes those conclusions through authenticated
authority configuration, ordered profile selection, typed evidence, retained
profile state, ordinary trust-update deliveries, and the common delivery log.
