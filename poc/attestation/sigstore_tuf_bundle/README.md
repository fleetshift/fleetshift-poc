# FleetShift logless Sigstore bundle POC

This is an independent peer to `../sigstore_tuf`, informed by it and by
`../hybrid`, but built around the standard Sigstore bundle and trust-root data
models. It asks the same FleetShift question:

> Can a delivery agent verify a complete authorization and fulfillment chain
> locally, while the management plane only couriers the evidence?

The answer demonstrated here is yes—with an important qualification: omitting
certificate and artifact transparency preserves most of the hybrid POC's
point-to-point verification guarantees, but it does **not** preserve public
detectability of Fulcio misissuance or equivocation.

## Architecture

```text
tenant user                  platform addon workload
    │ OIDC token + PoP             │ platform OIDC token + PoP
    ▼                              ▼
per-tenant Fulcio             platform Fulcio
    │ short-lived leaf             │ short-lived leaf
    └──────────┬───────────────────┘
               ▼
       DSSE(in-toto Statement/v1)
       + FleetShift predicate
               │ raw signature bytes
               ▼
       external RFC 3161 TSA
               │
               ▼
       Sigstore Bundle v0.3
       (leaf + RFC 3161 response; no tlog entries)
               │
               ▼
management plane assembles one DeliveryPackage
  - root attestation and complete derivation graph
  - every input/output/relation Sigstore bundle
  - TUF timestamp/snapshot/targets responses and target bytes
  - tenant, delivery ID, and delivery generation
               │ no OCI or trust HTTP GETs
               ▼
delivery agent
  - has a complete TUF root provisioned out of band
  - persists TUF metadata across deliveries
  - verifies TUF, Sigstore, identity policy, content, placement,
    mutation/preconditions, constraints, and generation
```

There are two intentionally different trust domains:

- Each tenant has its own IdP and Fulcio. A tenant certificate can authorize
  an input only for that tenant's configured identity policy.
- Platform workloads use the platform IdP and platform Fulcio. Their SPIFFE
  subjects are mapped by TUF policy to stable addon IDs and are accepted only
  for output or fulfillment-relation evidence. An addon certificate is not a
  tenant input authority.

The external TSA is a third operator in the intended deployment. The POC's
`RFC3161TimestampAuthority` runs locally for test determinism, but emits and
verifies real DER RFC 3161 responses through OpenSSL. It timestamps the **raw
DSSE signature bytes**, as required by the Sigstore bundle model—not an
artifact, base64 text, statement digest, or PAE digest.

## Protocol choices

### Sigstore bundle and in-toto

Every signature is represented as
`application/vnd.dev.sigstore.bundle.v0.3+json` using the maintained
`sigstore-protobuf-specs` bindings. The bundle contains:

- one DSSE envelope and exactly one signature;
- one short-lived Fulcio leaf certificate (not a root or ad hoc chain);
- one external RFC 3161 timestamp response;
- no Rekor entries.

The DSSE payload is an in-toto Statement/v1. FleetShift defines four predicate
types rather than overloading a generic payload:

- `delivery-authorization/v1`: input content, expiry, output constraints, and
  optional expected generation;
- `manifest-set/v1`: the exact typed manifest envelopes;
- `placement/v1`: deployment ID and allowed target IDs;
- `fulfillment-relation/v1`: relation type, addon ID, resource type, and
  delivered manifest type.

The in-toto subject binds the canonical payload digest and a purpose-specific
name. Predicate type, subject name, payload type, digest algorithm, statement
type, and cardinality are all checked explicitly.

### Fulcio identity

The test double does not skip the issuance ceremony. It validates an ES256
OIDC token (`iss`, `sub`, `aud`, `iat`, `exp`, `kid`), checks proof of
possession for the requested ephemeral public key, and issues a ten-minute
code-signing certificate. The certificate uses the Fulcio issuer-v2 and token
subject OIDs as DER UTF8Strings and projects the subject into SAN.

This profile does not emit a signer-selected identity or trust-anchor label in
its predicates. Policy selects exactly one anchor from the authenticated tuple:

```text
(certificate authority URI, OIDC issuer, certificate subject, evidence kind)
```

The logical signer/addon ID comes from that TUF-authenticated mapping. Zero or
multiple matches fail closed.

### TUF and offline delivery

`python-tuf` produces and verifies real root, timestamp, snapshot, and targets
metadata. The agent is provisioned with the complete `root.json` outside the
delivery channel and persists its trusted metadata. A package carries only the
responses a normal updater would fetch plus the target bytes:

- `sigstore-trusted-root.json`, an official Sigstore `TrustedRoot` containing
  the tenant Fulcio, platform Fulcio, and TSA authorities;
- `fleetshift-trust-policy.json`, containing identity mappings, evidence-kind
  separation, attributes, and CEL constraints.

The updater's fetcher accepts only `memory://` delivery objects. There is no
registry or trust-service fallback, so missing material fails instead of
causing a network request. Standard TUF checks cover signature thresholds,
hashes, lengths, expiry, freeze protection, and rollback for an initialized
agent.

The package deliberately contains no TUF root, root key IDs, or verifier
object. This avoids making the courier the source of bootstrap trust.

## FleetShift delivery semantics retained

The peer reuses the hybrid POC's content, CEL, strategy, and mutation model so
the security comparison is about provenance rather than a second policy
language. It retains:

- inline and addon-produced manifests;
- target-evaluated and addon-signed placement for put and remove;
- signed input expiry and arbitrary output constraints;
- nested, self-contained `DerivedInput` chains with preconditions;
- constraint carry-forward behavior from the current hybrid prototype;
- deployment and managed-resource content in the same verifier;
- addon-signed `RegisteredSelfTarget` evidence;
- optional signed expected generation and target-side generation fencing;
- explainable verification trees.

It strengthens a few bindings:

- `DeliveryPackage.generation` is first-class and must equal a signed
  `expected_generation` when present; target state always fences the package
  generation.
- Tenant identity is part of agent provisioning and package routing; a package
  for another tenant is rejected before evidence processing.
- Managed-resource relations sign addon ID, resource type, and manifest type,
  preventing interpretation changes outside the signature.
- Cyclic graphs and graphs over 64 visited nodes fail closed.

## Security comparison

| Property | Hybrid POC | This peer |
|---|---|---|
| User/workload key exposure | Long-lived signing key in POC | Ephemeral signing key; short Fulcio leaf |
| Identity binding | Custom key binding + configured key | OIDC-authenticated Fulcio certificate + SAN/OIDs |
| Trusted signing time | None | External RFC 3161 token over raw signature |
| Bootstrap trust | External in-memory trust store | Complete TUF root provisioned out of band |
| Trust update rollback/freeze | Not modeled | Stateful standard TUF client |
| Evidence transport | Self-contained bundle | Self-contained package; no OCI GET |
| Public misissuance detection | None | **None** (CT/Rekor intentionally omitted) |
| Public artifact audit/discovery | None | **None** (Rekor intentionally omitted) |

The omission of CT and Rekor is not made safe by adding a TSA. A valid TSA
token proves that particular signature existed at a trusted time; it does not
prove that Fulcio was entitled to issue the identity, expose other certificates
issued for the identity, or make a compromised issuer's behavior publicly
detectable. Per-tenant Fulcio limits the blast radius, TUF constrains which CA
and identity are accepted, and short certificates reduce endpoint key
exposure, but a compromised tenant IdP/Fulcio pair can still create
undetectable tenant-valid signatures. A compromised platform IdP/Fulcio pair
can impersonate configured addons. Adding CT/Rekor later is therefore a
detection and audit improvement, not merely a storage optimization.

Compared with the earlier hybrid model, keyless signing also adds online
dependencies at signing time: IdP, Fulcio, and TSA must be reachable. Delivery
verification remains offline. Per-tenant Fulcio improves isolation but creates
CA lifecycle, monitoring, upgrade, backup, and historical-root-retention work
per tenant. The platform Fulcio must be operated separately, with stricter
workload issuance policy.

Couriered trust responses and evidence remove agent-side OCI availability,
authorization, and garbage-collection coupling. The cost is duplicated bundle
bytes and the need to keep deliveries fresh enough for TUF timestamp expiry.
An agent that is offline beyond that window must receive a fresh signed trust
update before it can accept work.

## Critical differences from the sibling experiment

This peer intentionally does not reuse its custom provenance and TUF formats:

- bootstrap is a complete, out-of-band TUF root with persistent client state,
  not key IDs supplied alongside each delivery;
- TUF verification is performed by the reference Python updater rather than a
  repository-shaped custom verifier;
- bundles and trusted roots use the official Sigstore protobuf schemas;
- timestamps are interoperable RFC 3161 and bind the actual signature;
- Fulcio issuance validates an OIDC JWS and proof of possession;
- policy derives anchor and signer identity from authenticated certificate
  facts rather than bundle labels;
- fulfillment relations sign every field that affects their interpretation.

These choices make the trust boundaries demonstrable, but do not make the POC
production-ready.

## Limitations and production work

- The CA, IdP, and TSA are local protocol-faithful test doubles, not deployed
  Fulcio or `sigstore/timestamp-authority` services.
- Certificate path validation handles this POC's fixed
  leaf → intermediate → root shape. Production should use sigstore-go/cosign's
  maintained policy verifier and general path builder.
- TUF root rotation, delegated targets, offline threshold ceremonies, mirrors,
  repository recovery, and Sigstore authority rotation are not exercised.
  Production `TrustedRoot` targets must retain historical CA/TSA instances for
  old bundles.
- The default agent TUF state directory is temporary. A real fleetlet must
  persist it durably and protect it from rollback with the rest of target
  fulfillment state.
- No CT, Rekor, witnesses, revocation workflow, credential presentation,
  `PausedAuth`, apply loop, drift handling, observation reporting, or delivery
  acknowledgement protocol is implemented.
- `DeliveryPackage.generation` is cryptographically bound only when the input
  has `expected_generation`, matching hybrid compatibility. A production
  delivery contract should require the expected generation on mutating work.
- This prototype retains hybrid's constraint-accumulation behavior across
  derivations, although the hybrid README notes the intended design may be
  per-layer constraints. That design question should be resolved separately.

## Tests

The suite preserves all 126 hybrid test names: 48 core, 63 delivery, and 15
managed-resource tests. Name parity is not treated as proof by itself: all
ported hybrid negatives raise and pin a specific rejection detail, and a separate
porting-contract test forbids soft boolean rejection ports. Three key-binding
cases use their Sigstore equivalents; one literal duplicate-signer-field
mutation is unrepresentable in a standard bundle and is replaced by a negative
delivery-time identity-confusion test. `TEST_PORTING.md` documents the exact
mapping. Eleven additional tests cover the new trust and bundle profile.

From the repository root:

```bash
python3 -m venv /tmp/fleetshift-sigstore-bundle
/tmp/fleetshift-sigstore-bundle/bin/pip install \
  -r poc/attestation/requirements.txt \
  -r poc/attestation/sigstore_tuf_bundle/requirements.txt
PYTHONPATH=poc/attestation \
  /tmp/fleetshift-sigstore-bundle/bin/python -m pytest \
  poc/attestation/sigstore_tuf_bundle -q
```

The tests require an `openssl` executable with RFC 3161 `ts` support (OpenSSL
3 is used by the POC).

See `TEST_PORTING.md` for the exact inventory and semantic adjustments.

## File guide

| File | Purpose |
|---|---|
| `identity.py` | OIDC JWS, proof-of-possession, and Fulcio certificate test doubles |
| `tsa.py` | RFC 3161 TSA and verification via OpenSSL |
| `sigstore.py` | Official Bundle v0.3 construction and logless verification profile |
| `tuf.py` | Standard TUF publisher, memory fetcher, and persistent updater |
| `model.py` | Complete delivery graph and fulfillment evidence |
| `build.py` | Per-tenant/platform trust setup and signing/assembly helpers |
| `verify.py` | Stateful offline agent and FleetShift policy pipeline |
| `test_*_parity.py` | Name-for-name hybrid test ports |
| `test_security.py` | Sigstore/TUF-specific hardening tests |
| `test_porting_contract.py` | Assertion-fidelity and exact-inventory regression checks |

## Primary specifications and implementation references

- [Sigstore Bundle protobuf](https://github.com/sigstore/protobuf-specs/blob/main/protos/sigstore_bundle.proto)
- [Sigstore TrustedRoot protobuf](https://github.com/sigstore/protobuf-specs/blob/main/protos/sigstore_trustroot.proto)
- [Sigstore timestamp-authority guidance](https://github.com/sigstore/timestamp-authority)
- [Fulcio certificate issuance overview](https://docs.sigstore.dev/certificate_authority/certificate-issuing-overview/)
- [Fulcio certificate extension OIDs](https://github.com/sigstore/fulcio/blob/main/docs/oid-info.md)
- [The Update Framework specification](https://theupdateframework.github.io/specification/latest/)
- [Sigstore security model](https://docs.sigstore.dev/about/security/)
