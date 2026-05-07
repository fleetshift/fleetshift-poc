# Alternative provenance architecture: sigstore, OCI, and TUF

This document explores an alternative foundation for the attested provenance model described in `authentication.md`. It proposes replacing the custom key lifecycle, signing, and trust distribution mechanisms with three established, composable standards: [sigstore](https://www.sigstore.dev/) (identity-bound signing and transparency), [OCI artifacts](https://specs.opencontainers.org/image-spec/?v=v1.1.0) with the [referrers API](https://specs.opencontainers.org/distribution-spec/?v=v1.1.1) (content-addressed storage and distribution), and [TUF](https://theupdateframework.github.io/specification/latest/) (secure trust root distribution and rotation).

The constraint evaluation model, strategy-implied policy, derivation chains, and credential presentation concerns from `authentication.md` are not replaced. They are retained as a FleetShift-specific policy layer on top of these standards.

## Motivation

The current design in `authentication.md` achieves strong security properties -- user-level signing, externally anchored trust, zero-trust platform, comprehensive constraint evaluation -- but it does so by building several complex subsystems from scratch:

1. **Key lifecycle.** Long-lived per-user signing keys, OS keychain integration, key binding bundles (an IdP-issued token with a claim referencing the user's public key), binding TTLs and renewal ceremonies.
2. **Key-to-identity binding.** The JWT binding bundle protocol, with its security-vs-availability tension around JWKS history, IdP key rotation causing mass re-binding, and the open question of whether to use external key registries instead.
3. **Addon signing key management.** Separate CA management via SPIFFE/SPIRE, Kubernetes CSR API, cert-manager, or admin-provisioned keys.
4. **Trust root distribution.** Ad hoc mechanisms (pinned CA bundles by value, publication points by reference) for getting trust anchors to delivery agents.
5. **Transparency and audit.** Platform-controlled audit trails that a compromised platform could tamper with.

Many of the open questions in `authentication.md` stem from the complexity of these subsystems:

- "Trust anchor distribution -- this might be solved but it is tricky to think through end to end"
- "Trust bundle rotation: what is the acceptable overlap window for addon CA rotation?"
- "Root user -- there should be some non-IdP issued credential or out of band channel for configuring IdP trust"
- "Can different key registries be pluggable?"
- "Real OIDC key binding: the POC models key binding as a simple proof-of-possession over a raw public key"
- The general tension between IdP key rotation, JWKS history, binding TTLs, and availability

This alternative architecture addresses these by adopting standards that were designed to solve exactly these problems.

## The three layers

### Layer 0: TUF -- trust root distribution

**What it is.** [TUF (The Update Framework)](https://theupdateframework.github.io/specification/latest/) is a framework for securing software update systems. It defines a metadata structure with four roles (root, targets, snapshot, timestamp) that together provide secure distribution of trust configuration with protection against rollback, freeze, mix-and-match, and arbitrary software attacks.

**What it does here.** TUF distributes and rotates trust root material to delivery agents: Fulcio root certificates, Rekor public keys, addon trust anchor configuration, and tenant-scoped identity constraints. The TUF root keys serve as the tenant administrator's offline escape hatch for trust reconfiguration.

### Layer 1: Sigstore -- identity-bound signing and transparency

**What it is.** [Sigstore](https://www.sigstore.dev/) is an ecosystem for signing, verifying, and protecting software artifacts. Its core components are:

- **[Fulcio](https://docs.sigstore.dev/certificate_authority/certificate-issuing-overview/)**: A certificate authority that issues short-lived X.509 code signing certificates (valid for ~10 minutes) based on OIDC identity tokens. The certificate binds the signer's OIDC identity to an ephemeral public key.
- **[Rekor](https://docs.sigstore.dev/logging/overview/)**: An immutable, append-only transparency log that records signing events. It provides signed entry timestamps (SETs) and cryptographic inclusion proofs.
- **[Cosign](https://docs.sigstore.dev/cosign/verifying/attestation/)**: Tooling for signing and verifying artifacts, including in-toto attestations wrapped in [DSSE (Dead Simple Signing Envelope)](https://github.com/secure-systems-lab/dsse) format.

**What it does here.** Sigstore replaces the custom key lifecycle and key-to-identity binding with "keyless" signing: the signer authenticates via OIDC, gets an ephemeral certificate from Fulcio, signs the content, and discards the key. The transparency log provides non-repudiation and temporal proof. This eliminates key binding bundles, binding TTLs, renewal ceremonies, and the IdP key rotation problem.

### Layer 2: OCI registry -- content-addressed storage and distribution

**What it is.** The [OCI image specification](https://specs.opencontainers.org/image-spec/?v=v1.1.0) supports packaging non-image content as artifacts, identified by custom `artifactType` values. The [OCI distribution specification v1.1](https://specs.opencontainers.org/distribution-spec/?v=v1.1.1) adds a `subject` field on manifests and a referrers API (`GET /v2/<name>/referrers/<digest>`) that returns all manifests whose `subject` matches a given digest, creating a discoverable DAG of related artifacts.

**What it does here.** FleetShift objects (deployment intents, managed resource specs, addon-rendered manifests, placement evidence, update attestations) are stored as OCI artifacts in a registry. Sigstore bundles (signatures, certificates, Rekor proofs) are attached as referrer artifacts. The registry serves as the distribution mechanism for both the signed artifacts and their verification material, replacing the custom `VerificationBundle` carried in-band.

## What changes

### Key lifecycle is eliminated

The largest complexity reduction. The current design requires:

- Per-user long-lived key pair generation (Ed25519 in the POC, ECDSA via OS keychain in the design)
- Key binding bundles: `{key_binding_doc, key_binding_signature, jwt}` stored on the platform, with 30-90 day TTL
- Renewal ceremonies before TTL expiry (re-authenticate to IdP, create fresh bundle)
- IdP key rotation causing mass re-binding of all user keys signed under the rotating JWKS key
- The unresolved tension between keeping JWKS history (availability, but undermines rotation security) and discarding it (security, but causes PausedAuth toil)
- The open question of whether platform distribution or external key registries are better

**With Fulcio, all of this goes away.** At signing time:

1. The user (or addon) authenticates to a Fulcio instance via their OIDC provider
2. Fulcio verifies the OIDC token against the provider's JWKS, checks proof-of-possession of the submitted public key, and issues a short-lived X.509 certificate (~10 minutes) with the OIDC identity embedded as a Subject Alternative Name (SAN)
3. The signing event is recorded to Rekor, which returns a Signed Entry Timestamp (SET) and an inclusion proof
4. The signer signs the content with the ephemeral private key, then discards the key
5. The result is a sigstore bundle: the signature, the Fulcio certificate, and the Rekor entry (SET + inclusion proof)

There is no long-lived key. There is no binding to maintain. There is no renewal. IdP key rotation has no effect because there are no stored key bindings to invalidate -- each signing ceremony is self-contained.

The `KeyBinding` type in `poc/attestation/hybrid/model.py` and its proof-of-possession verification are replaced by Fulcio certificate chain validation. The `make_key_binding` helper in `build.py` is no longer needed.

### User signing flow

**Current flow (from `authentication.md` and POC):**

1. User generates key pair, stores private key in OS keychain
2. User authenticates to IdP, gets a token with a claim referencing their public key (e.g. via a public key claim, a key fingerprint, or a reference to where the key can be fetched)
3. User signs `{public_key, jwt.sub, jwt.iss, timestamp}` with the private key (proof of possession)
4. The bundle `{key_binding_doc, key_binding_signature, jwt}` stored on platform, renewed every 30-90 days
5. On each deploy: user signs `hash(intent)` with stored private key
6. Delivery agent validates: JWT signature against IdP JWKS, subject match, key reference match, binding PoP, signature over intent, temporal validity

**Proposed flow:**

1. On each deploy: user authenticates to Fulcio via their tenant's OIDC provider (standard OIDC, no key-referencing claims required in the token)
2. Fulcio issues ephemeral certificate binding OIDC identity to ephemeral key
3. User signs the attestation payload (DSSE envelope) with ephemeral key
4. Signing event recorded to Rekor
5. Delivery agent validates: Fulcio certificate chain (against TUF-distributed Fulcio root), Rekor inclusion proof (against TUF-distributed Rekor key), OIDC identity extraction (see "Signer identity model" below), trust anchor constraint evaluation

For CLI users, the experience is comparable: `fleetshift deploy` triggers an OIDC authentication flow (browser redirect or device code) instead of a keychain prompt. The cryptographic plumbing changes but the user-facing interaction remains "authenticate, then deploy."

**Passkeys / WebAuthn.** The current design in `authentication.md` uses the passkey to directly sign `hash(intent)` -- the WebAuthn challenge IS the content hash, so the hardware authenticator directly authorizes the specific deployment content. With keyless signing, this property is lost: the passkey authenticates the OIDC flow to Fulcio, and the ephemeral key signs the content. The hardware authenticator has no awareness of what is being signed by the ephemeral key. This is a genuine regression in user-consent binding. However, the WebAuthn-based intent signing in `authentication.md` is itself unvalidated as practical (passkey providers may not support arbitrary challenge data as a signing surface in the way the design assumes). Given this, we treat the passkey interaction as an open design area for both architectures rather than claiming equivalence. If a content-bound approval step is needed, it would require an additional mechanism (e.g., a confirmation dialog with content hash displayed before OIDC authentication proceeds).

### Addon signing flow

The current design devotes significant space to addon key lifecycle: SPIFFE/SPIRE, Kubernetes CSR API, cert-manager, admin-provisioned keys, and the tradeoffs between them. The JWT key binding bundle model is acknowledged as "likely impractical in practice" for addons.

**Fulcio supports workload identity.** Fulcio can issue certificates based on several workload identity token types:

- **Kubernetes ServiceAccount tokens**: OIDC tokens include namespace, pod, and service account information. The certificate SAN is `https://kubernetes.io/namespaces/{namespace}/serviceaccounts/{sa-name}`.
- **SPIFFE SVIDs**: SPIFFE ID is used as a URI SAN, scoped to a trust domain. The Fulcio configuration specifies `SPIFFETrustDomain` and tokens must match.
- **CI/CD workflow tokens**: GitHub Actions, GitLab CI/CD tokens with workflow and repository information.

This means addons can sign through Fulcio the same way users do. The addon authenticates with its workload identity token, gets an ephemeral certificate, signs the output (rendered manifests, placement decisions), and the signing event is logged to Rekor. The delivery agent verifies the same way regardless of whether the signer is a user or an addon.

The entire "Addon key lifecycle" section of `authentication.md` -- SPIFFE/SPIRE integration, CSR API, cert-manager, admin-provisioned keys, JWT key binding bundles for addons, addon key distribution to the delivery agent -- collapses to: "addons authenticate to Fulcio via workload identity."

There is a deployment nuance. SPIFFE-based Fulcio issuance requires the Fulcio instance to be configured with the SPIFFE trust domain. Kubernetes SA token issuance requires the OIDC issuer URL of the cluster's SA token issuer to be configured in Fulcio. Both are standard Fulcio configuration -- not custom FleetShift infrastructure.

### Attestation envelope format

The current POC defines a custom `Attestation(input, output)` model with custom `SignedInput`, `Signature`, `KeyBinding`, and `OutputConstraint` types.

The [sigstore bundle format](https://docs.sigstore.dev/about/bundle/) (v0.3) provides a standard structure for everything needed to verify a signature:

```
Bundle:
  mediaType: "application/vnd.dev.sigstore.bundle.v0.3+json"
  verificationMaterial:
    certificate: { rawBytes: <DER-encoded Fulcio leaf cert> }
    tlogEntries: [{ logIndex, logId, kindVersion, integratedTime,
                    inclusionPromise, inclusionProof, canonicalizedBody }]
  content: <one of>
    messageSignature: { messageDigest, signature }
    dsseEnvelope: { payload, payloadType, signatures }
```

For attestations (as opposed to bare signatures), cosign uses the DSSE content variant. The DSSE envelope wraps an in-toto statement with `payloadType: "application/vnd.in-toto+json"`. The in-toto statement has a `predicateType` and typed `predicate` body.

FleetShift would define custom in-toto predicate types for its content:

| Content | Predicate type (illustrative) |
|---|---|
| Deployment intent | `https://fleetshift.io/attestation/deployment/v1` |
| Managed resource | `https://fleetshift.io/attestation/managed-resource/v1` |
| Placement evidence | `https://fleetshift.io/attestation/placement/v1` |
| Update (spec_update) | `https://fleetshift.io/attestation/update/v1` |
| Fulfillment relation | `https://fleetshift.io/attestation/fulfillment-relation/v1` |

The `in-toto statement` structure:

```json
{
  "_type": "https://in-toto.io/Statement/v1",
  "subject": [
    {
      "name": "deployment/my-app",
      "digest": { "sha256": "<content-hash>" }
    }
  ],
  "predicateType": "https://fleetshift.io/attestation/deployment/v1",
  "predicate": {
    "deployment_id": "my-app",
    "manifest_strategy": { "type": "addon", "addon_id": "capi-provisioner" },
    "placement_strategy": { "type": "predicate", "expression": "..." },
    "output_constraints": [ { "name": "...", "expression": "..." } ],
    "valid_until": 1717200000,
    "expected_generation": 3
  }
}
```

The in-toto `subject` field identifies *what* is being attested about, by content digest. The `predicate` carries the typed content that the signer authorizes. The output constraints and validity bound that are currently part of the POC's `signed_input_envelope` (see `policy.py:signed_input_envelope`) move into the predicate body. They are still part of what the signer signs.

**Content integrity binding.** The `subject[0].digest.sha256` in the in-toto statement MUST match the OCI layer digest of the corresponding artifact (e.g., the deployment content blob in `layers[0].digest` of the OCI manifest). This is how the delivery agent binds the signed attestation to the actual content it fetched from the registry: it fetches the OCI artifact, computes or reads the layer digest, and verifies that the in-toto subject digest in the signed DSSE envelope matches. If they diverge, the attestation does not cover the fetched content and verification fails. This replaces the current POC's direct `hash(intent)` comparison, with the OCI content-addressing layer providing the same integrity guarantee.

**Two digest identities per object.** An OCI artifact has two meaningful digests: the *manifest digest* (the SHA-256 of the OCI image manifest JSON) and the *layer/payload digest* (the SHA-256 of the content blob in `layers[0]`). These serve different purposes:

- The **manifest digest** is the identity used by the OCI referrers API. A sigstore bundle's `subject.digest` points to the manifest digest of the artifact it attests. The referrers walk discovers artifacts by manifest digest.
- The **layer digest** is the identity used for content integrity. The in-toto statement's `subject[0].digest` is the layer digest -- the hash of the actual content bytes. This is what the signer authorizes.

The link between them is the manifest itself: the manifest's `layers[0].digest` field contains the layer digest. The delivery agent fetches the manifest (by manifest digest), reads the layer digest from it, fetches the layer blob, and verifies the in-toto subject digest matches. Both digests are deterministic and content-addressed, so the mapping is stable.

### What stays the same

The following are FleetShift-specific concerns that layer on top of the sigstore/OCI/TUF foundation and are retained unchanged:

- **Strategy-implied constraints** (`policy.py:derive_strategy_constraints`): The delivery agent derives verification constraints from the signed content's strategy declarations. `inline` manifests must match exactly; `addon` manifests must be signed by the named addon; `predicate` placement evaluates a CEL expression against target identity; `addon` placement requires signed evidence. Unknown strategy types fail closed. This logic has no equivalent in sigstore.

- **CEL output constraints** (`model.py:OutputConstraint`): Explicit signed CEL expressions evaluated over `{input, output, target, action, placement}` at verification time. These compose with strategy-implied constraints and are part of the signer's authorization, not just data.

- **Derivation chains** (`model.py:DerivedInput`, `mutation.py`): Update attestations that reconstruct new content versions from a prior input and a signed `spec_update`. CEL derivation expressions, preconditions, constraint inheritance.

- **Placement enforcement and removal protection**: Predicate-based self-assessment, addon-signed placement evidence, deployment-id binding, sticky deployments.

- **Credential presentation** (run-as-me, run-as-workload, run-as-platform): Orthogonal to provenance. Not affected.

- **PausedAuth**: When verification fails for any reason (expired certificate, stale trust configuration, missing signature, constraint failure), the fulfillment transitions to PausedAuth. Any layer failure surfaces as PausedAuth.

- **Transport**: Standard (gRPC) and hardened (buffered) transport remain. The OCI registry serves as a natural hardened transport option (see below).

- **Anti-replay via expected_generation**: Monotonic generation counters for replay protection at the delivery agent.

## OCI registry as storage and distribution

### FleetShift objects as OCI artifacts

Each FleetShift object type is stored as an OCI artifact with a FleetShift-specific `artifactType`. The OCI image manifest's [artifact usage guidelines](https://github.com/opencontainers/image-spec/blob/main/artifacts-guidance.md) support this: set `artifactType` to a custom media type, use the empty descriptor for `config` (if no separate config blob is needed), and place the content in `layers`.

Example manifest for a deployment intent:

```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "artifactType": "application/vnd.fleetshift.deployment.v1+json",
  "config": {
    "mediaType": "application/vnd.oci.empty.v1+json",
    "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
    "size": 2
  },
  "layers": [
    {
      "mediaType": "application/vnd.fleetshift.deployment.v1+json",
      "digest": "sha256:<content-digest>",
      "size": 1234
    }
  ]
}
```

The layer contains the serialized deployment content (the same content that would appear in `DeploymentContent.to_dict()` in the POC).

### Referrers API for the attestation graph

When a sigstore bundle (user signature, Fulcio certificate, Rekor proof) is attached to a deployment artifact, cosign stores it as a separate OCI artifact with a `subject` field pointing to the deployment's digest. The referrers API discovers it:

```
GET /v2/tenant/deployments/my-app/referrers/sha256:<deployment-digest>
```

Response: an OCI index listing all artifacts whose `subject.digest` matches the queried digest. The referrers API returns **direct referrers only** -- it does not recursively discover nested artifacts. The attestation graph for a deployment is multi-level:

- **Level 0 (deployment digest):** query referrers, discover:
  - Sigstore bundles (user signatures): `artifactType: application/vnd.dev.sigstore.bundle.v0.3+json`
  - Addon-signed manifests: `artifactType: application/vnd.fleetshift.addon-manifests.v1+json`
  - Placement evidence: `artifactType: application/vnd.fleetshift.placement-evidence.v1+json`
- **Level 1 (discovered artifact digests):** for each addon manifest or placement evidence artifact, query referrers to discover their sigstore bundles
- **Level N:** if any discovered artifact has its own referrers (e.g., a derivation chain node), continue walking

The delivery agent implements a **recursive referrers walk** to assemble the full attestation graph:

1. Start with the deployment artifact's digest
2. Query `GET /v2/<name>/referrers/<digest>` with `artifactType` filtering to limit results to known FleetShift and sigstore types
3. For each discovered artifact, fetch its manifest and content
4. For each discovered artifact that may have its own attestations (addon manifests, placement evidence, derivation chain nodes), recurse: query its referrers
5. Enforce a maximum traversal depth (bounded by the derivation chain depth limit) and track visited digests to prevent cycles
6. Handle pagination via the `Link` header per the OCI distribution spec

This replaces the custom `VerificationBundle` in `verify.py`, which manually assembles inputs, attestations, and fulfillment relations into a single object. The recursive walk is more complex than a single query, but the graph structure is well-defined by FleetShift's artifact types and the protocol is deterministic.

**Registry compatibility.** The referrers API is defined in OCI distribution spec v1.1. Registries that support it include Harbor (v2.6+), Zot, ACR, GCR/Artifact Registry, and ECR. For registries that do not yet support the referrers API natively, the [OCI referrers tag schema](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#listing-referrers) defines a fallback: referrers are discoverable via a tag of the form `sha256-<digest>`. Clients like [ORAS](https://oras.land/) implement both paths transparently. FleetShift should support the tag-schema fallback to avoid constraining registry choice, or explicitly document minimum registry requirements.

### Derivation chains as reference chains

The `DerivedInput` model (prior input -> update attestation -> derived content) maps to OCI `subject` references:

- Generation 1: `deployment@sha256:aaa` (base deployment artifact, signed by user)
- Update v2: `update-v2@sha256:bbb` with `subject: deployment@sha256:aaa` (update artifact, signed by upgrade planner, containing the `spec_update` payload)
- The delivery agent discovers the update via the referrers API on the base deployment's digest

The update attestation is itself a signed OCI artifact (with its own sigstore bundle). The recursive verification in `DerivedInput.verify()` maps to: walk the referrers chain, verify sigstore bundles at each node, then apply FleetShift derivation logic.

Concretely, at each node in the derivation chain:

1. **Sigstore verification**: verify the sigstore bundle (Fulcio certificate chain, Rekor inclusion proof, signature over DSSE envelope)
2. **Content integrity**: verify that the in-toto `subject[0].digest` matches the OCI layer digest of the artifact content
3. **Identity and trust anchor evaluation**: extract signer identity from Fulcio certificate extensions (issuer from OID `.1.8`, token subject from OID `.1.24`), evaluate against trust anchor constraints
4. **Precondition check**: evaluate the update attestation's preconditions (CEL expressions from `DerivedInput.preconditions`) against the prior input's content -- these are FleetShift-specific predicates that gate whether the update is valid given the prior state
5. **Constraint inheritance**: the derived content inherits output constraints from the prior input, combined with any new constraints introduced by the update attestation -- this is the `derive_strategy_constraints` logic from `policy.py`
6. **Derivation expression**: apply the CEL derivation expression to produce the derived content from the prior input and the update payload

Steps 4-6 are unchanged from the current POC. Steps 1-3 replace the POC's `SignedInput.verify()` (signer consistency, key binding, raw signature verification) with sigstore bundle verification.

### Multi-signature

The DSSE specification supports multiple signatures on a single envelope, but the sigstore bundle format restricts each bundle to a single signature. Multi-signature for FleetShift is achieved by attaching multiple sigstore bundles to the same artifact via the OCI referrers API -- each signer produces their own bundle, each referencing the same deployment artifact's digest. This supports the multi-signature model described in `authentication.md`: multiple authorized users sign the same deployment for high availability and audit.

The delivery agent discovers all sigstore bundles referencing a deployment via the referrers API (filtered by `artifactType`), verifies each, and applies the configured policy (any-of, quorum, etc.).

### Transport

The OCI registry serves as a natural option for the "hardened" buffer transport described in `authentication.md`:

| Transport | Mechanism | Latency | Coupling |
|---|---|---|---|
| Standard | gRPC over fleetlet connection, sigstore bundle inline | Low | Direct |
| OCI registry | Delivery agent pulls from registry | Medium | Decoupled |
| Buffer (S3/Kafka/NATS) | Existing design | Medium | Decoupled |

The OCI registry option has advantages over ad hoc buffers: content-addressable addressing (no inventing key schemes), built-in authentication and authorization, the referrers API for attestation graph discovery, replication for distribution across network boundaries, garbage collection and retention policies, and mature client libraries ([ORAS](https://oras.land/), [go-containerregistry](https://github.com/google/go-containerregistry)).

For the standard gRPC transport, the sigstore bundle can still be sent inline for low-latency delivery. The OCI registry serves as the **attestation distribution layer** -- the authoritative source for signed artifacts and their verification material. The platform's own database remains the authoritative store for deployment specs, fulfillment state, and operational metadata. The registry does not replace the platform database; it provides a content-addressed, replicable distribution plane for the attestation graph that the delivery agent needs for verification. gRPC is the real-time notification channel that tells the delivery agent a new delivery is available; the registry is where the agent fetches the signed content and verification material.

### Network curtain and replication

For the provider/consumer network curtain (factory clusters with restricted fleetlet profiles), OCI registry replication is a well-understood pattern. Implementations like [Harbor](https://goharbor.io/) and [Zot](https://zotregistry.dev/) support pull-through proxying and cross-site replication:

- A management-side registry receives pushes from the platform
- A factory-side registry replicates from the management-side registry
- The fleetlet pulls from the factory-side registry, never reaching out to the management network

The replication target is the same for both FleetShift attestation artifacts and the container images the deployment references.

## TUF for trust root management

### The problem TUF solves

The delivery agent needs trust roots to verify anything: Fulcio root certificates, Rekor public keys, and tenant-scoped identity/trust anchor configuration. These trust roots must be:

1. Bootstrapped to new delivery agents securely
2. Rotated when keys change
3. Consistent -- the delivery agent must see a coherent snapshot of all trust roots at a point in time
4. Fresh -- the delivery agent must detect stale trust configuration
5. Revocable -- compromised trust anchors must be removable
6. Independent of the platform -- the platform is a courier, not a trust authority

TUF provides all of these properties through its four-role metadata structure.

### Role mapping

| TUF role | FleetShift purpose | Key storage | Rotation |
|---|---|---|---|
| Root | Tenant administrator escape hatch | Offline (hardware token, air-gapped) | N-of-M threshold, chain of trust via versioned root.json |
| Targets | Trust anchor registry | Delegated to sub-roles | Standard TUF targets delegation |
| Snapshot | Consistent view of all trust anchors | Online (automated) | Frequent, prevents mix-and-match |
| Timestamp | Freshness guarantee | Online (automated) | Frequent (e.g. hourly), prevents freeze |

### Targets structure

The TUF targets role signs metadata describing which trust configuration files are current. In FleetShift, the "targets" are trust configuration artifacts:

```
targets.json
  ├── fulcio-root.pem        (Fulcio root certificate for this tenant's signing infra)
  ├── rekor-public-key.pem   (Rekor verification key)
  ├── ctlog-public-key.pem   (CT log verification key, if CT is enabled -- see below)
  ├── user-trust-anchor.json (OIDC issuer, subject constraints, CEL predicates)
  ├── addon-capi.json        (Trust anchor for capi-provisioner addon)
  ├── addon-upgrade.json     (Trust anchor for upgrade-planner addon)
  └── ...
```

Each entry in the targets metadata includes the file's cryptographic hash and size. The targets metadata file itself carries a version number (incremented on each update); individual target entries do not have their own version -- coherent versioning is provided by the snapshot role, which pins the version of every targets metadata file atomically. The delivery agent downloads these files, verifies the TUF signature chain, and uses them as its trust store. This set of files constitutes the delivery agent's `TrustedRoot` -- the sigstore-go equivalent is constructed from these TUF-distributed artifacts rather than from the public sigstore instance.

**Certificate Transparency log: deployment choice.** Fulcio can optionally embed a Signed Certificate Timestamp (SCT) from a Certificate Transparency log in issued certificates. The CT log provides auditability of certificate issuance separate from Rekor's signature event transparency. For self-hosted Fulcio deployments, operating a CT log is an additional infrastructure cost. The Fulcio specification notes that private deployments may omit CT if an alternative auditing mechanism exists -- Rekor itself provides this alternative, since every signing event (including the Fulcio certificate) is logged to Rekor.

FleetShift supports two modes:

- **CT-enabled (higher assurance):** The TUF targets include `ctlog-public-key.pem`. Fulcio is configured with a CT log backend. The delivery agent verifies the embedded SCT in the Fulcio certificate against the TUF-distributed CT log key. This provides two independent transparency mechanisms (CT for certificate issuance, Rekor for signing events), making undetected forgery harder.
- **CT-omitted (Rekor-only transparency):** The TUF targets omit `ctlog-public-key.pem`. Fulcio is configured without a CT log. The delivery agent skips SCT verification. Rekor alone provides transparency -- forged certificates still result in Rekor entries detectable by monitors. This reduces operational cost at the expense of a single point of transparency.

The choice is per-deployment and encoded in the TUF targets: the presence or absence of `ctlog-public-key.pem` determines the verification behavior.

### Delegation

TUF's delegation model can scope authority over different parts of the trust configuration:

```
tenant-admin (top-level targets role)
  ├── signing-infra/ (delegated role, paths: ["fulcio-root*", "rekor-*", "ctlog-*"])
  │     Keys: tenant admin keys (same keys as the top-level targets role, or a dedicated subset)
  │     Content: which Fulcio/Rekor/CT instances the delivery agent trusts
  │
  ├── user-trust/ (delegated role, paths: ["user-trust-*"])
  │     Keys: tenant identity admin keys
  │     Content: which Fulcio OIDC issuers/subjects are trusted for user signing
  │
  └── addon-trust/ (delegated role, paths: ["addon-*"])
        Keys: tenant addon admin keys
        Content: which workload identities are trusted for addon signing
```

The signing infrastructure trust roots (`fulcio-root.pem`, `rekor-public-key.pem`, `ctlog-public-key.pem`) remain under tenant-controlled keys. This is deliberate: `authentication.md` establishes that "the platform is never a trust root," and delegating Fulcio/Rekor trust to platform operator keys would reintroduce exactly the provider-controlled trust the design aims to avoid. A compromised platform operator with delegated authority over signing infrastructure roots could redirect the delivery agent to trust a malicious CA -- the same class of attack the trust model is designed to prevent.

If the platform provider operates the Fulcio/Rekor infrastructure on behalf of the tenant, the tenant admin still controls *which* infrastructure the delivery agent trusts. The provider can recommend or provision the infrastructure, but the tenant admin signs the TUF targets metadata that names the trusted Fulcio root and Rekor key. This is analogous to how tenants today configure their own IdP trust -- the IdP may be operated by a third party, but the tenant controls the trust relationship.

The TUF `paths` patterns enforce that each delegated role can only modify its scoped targets. A compromised addon admin cannot modify user trust configuration or signing infrastructure roots. TUF's `terminating` flag on delegations can enforce strict boundaries.

### Specific problems TUF resolves

**Trust anchor distribution.** The delivery agent bootstraps with a TUF root.json (shipped out-of-band, e.g. as part of the fleetlet binary or provisioning). From that single artifact, it discovers all trust anchors by walking the TUF metadata chain. No bespoke trust distribution protocol.

**Trust bundle rotation.** During an addon CA rotation: (1) admin signs new targets metadata listing both old and new CA, (2) delivery agents pick up the new configuration and trust both, (3) addons migrate to the new CA, (4) admin signs targets metadata removing the old CA, (5) delivery agents pick up the removal. The overlap window is explicitly controlled by the targets metadata version. TUF's rollback protection ensures delivery agents never go backward.

**Root user escape hatch.** TUF's root role IS the non-IdP escape hatch that `authentication.md` identifies as an open question. The root keys are offline, not IdP-derived, and are used to bootstrap or reconfigure all other trust. If the IdP is compromised or misconfigured, the TUF root key can sign a new trust configuration. TUF provides a well-defined ceremony and rotation protocol for this rather than leaving it as a bespoke mechanism.

**Freeze attack detection.** TUF's timestamp role is frequently re-signed (online key) and has an expiration. If the delivery agent can't obtain fresh timestamp metadata (e.g. network partition, registry unavailable), it knows its trust configuration is potentially stale. This composes with PausedAuth: the delivery agent refuses to apply new deliveries until trust metadata is refreshed.

**Mix-and-match prevention.** TUF's snapshot role signs a manifest listing the current version of every targets metadata file. A compromised platform cannot serve a delivery agent a user trust anchor from version 5 alongside an addon trust anchor from version 3 -- the snapshot pins all versions atomically.

### Where TUF metadata lives

TUF metadata is just signed JSON files. It can be served from:

- A static file server or CDN
- The same OCI registry used for FleetShift artifacts (TUF metadata as OCI artifacts)
- An S3 bucket or similar object store

This is the same pattern sigstore itself uses: the [sigstore TUF root](https://github.com/sigstore/root-signing) is distributed as a set of static JSON files that cosign ships with, and updated via the TUF client update protocol.

## Verification flow

### Delivery agent verification

The delivery agent's verification pipeline becomes:

**Phase 0: Trust root refresh (TUF).** Before verifying any delivery, the delivery agent runs the [TUF client update workflow](https://theupdateframework.github.io/specification/latest/#detailed-client-workflow): fetch timestamp.json, check freshness, update snapshot.json, update targets metadata, download any changed trust configuration files. This ensures the trust store is current. If the timestamp has expired and cannot be refreshed, verification is refused (PausedAuth). Using go-tuf v2 (`github.com/theupdateframework/go-tuf/v2`, currently at v2.4.1), this is implemented via the `Updater` package.

**Phase 1: Sigstore verification.** For each sigstore bundle in the attestation graph:

1. Validate the Fulcio certificate chain against the TUF-distributed Fulcio root certificate
2. If CT is enabled (i.e., `ctlog-public-key.pem` is present in TUF targets): verify the SCT embedded in the Fulcio certificate against the TUF-distributed CT log key
3. Verify the Rekor inclusion proof against the TUF-distributed Rekor public key
4. Verify the Rekor entry's `integratedTime` falls within the Fulcio certificate's validity window (this confirms the signature was created while the certificate was valid; the SET signs the log entry including this timestamp)
5. Verify the signature over the DSSE envelope / message digest
6. Extract the signer identity from the Fulcio certificate (see "Signer identity model" below)
7. Verify content integrity: the in-toto `subject[0].digest` matches the OCI layer digest of the fetched artifact

This phase replaces the current POC's signer consistency check, trust anchor key lookup, key binding verification, and signature verification steps in `SignedInput.verify()`.

Using [sigstore-go](https://pkg.go.dev/github.com/sigstore/sigstore-go) (currently at v1.1.4), this is implemented via the standard verification API with a custom `TrustedRoot` (constructed from TUF-distributed material rather than the public sigstore instance). The `TrustedRoot` includes the Fulcio root certificate, Rekor public key, and (if CT-enabled) the CT log public key.

**Signer identity model.** Fulcio certificates encode identity across multiple extensions and the SAN, and the encoding varies by OIDC issuer type. The relevant [Fulcio OID extensions](https://github.com/sigstore/fulcio/blob/main/docs/oid-info.md) are:

- **OID 1.3.6.1.4.1.57264.1.8** (Issuer V2): the OIDC token's `iss` claim, DER-encoded per RFC 5280. This identifies which identity provider issued the token. (Note: the original issuer extension `.1.1` is deprecated upstream; `.1.8` is the current version.)
- **OID 1.3.6.1.4.1.57264.1.24** (Token Subject): the raw `sub` claim from the OIDC ID token, preserved as-is regardless of how the provider maps it to other certificate fields. This is the most stable identifier for the signer across provider types.
- **SAN (Subject Alternative Name)**: a provider-specific projection of identity. The SAN encoding varies by issuer type:
  - Email-based human issuers: email address (e.g., `alice@example.com`)
  - Kubernetes ServiceAccount: URI SAN (e.g., `https://kubernetes.io/namespaces/{ns}/serviceaccounts/{sa}`)
  - SPIFFE: URI SAN (e.g., `spiffe://trust-domain/workload-id`)
  - GitHub Actions: URI SAN (e.g., `https://github.com/owner/repo/.github/workflows/...`)

FleetShift's canonical signer identity for trust anchor evaluation should be the tuple `(issuer, token_subject)` -- extracted from extensions `.1.8` and `.1.24` respectively. The SAN provides additional context (human-readable identity, workload URI) but should not be the primary key for policy evaluation, since its format is provider-dependent. Trust anchor constraints (CEL predicates) can reference both the canonical identity and the SAN where needed for provider-specific matching.

This means `sigstore-go` verification should extract the full certificate extension set rather than relying solely on the SAN-based identity matching that cosign uses by default. The `sigstore-go` verification API supports custom identity policies that can inspect arbitrary certificate extensions.

**Phase 2: Identity and trust anchor evaluation.** The extracted signer identity (issuer + token subject from Fulcio extensions) is checked against the TUF-distributed trust anchor configuration. Trust anchor constraints (CEL predicates from `TrustAnchorConstraint`) are evaluated against the signer's identity and the attested content. This phase is FleetShift-specific -- sigstore verifies the certificate chain but does not evaluate FleetShift's scoped trust model.

**Phase 3: Content constraint evaluation (unchanged).** Strategy-implied constraints are derived from the signed content's strategy declarations (`policy.py:derive_strategy_constraints`). Explicit output constraints (signed CEL expressions) are combined with strategy-implied constraints. All constraints are evaluated over `{input, output, target, action, placement}`. This is the same logic as today.

**Phase 4: Derivation chain verification (unchanged).** For `DerivedInput`: recursively verify the prior input and update attestation (each going through phases 1-3), check preconditions, apply derivation expression, verify identity preservation.

**Phase 5: Generation check (unchanged).** If `expected_generation` is present, check against local fulfillment state.

### Concrete type mapping

| Current POC type | Replacement |
|---|---|
| `KeyPair` (Ed25519, `crypto.py`) | Ephemeral key generated at signing time, discarded after use |
| `KeyBinding` (`model.py`) | Fulcio X.509 certificate (embedded in sigstore bundle) |
| `Signature` (`model.py`) | DSSE signature within sigstore bundle |
| `OutputSignature` (`model.py`) | Sigstore bundle attached to the output artifact via OCI referrers |
| `TrustAnchor.known_keys` (`model.py`) | TUF-distributed trust anchor config (OIDC issuer + subject constraints) |
| `TrustStore` (`verify.py`) | TUF targets metadata + downloaded trust configuration files |
| `VerificationBundle` (`verify.py`) | OCI referrers graph (artifacts + attached sigstore bundles) |
| `make_key_binding` (`build.py`) | Not needed -- Fulcio handles identity-to-key binding |
| `content_hash` (`crypto.py`) | OCI content digest (SHA-256) |

Types that remain unchanged: `DeploymentContent`, `ManagedResourceContent`, `OutputConstraint`, `TrustAnchorConstraint`, `TrustAnchorSubject`, `StrategySpec`, `DerivedInput`, `PlacementEvidence`, `PutManifests`, `RemoveByDeploymentId`, `ManifestEnvelope`, `VerifiedInput`, `VerifiedOutput`, `VerificationResult`, `VerificationContext`, `FulfillmentState`.

## Fulcio deployment model

The current design's governing principle is: "the platform is never a trust root." A Fulcio instance IS a trust root -- a compromised Fulcio CA can issue certificates for any identity it is configured to trust. This needs careful architectural thought.

### Options

**Per-tenant Fulcio.** Each tenant runs (or has run for them) a Fulcio instance configured to trust only that tenant's OIDC issuer. The delivery agent's Fulcio root (distributed via TUF) is tenant-specific. Blast radius of a Fulcio compromise is tenant-scoped, matching the current design's principle that "compromise is tenant- or user-scoped."

**Shared Fulcio with tenant-scoped verification.** A single Fulcio instance accepts OIDC tokens from multiple configured issuers. The delivery agent trusts the shared Fulcio root but scopes verification by OIDC issuer claim in the certificate. **This option is incompatible with the current trust model.** A shared Fulcio CA is exactly the "platform-level root (CA, signing service)" that `authentication.md` rejects: a Fulcio compromise has cross-tenant blast radius, and Rekor provides after-the-fact *detectability* of forged certificates, not *prevention*. The "platform is never a trust root" principle requires that compromise be tenant-scoped. If this option is chosen, the architecture is explicitly relaxing that principle in exchange for operational simplicity -- that tradeoff must be a conscious tenant decision, not a default.

**Tenant-operated Fulcio.** The tenant operates Fulcio as part of their own infrastructure, like they operate their own IdP. The platform is not in the CA path at all. This is the strongest alignment with the current trust model but requires tenant sophistication. Might be viable for the same class of tenants who today operate their own SPIFFE/SPIRE infrastructure.

### Rekor's role in mitigating Fulcio compromise

Rekor provides a meaningful mitigation that the current key-binding-bundle model lacks: transparency. If a Fulcio instance is compromised and issues a forged certificate, the forged signing event appears in the Rekor log. Monitors (which can be operated by the tenant, independently of the platform) can detect unexpected signing events for their identities.

The current model has no equivalent transparency mechanism. A compromised platform that can also access the IdP could swap key bindings, and the only detection mechanism is the platform's own audit trail -- which the compromised platform controls.

When CT is enabled, Fulcio embeds a Signed Certificate Timestamp (SCT) from its Certificate Transparency log in the issued certificate. Additionally, the sigstore bundle includes a Rekor entry proving the signing event was logged. Together these provide two independent transparency mechanisms: a forged certificate must be logged to both the CT log and Rekor to produce a valid bundle. The attacker must choose between: (a) logging the forgery (detectable by CT and Rekor monitors) or (b) not logging it (rejected at verification because the bundle lacks valid proofs).

When CT is omitted (see "Certificate Transparency log: deployment choice" above), Rekor alone provides the transparency guarantee. Forged signing events are still logged and detectable by Rekor monitors. The tradeoff is that Rekor becomes a single point of transparency rather than one of two independent mechanisms.

## Offline and air-gapped verification

Sigstore "keyless" verification typically assumes online access to verify Rekor inclusion. The delivery agent may operate in disconnected environments, especially with the hardened buffer transport.

The sigstore bundle format addresses this. The bundle contains everything needed for offline verification:

- The Fulcio leaf certificate (identity-to-key binding)
- The Rekor entry with SET and inclusion proof (temporal proof and non-repudiation)
- The signature itself

The delivery agent verifies the bundle against locally cached trust roots (Fulcio root cert, Rekor public key, and CT log public key if CT-enabled -- all from TUF). No online access to Fulcio or Rekor is needed at verification time. The only online requirement is at signing time (the signer must reach Fulcio and Rekor), and at TUF refresh time (the delivery agent must periodically refresh its trust metadata).

If TUF refresh fails (network partition), the delivery agent's existing TUF metadata remains valid until the timestamp expires. This is a configurable freshness bound: short expiry means faster detection of compromise but lower tolerance for partitions; long expiry means the reverse. The tradeoff is the same as the current key binding TTL, but with a well-defined protocol.

## Open questions from `authentication.md` -- how this architecture addresses them

| Open question | How addressed |
|---|---|
| "Can different key registries be pluggable?" | Fulcio is the universal key-to-identity binding mechanism. The "registry" is the Fulcio instance, configured with the tenant's OIDC issuer. No pluggable registries needed. |
| "Real OIDC key binding" | Fulcio provides OIDC key binding natively. The certificate is issued based on a verified OIDC token and proof of possession of the ephemeral key. No separate binding bundle or key-referencing token claims needed. |
| "Trust anchor distribution" | TUF targets metadata, signed by tenant admin offline keys, distributed to delivery agents via the TUF update protocol. |
| "Trust bundle rotation: overlap windows" | TUF targets metadata versions explicitly control the overlap: list both old and new, then remove old. Rollback protection prevents regression. |
| "Root user escape hatch" | TUF root role. Offline keys, threshold signatures, chain-of-trust rotation. |
| "Multi-signature policy" | Multiple sigstore bundles attached to the same artifact via OCI referrers (one bundle per signer). The delivery agent discovers all bundles and applies FleetShift policy (any-of, quorum) on top. |
| "Derivation chain depth" | Unchanged -- still a FleetShift concern. Each node in the chain is a sigstore-signed OCI artifact, but the depth question is the same. |
| "Claims freshness" | Still relevant, and **not bounded by Fulcio certificate validity**. Sigstore bundles are designed to be verifiable indefinitely after the certificate expires -- the Rekor timestamp evidence proves the signature was created during the cert's validity window, but the signed artifact has no sigstore-imposed expiry. The 10-minute cert validity bounds *when* the OIDC claims were current (at signing time), not *how long* FleetShift accepts the signed deployment. Long-term authorization freshness remains a FleetShift policy concern: `valid_until` in the signed predicate, re-signing (cheap with keyless signing), or SCIM/CAEP checks provide the actual freshness bound. |

## Open questions specific to this alternative

- **Fulcio deployment model**: Per-tenant vs shared vs tenant-operated. Trade-offs between blast radius, operational cost, and tenant sophistication. This is the most significant architectural decision.
- **Fulcio availability**: Fulcio must be reachable at signing time. If Fulcio is down, no new signatures can be created. This is a new availability dependency (the current model has no CA dependency at signing time, only at key enrollment time). Fulcio can be deployed with high availability (multiple replicas, load balancing), but it is a dependency that doesn't exist today.
- **Rekor log management**: Who operates the Rekor instance? A per-tenant Rekor is operationally expensive. A shared Rekor instance raises questions about log partitioning and tenant isolation (all signing events from all tenants in one log). Rekor sharding may help. This needs investigation.
- **OCI registry dependency for hardened transport**: Using the OCI registry as a transport introduces a registry availability dependency. This is the same class of dependency as S3/Kafka/NATS in the current design but is worth noting explicitly.
- **In-toto statement vs custom envelope**: Adopting the in-toto statement format for FleetShift attestations provides ecosystem interoperability but introduces a dependency on the in-toto specification. The `subject` field (identifying what is being attested about, by digest) maps naturally to FleetShift's content model, but the mapping of `predicate` to FleetShift's richer constraint model needs careful design. In particular, the in-toto spec does not define how to handle CEL constraint evaluation or strategy-implied policy -- those remain FleetShift-specific predicate semantics.
- **Content-bound user approval**: With keyless signing, there is no mechanism equivalent to the current design's `hash(intent)` as WebAuthn challenge, where the hardware authenticator directly authorizes the specific content. The OIDC authentication step (which can use a passkey or biometric) authenticates the user but has no awareness of what the ephemeral key will sign. If content-bound approval is needed, an additional mechanism must be designed (e.g., a content hash displayed in the OIDC consent screen, or a separate confirmation step between authentication and signing). This is an open design area for both this architecture and the current design, since the WebAuthn-based approach in `authentication.md` is itself unvalidated as practical.
- **Addon workload identity onboarding**: Kubernetes ServiceAccount OIDC issuers and SPIFFE trust domains must be configured in the Fulcio instance. For a shared or provider-hosted Fulcio serving many tenants, this means trusting arbitrary customer-managed cluster SA issuers across tenant environments. The operational cost and security implications of this need investigation -- especially for fleetlet-local addons running behind the network curtain on factory clusters with diverse OIDC issuers.
- **GitOps signing flow**: With keyless signing, `fleetshift gitops sign` authenticates to Fulcio and produces a sigstore bundle. This bundle must be stored somewhere alongside the git content. Options: (a) stored in the git repo as a file (similar to current `.sig` file approach), (b) pushed to the OCI registry as a referrer artifact, (c) both. Option (b) is cleaner but requires the GitOps tooling to push to the registry, not just commit to git.
- **Latency at signing time**: Keyless signing adds network round-trips at signing time (Fulcio certificate issuance + Rekor log entry). Fulcio issuance is typically sub-second. Rekor entry creation is also sub-second for the public instance. For a self-hosted instance, latency depends on the backend (Trillian). This is unlikely to be noticeable in interactive flows (deploy from CLI or web UI) but may matter for high-throughput automated signing (e.g. addon rendering manifests for many deployments).

## Relationship to the current design

This document is an **alternative** to the key lifecycle, key binding, addon signing, trust distribution, and transparency mechanisms in `authentication.md`. It is not a replacement for the entire authentication model. The following concerns from `authentication.md` are orthogonal and unchanged:

- Credential presentation (run-as-me, run-as-workload, run-as-platform)
- PausedAuth semantics
- The delivery problem (time and space separation)
- Transport architecture (standard, hardened, CRD-based)
- Cluster-side delivery architecture
- IdP trust management and OIDC discovery
- Bootstrap-time privilege constraints
- Verification level (intent signing vs output signing)

The constraint evaluation model, strategy-implied constraints, CEL output constraints, derivation chains, placement enforcement, and anti-replay mechanisms from `authentication.md` and `poc/attestation/hybrid/` are retained as-is. They are the FleetShift-specific policy layer that sits on top of whichever signing and trust infrastructure is used.

### Relationship to the single-pod viability invariant

The `core_model.md` design invariant states that the platform kernel must function correctly as a single pod with no mandatory external service dependencies. This alternative architecture introduces external service dependencies (Fulcio, Rekor, optionally a CT log, TUF metadata publication, an OCI registry) that are not compatible with that constraint taken literally.

However, the single-pod invariant is a constraint on the **core kernel** -- it encourages lightweight, flexible design for the strict core and ensures recursive platform instantiation remains economically viable. It is not a constraint on every subsystem or integration. Addons are explicitly exempted from it (`core_model.md`: "Addons are not bound by this constraint; they may have their own scaling and deployment requirements"). Similarly, `authentication.md` already requires an external IdP for trust anchoring -- provenance has always been an integration concern that extends beyond the single-pod boundary.

This alternative architecture is predicated on the assumption that higher-assurance provenance requires infrastructure beyond a single process. A single-pod instance without signing infrastructure still functions correctly: it operates in credential-presentation mode (the baseline defined in `authentication.md`), where the platform's audit trail records provenance and the delivery agent accepts deliveries based on credential presentation alone. Attested provenance is the higher level -- signed intent or signed manifests -- that requires the signing and verification infrastructure described here. The single-pod instance is correct without it; it simply operates at a lower provenance level.

This is analogous to how the addon model works: the kernel doesn't embed addon logic, but addons extend the kernel's capabilities when deployed alongside it. Signing infrastructure extends the kernel's provenance capabilities when deployed alongside it.

## References

- [Sigstore documentation](https://docs.sigstore.dev/)
- [Fulcio certificate issuing overview](https://docs.sigstore.dev/certificate_authority/certificate-issuing-overview/)
- [Fulcio OIDC identity types](https://docs.sigstore.dev/fulcio/oidc-in-fulcio)
- [Fulcio OID information](https://github.com/sigstore/fulcio/blob/main/docs/oid-info.md)
- [Sigstore bundle format specification](https://docs.sigstore.dev/about/bundle/)
- [Sigstore protobuf-specs](https://github.com/sigstore/protobuf-specs)
- [Rekor overview](https://docs.sigstore.dev/logging/overview/)
- [Cosign in-toto attestations](https://docs.sigstore.dev/cosign/verifying/attestation/)
- [DSSE (Dead Simple Signing Envelope)](https://github.com/secure-systems-lab/dsse)
- [in-toto attestation specification](https://github.com/in-toto/attestation)
- [OCI image manifest specification (v1.1)](https://specs.opencontainers.org/image-spec/?v=v1.1.0)
- [OCI distribution specification (v1.1.1)](https://specs.opencontainers.org/distribution-spec/?v=v1.1.1)
- [OCI artifacts guidance](https://github.com/opencontainers/image-spec/blob/main/artifacts-guidance.md)
- [TUF specification (v1.0.34)](https://theupdateframework.github.io/specification/latest/)
- [go-tuf v2](https://github.com/theupdateframework/go-tuf)
- [sigstore-go](https://github.com/sigstore/sigstore-go)
- [Sigstore TUF root signing](https://github.com/sigstore/root-signing)
- [ORAS (OCI Registry As Storage)](https://oras.land/)
