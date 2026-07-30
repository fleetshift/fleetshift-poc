# Sigstore + TUF attestation POC (Mode A)

End-to-end proof of concept for the lightweight Sigstore-based provenance
architecture discussed against `docs/design/authentication.md` and
`docs/design/sigstore_oci_alternative.md`.

It answers the same core question as `../hybrid/`:

> If the management plane is only a courier, what evidence does a target need
> in order to accept a delivery?

…using **Fulcio-shaped identity binding**, **TSA trusted time**, **DSSE +
in-toto** (cosign attestation shape), and **TUF** for trust + constraint
distribution — without a transparency log (Mode A parity with today’s model).

## Architecture

```
┌─────────────┐   OIDC-style identity    ┌──────────────────┐
│ User / CLI  │ ───────────────────────► │ User Fulcio (CA) │
└─────────────┘                          └──────────────────┘
       │ signs DSSE(in-toto Statement)            │ short-lived leaf
       ▼                                          │
┌─────────────┐   hash timestamp         ┌──────────────────┐
│ Addon       │ ───────────────────────► │ TSA              │
│ (workload)  │   via Workload Fulcio    └──────────────────┘
└─────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│ Management plane (PoCEnvironment.assemble)                  │
│  - embeds current TUF snapshot (Fulcio roots, TSA root,     │
│    trust-anchors.json with identity allowlists + CEL)       │
│  - packs attestation graph into DeliveryBundle              │
│  - no HTTP fetch required of the delivery agent             │
└─────────────────────────────────────────────────────────────┘
       │
       ▼ offline verify
┌─────────────────────────────────────────────────────────────┐
│ Delivery agent (verify_delivery)                            │
│  1. Verify embedded TUF (bootstrap root keyids only)        │
│  2. Verify Sigstore bundles (Fulcio chain + TSA + DSSE)     │
│  3. Evaluate trust-anchor CEL + strategy-implied + signed   │
│     output CEL (reuses hybrid policy semantics)             │
│  4. Anti-replay via signed expected_generation              │
└─────────────────────────────────────────────────────────────┘
```

### What is real vs in-process

| Component | POC choice | Production analogue |
|---|---|---|
| Fulcio | `LocalFulcio` — issues short-lived leaves with Fulcio OIDs | Real Fulcio (tenant user CA + optional locked-down workload CA) |
| TSA | `LocalTSA` — signed timestamp over signature PAE digest | `sigstore/timestamp-authority` or public TSA |
| Cosign / in-toto | DSSE + in-toto Statement/v1 predicate (cosign attestation shape) | cosign / sigstore-go |
| TUF | Minimal root/targets/snapshot/timestamp via securesystemslib | go-tuf / python-tuf repo |
| Trust fetch | **Embedded** in `DeliveryBundle` by the server | Same optimization; agents may also refresh TUF out-of-band |
| Rekor / Tessera | **Omitted** (Mode A) | Optional upgrade for transparency |

### Bundle contents (no live trust HTTP)

`DeliveryBundle` carries:

- `tuf_snapshot` — signed TUF metadata + target bytes (`fulcio-root.pem`,
  `workload-fulcio-root.pem`, `tsa-root.pem`, `trust-anchors.json`)
- `bootstrap_root_keyids` — the only out-of-band pin (provisioned with the agent)
- `attestation` — signed input + put/remove output with Sigstore bundles

Content addressing inside the bundle uses SHA-256 digests of canonical JSON
(OCI-like digests without requiring a registry round-trip).

## Relation to `hybrid/`

| Hybrid | This POC |
|---|---|
| `KeyBinding` + long-lived pubkey | Fulcio leaf (ephemeral key + OIDC identity) |
| Ed25519 detached sig over envelope | DSSE over in-toto statement + TSA |
| `TrustStore` in memory | TUF-distributed anchors embedded in delivery |
| Strategy + CEL output constraints | **Same** (`hybrid.policy` / `hybrid.cel_runtime`) |
| `DerivedInput` / fleet upgrades | **Same** (mutation + preconditions; bundle carries prior/update graph) |
| Managed resources + fulfillment relations | **Same** (Sigstore-signed `RegisteredSelfTarget`) |
| `expected_generation` anti-replay | **Same** semantics |

Trust-anchor CEL (e.g. tenant prefix on `deployment_id`) is distributed as TUF
target JSON and evaluated by the agent — not expressible in Cosign identity
flags alone.

## Running tests

From repo root (prefer `/tmp` venv — repo `.venv` dirs may be gitignored):

```bash
python3 -m venv /tmp/sigstore-poc-venv
/tmp/sigstore-poc-venv/bin/pip install -r poc/attestation/sigstore_tuf/requirements.txt -r poc/attestation/requirements.txt
/tmp/sigstore-poc-venv/bin/python -m pytest poc/attestation/sigstore_tuf/ -v
```

Hybrid test inventory and KeyBinding→Fulcio mappings: see
[`TEST_PORTING.md`](TEST_PORTING.md) (126/126 hybrid tests ported or
equivalently replaced; 0 cannot-port).

## File guide

| File | Role |
|---|---|
| `fulcio.py` | Local Fulcio-compatible CA |
| `tsa.py` | Local timestamp authority |
| `dsse.py` | DSSE PAE + in-toto statement helpers |
| `sigstore_sign.py` | Keyless sign / verify bundles |
| `tuf_store.py` | TUF publish + verify embedded snapshot |
| `build.py` | `PoCEnvironment`, signers, `assemble()` |
| `model.py` | Delivery structures (reuses hybrid content types) |
| `verify.py` | Offline `verify_delivery` / `explain_verification` |
| `test_hybrid_equiv.py` | Full port of `hybrid/test_hybrid.py` |
| `test_delivery_equiv.py` | Full port of `hybrid/test_delivery.py` |
| `test_managed_resource_equiv.py` | Full port of `hybrid/test_managed_resource.py` |
| `test_provenance.py` | Sigstore/TUF-focused smoke (subset overlap) |
| `test_scenarios.py` | Fleet placement / co-sign / generation scenarios |
| `TEST_PORTING.md` | Per-test port inventory + KeyBinding equivalents |

## Explicit non-goals

- Transparency log / witnesses (Mode B) — optional later, not required for parity
- Live OCI registry pulls at verify time
- Credential presentation (run-as-me / platform)
- `PausedAuth` workflow states
- Full python-tuf client (roles are real; metadata is a minimal faithful subset)
