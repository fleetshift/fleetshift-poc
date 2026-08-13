# Hybrid → Sigstore/TUF test porting

Inventory of every test in `../hybrid/` and how it maps into this POC.

## Summary

| Source | Tests | Ported | Equivalent replacement | Cannot port |
|---|---:|---:|---:|---:|
| `test_hybrid.py` | 48 | 45 | 3 | 0 |
| `test_delivery.py` | 63 | 63 | 0 | 0 |
| `test_managed_resource.py` | 15 | 15 | 0 | 0 |
| **Total** | **126** | **123** | **3** | **0** |

Additional Sigstore-specific coverage (not from hybrid): `test_provenance.py` /
`test_scenarios.py` (embedded TUF, Fulcio identity allowlists, etc.).

All ported and equivalent tests live in:

- `test_hybrid_equiv.py`
- `test_delivery_equiv.py`
- `test_managed_resource_equiv.py`

## KeyBinding → Fulcio/TSA/DSSE equivalents

Hybrid binds long-lived Ed25519 keys via `KeyBinding` proof-of-possession.
Mode A replaces that with Fulcio identity binding + TSA. The three KeyBinding
attack tests keep the same method names but assert the analogous guarantee:

| Hybrid test | Equivalent guarantee in this POC |
|---|---|
| `test_key_binding_signer_mismatch` | Bundle `identity.subject` disagrees with Fulcio leaf OID extensions |
| `test_key_binding_forged_proof` | TSA token digest does not bind to the DSSE PAE hash |
| `test_key_binding_wrong_public_key` | Fulcio leaf public key does not verify the DSSE signature |

## Capabilities added to support ports

These were required to reach hybrid parity under Mode A (no Rekor):

- **DerivedInput** chains (`prior_inputs` + `update_attestations` in `DeliveryBundle`)
- **Spec-update mutation** via `hybrid.mutation` (derive expression, preconditions, constraint carry-forward)
- **Managed resources** + Sigstore-signed `RegisteredSelfTarget` fulfillment relations
- **explain_verification** tree (same shape as hybrid; returned by `verify_delivery`)
- Flexible **PoCEnvironment** (optional tenant CEL, extra addon identities, republish)

## Cannot-port

None. Every hybrid test either ports directly or has an equivalent-replacement
row above.

## Overlap with existing suite

`test_provenance.py` / `test_scenarios.py` already covered a subset of delivery
/ forgery / generation cases before this port. The `*_equiv.py` files are the
authoritative 1:1 hybrid inventory; the earlier files remain as
Sigstore/TUF-focused smoke coverage.
