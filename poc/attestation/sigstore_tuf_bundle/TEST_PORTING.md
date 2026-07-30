# Hybrid test port inventory

The parity suite preserves every test method name from `../hybrid`, but names
are only the inventory. `test_porting_contract.py` additionally requires every
hybrid negative test to remain a raising negative test, retains a pinned
failure detail wherever hybrid had one, and forbids soft `assertFalse(valid)`
ports. Behavioral tests still prove the property; this source-level contract
prevents assertion inversion or soft-rejection regressions from hiding behind
the original name.

| Hybrid source | Peer port | Tests | Same test names |
|---|---|---:|---:|
| `test_hybrid.py` | `test_hybrid_parity.py` | 48 | 48 |
| `test_delivery.py` | `test_delivery_parity.py` | 63 | 63 |
| `test_managed_resource.py` | `test_managed_resource_parity.py` | 15 | 15 |
| **Total** | | **126** | **126** |

The following check emits no diff:

```bash
for pair in \
  'test_hybrid.py test_hybrid_parity.py' \
  'test_delivery.py test_delivery_parity.py' \
  'test_managed_resource.py test_managed_resource_parity.py'
do
  set -- $pair
  diff -u \
    <(rg '^    def test_' "poc/attestation/hybrid/$1" | sed 's/.*def //' | sort) \
    <(rg '^    def test_' "poc/attestation/sigstore_tuf_bundle/$2" | sed 's/.*def //' | sort)
done
```

## Tests whose mechanism changes

Most ports change only their signing and verification fixtures. The cases
below deserve explicit explanation.

| Test | Hybrid mechanism | Peer mechanism or reason |
|---|---|---|
| `test_forged_input_signature_wrong_key` | Invalid Ed25519 signature | Invalid DSSE ECDSA signature under the Fulcio leaf. The forged bytes receive a valid RFC 3161 timestamp so the test reaches and pins DSSE signature verification |
| `test_key_binding_forged_proof` | Forged custom key-binding proof | Fulcio refuses issuance when proof of possession was made by a key other than the requested ephemeral key. This check moves from every delivery verifier into the trusted CA issuance ceremony |
| `test_key_binding_wrong_public_key` | Key binding and content signature use different public keys | Replace the bundle leaf with another valid Fulcio leaf; DSSE verification fails |
| `test_key_binding_signer_mismatch` | Mutate a duplicate signer ID beside the signed key binding | **Cannot be ported literally:** standard Bundle v0.3 has no duplicate signer-ID field. The negative port admits Alice but delivers Bob's Fulcio certificate and proves it cannot be confused with Alice at delivery time |
| `test_user_registers_in_addon_anchor_fails` | A signer-selected anchor label is rejected | Bundle has no signer-selected anchor. With only the addon anchor present, a tenant Fulcio certificate is rejected rather than interpreted as a platform identity |
| `test_addon_self_authorises_fails_constraint` | Addon input is allowed, then its output constraint fails | A test-local TUF policy admits platform input evidence so the attack reaches the signed CEL constraint and is rejected for not being `cluster-lifecycle`. Production policy also rejects it earlier |
| `test_user_key_cannot_satisfy_addon_constraint` | User output reaches CEL signer mismatch | A test-local TUF policy admits tenant output evidence so the attack reaches CEL and is rejected for not being `capi-provisioner`. Production policy also rejects it earlier |
| `test_derived_managed_resource` | Platform planner signs both update authorization and patch output | A test-local TUF policy admits platform input evidence; the same planner signs update authorization and patch output, matching the hybrid derivation topology |
| relation tamper tests | Custom relation signature fields | Standard DSSE/in-toto relation predicate signs addon ID, resource type, and manifest type |

No hybrid test is omitted. The duplicate signer-ID mutation is the only attack
that cannot be represented literally because standard Bundle v0.3 removes the
conflicting unauthenticated field; its replacement is a negative delivery-time
identity-confusion test, not a positive smoke test.

## High-risk setup fidelity

The ports intentionally retain the original attack preconditions in cases
where a simpler rejection would test a different property:

- `test_mixed_trust_both_anchors_needed` verifies a complete derivation after
  removing `fleet-addons` from TUF policy;
- `test_replay_output_from_different_attestation` reuses valid addon-signed
  manifests under a conflicting signed namespace constraint, so CEL—not a
  signature digest mismatch—rejects the replay;
- `test_self_signed_bypass_untrusted` has a rogue Fulcio identity sign both
  input and output, with a valid trusted TSA timestamp, and fails CA trust;
- `test_user_trusted_addon_not` and `test_addon_trusted_user_not` remove the
  entire opposite authority from the delivered TUF policy;
- `test_addon_manifest_unknown_trust_anchor_rejected` removes the structural
  addon authority without adding a CEL constraint for a fictitious anchor;
- the two fleet-upgrade replay tests separately exercise stolen final
  placement evidence (`deployment_id mismatch`) and a valid update applied to
  a deployment outside its placement decision.

## Additional coverage

`test_security.py` adds eleven tests not present in hybrid:

- complete logless Bundle v0.3 shape and absence of couriered bootstrap trust;
- stateful TUF rollback rejection;
- TUF target hash/length tamper rejection;
- expired TUF timestamp rejection;
- cross-tenant package rejection;
- relabelled cross-tenant TUF bootstrap rejection;
- verification without network connections;
- delivery-generation tamper rejection when generation is signed;
- DSSE multi-signature rejection;
- corrupted RFC 3161 response rejection;
- verification of a timestamped signature after its short-lived Fulcio leaf
  expires, while FleetShift's separate authorization expiry remains active.

`test_porting_contract.py` adds three regression guardrails for exact name
inventory, negative/pinned assertion shape, and the absence of soft boolean
rejections.
