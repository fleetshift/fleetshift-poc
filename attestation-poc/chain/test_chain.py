"""
Tests for provenance chain verification.

The primary test models this scenario:

  1. User A creates deployment D1 (v1) with an addon manifest strategy.
     - Input: user-signed spec (capi-provisioner addon, namespace=prod, version=1.29.5).
     - Output: CAPI manifests, signed by the capi-provisioner addon.

  2. User B creates deployment D2 with an addon manifest strategy.
     - Input: user-signed spec (cluster-lifecycle addon).
     - Output: an update instruction (D3) targeting D1, signed by the
       cluster-lifecycle addon.

  3. D2's output (D3) updates D1, producing D1 v2.
     - Input: derived from D1 v1's spec + D3's mutation expression.
     - Output: new CAPI manifests (version=1.30.2), signed by capi-provisioner.

  Verification of D1 v2 walks the full graph:
    (a) Verify D1 v1's input — User A signed it.
    (b) Verify D2 fully — User B signed D2's input, cluster-lifecycle
        addon signed D2's output.
    (c) Re-execute the mutation expression against D1 v1's spec and
        confirm the result matches D1 v2's spec.
    (d) Verify D1 v2's output — capi-provisioner addon signed it.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass

import pytest

from .crypto import KeyPair, content_hash, generate_keypair, sign
from .model import (
    AddonSignature,
    Attestation,
    KeyBinding,
    Signature,
    TrustAnchor,
    UpdateDerivedInput,
    UserSignedInput,
)
from .mutation import apply_mutation
from .verify import AttestationStore, TrustStore, VerificationResult, verify_attestation


# ---------------------------------------------------------------------------
# Test helpers – keypair wrappers that know their identity
# ---------------------------------------------------------------------------


@dataclass
class UserKeys:
    user_id: str
    trust_anchor_id: str
    keys: KeyPair
    key_binding: KeyBinding

    @classmethod
    def generate(cls, user_id: str, trust_anchor_id: str) -> UserKeys:
        kp = generate_keypair()
        binding_hash = content_hash(
            {
                "user_id": user_id,
                "public_key": kp.public_key_bytes.hex(),
                "trust_anchor_id": trust_anchor_id,
            }
        )
        binding_proof = sign(kp.private_key, binding_hash)
        kb = KeyBinding(
            user_id=user_id,
            public_key=kp.public_key_bytes,
            trust_anchor_id=trust_anchor_id,
            binding_proof=binding_proof,
        )
        return cls(user_id=user_id, trust_anchor_id=trust_anchor_id, keys=kp, key_binding=kb)


@dataclass
class AddonKeys:
    addon_id: str
    trust_anchor_id: str
    keys: KeyPair

    @classmethod
    def generate(cls, addon_id: str, trust_anchor_id: str) -> AddonKeys:
        return cls(addon_id=addon_id, trust_anchor_id=trust_anchor_id, keys=generate_keypair())


def _sign_input(user: UserKeys, input_spec: dict) -> Signature:
    h = content_hash(input_spec)
    return Signature(
        signer_id=user.user_id,
        public_key=user.keys.public_key_bytes,
        content_hash=h,
        signature_bytes=sign(user.keys.private_key, h),
    )


def _sign_output(addon: AddonKeys, output: dict) -> AddonSignature:
    h = content_hash(output)
    sig = Signature(
        signer_id=addon.addon_id,
        public_key=addon.keys.public_key_bytes,
        content_hash=h,
        signature_bytes=sign(addon.keys.private_key, h),
    )
    return AddonSignature(
        addon_id=addon.addon_id,
        signature=sig,
        trust_anchor_id=addon.trust_anchor_id,
    )


def build_user_attestation(
    att_id: str,
    input_spec: dict,
    user: UserKeys,
    output: dict,
    addon: AddonKeys,
) -> Attestation:
    """Build an attestation with user-signed input and addon-signed output."""
    return Attestation(
        attestation_id=att_id,
        input_spec=input_spec,
        input_provenance=UserSignedInput(
            signature=_sign_input(user, input_spec),
            key_binding=user.key_binding,
        ),
        output=output,
        output_provenance=_sign_output(addon, output),
    )


def build_update_attestation(
    att_id: str,
    prior: Attestation,
    update: Attestation,
    mutation_expr: str,
    output: dict,
    addon: AddonKeys,
) -> Attestation:
    """Build an attestation whose input is derived from a prior + update."""
    derived_spec = apply_mutation(mutation_expr, prior.input_spec)
    return Attestation(
        attestation_id=att_id,
        input_spec=derived_spec,
        input_provenance=UpdateDerivedInput(
            prior_attestation_id=prior.attestation_id,
            update_attestation_id=update.attestation_id,
            mutation_expr=mutation_expr,
        ),
        output=output,
        output_provenance=_sign_output(addon, output),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def trust_store():
    return TrustStore()


@pytest.fixture()
def attestation_store():
    return AttestationStore()


@pytest.fixture()
def alice():
    return UserKeys.generate("alice", "tenant-idp")


@pytest.fixture()
def bob():
    return UserKeys.generate("bob", "tenant-idp")


@pytest.fixture()
def capi_addon():
    return AddonKeys.generate("capi-provisioner", "fleet-addons")


@pytest.fixture()
def lifecycle_addon():
    return AddonKeys.generate("cluster-lifecycle", "fleet-addons")


@pytest.fixture()
def setup_trust(trust_store, alice, bob, capi_addon, lifecycle_addon):
    """Register all keys with their trust anchors."""
    trust_store.add(
        TrustAnchor(
            anchor_id="tenant-idp",
            known_keys={
                "alice": alice.keys.public_key_bytes,
                "bob": bob.keys.public_key_bytes,
            },
        )
    )
    trust_store.add(
        TrustAnchor(
            anchor_id="fleet-addons",
            known_keys={
                "capi-provisioner": capi_addon.keys.public_key_bytes,
                "cluster-lifecycle": lifecycle_addon.keys.public_key_bytes,
            },
        )
    )
    return trust_store


# ---------------------------------------------------------------------------
# The scenario
# ---------------------------------------------------------------------------


D1_V1_SPEC = {
    "manifest_strategy": {
        "type": "addon",
        "addon": "capi-provisioner",
        "trust_anchor": "fleet-addons",
    },
    "namespace": "prod",
    "version": "1.29.5",
}

D1_V1_OUTPUT = {
    "manifests": [
        {"kind": "Cluster", "apiVersion": "cluster.x-k8s.io/v1beta1",
         "spec": {"topology": {"version": "1.29.5"}}},
    ],
}

D2_SPEC = {
    "manifest_strategy": {
        "type": "addon",
        "addon": "cluster-lifecycle",
        "trust_anchor": "fleet-addons",
    },
    "target_selector": {"label": "type=cluster-provisioning"},
}

MUTATION_EXPR = '{"version": "1.30.2"}'

D2_OUTPUT = {
    "type": "update",
    "mutation": MUTATION_EXPR,
    "target_selector": {"label": "type=cluster-provisioning"},
}

D1_V2_OUTPUT = {
    "manifests": [
        {"kind": "Cluster", "apiVersion": "cluster.x-k8s.io/v1beta1",
         "spec": {"topology": {"version": "1.30.2"}}},
    ],
}


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestFullProvenanceChain:
    """
    Walk the complete graph:

      D1 v2 ──(prior)──► D1 v1  (verify input only)
        │
        └──(update)──► D2       (verify fully)

    Then verify D1 v2's output against constraints derived from
    its (now-trusted) input spec.
    """

    def test_full_chain_verifies(
        self, setup_trust, attestation_store, alice, bob, capi_addon, lifecycle_addon,
    ):
        d1_v1 = build_user_attestation("d1-v1", D1_V1_SPEC, alice, D1_V1_OUTPUT, capi_addon)
        d2 = build_user_attestation("d2", D2_SPEC, bob, D2_OUTPUT, lifecycle_addon)
        d1_v2 = build_update_attestation("d1-v2", d1_v1, d2, MUTATION_EXPR, D1_V2_OUTPUT, capi_addon)

        attestation_store.add(d1_v1)
        attestation_store.add(d2)
        attestation_store.add(d1_v2)

        result = verify_attestation(d1_v2, attestation_store, setup_trust)

        print()
        print(result.pretty())

        assert result.valid
        assert "fully verified" in result.detail

        # The tree should show all four verification legs:
        #   input (update-derived) → prior input (alice) + update (d2 fully) → output (capi addon)
        assert any("alice" in c.detail for c in _all_nodes(result))
        assert any("bob" in c.detail for c in _all_nodes(result))
        assert any("capi-provisioner" in c.detail for c in _all_nodes(result))
        assert any("cluster-lifecycle" in c.detail for c in _all_nodes(result))

    def test_mutation_produces_correct_derived_spec(self, alice, capi_addon, lifecycle_addon):
        """The derived spec should reflect the mutation."""
        d1_v1 = build_user_attestation("d1-v1", D1_V1_SPEC, alice, D1_V1_OUTPUT, capi_addon)
        d2 = build_user_attestation("d2", D2_SPEC, alice, D2_OUTPUT, lifecycle_addon)
        d1_v2 = build_update_attestation("d1-v2", d1_v1, d2, MUTATION_EXPR, D1_V2_OUTPUT, capi_addon)

        assert d1_v2.input_spec["version"] == "1.30.2"
        assert d1_v2.input_spec["namespace"] == "prod"  # unchanged
        assert d1_v2.input_spec["manifest_strategy"] == D1_V1_SPEC["manifest_strategy"]  # unchanged


# ---------------------------------------------------------------------------
# Simple user-signed attestation (no update chain)
# ---------------------------------------------------------------------------


class TestUserSignedAttestation:
    def test_direct_attestation_verifies(
        self, setup_trust, attestation_store, alice, capi_addon,
    ):
        att = build_user_attestation("d1", D1_V1_SPEC, alice, D1_V1_OUTPUT, capi_addon)
        attestation_store.add(att)

        result = verify_attestation(att, attestation_store, setup_trust)
        assert result.valid

    def test_unknown_trust_anchor_fails(self, attestation_store, alice, capi_addon):
        att = build_user_attestation("d1", D1_V1_SPEC, alice, D1_V1_OUTPUT, capi_addon)
        attestation_store.add(att)
        empty_trust = TrustStore()

        result = verify_attestation(att, attestation_store, empty_trust)
        assert not result.valid
        assert "trust anchor not found" in _all_details(result)


# ---------------------------------------------------------------------------
# Tampered signatures
# ---------------------------------------------------------------------------


class TestTamperedSignatures:
    def test_tampered_user_signature_fails(
        self, setup_trust, attestation_store, alice, bob, capi_addon,
    ):
        """Forge Alice's signature with Bob's key."""
        forged_spec = copy.deepcopy(D1_V1_SPEC)
        forged_hash = content_hash(forged_spec)
        forged_sig = Signature(
            signer_id="alice",
            public_key=bob.keys.public_key_bytes,  # wrong key
            content_hash=forged_hash,
            signature_bytes=sign(bob.keys.private_key, forged_hash),
        )
        att = Attestation(
            attestation_id="forged",
            input_spec=forged_spec,
            input_provenance=UserSignedInput(
                signature=forged_sig,
                key_binding=alice.key_binding,  # claims to be alice
            ),
            output=D1_V1_OUTPUT,
            output_provenance=_sign_output(capi_addon, D1_V1_OUTPUT),
        )
        attestation_store.add(att)

        result = verify_attestation(att, attestation_store, setup_trust)
        assert not result.valid
        assert "does not match key binding" in _all_details(result)

    def test_tampered_addon_signature_fails(
        self, setup_trust, attestation_store, alice, capi_addon,
    ):
        """Modify the output after the addon signed it."""
        tampered_output = {"manifests": [{"kind": "Cluster", "spec": {"HACKED": True}}]}
        att = Attestation(
            attestation_id="tampered",
            input_spec=D1_V1_SPEC,
            input_provenance=UserSignedInput(
                signature=_sign_input(alice, D1_V1_SPEC),
                key_binding=alice.key_binding,
            ),
            output=tampered_output,
            output_provenance=_sign_output(capi_addon, D1_V1_OUTPUT),  # signed original
        )
        attestation_store.add(att)

        result = verify_attestation(att, attestation_store, setup_trust)
        assert not result.valid
        assert "hash mismatch" in _all_details(result)

    def test_wrong_addon_fails(
        self, setup_trust, attestation_store, alice, lifecycle_addon,
    ):
        """Output signed by the wrong addon (lifecycle instead of capi)."""
        att = build_user_attestation(
            "wrong-addon", D1_V1_SPEC, alice, D1_V1_OUTPUT, lifecycle_addon,
        )
        attestation_store.add(att)

        result = verify_attestation(att, attestation_store, setup_trust)
        assert not result.valid
        assert "expected capi-provisioner" in _all_details(result)


# ---------------------------------------------------------------------------
# Update-derived failures
# ---------------------------------------------------------------------------


class TestUpdateDerivedFailures:
    def test_mutation_mismatch_fails(
        self, setup_trust, attestation_store, alice, bob, capi_addon, lifecycle_addon,
    ):
        """Mutation expression doesn't match update attestation's output."""
        d1_v1 = build_user_attestation("d1-v1", D1_V1_SPEC, alice, D1_V1_OUTPUT, capi_addon)
        d2 = build_user_attestation("d2", D2_SPEC, bob, D2_OUTPUT, lifecycle_addon)

        different_mutation = '{"version": "9.9.9"}'
        d1_v2 = build_update_attestation(
            "d1-v2", d1_v1, d2, different_mutation, D1_V2_OUTPUT, capi_addon,
        )

        attestation_store.add(d1_v1)
        attestation_store.add(d2)
        attestation_store.add(d1_v2)

        result = verify_attestation(d1_v2, attestation_store, setup_trust)
        assert not result.valid
        assert "does not match" in _all_details(result)

    def test_tampered_derived_spec_fails(
        self, setup_trust, attestation_store, alice, bob, capi_addon, lifecycle_addon,
    ):
        """
        Someone claims a derived spec that doesn't match what the
        mutation expression would actually produce.
        """
        d1_v1 = build_user_attestation("d1-v1", D1_V1_SPEC, alice, D1_V1_OUTPUT, capi_addon)
        d2 = build_user_attestation("d2", D2_SPEC, bob, D2_OUTPUT, lifecycle_addon)

        # Manually build an attestation with a wrong derived spec
        wrong_spec = apply_mutation(MUTATION_EXPR, D1_V1_SPEC)
        wrong_spec["namespace"] = "HIJACKED"

        d1_v2 = Attestation(
            attestation_id="d1-v2",
            input_spec=wrong_spec,
            input_provenance=UpdateDerivedInput(
                prior_attestation_id="d1-v1",
                update_attestation_id="d2",
                mutation_expr=MUTATION_EXPR,
            ),
            output=D1_V2_OUTPUT,
            output_provenance=_sign_output(capi_addon, D1_V2_OUTPUT),
        )

        attestation_store.add(d1_v1)
        attestation_store.add(d2)
        attestation_store.add(d1_v2)

        result = verify_attestation(d1_v2, attestation_store, setup_trust)
        assert not result.valid
        assert "different spec" in _all_details(result)

    def test_missing_prior_attestation_fails(
        self, setup_trust, attestation_store, alice, bob, capi_addon, lifecycle_addon,
    ):
        d1_v1 = build_user_attestation("d1-v1", D1_V1_SPEC, alice, D1_V1_OUTPUT, capi_addon)
        d2 = build_user_attestation("d2", D2_SPEC, bob, D2_OUTPUT, lifecycle_addon)
        d1_v2 = build_update_attestation("d1-v2", d1_v1, d2, MUTATION_EXPR, D1_V2_OUTPUT, capi_addon)

        # Only add d2, not d1_v1
        attestation_store.add(d2)
        attestation_store.add(d1_v2)

        result = verify_attestation(d1_v2, attestation_store, setup_trust)
        assert not result.valid
        assert "not found" in _all_details(result)


# ---------------------------------------------------------------------------
# Deeper chains
# ---------------------------------------------------------------------------


class TestDeeperChains:
    def test_chained_updates(
        self, setup_trust, attestation_store, alice, bob, capi_addon, lifecycle_addon,
    ):
        """
        Two sequential updates: v1 → v2 → v3.

        Each update is its own deployment with a separate user
        authorization.  The final attestation's verification walks
        the full history.
        """
        d1_v1 = build_user_attestation("d1-v1", D1_V1_SPEC, alice, D1_V1_OUTPUT, capi_addon)

        # First update: 1.29.5 → 1.30.2
        mutation_1 = '{"version": "1.30.2"}'
        d_update_1_output = {"type": "update", "mutation": mutation_1}
        d_update_1 = build_user_attestation(
            "update-1",
            {**D2_SPEC},
            bob,
            d_update_1_output,
            lifecycle_addon,
        )

        v2_output = {"manifests": [{"kind": "Cluster", "spec": {"version": "1.30.2"}}]}
        d1_v2 = build_update_attestation("d1-v2", d1_v1, d_update_1, mutation_1, v2_output, capi_addon)

        # Second update: 1.30.2 → 1.31.0
        mutation_2 = '{"version": "1.31.0"}'
        d_update_2_output = {"type": "update", "mutation": mutation_2}
        d_update_2 = build_user_attestation(
            "update-2",
            {**D2_SPEC},
            alice,  # Alice authorizes this one
            d_update_2_output,
            lifecycle_addon,
        )

        v3_output = {"manifests": [{"kind": "Cluster", "spec": {"version": "1.31.0"}}]}
        d1_v3 = build_update_attestation("d1-v3", d1_v2, d_update_2, mutation_2, v3_output, capi_addon)

        for att in [d1_v1, d_update_1, d1_v2, d_update_2, d1_v3]:
            attestation_store.add(att)

        result = verify_attestation(d1_v3, attestation_store, setup_trust)

        print()
        print(result.pretty())

        assert result.valid

        all_nodes = _all_nodes(result)
        assert any("alice" in n.detail for n in all_nodes)
        assert any("bob" in n.detail for n in all_nodes)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _all_nodes(result: VerificationResult) -> list[VerificationResult]:
    """Flatten a VerificationResult tree."""
    nodes = [result]
    for c in result.children:
        nodes.extend(_all_nodes(c))
    return nodes


def _all_details(result: VerificationResult) -> str:
    """Concatenate all detail strings in the tree for easy assertion."""
    return " | ".join(n.detail for n in _all_nodes(result) if n.detail)
