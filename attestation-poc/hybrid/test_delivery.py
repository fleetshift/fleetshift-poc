"""Tests for delivery-aware attestation verification."""

from __future__ import annotations

import time
from dataclasses import dataclass
import unittest

from .build import (
    make_key_binding,
    make_placement_evidence,
    make_put_manifests,
    make_remove_by_delivery_id,
    make_signed_input,
    sign_put_manifests,
)
from .crypto import KeyPair, content_hash, generate_keypair, sign
from .model import (
    Attestation,
    KeyBinding,
    OutputConstraint,
    OutputSignature,
    PlacementEvidence,
    PutManifests,
    RemoveByDeliveryId,
    Signature,
    TrustAnchor,
)
from .verify import (
    TrustStore,
    VerificationBundle,
    VerificationError,
    verify_attestation,
)


@dataclass(frozen=True)
class Identity:
    signer_id: str
    trust_anchor_id: str
    keys: KeyPair
    key_binding: KeyBinding


def make_identity(signer_id: str, trust_anchor_id: str) -> Identity:
    keys = generate_keypair()
    return Identity(
        signer_id=signer_id,
        trust_anchor_id=trust_anchor_id,
        keys=keys,
        key_binding=make_key_binding(keys, signer_id, trust_anchor_id),
    )


SAMPLE_MANIFESTS = [
    {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "nginx", "namespace": "default"},
    },
]


class DeliveryVerificationTests(unittest.TestCase):
    """Delivery-aware attestation verification."""

    def setUp(self) -> None:
        self.alice = make_identity("alice", "tenant-idp")
        self.obs_addon = make_identity("observability", "fleet-addons")
        self.placer_addon = make_identity("capacity-planner", "fleet-addons")
        self.evil = make_identity("evil", "evil-anchor")

        self.trust_store = TrustStore()
        self.trust_store.add(
            TrustAnchor(
                anchor_id="tenant-idp",
                known_keys={"alice": self.alice.keys.public_key_bytes},
            )
        )
        self.trust_store.add(
            TrustAnchor(
                anchor_id="fleet-addons",
                known_keys={
                    "observability": self.obs_addon.keys.public_key_bytes,
                    "capacity-planner": self.placer_addon.keys.public_key_bytes,
                },
            )
        )

        self.empty_bundle = VerificationBundle()

        self.prod_target = {
            "id": "cluster-prod-1",
            "labels": {"env": "prod", "region": "us-east-1"},
        }
        self.staging_target = {
            "id": "cluster-staging-1",
            "labels": {"env": "staging", "region": "us-west-2"},
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _inline_predicate_input(
        self,
        manifests,
        predicate: str,
        *,
        output_constraints=(),
    ):
        return make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content={
                "deployment_id": "deploy-1",
                "manifest_strategy": {"type": "inline", "manifests": manifests},
                "placement_strategy": {"type": "predicate", "expression": predicate},
            },
            output_constraints=output_constraints,
        )

    def _addon_predicate_input(
        self,
        addon_id: str = "observability",
        trust_anchor_id: str = "fleet-addons",
        predicate: str = 'target.labels.env == "prod"',
        *,
        output_constraints=(),
    ):
        return make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content={
                "deployment_id": "deploy-1",
                "manifest_strategy": {
                    "type": "addon",
                    "addon_id": addon_id,
                    "trust_anchor_id": trust_anchor_id,
                },
                "placement_strategy": {"type": "predicate", "expression": predicate},
            },
            output_constraints=output_constraints,
        )

    def _addon_addon_input(
        self,
        manifest_addon: str = "observability",
        manifest_anchor: str = "fleet-addons",
        placement_addon: str = "capacity-planner",
        placement_anchor: str = "fleet-addons",
        *,
        output_constraints=(),
    ):
        return make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content={
                "deployment_id": "deploy-1",
                "manifest_strategy": {
                    "type": "addon",
                    "addon_id": manifest_addon,
                    "trust_anchor_id": manifest_anchor,
                },
                "placement_strategy": {
                    "type": "addon",
                    "addon_id": placement_addon,
                    "trust_anchor_id": placement_anchor,
                },
            },
            output_constraints=output_constraints,
        )

    def _inline_addon_input(
        self,
        manifests,
        placement_addon: str = "capacity-planner",
        placement_anchor: str = "fleet-addons",
        *,
        output_constraints=(),
    ):
        return make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content={
                "deployment_id": "deploy-1",
                "manifest_strategy": {"type": "inline", "manifests": manifests},
                "placement_strategy": {
                    "type": "addon",
                    "addon_id": placement_addon,
                    "trust_anchor_id": placement_anchor,
                },
            },
            output_constraints=output_constraints,
        )

    # ==================================================================
    # Inline manifest strategy
    # ==================================================================

    def test_inline_manifest_put_happy_path(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, SAMPLE_MANIFESTS)

    def test_inline_manifest_tampered_output(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        tampered = [{"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "evil", "namespace": "default"}}]
        output = make_put_manifests(tampered)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("manifests must match inline spec", str(ctx.exception))

    # ==================================================================
    # Addon manifest strategy
    # ==================================================================

    def test_addon_manifest_put_happy_path(self) -> None:
        si = self._addon_predicate_input()
        output = sign_put_manifests(
            self.obs_addon.keys, "observability", "fleet-addons",
            SAMPLE_MANIFESTS,
        )
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, SAMPLE_MANIFESTS)
        self.assertEqual(result.signer_id, "observability")

    def test_addon_manifest_wrong_addon_signs(self) -> None:
        si = self._addon_predicate_input(addon_id="observability")
        output = sign_put_manifests(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            SAMPLE_MANIFESTS,
        )
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("manifests must be signed by observability", str(ctx.exception))

    def test_addon_manifest_missing_signature(self) -> None:
        si = self._addon_predicate_input()
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("manifests must be signed by observability", str(ctx.exception))

    def test_addon_manifest_wrong_trust_anchor(self) -> None:
        other_anchor = TrustAnchor(
            anchor_id="other-addons",
            known_keys={"observability": self.obs_addon.keys.public_key_bytes},
        )
        ts = TrustStore()
        ts.add(TrustAnchor(
            anchor_id="tenant-idp",
            known_keys={"alice": self.alice.keys.public_key_bytes},
        ))
        ts.add(other_anchor)

        si = self._addon_predicate_input(trust_anchor_id="fleet-addons")
        output = sign_put_manifests(
            self.obs_addon.keys, "observability", "other-addons",
            SAMPLE_MANIFESTS,
        )
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, ts,
                target_identity=self.prod_target,
            )
        self.assertIn("manifests must be signed by observability via fleet-addons", str(ctx.exception))

    # ==================================================================
    # Predicate placement (self-assessment)
    # ==================================================================

    def test_predicate_placement_target_matches_put_accepted(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, SAMPLE_MANIFESTS)

    def test_predicate_placement_target_no_match_put_rejected(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.staging_target,
            )
        self.assertIn("target matches placement predicate for put", str(ctx.exception))

    def test_predicate_placement_target_no_match_remove_accepted(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_remove_by_delivery_id("delivery-1")
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.staging_target,
        )
        self.assertEqual(result.content, {"delivery_id": "delivery-1"})

    def test_predicate_placement_target_matches_remove_rejected(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_remove_by_delivery_id("delivery-1")
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("removal requires placement predicate non-match", str(ctx.exception))

    # ==================================================================
    # Addon placement
    # ==================================================================

    def test_addon_placement_signed_evidence_accepted(self) -> None:
        evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-1", "cluster-prod-2"),
            deployment_id="deploy-1",
        )
        si = self._inline_addon_input(SAMPLE_MANIFESTS)
        output = make_put_manifests(SAMPLE_MANIFESTS, placement=evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, SAMPLE_MANIFESTS)

    def test_addon_placement_missing_evidence_rejected(self) -> None:
        si = self._inline_addon_input(SAMPLE_MANIFESTS)
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("placement must be signed by capacity-planner", str(ctx.exception))

    def test_addon_placement_wrong_addon_rejected(self) -> None:
        evidence = make_placement_evidence(
            self.obs_addon.keys, "observability", "fleet-addons",
            targets=("cluster-prod-1",),
            deployment_id="deploy-1",
        )
        si = self._inline_addon_input(SAMPLE_MANIFESTS)
        output = make_put_manifests(SAMPLE_MANIFESTS, placement=evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("placement must be signed by capacity-planner", str(ctx.exception))

    def test_addon_placement_target_not_in_decision_rejected(self) -> None:
        evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-2", "cluster-prod-3"),
            deployment_id="deploy-1",
        )
        si = self._inline_addon_input(SAMPLE_MANIFESTS)
        output = make_put_manifests(SAMPLE_MANIFESTS, placement=evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("action consistent with placement decision", str(ctx.exception))

    # ==================================================================
    # Removal scenarios
    # ==================================================================

    def test_remove_predicate_non_match_accepted(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_remove_by_delivery_id("delivery-1")
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.staging_target,
        )
        self.assertEqual(result.content, {"delivery_id": "delivery-1"})

    def test_remove_predicate_match_rejected(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_remove_by_delivery_id("delivery-1")
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("removal requires placement predicate non-match", str(ctx.exception))

    def test_remove_addon_placement_accepted(self) -> None:
        evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-2",),
            deployment_id="deploy-1",
        )
        si = self._inline_addon_input(SAMPLE_MANIFESTS)
        output = make_remove_by_delivery_id("delivery-1", placement=evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, {"delivery_id": "delivery-1"})

    # ==================================================================
    # Explicit user constraints (additive)
    # ==================================================================

    def test_namespace_constraint_plus_addon_strategy(self) -> None:
        ns_constraint = OutputConstraint(
            name="must be in namespace default",
            expression='output.manifests.all(m, m.metadata.namespace == "default")',
        )
        si = self._addon_predicate_input(output_constraints=(ns_constraint,))
        output = sign_put_manifests(
            self.obs_addon.keys, "observability", "fleet-addons",
            SAMPLE_MANIFESTS,
        )
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, SAMPLE_MANIFESTS)

    def test_namespace_constraint_fails_with_wrong_namespace(self) -> None:
        ns_constraint = OutputConstraint(
            name="must be in namespace kube-system",
            expression='output.manifests.all(m, m.metadata.namespace == "kube-system")',
        )
        si = self._addon_predicate_input(output_constraints=(ns_constraint,))
        output = sign_put_manifests(
            self.obs_addon.keys, "observability", "fleet-addons",
            SAMPLE_MANIFESTS,
        )
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("must be in namespace kube-system", str(ctx.exception))

    def test_gvk_allowlist_plus_inline_strategy(self) -> None:
        gvk_constraint = OutputConstraint(
            name="only Deployments allowed",
            expression='output.manifests.all(m, (m.apiVersion + "/" + m.kind) == "apps/v1/Deployment")',
        )
        si = self._inline_predicate_input(
            SAMPLE_MANIFESTS, 'target.labels.env == "prod"',
            output_constraints=(gvk_constraint,),
        )
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, SAMPLE_MANIFESTS)

    def test_user_constraint_fails_even_though_strategy_passes(self) -> None:
        strict_constraint = OutputConstraint(
            name="no Deployments allowed",
            expression='output.manifests.all(m, m.kind != "Deployment")',
        )
        si = self._inline_predicate_input(
            SAMPLE_MANIFESTS, 'target.labels.env == "prod"',
            output_constraints=(strict_constraint,),
        )
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("no Deployments allowed", str(ctx.exception))

    def test_placement_batch_size_constraint(self) -> None:
        batch_constraint = OutputConstraint(
            name="batch size limit",
            expression="size(placement.targets) <= 2",
        )
        evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("c1", "c2", "c3"),
            deployment_id="deploy-1",
        )
        si = self._inline_addon_input(
            SAMPLE_MANIFESTS,
            output_constraints=(batch_constraint,),
        )
        output = make_put_manifests(SAMPLE_MANIFESTS, placement=evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity={"id": "c1", "labels": {}},
            )
        self.assertIn("batch size limit", str(ctx.exception))

    # ==================================================================
    # Adversarial
    # ==================================================================

    def test_swap_manifests_between_attestations(self) -> None:
        """Manifests from deploy-2 cannot satisfy deploy-1's inline constraint."""
        manifests_a = [{"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "a", "namespace": "default"}}]
        manifests_b = [{"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "b", "namespace": "default"}}]

        si_a = make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content={
                "deployment_id": "deploy-1",
                "manifest_strategy": {"type": "inline", "manifests": manifests_a},
                "placement_strategy": {"type": "predicate", "expression": 'target.labels.env == "prod"'},
            },
        )
        output_b = make_put_manifests(manifests_b)
        att = Attestation(attestation_id="att-1", input=si_a, output=output_b)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("manifests must match inline spec", str(ctx.exception))

    def test_expired_attestation_rejected(self) -> None:
        si = make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content={
                "deployment_id": "deploy-1",
                "manifest_strategy": {"type": "inline", "manifests": SAMPLE_MANIFESTS},
                "placement_strategy": {"type": "predicate", "expression": 'target.labels.env == "prod"'},
            },
            valid_duration_sec=-1,
        )
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("expired", str(ctx.exception))

    def test_placement_evidence_cross_deployment_replay_rejected(self) -> None:
        """Evidence signed for deploy-2 cannot be used with deploy-1's attestation."""
        evidence_for_other = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-1",),
            deployment_id="deploy-2",
        )
        si = self._inline_addon_input(SAMPLE_MANIFESTS)
        output = make_put_manifests(SAMPLE_MANIFESTS, placement=evidence_for_other)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("deployment_id mismatch", str(ctx.exception))

    def test_forged_placement_evidence_wrong_key(self) -> None:
        forged_evidence = make_placement_evidence(
            self.evil.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-1",),
            deployment_id="deploy-1",
        )
        si = self._inline_addon_input(SAMPLE_MANIFESTS)
        output = make_put_manifests(SAMPLE_MANIFESTS, placement=forged_evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("placement evidence", str(ctx.exception).lower())

    def test_unknown_manifest_strategy_fails_closed(self) -> None:
        si = make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content={
                "deployment_id": "deploy-1",
                "manifest_strategy": {"type": "custom-unknown"},
                "placement_strategy": {"type": "predicate", "expression": 'target.labels.env == "prod"'},
            },
        )
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("unknown manifest strategy type", str(ctx.exception))

    def test_unknown_placement_strategy_fails_closed(self) -> None:
        si = make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content={
                "deployment_id": "deploy-1",
                "manifest_strategy": {"type": "inline", "manifests": SAMPLE_MANIFESTS},
                "placement_strategy": {"type": "custom-unknown"},
            },
        )
        output = make_put_manifests(SAMPLE_MANIFESTS)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("unknown placement strategy type", str(ctx.exception))

    # ==================================================================
    # Combined strategy tests
    # ==================================================================

    def test_addon_manifest_addon_placement_put_happy_path(self) -> None:
        evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-1",),
            deployment_id="deploy-1",
        )
        si = self._addon_addon_input()
        output = sign_put_manifests(
            self.obs_addon.keys, "observability", "fleet-addons",
            SAMPLE_MANIFESTS,
            placement=evidence,
        )
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, SAMPLE_MANIFESTS)
        self.assertEqual(result.signer_id, "observability")

    def test_addon_manifest_addon_placement_remove_happy_path(self) -> None:
        evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-2",),
            deployment_id="deploy-1",
        )
        si = self._addon_addon_input()
        output = make_remove_by_delivery_id("delivery-1", placement=evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, {"delivery_id": "delivery-1"})

    def test_inline_manifest_addon_placement_put(self) -> None:
        evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-1",),
            deployment_id="deploy-1",
        )
        si = self._inline_addon_input(SAMPLE_MANIFESTS)
        output = make_put_manifests(SAMPLE_MANIFESTS, placement=evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, SAMPLE_MANIFESTS)

    def test_forged_manifest_signature_untrusted_key(self) -> None:
        """Addon signature from a key not in the trust anchor should fail."""
        si = self._addon_predicate_input()
        output = sign_put_manifests(
            self.evil.keys, "observability", "fleet-addons",
            SAMPLE_MANIFESTS,
        )
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("manifest signature", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
