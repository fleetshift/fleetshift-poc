"""Tests for delivery-aware attestation verification."""

from __future__ import annotations

import time
from dataclasses import dataclass
import unittest

from .build import (
    make_key_binding,
    make_placement_evidence,
    make_put_manifests,
    make_remove_by_deployment_id,
    make_signed_input,
    sign_put_manifests,
)
from .crypto import KeyPair, content_hash, generate_keypair, sign
from .model import (
    Attestation,
    DeploymentContent,
    DerivedInput,
    KeyBinding,
    ManifestEnvelope,
    OutputConstraint,
    OutputSignature,
    PlacementEvidence,
    PutManifests,
    RemoveByDeploymentId,
    Signature,
    StrategySpec,
    TrustAnchor,
)
from .policy import constraint_to_document
from .verify import (
    DeploymentState,
    TrustStore,
    VerificationBundle,
    VerificationError,
    explain_verification,
    verify_attestation,
)


# ---------------------------------------------------------------------------
# Envelope helpers
# ---------------------------------------------------------------------------


def k8s_manifests(*objects: dict) -> tuple[ManifestEnvelope, ...]:
    """Wrap raw Kubernetes objects as typed manifest envelopes."""
    return tuple(
        ManifestEnvelope(resource_type="kubernetes", content=obj)
        for obj in objects
    )


def spec_update_manifest(directive: dict) -> tuple[ManifestEnvelope, ...]:
    """Wrap a spec_update directive as a single-item manifest envelope."""
    return (ManifestEnvelope(resource_type="spec_update", content=directive),)


def serialize_envelopes(envelopes: tuple[ManifestEnvelope, ...]) -> list[dict]:
    return [{"resource_type": m.resource_type, "content": m.content} for m in envelopes]


# ---------------------------------------------------------------------------
# Identities
# ---------------------------------------------------------------------------


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


_NOOP_PLACEMENT = StrategySpec(type="predicate", attributes={"expression": "true"})


def _addon_content(
    addon_id: str,
    trust_anchor_id: str = "fleet-addons",
    deployment_id: str = "update-request",
) -> DeploymentContent:
    return DeploymentContent(
        deployment_id=deployment_id,
        manifest_strategy=StrategySpec(
            type="addon",
            attributes={"addon_id": addon_id, "trust_anchor_id": trust_anchor_id},
        ),
        placement_strategy=_NOOP_PLACEMENT,
    )


SAMPLE_MANIFESTS = k8s_manifests(
    {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "nginx", "namespace": "default"},
    },
)

SAMPLE_MANIFESTS_SERIALIZED = serialize_envelopes(SAMPLE_MANIFESTS)


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
        manifests: tuple[ManifestEnvelope, ...],
        predicate: str,
        *,
        output_constraints=(),
    ):
        return make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content=DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="inline",
                    attributes={"manifests": serialize_envelopes(manifests)},
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": predicate},
                ),
            ),
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
            content=DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": addon_id,
                        "trust_anchor_id": trust_anchor_id,
                    },
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": predicate},
                ),
            ),
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
            content=DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": manifest_addon,
                        "trust_anchor_id": manifest_anchor,
                    },
                ),
                placement_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": placement_addon,
                        "trust_anchor_id": placement_anchor,
                    },
                ),
            ),
            output_constraints=output_constraints,
        )

    def _inline_addon_input(
        self,
        manifests: tuple[ManifestEnvelope, ...],
        placement_addon: str = "capacity-planner",
        placement_anchor: str = "fleet-addons",
        *,
        output_constraints=(),
    ):
        return make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content=DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="inline",
                    attributes={"manifests": serialize_envelopes(manifests)},
                ),
                placement_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": placement_addon,
                        "trust_anchor_id": placement_anchor,
                    },
                ),
            ),
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
        self.assertEqual(result.content, SAMPLE_MANIFESTS_SERIALIZED)

    def test_inline_manifest_tampered_output(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        tampered = k8s_manifests(
            {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "evil", "namespace": "default"}},
        )
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
        self.assertEqual(result.content, SAMPLE_MANIFESTS_SERIALIZED)
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

    def test_addon_manifest_unknown_trust_anchor_rejected(self) -> None:
        """Output signed via a trust anchor not in the trust store is rejected
        by the structural check in _verify_output_sig, even though the
        user-signed content does not constrain the trust anchor."""
        ts = TrustStore()
        ts.add(TrustAnchor(
            anchor_id="tenant-idp",
            known_keys={"alice": self.alice.keys.public_key_bytes},
        ))

        si = self._addon_predicate_input()
        output = sign_put_manifests(
            self.obs_addon.keys, "observability", "unknown-anchor",
            SAMPLE_MANIFESTS,
        )
        att = Attestation(attestation_id="att-1", input=si, output=output)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, ts,
                target_identity=self.prod_target,
            )
        self.assertIn("trust anchor not found", str(ctx.exception))

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
        self.assertEqual(result.content, SAMPLE_MANIFESTS_SERIALIZED)

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
        output = make_remove_by_deployment_id("deploy-1")
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.staging_target,
        )
        self.assertEqual(result.content, {"deployment_id": "deploy-1"})

    def test_predicate_placement_target_matches_remove_rejected(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_remove_by_deployment_id("deploy-1")
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
        self.assertEqual(result.content, SAMPLE_MANIFESTS_SERIALIZED)

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
        output = make_remove_by_deployment_id("deploy-1")
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.staging_target,
        )
        self.assertEqual(result.content, {"deployment_id": "deploy-1"})

    def test_remove_predicate_match_rejected(self) -> None:
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_remove_by_deployment_id("deploy-1")
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
        output = make_remove_by_deployment_id("deploy-1", placement=evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, {"deployment_id": "deploy-1"})

    def test_remove_deployment_id_mismatch_rejected(self) -> None:
        """Remove output targeting a different deployment than the signed input."""
        si = self._inline_predicate_input(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        output = make_remove_by_deployment_id("some-other-deployment")
        att = Attestation(attestation_id="att-remove-mismatch", input=si, output=output)

        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.empty_bundle, self.trust_store,
                target_identity=self.staging_target,
            )
        self.assertIn("remove deployment_id mismatch", str(ctx.exception))

    # ==================================================================
    # Explicit user constraints (additive)
    # ==================================================================

    def test_namespace_constraint_plus_addon_strategy(self) -> None:
        ns_constraint = OutputConstraint(
            name="must be in namespace default",
            expression='output.manifests.all(m, m.content.metadata.namespace == "default")',
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
        self.assertEqual(result.content, SAMPLE_MANIFESTS_SERIALIZED)

    def test_namespace_constraint_fails_with_wrong_namespace(self) -> None:
        ns_constraint = OutputConstraint(
            name="must be in namespace kube-system",
            expression='output.manifests.all(m, m.content.metadata.namespace == "kube-system")',
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
            expression=(
                'output.manifests.all(m, '
                '(m.content.apiVersion + "/" + m.content.kind) == "apps/v1/Deployment")'
            ),
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
        self.assertEqual(result.content, SAMPLE_MANIFESTS_SERIALIZED)

    def test_user_constraint_fails_even_though_strategy_passes(self) -> None:
        strict_constraint = OutputConstraint(
            name="no Deployments allowed",
            expression='output.manifests.all(m, m.content.kind != "Deployment")',
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
        manifests_a = k8s_manifests(
            {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "a", "namespace": "default"}},
        )
        manifests_b = k8s_manifests(
            {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "b", "namespace": "default"}},
        )

        si_a = make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content=DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="inline",
                    attributes={"manifests": serialize_envelopes(manifests_a)},
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": 'target.labels.env == "prod"'},
                ),
            ),
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
            content=DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="inline",
                    attributes={"manifests": SAMPLE_MANIFESTS_SERIALIZED},
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": 'target.labels.env == "prod"'},
                ),
            ),
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
            content=DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(type="custom-unknown"),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": 'target.labels.env == "prod"'},
                ),
            ),
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
            content=DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="inline",
                    attributes={"manifests": SAMPLE_MANIFESTS_SERIALIZED},
                ),
                placement_strategy=StrategySpec(type="custom-unknown"),
            ),
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
        self.assertEqual(result.content, SAMPLE_MANIFESTS_SERIALIZED)
        self.assertEqual(result.signer_id, "observability")

    def test_addon_manifest_addon_placement_remove_happy_path(self) -> None:
        evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-2",),
            deployment_id="deploy-1",
        )
        si = self._addon_addon_input()
        output = make_remove_by_deployment_id("deploy-1", placement=evidence)
        att = Attestation(attestation_id="att-1", input=si, output=output)
        result = verify_attestation(
            att, self.empty_bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.content, {"deployment_id": "deploy-1"})

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
        self.assertEqual(result.content, SAMPLE_MANIFESTS_SERIALIZED)

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


class FleetWideUpgradeTests(unittest.TestCase):
    """Controlled fleet-wide upgrades via derived input + delivery verification.

    Demonstrates the original motivating problem: a fleet operator pushes
    a Kubernetes version bump across all CAPI-managed clusters.  Each
    target cluster's agent verifies the full attestation chain before
    applying the upgrade.

    Actors:
      alice          -- tenant user, creates the base deployment
      bob            -- fleet operator, requests the upgrade
      upgrade-planner-- fleet addon, produces the version-patch directive
      capi-provisioner- fleet addon, renders the final manifests per target
      capacity-planner- fleet addon, signs placement decisions

    Flow:
      1. alice creates deployment "cluster-01" (v1.29.5, addon manifests,
         predicate placement, constraint: capi-provisioner must sign).
      2. bob requests an upgrade; upgrade-planner produces a spec_update
         that bumps version to 1.30.2 and carries new constraints.
      3. At each target, the agent constructs an Attestation whose input
         is DerivedInput(prior=cluster-01-v1, update=upgrade-1).
         The derived spec inherits manifest/placement strategies.
      4. capi-provisioner renders manifests for the target.
      5. verify_attestation checks the full chain + strategies + target.
    """

    def setUp(self) -> None:
        self.alice = make_identity("alice", "tenant-idp")
        self.bob = make_identity("bob", "tenant-idp")
        self.upgrade_planner = make_identity("upgrade-planner", "fleet-addons")
        self.capi_addon = make_identity("capi-provisioner", "fleet-addons")
        self.placer_addon = make_identity("capacity-planner", "fleet-addons")
        self.evil = make_identity("evil", "evil-anchor")

        self.trust_store = TrustStore()
        self.trust_store.add(
            TrustAnchor(
                anchor_id="tenant-idp",
                known_keys={
                    "alice": self.alice.keys.public_key_bytes,
                    "bob": self.bob.keys.public_key_bytes,
                },
            )
        )
        self.trust_store.add(
            TrustAnchor(
                anchor_id="fleet-addons",
                known_keys={
                    "upgrade-planner": self.upgrade_planner.keys.public_key_bytes,
                    "capi-provisioner": self.capi_addon.keys.public_key_bytes,
                    "capacity-planner": self.placer_addon.keys.public_key_bytes,
                },
            )
        )

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

    def _base_deployment_input(
        self,
        *,
        deployment_id: str = "cluster-01",
        version: str = "1.29.5",
        predicate: str = 'target.labels.env == "prod"',
    ):
        """v1 signed input: alice creates a CAPI cluster deployment."""
        return make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content=DeploymentContent(
                deployment_id=deployment_id,
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capi-provisioner",
                        "trust_anchor_id": "fleet-addons",
                        "config": {"version": version},
                    },
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": predicate},
                ),
            ),
            output_constraints=(
                OutputConstraint(
                    name="output must be signed by capi-provisioner via fleet-addons",
                    expression=(
                        'output.has_signature && '
                        'output.signature.trust_anchor_id == "fleet-addons" && '
                        'output.signer_id == "capi-provisioner"'
                    ),
                ),
            ),
        )

    def _upgrade_attestation(
        self,
        *,
        new_version: str = "1.30.2",
        extra_constraints: tuple[OutputConstraint, ...] = (),
        target_deployments: tuple[str, ...] = ("cluster-01",),
    ) -> Attestation:
        """The upgrade-planner addon produces a spec_update for the version bump.

        The upgrade request is itself a deployment: it has manifest and
        placement strategies.  The placement strategy gates which target
        deployments this upgrade applies to -- the capacity-planner addon
        signs placement evidence listing allowed deployment IDs.
        """
        update_directive = {
            "derive_input_expression": (
                f'set_path(prior, "manifest_strategy.config.version", "{new_version}")'
            ),
            "output_constraints": [
                constraint_to_document(OutputConstraint(
                    name="all manifests must be in namespace capi-system",
                    expression=(
                        'output.manifests.all(m, m.content.metadata.namespace == "capi-system")'
                    ),
                )),
                *(constraint_to_document(c) for c in extra_constraints),
            ],
        }
        return Attestation(
            attestation_id="upgrade-1",
            input=make_signed_input(
                self.bob.keys,
                self.bob.key_binding,
                content=DeploymentContent(
                    deployment_id="upgrade-request-1",
                    manifest_strategy=StrategySpec(
                        type="addon",
                        attributes={
                            "addon_id": "upgrade-planner",
                            "trust_anchor_id": "fleet-addons",
                        },
                    ),
                    placement_strategy=StrategySpec(
                        type="addon",
                        attributes={
                            "addon_id": "capacity-planner",
                            "trust_anchor_id": "fleet-addons",
                        },
                    ),
                ),
                output_constraints=(
                    OutputConstraint(
                        name="output must be signed by upgrade-planner via fleet-addons",
                        expression=(
                            'output.has_signature && '
                            'output.signature.trust_anchor_id == "fleet-addons" && '
                            'output.signer_id == "upgrade-planner"'
                        ),
                    ),
                ),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest(update_directive),
                placement=make_placement_evidence(
                    self.placer_addon.keys, "capacity-planner", "fleet-addons",
                    targets=target_deployments,
                    deployment_id="upgrade-request-1",
                ),
            ),
        )

    # ==================================================================
    # Happy path: full chain verified at target
    # ==================================================================

    def test_fleet_upgrade_happy_path(self) -> None:
        """Full end-to-end: base deployment -> upgrade patch -> delivery at target."""
        v1_input = self._base_deployment_input()
        upgrade_att = self._upgrade_attestation()

        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                target_manifests,
            ),
        )

        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        result = verify_attestation(
            final_attestation, bundle, self.trust_store,
            target_identity=self.prod_target,
        )

        self.assertEqual(result.signer_id, "capi-provisioner")
        serialized = serialize_envelopes(target_manifests)
        self.assertEqual(result.content, serialized)
        self.assertEqual(
            serialized[0]["content"]["spec"]["topology"]["version"],
            "1.30.2",
        )

    def test_fleet_upgrade_explanation_shows_full_chain(self) -> None:
        """The explanation tree contains the derivation, upgrade signer, and strategy."""
        v1_input = self._base_deployment_input()
        upgrade_att = self._upgrade_attestation()

        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                target_manifests,
            ),
        )

        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        explanation = explain_verification(
            final_attestation, bundle, self.trust_store,
            target_identity=self.prod_target,
        )

        all_details = _all_details(explanation)
        self.assertIn("derived from prior=cluster-01-v1 + update=upgrade-1", all_details)
        self.assertIn("upgrade-planner", all_details)
        self.assertIn("capi-provisioner", all_details)

    # ==================================================================
    # Target mismatch: predicate rejects non-prod target
    # ==================================================================

    def test_fleet_upgrade_wrong_target_rejected(self) -> None:
        """Derived placement predicate still gates delivery to non-matching targets."""
        v1_input = self._base_deployment_input()
        upgrade_att = self._upgrade_attestation()

        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                target_manifests,
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                final_attestation, bundle, self.trust_store,
                target_identity=self.staging_target,
            )
        self.assertIn("placement predicate", str(ctx.exception))

    # ==================================================================
    # Wrong addon signs the output after upgrade
    # ==================================================================

    def test_fleet_upgrade_wrong_manifest_signer_rejected(self) -> None:
        """Manifests signed by the wrong addon fail the derived constraint."""
        v1_input = self._base_deployment_input()
        upgrade_att = self._upgrade_attestation()

        wrong_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                wrong_manifests,
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                final_attestation, bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("capi-provisioner", str(ctx.exception))

    # ==================================================================
    # Prior constraints carry forward without repetition
    # ==================================================================

    def test_fleet_upgrade_prior_constraints_carry_forward(self) -> None:
        """Prior's explicit constraint is enforced even when update adds none."""
        v1_input = self._base_deployment_input()
        bare_upgrade = Attestation(
            attestation_id="upgrade-1",
            input=make_signed_input(
                self.bob.keys,
                self.bob.key_binding,
                content=_addon_content("upgrade-planner"),
                output_constraints=(
                    OutputConstraint(
                        name="output must be signed by upgrade-planner via fleet-addons",
                        expression=(
                            'output.has_signature && '
                            'output.signature.trust_anchor_id == "fleet-addons" && '
                            'output.signer_id == "upgrade-planner"'
                        ),
                    ),
                ),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
                    ),
                }),
            ),
        )
        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            },
        )

        # Output signed by wrong addon -- should fail from the *prior's* constraint
        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                target_manifests,
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": bare_upgrade},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                final_attestation, bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("capi-provisioner", str(ctx.exception))

    def test_fleet_upgrade_prior_constraints_carry_forward_happy_path(self) -> None:
        """Prior's explicit constraint passes when satisfied, even if update adds nothing."""
        v1_input = self._base_deployment_input()
        bare_upgrade = Attestation(
            attestation_id="upgrade-1",
            input=make_signed_input(
                self.bob.keys,
                self.bob.key_binding,
                content=_addon_content("upgrade-planner"),
                output_constraints=(
                    OutputConstraint(
                        name="output must be signed by upgrade-planner via fleet-addons",
                        expression=(
                            'output.has_signature && '
                            'output.signature.trust_anchor_id == "fleet-addons" && '
                            'output.signer_id == "upgrade-planner"'
                        ),
                    ),
                ),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
                    ),
                }),
            ),
        )
        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            },
        )

        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                target_manifests,
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": bare_upgrade},
        )
        result = verify_attestation(
            final_attestation, bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

    # ==================================================================
    # Namespace constraint from upgrade propagates (additive)
    # ==================================================================

    def test_fleet_upgrade_namespace_violation_rejected(self) -> None:
        """Upgrade-derived namespace constraint rejects wrong namespace."""
        v1_input = self._base_deployment_input()
        upgrade_att = self._upgrade_attestation()

        bad_ns_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "default"},
                "spec": {"topology": {"version": "1.30.2"}},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                bad_ns_manifests,
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                final_attestation, bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("capi-system", str(ctx.exception))

    # ==================================================================
    # Untrusted upgrade signer
    # ==================================================================

    def test_fleet_upgrade_untrusted_upgrade_signer_rejected(self) -> None:
        """An upgrade signed by an untrusted key cannot produce valid derived input."""
        v1_input = self._base_deployment_input()

        evil_upgrade = Attestation(
            attestation_id="upgrade-1",
            input=make_signed_input(
                self.evil.keys,
                self.evil.key_binding,
                content=_addon_content("upgrade-planner"),
            ),
            output=sign_put_manifests(
                self.evil.keys, "evil", "evil-anchor",
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "0.0.0-pwned")'
                    ),
                }),
            ),
        )
        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                target_manifests,
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": evil_upgrade},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                final_attestation, bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("trust anchor not found", str(ctx.exception))

    # ==================================================================
    # Chained upgrades: v1 -> v2 -> v3, delivery at target
    # ==================================================================

    def test_fleet_upgrade_chained_two_hops(self) -> None:
        """Two successive upgrades: v1.29 -> v1.30 -> v1.31, delivered at target."""
        v1_input = self._base_deployment_input(version="1.29.5")

        upgrade_1_att = Attestation(
            attestation_id="upgrade-1",
            input=make_signed_input(
                self.bob.keys,
                self.bob.key_binding,
                content=_addon_content("upgrade-planner"),
                output_constraints=(
                    OutputConstraint(
                        name="output must be signed by upgrade-planner via fleet-addons",
                        expression=(
                            'output.has_signature && '
                            'output.signature.trust_anchor_id == "fleet-addons" && '
                            'output.signer_id == "upgrade-planner"'
                        ),
                    ),
                ),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
                    ),
                }),
            ),
        )

        upgrade_2_att = Attestation(
            attestation_id="upgrade-2",
            input=make_signed_input(
                self.bob.keys,
                self.bob.key_binding,
                content=_addon_content("upgrade-planner"),
                output_constraints=(
                    OutputConstraint(
                        name="output must be signed by upgrade-planner via fleet-addons",
                        expression=(
                            'output.has_signature && '
                            'output.signature.trust_anchor_id == "fleet-addons" && '
                            'output.signer_id == "upgrade-planner"'
                        ),
                    ),
                ),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.31.0")'
                    ),
                }),
            ),
        )

        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.31.0"}},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-01-v3",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v2",
                update_attestation_id="upgrade-2",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                target_manifests,
            ),
        )

        bundle = VerificationBundle(
            inputs={
                "cluster-01-v1": v1_input,
                "cluster-01-v2": DerivedInput(
                    prior_content_id="cluster-01",
                    prior_content_type="deployment",
                    prior_input_id="cluster-01-v1",
                    update_attestation_id="upgrade-1",
                ),
            },
            attestations={
                "upgrade-1": upgrade_1_att,
                "upgrade-2": upgrade_2_att,
            },
        )
        result = verify_attestation(
            final_attestation, bundle, self.trust_store,
            target_identity=self.prod_target,
        )

        self.assertEqual(result.signer_id, "capi-provisioner")
        serialized = serialize_envelopes(target_manifests)
        self.assertEqual(
            serialized[0]["content"]["spec"]["topology"]["version"],
            "1.31.0",
        )

    # ==================================================================
    # Upgrade with addon placement (placement evidence bound to deployment)
    # ==================================================================

    def test_fleet_upgrade_with_addon_placement(self) -> None:
        """Upgrade flow with addon placement: evidence is bound to deployment."""
        v1_input = make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content=DeploymentContent(
                deployment_id="cluster-01",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capi-provisioner",
                        "trust_anchor_id": "fleet-addons",
                        "config": {"version": "1.29.5"},
                    },
                ),
                placement_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capacity-planner",
                        "trust_anchor_id": "fleet-addons",
                    },
                ),
            ),
            output_constraints=(
                OutputConstraint(
                    name="output must be signed by capi-provisioner via fleet-addons",
                    expression=(
                        'output.has_signature && '
                        'output.signature.trust_anchor_id == "fleet-addons" && '
                        'output.signer_id == "capi-provisioner"'
                    ),
                ),
            ),
        )
        upgrade_att = self._upgrade_attestation()

        evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-1", "cluster-prod-2"),
            deployment_id="cluster-01",
        )
        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                target_manifests,
                placement=evidence,
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        result = verify_attestation(
            final_attestation, bundle, self.trust_store,
            target_identity=self.prod_target,
        )

        self.assertEqual(result.signer_id, "capi-provisioner")

    # ==================================================================
    # Cross-deployment evidence replay after upgrade
    # ==================================================================

    def test_fleet_upgrade_cross_deployment_evidence_rejected(self) -> None:
        """Placement evidence for cluster-02 cannot satisfy cluster-01's attestation."""
        v1_input = make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content=DeploymentContent(
                deployment_id="cluster-01",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capi-provisioner",
                        "trust_anchor_id": "fleet-addons",
                        "config": {"version": "1.29.5"},
                    },
                ),
                placement_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capacity-planner",
                        "trust_anchor_id": "fleet-addons",
                    },
                ),
            ),
            output_constraints=(
                OutputConstraint(
                    name="output must be signed by capi-provisioner via fleet-addons",
                    expression=(
                        'output.has_signature && '
                        'output.signature.trust_anchor_id == "fleet-addons" && '
                        'output.signer_id == "capi-provisioner"'
                    ),
                ),
            ),
        )
        upgrade_att = self._upgrade_attestation()

        stolen_evidence = make_placement_evidence(
            self.placer_addon.keys, "capacity-planner", "fleet-addons",
            targets=("cluster-prod-1",),
            deployment_id="cluster-02",
        )
        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                target_manifests,
                placement=stolen_evidence,
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                final_attestation, bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("deployment_id mismatch", str(ctx.exception))


    # ==================================================================
    # Cross-deployment upgrade replay (upgrade targets wrong deployment)
    # ==================================================================

    def test_fleet_upgrade_replay_against_different_deployment_rejected(self) -> None:
        """An upgrade targeting cluster-01 cannot be replayed against cluster-02.

        The upgrade attestation's placement strategy gates which deployment
        IDs the update applies to.  When DerivedInput.verify verifies the
        update with target_identity={"id": "cluster-02"}, the placement
        constraint rejects because "cluster-02" is not in the upgrade's
        placement targets.
        """
        cluster_02_input = self._base_deployment_input(deployment_id="cluster-02")
        upgrade_att = self._upgrade_attestation(target_deployments=("cluster-01",))

        target_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-02", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            },
        )
        final_attestation = Attestation(
            attestation_id="cluster-02-v2",
            input=DerivedInput(
                prior_content_id="cluster-02",
                prior_content_type="deployment",
                prior_input_id="cluster-02-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                target_manifests,
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-02-v1": cluster_02_input},
            attestations={"upgrade-1": upgrade_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                final_attestation, bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("placement", str(ctx.exception).lower())

    def test_fleet_upgrade_update_must_not_retarget_deployment_identity(self) -> None:
        """A signed spec_update must not be able to rewrite deployment identity."""
        v1_input = make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content=DeploymentContent(
                deployment_id="cluster-01",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capi-provisioner",
                        "trust_anchor_id": "fleet-addons",
                    },
                ),
                placement_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capacity-planner",
                        "trust_anchor_id": "fleet-addons",
                    },
                ),
            ),
        )
        update_att = Attestation(
            attestation_id="upgrade-1",
            input=make_signed_input(
                self.bob.keys,
                self.bob.key_binding,
                content=DeploymentContent(
                    deployment_id="upgrade-request-1",
                    manifest_strategy=StrategySpec(
                        type="addon",
                        attributes={
                            "addon_id": "upgrade-planner",
                            "trust_anchor_id": "fleet-addons",
                        },
                    ),
                    placement_strategy=StrategySpec(
                        type="addon",
                        attributes={
                            "addon_id": "capacity-planner",
                            "trust_anchor_id": "fleet-addons",
                        },
                    ),
                ),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest({
                    "derive_input_expression": 'set_path(prior, "deployment_id", "cluster-02")',
                }),
                placement=make_placement_evidence(
                    self.placer_addon.keys, "capacity-planner", "fleet-addons",
                    targets=("cluster-01",),
                    deployment_id="upgrade-request-1",
                ),
            ),
        )

        final_attestation = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                k8s_manifests(
                    {
                        "apiVersion": "cluster.x-k8s.io/v1beta1",
                        "kind": "Cluster",
                        "metadata": {"name": "workload-01"},
                    }
                ),
                placement=make_placement_evidence(
                    self.placer_addon.keys, "capacity-planner", "fleet-addons",
                    targets=(self.prod_target["id"],),
                    deployment_id="cluster-02",
                ),
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": update_att},
        )

        with self.assertRaises(VerificationError):
            verify_attestation(
                final_attestation, bundle, self.trust_store,
                target_identity=self.prod_target,
            )


class GenerationAndPreconditionTests(unittest.TestCase):
    """Tests for expected_generation, target-side replay protection,
    derived generation propagation, and signed update preconditions."""

    def setUp(self) -> None:
        self.alice = make_identity("alice", "tenant-idp")
        self.bob = make_identity("bob", "tenant-idp")
        self.upgrade_planner = make_identity("upgrade-planner", "fleet-addons")
        self.capi_addon = make_identity("capi-provisioner", "fleet-addons")
        self.placer_addon = make_identity("capacity-planner", "fleet-addons")

        self.trust_store = TrustStore()
        self.trust_store.add(
            TrustAnchor(
                anchor_id="tenant-idp",
                known_keys={
                    "alice": self.alice.keys.public_key_bytes,
                    "bob": self.bob.keys.public_key_bytes,
                },
            )
        )
        self.trust_store.add(
            TrustAnchor(
                anchor_id="fleet-addons",
                known_keys={
                    "upgrade-planner": self.upgrade_planner.keys.public_key_bytes,
                    "capi-provisioner": self.capi_addon.keys.public_key_bytes,
                    "capacity-planner": self.placer_addon.keys.public_key_bytes,
                },
            )
        )

        self.prod_target = {
            "id": "cluster-prod-1",
            "labels": {"env": "prod", "region": "us-east-1"},
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _base_input(
        self,
        *,
        deployment_id: str = "cluster-01",
        version: str = "1.29.5",
        expected_generation: int | None = None,
    ):
        return make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content=DeploymentContent(
                deployment_id=deployment_id,
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capi-provisioner",
                        "trust_anchor_id": "fleet-addons",
                        "config": {"version": version},
                    },
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": 'target.labels.env == "prod"'},
                ),
            ),
            output_constraints=(
                OutputConstraint(
                    name="output must be signed by capi-provisioner via fleet-addons",
                    expression=(
                        'output.has_signature && '
                        'output.signature.trust_anchor_id == "fleet-addons" && '
                        'output.signer_id == "capi-provisioner"'
                    ),
                ),
            ),
            expected_generation=expected_generation,
        )

    def _upgrade_attestation(
        self,
        *,
        new_version: str = "1.30.2",
        preconditions: list[str] | None = None,
    ) -> Attestation:
        directive: dict = {
            "derive_input_expression": (
                f'set_path(prior, "manifest_strategy.config.version", "{new_version}")'
            ),
        }
        if preconditions is not None:
            directive["preconditions"] = preconditions
        return Attestation(
            attestation_id="upgrade-1",
            input=make_signed_input(
                self.bob.keys,
                self.bob.key_binding,
                content=_addon_content("upgrade-planner"),
                output_constraints=(
                    OutputConstraint(
                        name="output must be signed by upgrade-planner via fleet-addons",
                        expression=(
                            'output.has_signature && '
                            'output.signature.trust_anchor_id == "fleet-addons" && '
                            'output.signer_id == "upgrade-planner"'
                        ),
                    ),
                ),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest(directive),
            ),
        )

    def _target_manifests(self, version: str = "1.30.2"):
        return k8s_manifests({
            "apiVersion": "cluster.x-k8s.io/v1beta1",
            "kind": "Cluster",
            "metadata": {"name": "workload-01", "namespace": "capi-system"},
            "spec": {"topology": {"version": version}},
        })

    # ==================================================================
    # Stateless: expected_generation absent, no target state
    # ==================================================================

    def test_no_generation_still_verifies(self) -> None:
        """Without expected_generation or target state, verification works as before."""
        v1_input = self._base_input()
        att = Attestation(
            attestation_id="cluster-01-v1",
            input=v1_input,
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.29.5"),
            ),
        )
        result = verify_attestation(
            att, VerificationBundle(), self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

    # ==================================================================
    # Stateful: expected_generation present, target state present
    # ==================================================================

    def test_generation_matches_target_state_put_accepted(self) -> None:
        """expected_generation == target.generation + 1 passes."""
        v1_input = self._base_input(expected_generation=1)
        att = Attestation(
            attestation_id="cluster-01-v1",
            input=v1_input,
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.29.5"),
            ),
        )
        state = DeploymentState(content_id="cluster-01", generation=0)
        result = verify_attestation(
            att, VerificationBundle(), self.trust_store,
            target_identity=self.prod_target,
            current_deployment_state=state,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

    def test_stale_generation_put_rejected(self) -> None:
        """expected_generation behind target's current generation is rejected."""
        v1_input = self._base_input(expected_generation=1)
        att = Attestation(
            attestation_id="cluster-01-v1",
            input=v1_input,
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.29.5"),
            ),
        )
        state = DeploymentState(content_id="cluster-01", generation=1)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, VerificationBundle(), self.trust_store,
                target_identity=self.prod_target,
                current_deployment_state=state,
            )
        self.assertIn("generation mismatch", str(ctx.exception))

    def test_future_generation_put_rejected(self) -> None:
        """expected_generation ahead of target.generation + 1 is rejected."""
        v1_input = self._base_input(expected_generation=5)
        att = Attestation(
            attestation_id="cluster-01-v5",
            input=v1_input,
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.29.5"),
            ),
        )
        state = DeploymentState(content_id="cluster-01", generation=1)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, VerificationBundle(), self.trust_store,
                target_identity=self.prod_target,
                current_deployment_state=state,
            )
        self.assertIn("generation mismatch", str(ctx.exception))

    def test_stale_generation_remove_rejected(self) -> None:
        """Stale generation on a remove operation is also rejected."""
        v1_input = make_signed_input(
            self.alice.keys,
            self.alice.key_binding,
            content=DeploymentContent(
                deployment_id="cluster-01",
                manifest_strategy=StrategySpec(
                    type="inline",
                    attributes={"manifests": serialize_envelopes(k8s_manifests({"kind": "Cluster"}))},
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": 'target.labels.env == "prod"'},
                ),
            ),
            expected_generation=1,
        )
        staging = {"id": "staging-1", "labels": {"env": "staging"}}
        att = Attestation(
            attestation_id="cluster-01-remove",
            input=v1_input,
            output=make_remove_by_deployment_id("cluster-01"),
        )
        state = DeploymentState(content_id="cluster-01", generation=2)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, VerificationBundle(), self.trust_store,
                target_identity=staging,
                current_deployment_state=state,
            )
        self.assertIn("generation mismatch", str(ctx.exception))

    def test_generation_state_wrong_deployment_fails_closed(self) -> None:
        """Target state for a different deployment ID fails closed."""
        v1_input = self._base_input(expected_generation=1)
        att = Attestation(
            attestation_id="cluster-01-v1",
            input=v1_input,
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.29.5"),
            ),
        )
        wrong_state = DeploymentState(content_id="cluster-99", generation=0)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, VerificationBundle(), self.trust_store,
                target_identity=self.prod_target,
                current_deployment_state=wrong_state,
            )
        self.assertIn("content state mismatch", str(ctx.exception))

    # ==================================================================
    # Mixed: expected_generation present, no target state (stateless target)
    # ==================================================================

    def test_generation_present_but_no_target_state_still_verifies(self) -> None:
        """Stateless target skips generation enforcement even when signed."""
        v1_input = self._base_input(expected_generation=1)
        att = Attestation(
            attestation_id="cluster-01-v1",
            input=v1_input,
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.29.5"),
            ),
        )
        result = verify_attestation(
            att, VerificationBundle(), self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

    # ==================================================================
    # Derived generation propagation
    # ==================================================================

    def test_derived_generation_increments(self) -> None:
        """A derived input's resolved generation is parent + 1."""
        v1_input = self._base_input(expected_generation=1)
        upgrade_att = self._upgrade_attestation()

        final_att = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests(),
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        state = DeploymentState(content_id="cluster-01", generation=1)
        result = verify_attestation(
            final_att, bundle, self.trust_store,
            target_identity=self.prod_target,
            current_deployment_state=state,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

    def test_derived_chain_generation_accumulates(self) -> None:
        """v1(gen=1) -> v2(gen=2) -> v3(gen=3), target at gen=2 accepts gen=3."""
        v1_input = self._base_input(expected_generation=1)

        def _upgrade(att_id: str, new_ver: str) -> Attestation:
            return Attestation(
                attestation_id=att_id,
                input=make_signed_input(
                    self.bob.keys,
                    self.bob.key_binding,
                    content=_addon_content("upgrade-planner"),
                    output_constraints=(
                        OutputConstraint(
                            name="output must be signed by upgrade-planner",
                            expression=(
                                'output.has_signature && '
                                'output.signer_id == "upgrade-planner"'
                            ),
                        ),
                    ),
                ),
                output=sign_put_manifests(
                    self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                    spec_update_manifest({
                        "derive_input_expression": (
                            f'set_path(prior, "manifest_strategy.config.version", "{new_ver}")'
                        ),
                    }),
                ),
            )

        bundle = VerificationBundle(
            inputs={
                "v1": v1_input,
                "v2": DerivedInput(
                    prior_content_id="cluster-01",
                    prior_content_type="deployment",
                    prior_input_id="v1",
                    update_attestation_id="u1",
                ),
            },
            attestations={
                "u1": _upgrade("u1", "1.30.0"),
                "u2": _upgrade("u2", "1.31.0"),
            },
        )
        final_att = Attestation(
            attestation_id="v3",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="v2",
                update_attestation_id="u2",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.31.0"),
            ),
        )
        state = DeploymentState(content_id="cluster-01", generation=2)
        result = verify_attestation(
            final_att, bundle, self.trust_store,
            target_identity=self.prod_target,
            current_deployment_state=state,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

    def test_derived_chain_stale_generation_rejected(self) -> None:
        """Chained gen=3 is rejected if target is already at gen=3."""
        v1_input = self._base_input(expected_generation=1)

        def _upgrade(att_id: str, new_ver: str) -> Attestation:
            return Attestation(
                attestation_id=att_id,
                input=make_signed_input(
                    self.bob.keys, self.bob.key_binding,
                    content=_addon_content("upgrade-planner"),
                ),
                output=sign_put_manifests(
                    self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                    spec_update_manifest({
                        "derive_input_expression": (
                            f'set_path(prior, "manifest_strategy.config.version", "{new_ver}")'
                        ),
                    }),
                ),
            )

        bundle = VerificationBundle(
            inputs={
                "v1": v1_input,
                "v2": DerivedInput(
                    prior_content_id="cluster-01",
                    prior_content_type="deployment",
                    prior_input_id="v1",
                    update_attestation_id="u1",
                ),
            },
            attestations={
                "u1": _upgrade("u1", "1.30.0"),
                "u2": _upgrade("u2", "1.31.0"),
            },
        )
        final_att = Attestation(
            attestation_id="v3",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="v2",
                update_attestation_id="u2",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.31.0"),
            ),
        )
        state = DeploymentState(content_id="cluster-01", generation=3)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                final_att, bundle, self.trust_store,
                target_identity=self.prod_target,
                current_deployment_state=state,
            )
        self.assertIn("generation mismatch", str(ctx.exception))

    def test_derived_generation_absent_when_root_has_none(self) -> None:
        """If the root has no expected_generation, derived chain stays None."""
        v1_input = self._base_input()  # no expected_generation
        upgrade_att = self._upgrade_attestation()

        final_att = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests(),
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        result = verify_attestation(
            final_att, bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

    def test_intermediate_updates_do_not_check_target_generation(self) -> None:
        """Recursive update verification does not carry target-local state."""
        v1_input = self._base_input(expected_generation=1)
        upgrade_att = self._upgrade_attestation()

        final_att = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="cluster-01-v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests(),
            ),
        )
        bundle = VerificationBundle(
            inputs={"cluster-01-v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        state = DeploymentState(content_id="cluster-01", generation=1)
        result = verify_attestation(
            final_att, bundle, self.trust_store,
            target_identity=self.prod_target,
            current_deployment_state=state,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

    # ==================================================================
    # Signed update preconditions
    # ==================================================================

    def test_precondition_satisfied_update_applies(self) -> None:
        """Precondition that matches prior content lets derivation proceed."""
        v1_input = self._base_input(expected_generation=1)
        upgrade_att = self._upgrade_attestation(
            preconditions=[
                'prior.manifest_strategy.config.version == "1.29.5"',
            ],
        )

        final_att = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests(),
            ),
        )
        bundle = VerificationBundle(
            inputs={"v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        result = verify_attestation(
            final_att, bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

    def test_precondition_false_fails_closed(self) -> None:
        """A false precondition halts reconciliation of the attested history."""
        v1_input = self._base_input(expected_generation=1)
        upgrade_att = self._upgrade_attestation(
            preconditions=[
                'prior.manifest_strategy.config.version == "1.28.0"',
            ],
        )

        final_att = Attestation(
            attestation_id="cluster-01-v2",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="v1",
                update_attestation_id="upgrade-1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests(),
            ),
        )
        bundle = VerificationBundle(
            inputs={"v1": v1_input},
            attestations={"upgrade-1": upgrade_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                final_att, bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("precondition failed", str(ctx.exception))

    def test_reordered_updates_fail_when_preconditions_conflict(self) -> None:
        """Two updates that are order-sensitive: swapping them fails on precondition."""
        v1_input = self._base_input(expected_generation=1, version="1.28.0")

        upgrade_1 = Attestation(
            attestation_id="u1",
            input=make_signed_input(
                self.bob.keys, self.bob.key_binding,
                content=_addon_content("upgrade-planner"),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.29.0")'
                    ),
                    "preconditions": [
                        'prior.manifest_strategy.config.version == "1.28.0"',
                    ],
                }),
            ),
        )
        upgrade_2 = Attestation(
            attestation_id="u2",
            input=make_signed_input(
                self.bob.keys, self.bob.key_binding,
                content=_addon_content("upgrade-planner"),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30.0")'
                    ),
                    "preconditions": [
                        'prior.manifest_strategy.config.version == "1.29.0"',
                    ],
                }),
            ),
        )

        # Correct order: v1 -> u1 -> u2 (should work)
        bundle_ok = VerificationBundle(
            inputs={
                "v1": v1_input,
                "v2": DerivedInput(
                    prior_content_id="cluster-01",
                    prior_content_type="deployment",
                    prior_input_id="v1",
                    update_attestation_id="u1",
                ),
            },
            attestations={"u1": upgrade_1, "u2": upgrade_2},
        )
        att_ok = Attestation(
            attestation_id="v3-ok",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="v2",
                update_attestation_id="u2",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.30.0"),
            ),
        )
        result = verify_attestation(
            att_ok, bundle_ok, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(result.signer_id, "capi-provisioner")

        # Swapped order: v1 -> u2 -> u1 (u2 precondition fails against v1)
        bundle_bad = VerificationBundle(
            inputs={
                "v1": v1_input,
                "v2-bad": DerivedInput(
                    prior_content_id="cluster-01",
                    prior_content_type="deployment",
                    prior_input_id="v1",
                    update_attestation_id="u2",
                ),
            },
            attestations={"u1": upgrade_1, "u2": upgrade_2},
        )
        att_bad = Attestation(
            attestation_id="v3-bad",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="v2-bad",
                update_attestation_id="u1",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.30.0"),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att_bad, bundle_bad, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("precondition failed", str(ctx.exception))

    def test_preconditionless_updates_are_reorderable(self) -> None:
        """Updates without preconditions can be applied in any order."""
        v1_input = self._base_input(expected_generation=1, version="1.28.0")

        upgrade_a = Attestation(
            attestation_id="ua",
            input=make_signed_input(
                self.bob.keys, self.bob.key_binding,
                content=_addon_content("upgrade-planner"),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.region", "us-east-1")'
                    ),
                }),
            ),
        )
        upgrade_b = Attestation(
            attestation_id="ub",
            input=make_signed_input(
                self.bob.keys, self.bob.key_binding,
                content=_addon_content("upgrade-planner"),
            ),
            output=sign_put_manifests(
                self.upgrade_planner.keys, "upgrade-planner", "fleet-addons",
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.tier", "premium")'
                    ),
                }),
            ),
        )

        # Order A->B
        bundle_ab = VerificationBundle(
            inputs={
                "v1": v1_input,
                "v2": DerivedInput(
                    prior_content_id="cluster-01",
                    prior_content_type="deployment",
                    prior_input_id="v1",
                    update_attestation_id="ua",
                ),
            },
            attestations={"ua": upgrade_a, "ub": upgrade_b},
        )
        att_ab = Attestation(
            attestation_id="v3-ab",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="v2",
                update_attestation_id="ub",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.28.0"),
            ),
        )
        verify_attestation(
            att_ab, bundle_ab, self.trust_store,
            target_identity=self.prod_target,
        )

        # Order B->A
        bundle_ba = VerificationBundle(
            inputs={
                "v1": v1_input,
                "v2": DerivedInput(
                    prior_content_id="cluster-01",
                    prior_content_type="deployment",
                    prior_input_id="v1",
                    update_attestation_id="ub",
                ),
            },
            attestations={"ua": upgrade_a, "ub": upgrade_b},
        )
        att_ba = Attestation(
            attestation_id="v3-ba",
            input=DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="v2",
                update_attestation_id="ua",
            ),
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.28.0"),
            ),
        )
        verify_attestation(
            att_ba, bundle_ba, self.trust_store,
            target_identity=self.prod_target,
        )

    def test_expected_generation_is_signed(self) -> None:
        """Tampering with expected_generation after signing breaks the envelope."""
        si = self._base_input(expected_generation=1)
        from .model import SignedInput
        tampered = SignedInput(
            content=si.content,
            signature=si.signature,
            key_binding=si.key_binding,
            valid_until=si.valid_until,
            output_constraints=si.output_constraints,
            expected_generation=999,
        )
        att = Attestation(
            attestation_id="tampered-gen",
            input=tampered,
            output=sign_put_manifests(
                self.capi_addon.keys, "capi-provisioner", "fleet-addons",
                self._target_manifests("1.29.5"),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, VerificationBundle(), self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("signed input hash mismatch", str(ctx.exception))


def _all_details(result) -> str:
    """Collect all detail strings from a verification result tree."""
    nodes = _all_nodes(result)
    return " | ".join(node.detail for node in nodes if node.detail)


def _all_nodes(result) -> list:
    nodes = [result]
    for child in result.children:
        nodes.extend(_all_nodes(child))
    return nodes


if __name__ == "__main__":
    unittest.main()
