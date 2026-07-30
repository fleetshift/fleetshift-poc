"""Core provenance, trust, anti-replay, and forgery tests (hybrid-equivalent)."""

from __future__ import annotations

import time
import unittest

from hybrid.model import DeploymentContent, ManifestEnvelope, OutputConstraint, StrategySpec

from sigstore_tuf.build import (
    PoCEnvironment,
    make_put_attestation,
    make_remove_attestation,
    make_signed_input,
    sign_manifests,
    sign_placement,
)
from sigstore_tuf.model import FulfillmentState
from sigstore_tuf.verify import VerificationError, verify_attestation_or_raise, verify_delivery


def _inline(manifests: list[dict], deployment_id: str = "tenant-a/app") -> DeploymentContent:
    return DeploymentContent(
        deployment_id=deployment_id,
        manifest_strategy=StrategySpec(
            type="inline",
            attributes={"manifests": manifests},
        ),
        placement_strategy=StrategySpec(
            type="predicate",
            attributes={"expression": 'target.id == "cluster-1"'},
        ),
    )


def _addon_manifest(deployment_id: str = "tenant-a/app") -> DeploymentContent:
    return DeploymentContent(
        deployment_id=deployment_id,
        manifest_strategy=StrategySpec(
            type="addon",
            attributes={
                "addon_id": "capi-provisioner",
                "trust_anchor_id": "fleet-addons",
            },
        ),
        placement_strategy=StrategySpec(
            type="predicate",
            attributes={"expression": 'target.id == "cluster-1"'},
        ),
    )


def _ns_constraint(ns: str) -> OutputConstraint:
    return OutputConstraint(
        name=f"namespace {ns}",
        expression=f'output.manifests.all(m, m.content.metadata.namespace == "{ns}")',
    )


class ProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env = PoCEnvironment()
        self.target = {"id": "cluster-1"}

    def test_direct_inline_attestation_verifies(self) -> None:
        manifests = (
            ManifestEnvelope(
                resource_type="apps/v1/Deployment",
                content={
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {"namespace": "prod", "name": "app"},
                },
            ),
        )
        serialized = [
            {"resource_type": m.resource_type, "content": m.content} for m in manifests
        ]
        content = _inline(serialized)
        signed = make_signed_input(
            self.env.user_signer(),
            content,
            output_constraints=(_ns_constraint("prod"),),
        )
        att = make_put_attestation("a1", signed, manifests)
        delivery = self.env.assemble(att)
        result = verify_delivery(delivery, target_identity=self.target)
        self.assertTrue(result.valid, result)

    def test_untrusted_identity_rejected(self) -> None:
        content = _inline([])
        # Sign with an identity not on the allowlist.
        from sigstore_tuf.sigstore_sign import Identity, Signer

        rogue = Signer(
            fulcio=self.env.user_fulcio,
            tsa=self.env.tsa,
            identity=Identity(
                issuer="https://issuer.example/tenant-a",
                subject="eve@other.example",
            ),
            trust_anchor_id="tenant-users",
        )
        signed = make_signed_input(rogue, content)
        delivery = self.env.assemble(make_put_attestation("a1", signed, ()))
        result = verify_delivery(delivery, target_identity=self.target)
        self.assertFalse(result.valid)

    def test_tenant_anchor_constraint_rejects_other_tenant_deployment(self) -> None:
        content = _inline([], deployment_id="tenant-b/app")
        signed = make_signed_input(self.env.user_signer(), content)
        delivery = self.env.assemble(make_put_attestation("a1", signed, ()))
        result = verify_delivery(delivery, target_identity=self.target)
        self.assertFalse(result.valid)
        self.assertIn("trust anchor constraint", str(result))

    def test_tampered_envelope_rejected(self) -> None:
        content = _inline([])
        signed = make_signed_input(self.env.user_signer(), content)
        # Tamper after signing.
        object.__setattr__(signed, "valid_until", signed.valid_until + 1000)
        delivery = self.env.assemble(make_put_attestation("a1", signed, ()))
        result = verify_delivery(delivery, target_identity=self.target)
        self.assertFalse(result.valid)

    def test_expired_input_rejected(self) -> None:
        content = _inline([])
        signed = make_signed_input(
            self.env.user_signer(), content, valid_duration_sec=-1
        )
        delivery = self.env.assemble(make_put_attestation("a1", signed, ()))
        result = verify_delivery(delivery, target_identity=self.target)
        self.assertFalse(result.valid)

    def test_addon_manifest_happy_path(self) -> None:
        content = _addon_manifest()
        signed = make_signed_input(self.env.user_signer(), content)
        manifests = (
            ManifestEnvelope(
                resource_type="cluster.x-k8s.io/v1beta1/Cluster",
                content={"metadata": {"namespace": "capi", "name": "c1"}},
            ),
        )
        manifests, mbundle = sign_manifests(self.env.addon_signer("capi-provisioner"), manifests)
        att = make_put_attestation("a1", signed, manifests, manifest_bundle=mbundle)
        result = verify_delivery(self.env.assemble(att), target_identity=self.target)
        self.assertTrue(result.valid, result)

    def test_wrong_addon_signs_manifests_rejected(self) -> None:
        content = _addon_manifest()
        signed = make_signed_input(self.env.user_signer(), content)
        manifests = (
            ManifestEnvelope(
                resource_type="v1/ConfigMap",
                content={"metadata": {"namespace": "capi", "name": "x"}},
            ),
        )
        manifests, mbundle = sign_manifests(
            self.env.addon_signer("upgrade-planner"), manifests
        )
        att = make_put_attestation("a1", signed, manifests, manifest_bundle=mbundle)
        result = verify_delivery(self.env.assemble(att), target_identity=self.target)
        self.assertFalse(result.valid)

    def test_unsigned_addon_manifests_rejected(self) -> None:
        content = _addon_manifest()
        signed = make_signed_input(self.env.user_signer(), content)
        manifests = (
            ManifestEnvelope(
                resource_type="v1/ConfigMap",
                content={"metadata": {"namespace": "capi", "name": "x"}},
            ),
        )
        att = make_put_attestation("a1", signed, manifests)
        result = verify_delivery(self.env.assemble(att), target_identity=self.target)
        self.assertFalse(result.valid)

    def test_namespace_constraint_enforced(self) -> None:
        manifests = (
            ManifestEnvelope(
                resource_type="v1/ConfigMap",
                content={"metadata": {"namespace": "wrong", "name": "x"}},
            ),
        )
        serialized = [
            {"resource_type": m.resource_type, "content": m.content} for m in manifests
        ]
        content = _inline(serialized)
        signed = make_signed_input(
            self.env.user_signer(), content, output_constraints=(_ns_constraint("prod"),)
        )
        att = make_put_attestation("a1", signed, manifests)
        result = verify_delivery(self.env.assemble(att), target_identity=self.target)
        self.assertFalse(result.valid)

    def test_predicate_placement_wrong_target_rejected(self) -> None:
        content = _inline([])
        signed = make_signed_input(self.env.user_signer(), content)
        att = make_put_attestation("a1", signed, ())
        result = verify_delivery(
            self.env.assemble(att), target_identity={"id": "cluster-9"}
        )
        self.assertFalse(result.valid)

    def test_placement_evidence_cross_deployment_replay_rejected(self) -> None:
        content = DeploymentContent(
            deployment_id="tenant-a/app",
            manifest_strategy=StrategySpec(type="inline", attributes={"manifests": []}),
            placement_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "placement-addon",
                    "trust_anchor_id": "fleet-addons",
                },
            ),
        )
        signed = make_signed_input(self.env.user_signer(), content)
        # Evidence bound to a different deployment id.
        pbundle = sign_placement(
            self.env.addon_signer("placement-addon"),
            deployment_id="tenant-a/other",
            targets=("cluster-1",),
        )
        att = make_put_attestation(
            "a1",
            signed,
            (),
            placement_targets=("cluster-1",),
            placement_bundle=pbundle,
            placement_deployment_id="tenant-a/other",
        )
        result = verify_delivery(self.env.assemble(att), target_identity=self.target)
        self.assertFalse(result.valid)

    def test_remove_deployment_id_mismatch_rejected(self) -> None:
        content = _inline([])
        signed = make_signed_input(self.env.user_signer(), content)
        att = make_remove_attestation("a1", signed, "tenant-a/other")
        # Remove with wrong target predicate still fails on id mismatch first.
        result = verify_delivery(
            self.env.assemble(att), target_identity={"id": "cluster-9"}
        )
        self.assertFalse(result.valid)

    def test_stale_generation_put_rejected(self) -> None:
        content = _inline([])
        signed = make_signed_input(
            self.env.user_signer(), content, expected_generation=1
        )
        att = make_put_attestation("a1", signed, ())
        result = verify_delivery(
            self.env.assemble(att),
            target_identity=self.target,
            current_fulfillment_state=FulfillmentState(
                content_id="tenant-a/app", generation=5
            ),
        )
        self.assertFalse(result.valid)
        self.assertIn("generation", str(result).lower())

    def test_generation_matches_accepted(self) -> None:
        content = _inline([])
        signed = make_signed_input(
            self.env.user_signer(), content, expected_generation=6
        )
        att = make_put_attestation("a1", signed, ())
        result = verify_delivery(
            self.env.assemble(att),
            target_identity=self.target,
            current_fulfillment_state=FulfillmentState(
                content_id="tenant-a/app", generation=5
            ),
        )
        self.assertTrue(result.valid, result)

    def test_expected_generation_is_signed(self) -> None:
        content = _inline([])
        signed = make_signed_input(
            self.env.user_signer(), content, expected_generation=2
        )
        # Mutate generation after signing — envelope digest no longer matches.
        object.__setattr__(signed, "expected_generation", 99)
        result = verify_delivery(
            self.env.assemble(make_put_attestation("a1", signed, ())),
            target_identity=self.target,
        )
        self.assertFalse(result.valid)

    def test_embedded_tuf_required_no_http_fetch(self) -> None:
        """Agent verifies using only the preassembled bundle + bootstrap keyids."""
        content = _inline([])
        signed = make_signed_input(self.env.user_signer(), content)
        delivery = self.env.assemble(make_put_attestation("a1", signed, ()))
        # No network: verify_delivery only reads delivery.tuf_snapshot.
        verify_attestation_or_raise(delivery, target_identity=self.target)

    def test_tuf_tamper_rejected(self) -> None:
        content = _inline([])
        signed = make_signed_input(self.env.user_signer(), content)
        delivery = self.env.assemble(make_put_attestation("a1", signed, ()))
        delivery.tuf_snapshot["_targets_raw"]["trust-anchors.json"] = "00"
        result = verify_delivery(delivery, target_identity=self.target)
        self.assertFalse(result.valid)


if __name__ == "__main__":
    unittest.main()
