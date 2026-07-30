"""Fleet-management scenarios: placement, addon co-sign, generation fencing."""

from __future__ import annotations

import unittest

from hybrid.model import DeploymentContent, ManifestEnvelope, StrategySpec

from sigstore_tuf.build import (
    PoCEnvironment,
    make_put_attestation,
    make_remove_attestation,
    make_signed_input,
    sign_manifests,
    sign_placement,
)
from sigstore_tuf.model import FulfillmentState
from sigstore_tuf.verify import verify_delivery


class FleetScenarioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env = PoCEnvironment()

    def test_addon_manifest_addon_placement_put(self) -> None:
        content = DeploymentContent(
            deployment_id="tenant-a/worker",
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
                    "addon_id": "placement-addon",
                    "trust_anchor_id": "fleet-addons",
                },
            ),
        )
        signed = make_signed_input(self.env.user_signer(), content)
        manifests = (
            ManifestEnvelope(
                resource_type="cluster.x-k8s.io/v1beta1/MachineDeployment",
                content={"metadata": {"name": "md", "namespace": "capi"}},
            ),
        )
        manifests, mb = sign_manifests(self.env.addon_signer("capi-provisioner"), manifests)
        pb = sign_placement(
            self.env.addon_signer("placement-addon"),
            deployment_id="tenant-a/worker",
            targets=("cluster-1", "cluster-2"),
        )
        att = make_put_attestation(
            "deploy-1",
            signed,
            manifests,
            manifest_bundle=mb,
            placement_targets=("cluster-1", "cluster-2"),
            placement_bundle=pb,
            placement_deployment_id="tenant-a/worker",
        )
        result = verify_delivery(
            self.env.assemble(att),
            target_identity={"id": "cluster-1"},
        )
        self.assertTrue(result.valid, result)

    def test_addon_placement_target_not_in_decision_rejected(self) -> None:
        content = DeploymentContent(
            deployment_id="tenant-a/worker",
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
        pb = sign_placement(
            self.env.addon_signer("placement-addon"),
            deployment_id="tenant-a/worker",
            targets=("cluster-1",),
        )
        att = make_put_attestation(
            "deploy-1",
            signed,
            (),
            placement_targets=("cluster-1",),
            placement_bundle=pb,
            placement_deployment_id="tenant-a/worker",
        )
        result = verify_delivery(
            self.env.assemble(att),
            target_identity={"id": "cluster-99"},
        )
        self.assertFalse(result.valid)

    def test_predicate_remove_when_target_no_longer_matches(self) -> None:
        content = DeploymentContent(
            deployment_id="tenant-a/app",
            manifest_strategy=StrategySpec(type="inline", attributes={"manifests": []}),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.id == "cluster-1"'},
            ),
        )
        signed = make_signed_input(self.env.user_signer(), content)
        att = make_remove_attestation("rm-1", signed, "tenant-a/app")
        # Target no longer matches predicate → remove allowed.
        result = verify_delivery(
            self.env.assemble(att),
            target_identity={"id": "cluster-retired"},
        )
        self.assertTrue(result.valid, result)

    def test_generation_fencing_across_rollout(self) -> None:
        content = DeploymentContent(
            deployment_id="tenant-a/app",
            manifest_strategy=StrategySpec(type="inline", attributes={"manifests": []}),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": "true"},
            ),
        )
        # First apply at generation 0 -> expect 1
        signed_v1 = make_signed_input(
            self.env.user_signer(), content, expected_generation=1
        )
        d1 = self.env.assemble(make_put_attestation("v1", signed_v1, ()))
        self.assertTrue(
            verify_delivery(
                d1,
                target_identity={"id": "cluster-1"},
                current_fulfillment_state=FulfillmentState("tenant-a/app", 0),
            ).valid
        )
        # Stale retry of generation 1 after local state advanced.
        self.assertFalse(
            verify_delivery(
                d1,
                target_identity={"id": "cluster-1"},
                current_fulfillment_state=FulfillmentState("tenant-a/app", 1),
            ).valid
        )
        signed_v2 = make_signed_input(
            self.env.user_signer(), content, expected_generation=2
        )
        d2 = self.env.assemble(make_put_attestation("v2", signed_v2, ()))
        self.assertTrue(
            verify_delivery(
                d2,
                target_identity={"id": "cluster-1"},
                current_fulfillment_state=FulfillmentState("tenant-a/app", 1),
            ).valid
        )


if __name__ == "__main__":
    unittest.main()
