"""Delivery-equivalence tests ported from hybrid/test_delivery.py."""

from __future__ import annotations

import time
import unittest

from hybrid.model import DeploymentContent, ManifestEnvelope, OutputConstraint, StrategySpec
from hybrid.policy import constraint_to_document

from sigstore_tuf._test_helpers import (
    addon_content,
    addon_must_sign,
    all_details,
    k8s_manifests,
    make_env,
    namespace_constraint,
    serialize_envelopes,
    signed_put,
    spec_update_manifest,
    unsigned_put,
)
from sigstore_tuf.build import (
    make_put_attestation,
    make_remove_attestation,
    make_signed_input,
    sign_manifests,
    sign_placement,
)
from sigstore_tuf.model import DerivedInput, FulfillmentState
from sigstore_tuf.sigstore_sign import Identity, Signer
from sigstore_tuf.verify import (
    VerificationError,
    explain_verification,
    verify_attestation_or_raise,
    verify_delivery,
)


SAMPLE_MANIFESTS = k8s_manifests(
    {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "nginx", "namespace": "default"},
    },
)
SAMPLE_SERIALIZED = serialize_envelopes(SAMPLE_MANIFESTS)


class DeliveryVerificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env = make_env(extra_addons=("evil",))
        self.alice = self.env.user_signer("alice@tenant-a.example")
        self.obs = self.env.addon_signer("observability")
        self.placer = self.env.addon_signer("capacity-planner")
        self.evil = Signer(
            fulcio=self.env.workload_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer="https://oidc.addon.local", subject="evil"),
            trust_anchor_id="evil-anchor",
        )
        self.prod_target = {
            "id": "cluster-prod-1",
            "labels": {"env": "prod", "region": "us-east-1"},
        }
        self.staging_target = {
            "id": "cluster-staging-1",
            "labels": {"env": "staging", "region": "us-west-2"},
        }

    def _inline_predicate(self, manifests, predicate, *, output_constraints=()):
        return make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="inline",
                    attributes={"manifests": serialize_envelopes(manifests)},
                ),
                placement_strategy=StrategySpec(
                    type="predicate", attributes={"expression": predicate},
                ),
            ),
            output_constraints=output_constraints,
        )

    def _addon_predicate(self, addon_id="observability", predicate='target.labels.env == "prod"', *, output_constraints=()):
        return make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": addon_id, "trust_anchor_id": "fleet-addons"},
                ),
                placement_strategy=StrategySpec(
                    type="predicate", attributes={"expression": predicate},
                ),
            ),
            output_constraints=output_constraints,
        )

    def _addon_addon(self):
        return make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": "observability", "trust_anchor_id": "fleet-addons"},
                ),
                placement_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": "capacity-planner", "trust_anchor_id": "fleet-addons"},
                ),
            ),
        )

    def _inline_addon(self, manifests):
        return make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="inline",
                    attributes={"manifests": serialize_envelopes(manifests)},
                ),
                placement_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": "capacity-planner", "trust_anchor_id": "fleet-addons"},
                ),
            ),
        )

    def test_inline_manifest_put_happy_path(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_inline_manifest_tampered_output(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        tampered = k8s_manifests({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "x"}})
        d = unsigned_put(self.env, "att-1", si, tampered)
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_addon_manifest_put_happy_path(self) -> None:
        si = self._addon_predicate()
        d = signed_put(self.env, "att-1", self.obs, si, SAMPLE_MANIFESTS)
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_addon_manifest_wrong_addon_signs(self) -> None:
        si = self._addon_predicate()
        d = signed_put(self.env, "att-1", self.placer, si, SAMPLE_MANIFESTS)
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_addon_manifest_missing_signature(self) -> None:
        si = self._addon_predicate()
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_addon_manifest_unknown_trust_anchor_rejected(self) -> None:
        si = make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": "observability", "trust_anchor_id": "missing-ca"},
                ),
                placement_strategy=StrategySpec(
                    type="predicate", attributes={"expression": 'target.labels.env == "prod"'},
                ),
            ),
            output_constraints=(addon_must_sign("observability", "missing-ca"),),
        )
        # Strategy only checks signer_id; explicit constraint requires missing-ca.
        # Sign with fleet-addons trust_anchor_id — constraint fails.
        d = signed_put(self.env, "att-1", self.obs, si, SAMPLE_MANIFESTS)
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_predicate_placement_target_matches_put_accepted(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_predicate_placement_target_no_match_put_rejected(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        self.assertFalse(verify_delivery(d, target_identity=self.staging_target).valid)

    def test_predicate_placement_target_no_match_remove_accepted(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        att = make_remove_attestation("att-1", si, "deploy-1")
        self.assertTrue(verify_delivery(self.env.assemble(att), target_identity=self.staging_target).valid)

    def test_predicate_placement_target_matches_remove_rejected(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        att = make_remove_attestation("att-1", si, "deploy-1")
        self.assertFalse(verify_delivery(self.env.assemble(att), target_identity=self.prod_target).valid)

    def test_addon_placement_signed_evidence_accepted(self) -> None:
        si = self._inline_addon(SAMPLE_MANIFESTS)
        pb = sign_placement(self.placer, deployment_id="deploy-1", targets=("cluster-prod-1",))
        att = make_put_attestation(
            "att-1", si, SAMPLE_MANIFESTS,
            placement_targets=("cluster-prod-1",), placement_bundle=pb,
            placement_deployment_id="deploy-1",
        )
        self.assertTrue(verify_delivery(self.env.assemble(att), target_identity=self.prod_target).valid)

    def test_addon_placement_missing_evidence_rejected(self) -> None:
        si = self._inline_addon(SAMPLE_MANIFESTS)
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_addon_placement_wrong_addon_rejected(self) -> None:
        si = self._inline_addon(SAMPLE_MANIFESTS)
        pb = sign_placement(self.obs, deployment_id="deploy-1", targets=("cluster-prod-1",))
        att = make_put_attestation(
            "att-1", si, SAMPLE_MANIFESTS,
            placement_targets=("cluster-prod-1",), placement_bundle=pb,
            placement_deployment_id="deploy-1",
        )
        self.assertFalse(verify_delivery(self.env.assemble(att), target_identity=self.prod_target).valid)

    def test_addon_placement_target_not_in_decision_rejected(self) -> None:
        si = self._inline_addon(SAMPLE_MANIFESTS)
        pb = sign_placement(self.placer, deployment_id="deploy-1", targets=("cluster-prod-1",))
        att = make_put_attestation(
            "att-1", si, SAMPLE_MANIFESTS,
            placement_targets=("cluster-prod-1",), placement_bundle=pb,
            placement_deployment_id="deploy-1",
        )
        self.assertFalse(verify_delivery(self.env.assemble(att), target_identity=self.staging_target).valid)

    def test_remove_predicate_non_match_accepted(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        att = make_remove_attestation("att-1", si, "deploy-1")
        self.assertTrue(verify_delivery(self.env.assemble(att), target_identity=self.staging_target).valid)

    def test_remove_predicate_match_rejected(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        att = make_remove_attestation("att-1", si, "deploy-1")
        self.assertFalse(verify_delivery(self.env.assemble(att), target_identity=self.prod_target).valid)

    def test_remove_addon_placement_accepted(self) -> None:
        si = self._addon_addon()
        pb = sign_placement(self.placer, deployment_id="deploy-1", targets=("cluster-prod-2",))
        att = make_remove_attestation(
            "att-1", si, "deploy-1",
            placement_targets=("cluster-prod-2",), placement_bundle=pb,
            placement_deployment_id="deploy-1",
        )
        self.assertTrue(verify_delivery(self.env.assemble(att), target_identity=self.prod_target).valid)

    def test_remove_deployment_id_mismatch_rejected(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"')
        att = make_remove_attestation("att-1", si, "other-deploy")
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(self.env.assemble(att), target_identity=self.staging_target)
        self.assertIn("deployment_id mismatch", str(ctx.exception))

    def test_namespace_constraint_plus_addon_strategy(self) -> None:
        manifests = k8s_manifests({"metadata": {"namespace": "obs", "name": "x"}, "kind": "ConfigMap"})
        si = self._addon_predicate(output_constraints=(namespace_constraint("obs"),))
        d = signed_put(self.env, "att-1", self.obs, si, manifests)
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_namespace_constraint_fails_with_wrong_namespace(self) -> None:
        manifests = k8s_manifests({"metadata": {"namespace": "wrong", "name": "x"}, "kind": "ConfigMap"})
        si = self._addon_predicate(output_constraints=(namespace_constraint("obs"),))
        d = signed_put(self.env, "att-1", self.obs, si, manifests)
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_gvk_allowlist_plus_inline_strategy(self) -> None:
        from sigstore_tuf._test_helpers import allowed_gvks
        si = self._inline_predicate(
            SAMPLE_MANIFESTS, 'target.labels.env == "prod"',
            output_constraints=(allowed_gvks("apps/v1/Deployment"),),
        )
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_user_constraint_fails_even_though_strategy_passes(self) -> None:
        si = self._inline_predicate(
            SAMPLE_MANIFESTS, 'target.labels.env == "prod"',
            output_constraints=(namespace_constraint("forbidden"),),
        )
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_placement_batch_size_constraint(self) -> None:
        si = make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(type="inline", attributes={"manifests": SAMPLE_SERIALIZED}),
                placement_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": "capacity-planner", "trust_anchor_id": "fleet-addons"},
                ),
            ),
            output_constraints=(
                OutputConstraint(
                    name="batch size at most 1",
                    expression="size(placement.targets) <= 1",
                ),
            ),
        )
        pb = sign_placement(self.placer, deployment_id="deploy-1", targets=("a", "b"))
        att = make_put_attestation(
            "att-1", si, SAMPLE_MANIFESTS,
            placement_targets=("a", "b"), placement_bundle=pb, placement_deployment_id="deploy-1",
        )
        self.assertFalse(verify_delivery(self.env.assemble(att), target_identity={"id": "a"}).valid)

    def test_swap_manifests_between_attestations(self) -> None:
        m1 = k8s_manifests({"kind": "A"})
        m2 = k8s_manifests({"kind": "B"})
        m1s, mb1 = sign_manifests(self.obs, m1)
        si = self._addon_predicate()
        att = make_put_attestation("att-1", si, m2, manifest_bundle=mb1)
        self.assertFalse(verify_delivery(self.env.assemble(att), target_identity=self.prod_target).valid)

    def test_expired_attestation_rejected(self) -> None:
        si = self._inline_predicate(SAMPLE_MANIFESTS, 'target.labels.env == "prod"', )
        # recreate with expired
        si = make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(type="inline", attributes={"manifests": SAMPLE_SERIALIZED}),
                placement_strategy=StrategySpec(type="predicate", attributes={"expression": 'target.labels.env == "prod"'}),
            ),
            valid_duration_sec=-1,
        )
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_placement_evidence_cross_deployment_replay_rejected(self) -> None:
        si = self._inline_addon(SAMPLE_MANIFESTS)
        pb = sign_placement(self.placer, deployment_id="other", targets=("cluster-prod-1",))
        att = make_put_attestation(
            "att-1", si, SAMPLE_MANIFESTS,
            placement_targets=("cluster-prod-1",), placement_bundle=pb,
            placement_deployment_id="other",
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(self.env.assemble(att), target_identity=self.prod_target)
        self.assertIn("deployment_id mismatch", str(ctx.exception))

    def test_forged_placement_evidence_wrong_key(self) -> None:
        # evil identity not allowed under fleet-addons — sign claiming capacity-planner subject via wrong trust
        evil_placer = Signer(
            fulcio=self.env.workload_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer="https://oidc.addon.local", subject="not-registered"),
            trust_anchor_id="fleet-addons",
        )
        si = self._inline_addon(SAMPLE_MANIFESTS)
        pb = sign_placement(evil_placer, deployment_id="deploy-1", targets=("cluster-prod-1",))
        att = make_put_attestation(
            "att-1", si, SAMPLE_MANIFESTS,
            placement_targets=("cluster-prod-1",), placement_bundle=pb,
            placement_deployment_id="deploy-1",
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(self.env.assemble(att), target_identity=self.prod_target)
        self.assertIn("placement", str(ctx.exception).lower())

    def test_unknown_manifest_strategy_fails_closed(self) -> None:
        si = make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(type="custom-unknown"),
                placement_strategy=StrategySpec(type="predicate", attributes={"expression": 'target.labels.env == "prod"'}),
            ),
        )
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target)
        self.assertIn("unknown manifest strategy type", str(ctx.exception))

    def test_unknown_placement_strategy_fails_closed(self) -> None:
        si = make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(type="inline", attributes={"manifests": SAMPLE_SERIALIZED}),
                placement_strategy=StrategySpec(type="custom-unknown"),
            ),
        )
        d = unsigned_put(self.env, "att-1", si, SAMPLE_MANIFESTS)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target)
        self.assertIn("unknown placement strategy type", str(ctx.exception))

    def test_addon_manifest_addon_placement_put_happy_path(self) -> None:
        si = self._addon_addon()
        manifests, mb = sign_manifests(self.obs, SAMPLE_MANIFESTS)
        pb = sign_placement(self.placer, deployment_id="deploy-1", targets=("cluster-prod-1",))
        att = make_put_attestation(
            "att-1", si, manifests, manifest_bundle=mb,
            placement_targets=("cluster-prod-1",), placement_bundle=pb,
            placement_deployment_id="deploy-1",
        )
        self.assertTrue(verify_delivery(self.env.assemble(att), target_identity=self.prod_target).valid)

    def test_addon_manifest_addon_placement_remove_happy_path(self) -> None:
        si = self._addon_addon()
        pb = sign_placement(self.placer, deployment_id="deploy-1", targets=("cluster-prod-2",))
        att = make_remove_attestation(
            "att-1", si, "deploy-1",
            placement_targets=("cluster-prod-2",), placement_bundle=pb,
            placement_deployment_id="deploy-1",
        )
        self.assertTrue(verify_delivery(self.env.assemble(att), target_identity=self.prod_target).valid)

    def test_inline_manifest_addon_placement_put(self) -> None:
        si = self._inline_addon(SAMPLE_MANIFESTS)
        pb = sign_placement(self.placer, deployment_id="deploy-1", targets=("cluster-prod-1",))
        att = make_put_attestation(
            "att-1", si, SAMPLE_MANIFESTS,
            placement_targets=("cluster-prod-1",), placement_bundle=pb,
            placement_deployment_id="deploy-1",
        )
        self.assertTrue(verify_delivery(self.env.assemble(att), target_identity=self.prod_target).valid)

    def test_forged_manifest_signature_untrusted_key(self) -> None:
        si = self._addon_predicate()
        forged = Signer(
            fulcio=self.env.workload_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer="https://oidc.addon.local", subject="observability"),
            trust_anchor_id="fleet-addons",
        )
        # Use unregistered subject pretending to be observability — wait, observability is registered.
        # Use not-registered subject:
        forged = Signer(
            fulcio=self.env.workload_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer="https://oidc.addon.local", subject="not-registered"),
            trust_anchor_id="fleet-addons",
        )
        d = signed_put(self.env, "att-1", forged, si, SAMPLE_MANIFESTS)
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target)
        self.assertIn("manifest signature", str(ctx.exception).lower())


class FleetWideUpgradeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env = make_env()
        self.alice = self.env.user_signer("alice@tenant-a.example")
        self.bob = self.env.user_signer("bob@tenant-a.example")
        self.planner = self.env.addon_signer("upgrade-planner")
        self.capi = self.env.addon_signer("capi-provisioner")
        self.placer = self.env.addon_signer("capacity-planner")
        self.prod_target = {"id": "cluster-prod-1", "labels": {"env": "prod", "region": "us-east-1"}}
        self.staging_target = {"id": "cluster-staging-1", "labels": {"env": "staging", "region": "us-west-2"}}

    def _base(self, *, deployment_id="cluster-01", version="1.29.5", predicate='target.labels.env == "prod"', expected_generation=None):
        return make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id=deployment_id,
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": "capi-provisioner", "trust_anchor_id": "fleet-addons", "config": {"version": version}},
                ),
                placement_strategy=StrategySpec(type="predicate", attributes={"expression": predicate}),
            ),
            output_constraints=(addon_must_sign("capi-provisioner"),),
            expected_generation=expected_generation,
        )

    def _upgrade(self, *, new_version="1.30.2", extra_constraints=(), target_deployments=("cluster-01",), preconditions=None, att_id="upgrade-1"):
        update_directive = {
            "derive_input_expression": f'set_path(prior, "manifest_strategy.config.version", "{new_version}")',
            "output_constraints": [
                constraint_to_document(namespace_constraint("capi-system")),
                *(constraint_to_document(c) for c in extra_constraints),
            ],
        }
        if preconditions is not None:
            update_directive["preconditions"] = preconditions
            # when testing preconditions alone, still ok to have namespace constraint
        um = spec_update_manifest(update_directive)
        um, umb = sign_manifests(self.planner, um)
        pb = sign_placement(self.placer, deployment_id="upgrade-request-1", targets=target_deployments)
        return make_put_attestation(
            att_id,
            make_signed_input(
                self.bob,
                DeploymentContent(
                    deployment_id="upgrade-request-1",
                    manifest_strategy=StrategySpec(
                        type="addon",
                        attributes={"addon_id": "upgrade-planner", "trust_anchor_id": "fleet-addons"},
                    ),
                    placement_strategy=StrategySpec(
                        type="addon",
                        attributes={"addon_id": "capacity-planner", "trust_anchor_id": "fleet-addons"},
                    ),
                ),
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            um,
            manifest_bundle=umb,
            placement_targets=target_deployments,
            placement_bundle=pb,
            placement_deployment_id="upgrade-request-1",
        )

    def _simple_upgrade(self, *, new_version="1.30.2", preconditions=None, att_id="upgrade-1"):
        directive = {
            "derive_input_expression": f'set_path(prior, "manifest_strategy.config.version", "{new_version}")',
        }
        if preconditions is not None:
            directive["preconditions"] = preconditions
        um = spec_update_manifest(directive)
        um, umb = sign_manifests(self.planner, um)
        return make_put_attestation(
            att_id,
            make_signed_input(
                self.bob,
                addon_content("upgrade-planner", deployment_id="upgrade-request-1"),
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            um,
            manifest_bundle=umb,
        )

    def _target_manifests(self, version="1.30.2"):
        return k8s_manifests({
            "apiVersion": "cluster.x-k8s.io/v1beta1",
            "kind": "Cluster",
            "metadata": {"name": "workload-01", "namespace": "capi-system"},
            "spec": {"topology": {"version": version}},
        })

    def _final(self, prior_id, update_id, manifests=None, att_id="cluster-01-v2"):
        return signed_put(
            self.env,
            att_id,
            self.capi,
            DerivedInput("cluster-01", "deployment", prior_id, update_id),
            manifests or self._target_manifests(),
            prior_inputs={prior_id: self._base()} if False else None,  # filled by caller via kwargs
        )

    def test_fleet_upgrade_happy_path(self) -> None:
        v1 = self._base()
        up = self._upgrade()
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": up},
        )
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_fleet_upgrade_explanation_shows_full_chain(self) -> None:
        v1 = self._base()
        up = self._upgrade()
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": up},
        )
        explanation = explain_verification(d, target_identity=self.prod_target)
        details = all_details(explanation)
        self.assertIn("derived from prior=cluster-01-v1 + update=upgrade-1", details)
        self.assertIn("upgrade-planner", details)
        self.assertIn("capi-provisioner", details)

    def test_fleet_upgrade_wrong_target_rejected(self) -> None:
        v1 = self._base()
        up = self._upgrade()
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": up},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.staging_target)
        self.assertIn("placement predicate", str(ctx.exception))

    def test_fleet_upgrade_wrong_manifest_signer_rejected(self) -> None:
        v1 = self._base()
        up = self._upgrade()
        d = signed_put(
            self.env, "cluster-01-v2", self.planner,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": up},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target)
        self.assertIn("capi-provisioner", str(ctx.exception))

    def test_fleet_upgrade_prior_constraints_carry_forward(self) -> None:
        v1 = self._base()
        # bare upgrade without namespace constraint in directive — but prior still requires capi
        um = spec_update_manifest({
            "derive_input_expression": 'set_path(prior, "manifest_strategy.config.version", "1.30.2")',
        })
        um, umb = sign_manifests(self.planner, um)
        pb = sign_placement(self.placer, deployment_id="upgrade-request-1", targets=("cluster-01",))
        bare = make_put_attestation(
            "upgrade-1",
            make_signed_input(
                self.bob,
                DeploymentContent(
                    deployment_id="upgrade-request-1",
                    manifest_strategy=StrategySpec(type="addon", attributes={"addon_id": "upgrade-planner", "trust_anchor_id": "fleet-addons"}),
                    placement_strategy=StrategySpec(type="addon", attributes={"addon_id": "capacity-planner", "trust_anchor_id": "fleet-addons"}),
                ),
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            um, manifest_bundle=umb,
            placement_targets=("cluster-01",), placement_bundle=pb, placement_deployment_id="upgrade-request-1",
        )
        d = signed_put(
            self.env, "cluster-01-v2", self.placer,  # wrong final signer
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": bare},
        )
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_fleet_upgrade_prior_constraints_carry_forward_happy_path(self) -> None:
        v1 = self._base()
        um = spec_update_manifest({
            "derive_input_expression": 'set_path(prior, "manifest_strategy.config.version", "1.30.2")',
        })
        um, umb = sign_manifests(self.planner, um)
        pb = sign_placement(self.placer, deployment_id="upgrade-request-1", targets=("cluster-01",))
        bare = make_put_attestation(
            "upgrade-1",
            make_signed_input(
                self.bob,
                DeploymentContent(
                    deployment_id="upgrade-request-1",
                    manifest_strategy=StrategySpec(type="addon", attributes={"addon_id": "upgrade-planner", "trust_anchor_id": "fleet-addons"}),
                    placement_strategy=StrategySpec(type="addon", attributes={"addon_id": "capacity-planner", "trust_anchor_id": "fleet-addons"}),
                ),
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            um, manifest_bundle=umb,
            placement_targets=("cluster-01",), placement_bundle=pb, placement_deployment_id="upgrade-request-1",
        )
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": bare},
        )
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_fleet_upgrade_namespace_violation_rejected(self) -> None:
        v1 = self._base()
        up = self._upgrade()
        bad = k8s_manifests({
            "apiVersion": "cluster.x-k8s.io/v1beta1", "kind": "Cluster",
            "metadata": {"name": "workload-01", "namespace": "wrong"},
            "spec": {"topology": {"version": "1.30.2"}},
        })
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            bad,
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": up},
        )
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_fleet_upgrade_untrusted_upgrade_signer_rejected(self) -> None:
        v1 = self._base()
        eve = Signer(
            fulcio=self.env.user_fulcio, tsa=self.env.tsa,
            identity=Identity(issuer=self.env.user_issuer, subject="eve@evil.example"),
            trust_anchor_id=self.env.user_anchor_id,
        )
        um = spec_update_manifest({
            "derive_input_expression": 'set_path(prior, "manifest_strategy.config.version", "1.30.2")',
            "output_constraints": [constraint_to_document(namespace_constraint("capi-system"))],
        })
        um, umb = sign_manifests(self.planner, um)
        pb = sign_placement(self.placer, deployment_id="upgrade-request-1", targets=("cluster-01",))
        up = make_put_attestation(
            "upgrade-1",
            make_signed_input(
                eve,
                DeploymentContent(
                    deployment_id="upgrade-request-1",
                    manifest_strategy=StrategySpec(type="addon", attributes={"addon_id": "upgrade-planner", "trust_anchor_id": "fleet-addons"}),
                    placement_strategy=StrategySpec(type="addon", attributes={"addon_id": "capacity-planner", "trust_anchor_id": "fleet-addons"}),
                ),
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            um, manifest_bundle=umb,
            placement_targets=("cluster-01",), placement_bundle=pb, placement_deployment_id="upgrade-request-1",
        )
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": up},
        )
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_fleet_upgrade_chained_two_hops(self) -> None:
        v1 = self._base(version="1.28.0")
        u1 = self._upgrade(new_version="1.29.0", att_id="u1")
        # second hop needs different att and placement
        u2 = self._upgrade(new_version="1.30.2", att_id="u2")
        # Fix: _upgrade always uses upgrade-request-1 — for second update reuse is fine as separate attestations
        d = signed_put(
            self.env, "v3", self.capi,
            DerivedInput("cluster-01", "deployment", "v2", "u2"),
            self._target_manifests("1.30.2"),
            prior_inputs={
                "v1": v1,
                "v2": DerivedInput("cluster-01", "deployment", "v1", "u1"),
            },
            update_attestations={"u1": u1, "u2": u2},
        )
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_fleet_upgrade_with_addon_placement(self) -> None:
        v1 = make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="cluster-01",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": "capi-provisioner", "trust_anchor_id": "fleet-addons", "config": {"version": "1.29.5"}},
                ),
                placement_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": "capacity-planner", "trust_anchor_id": "fleet-addons"},
                ),
            ),
            output_constraints=(addon_must_sign("capi-provisioner"),),
        )
        up = self._upgrade()
        manifests, mb = sign_manifests(self.capi, self._target_manifests())
        pb = sign_placement(self.placer, deployment_id="cluster-01", targets=("cluster-prod-1",))
        att = make_put_attestation(
            "cluster-01-v2",
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            manifests, manifest_bundle=mb,
            placement_targets=("cluster-prod-1",), placement_bundle=pb, placement_deployment_id="cluster-01",
        )
        d = self.env.assemble(att, prior_inputs={"cluster-01-v1": v1}, update_attestations={"upgrade-1": up})
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_fleet_upgrade_cross_deployment_evidence_rejected(self) -> None:
        v1 = self._base()
        up = self._upgrade(target_deployments=("cluster-99",))  # upgrade not for cluster-01
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": up},
        )
        # Update placement targets cluster-99; update verified with target id=cluster-01 → reject
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_fleet_upgrade_replay_against_different_deployment_rejected(self) -> None:
        v1 = self._base(deployment_id="cluster-01")
        up = self._upgrade()
        # Claim prior_content_id of different deployment
        d = signed_put(
            self.env, "cluster-02-v2", self.capi,
            DerivedInput("cluster-02", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": up},
        )
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_fleet_upgrade_update_must_not_retarget_deployment_identity(self) -> None:
        v1 = self._base()
        um = spec_update_manifest({
            "derive_input_expression": 'set_path(prior, "deployment_id", "hijacked")',
        })
        um, umb = sign_manifests(self.planner, um)
        pb = sign_placement(self.placer, deployment_id="upgrade-request-1", targets=("cluster-01",))
        up = make_put_attestation(
            "upgrade-1",
            make_signed_input(
                self.bob,
                DeploymentContent(
                    deployment_id="upgrade-request-1",
                    manifest_strategy=StrategySpec(type="addon", attributes={"addon_id": "upgrade-planner", "trust_anchor_id": "fleet-addons"}),
                    placement_strategy=StrategySpec(type="addon", attributes={"addon_id": "capacity-planner", "trust_anchor_id": "fleet-addons"}),
                ),
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            um, manifest_bundle=umb,
            placement_targets=("cluster-01",), placement_bundle=pb, placement_deployment_id="upgrade-request-1",
        )
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1},
            update_attestations={"upgrade-1": up},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target)
        self.assertIn("must not rewrite content identity", str(ctx.exception))


class GenerationFencingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env = make_env()
        self.alice = self.env.user_signer()
        self.bob = self.env.user_signer("bob@tenant-a.example")
        self.planner = self.env.addon_signer("upgrade-planner")
        self.capi = self.env.addon_signer("capi-provisioner")
        self.prod_target = {"id": "cluster-prod-1", "labels": {"env": "prod"}}

    def _base_input(self, *, expected_generation=None, version="1.29.5"):
        return make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="cluster-01",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={"addon_id": "capi-provisioner", "trust_anchor_id": "fleet-addons", "config": {"version": version}},
                ),
                placement_strategy=StrategySpec(type="predicate", attributes={"expression": 'target.labels.env == "prod"'}),
            ),
            output_constraints=(addon_must_sign("capi-provisioner"),),
            expected_generation=expected_generation,
        )

    def _upgrade_attestation(self, *, new_version="1.30.2", preconditions=None, att_id="upgrade-1"):
        directive = {"derive_input_expression": f'set_path(prior, "manifest_strategy.config.version", "{new_version}")'}
        if preconditions is not None:
            directive["preconditions"] = preconditions
        um = spec_update_manifest(directive)
        um, umb = sign_manifests(self.planner, um)
        return make_put_attestation(
            att_id,
            make_signed_input(self.bob, addon_content("upgrade-planner"), output_constraints=(addon_must_sign("upgrade-planner"),)),
            um, manifest_bundle=umb,
        )

    def _target_manifests(self, version="1.30.2"):
        return k8s_manifests({
            "apiVersion": "cluster.x-k8s.io/v1beta1", "kind": "Cluster",
            "metadata": {"name": "workload-01", "namespace": "capi-system"},
            "spec": {"topology": {"version": version}},
        })

    def test_no_generation_still_verifies(self) -> None:
        d = signed_put(self.env, "cluster-01-v1", self.capi, self._base_input(), self._target_manifests("1.29.5"))
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_generation_matches_target_state_put_accepted(self) -> None:
        d = signed_put(self.env, "cluster-01-v1", self.capi, self._base_input(expected_generation=1), self._target_manifests("1.29.5"))
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target, current_fulfillment_state=FulfillmentState("cluster-01", 0)).valid)

    def test_stale_generation_put_rejected(self) -> None:
        d = signed_put(self.env, "cluster-01-v1", self.capi, self._base_input(expected_generation=1), self._target_manifests("1.29.5"))
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target, current_fulfillment_state=FulfillmentState("cluster-01", 1))
        self.assertIn("generation mismatch", str(ctx.exception))

    def test_future_generation_put_rejected(self) -> None:
        d = signed_put(self.env, "cluster-01-v5", self.capi, self._base_input(expected_generation=5), self._target_manifests("1.29.5"))
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target, current_fulfillment_state=FulfillmentState("cluster-01", 1))
        self.assertIn("generation mismatch", str(ctx.exception))

    def test_stale_generation_remove_rejected(self) -> None:
        si = make_signed_input(
            self.alice,
            DeploymentContent(
                deployment_id="cluster-01",
                manifest_strategy=StrategySpec(type="inline", attributes={"manifests": serialize_envelopes(k8s_manifests({"kind": "Cluster"}))}),
                placement_strategy=StrategySpec(type="predicate", attributes={"expression": 'target.labels.env == "prod"'}),
            ),
            expected_generation=1,
        )
        att = make_remove_attestation("rm", si, "cluster-01")
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(
                self.env.assemble(att),
                target_identity={"id": "staging-1", "labels": {"env": "staging"}},
                current_fulfillment_state=FulfillmentState("cluster-01", 2),
            )
        self.assertIn("generation mismatch", str(ctx.exception))

    def test_generation_state_wrong_deployment_fails_closed(self) -> None:
        d = signed_put(self.env, "cluster-01-v1", self.capi, self._base_input(expected_generation=1), self._target_manifests("1.29.5"))
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target, current_fulfillment_state=FulfillmentState("cluster-99", 0))
        self.assertIn("content state mismatch", str(ctx.exception))

    def test_generation_present_but_no_target_state_still_verifies(self) -> None:
        d = signed_put(self.env, "cluster-01-v1", self.capi, self._base_input(expected_generation=1), self._target_manifests("1.29.5"))
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_derived_generation_increments(self) -> None:
        v1 = self._base_input(expected_generation=1)
        up = self._upgrade_attestation()
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1}, update_attestations={"upgrade-1": up},
        )
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target, current_fulfillment_state=FulfillmentState("cluster-01", 1)).valid)

    def test_derived_chain_generation_accumulates(self) -> None:
        v1 = self._base_input(expected_generation=1)
        u1 = self._upgrade_attestation(new_version="1.30.0", att_id="u1")
        u2 = self._upgrade_attestation(new_version="1.31.0", att_id="u2")
        d = signed_put(
            self.env, "v3", self.capi,
            DerivedInput("cluster-01", "deployment", "v2", "u2"),
            self._target_manifests("1.31.0"),
            prior_inputs={"v1": v1, "v2": DerivedInput("cluster-01", "deployment", "v1", "u1")},
            update_attestations={"u1": u1, "u2": u2},
        )
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target, current_fulfillment_state=FulfillmentState("cluster-01", 2)).valid)

    def test_derived_chain_stale_generation_rejected(self) -> None:
        v1 = self._base_input(expected_generation=1)
        u1 = self._upgrade_attestation(new_version="1.30.0", att_id="u1")
        u2 = self._upgrade_attestation(new_version="1.31.0", att_id="u2")
        d = signed_put(
            self.env, "v3", self.capi,
            DerivedInput("cluster-01", "deployment", "v2", "u2"),
            self._target_manifests("1.31.0"),
            prior_inputs={"v1": v1, "v2": DerivedInput("cluster-01", "deployment", "v1", "u1")},
            update_attestations={"u1": u1, "u2": u2},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target, current_fulfillment_state=FulfillmentState("cluster-01", 3))
        self.assertIn("generation mismatch", str(ctx.exception))

    def test_derived_generation_absent_when_root_has_none(self) -> None:
        v1 = self._base_input()
        up = self._upgrade_attestation()
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1}, update_attestations={"upgrade-1": up},
        )
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_intermediate_updates_do_not_check_target_generation(self) -> None:
        v1 = self._base_input(expected_generation=1)
        up = self._upgrade_attestation()
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "cluster-01-v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"cluster-01-v1": v1}, update_attestations={"upgrade-1": up},
        )
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target, current_fulfillment_state=FulfillmentState("cluster-01", 1)).valid)

    def test_precondition_satisfied_update_applies(self) -> None:
        v1 = self._base_input(expected_generation=1)
        up = self._upgrade_attestation(preconditions=['prior.manifest_strategy.config.version == "1.29.5"'])
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"v1": v1}, update_attestations={"upgrade-1": up},
        )
        self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid)

    def test_precondition_false_fails_closed(self) -> None:
        v1 = self._base_input(expected_generation=1)
        up = self._upgrade_attestation(preconditions=['prior.manifest_strategy.config.version == "1.28.0"'])
        d = signed_put(
            self.env, "cluster-01-v2", self.capi,
            DerivedInput("cluster-01", "deployment", "v1", "upgrade-1"),
            self._target_manifests(),
            prior_inputs={"v1": v1}, update_attestations={"upgrade-1": up},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity=self.prod_target)
        self.assertIn("precondition failed", str(ctx.exception))

    def test_reordered_updates_fail_when_preconditions_conflict(self) -> None:
        v1 = self._base_input(expected_generation=1, version="1.28.0")
        u1 = self._upgrade_attestation(new_version="1.29.0", att_id="u1", preconditions=['prior.manifest_strategy.config.version == "1.28.0"'])
        u2 = self._upgrade_attestation(new_version="1.30.0", att_id="u2", preconditions=['prior.manifest_strategy.config.version == "1.29.0"'])
        ok = signed_put(
            self.env, "v3-ok", self.capi,
            DerivedInput("cluster-01", "deployment", "v2", "u2"),
            self._target_manifests("1.30.0"),
            prior_inputs={"v1": v1, "v2": DerivedInput("cluster-01", "deployment", "v1", "u1")},
            update_attestations={"u1": u1, "u2": u2},
        )
        self.assertTrue(verify_delivery(ok, target_identity=self.prod_target).valid)
        bad = signed_put(
            self.env, "v3-bad", self.capi,
            DerivedInput("cluster-01", "deployment", "v2", "u1"),  # swap: apply u1 after claiming u2 first wrongly
            self._target_manifests("1.30.0"),
            prior_inputs={"v1": v1, "v2": DerivedInput("cluster-01", "deployment", "v1", "u2")},
            update_attestations={"u1": u1, "u2": u2},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(bad, target_identity=self.prod_target)
        self.assertIn("precondition failed", str(ctx.exception))

    def test_preconditionless_updates_are_reorderable(self) -> None:
        v1 = self._base_input(expected_generation=1, version="1.28.0")
        u1 = self._upgrade_attestation(new_version="1.29.0", att_id="u1")
        u2 = self._upgrade_attestation(new_version="1.30.0", att_id="u2")
        # Both orders succeed when no preconditions (last write wins on version field)
        for order in (("u1", "u2"), ("u2", "u1")):
            d = signed_put(
                self.env, f"v3-{order[0]}", self.capi,
                DerivedInput("cluster-01", "deployment", "v2", order[1]),
                self._target_manifests("1.30.0"),
                prior_inputs={"v1": v1, "v2": DerivedInput("cluster-01", "deployment", "v1", order[0])},
                update_attestations={"u1": u1, "u2": u2},
            )
            self.assertTrue(verify_delivery(d, target_identity=self.prod_target).valid, order)

    def test_expected_generation_is_signed(self) -> None:
        signed = self._base_input(expected_generation=2)
        object.__setattr__(signed, "expected_generation", 99)
        d = signed_put(self.env, "g", self.capi, signed, self._target_manifests("1.29.5"))
        self.assertFalse(verify_delivery(d, target_identity=self.prod_target).valid)


if __name__ == "__main__":
    unittest.main()
