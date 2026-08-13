"""Managed-resource equivalence tests ported from hybrid/test_managed_resource.py."""

from __future__ import annotations

import unittest

from hybrid.model import DeploymentContent, ManagedResourceContent, ManifestEnvelope, StrategySpec

from sigstore_tuf._test_helpers import (
    addon_content,
    k8s_manifests,
    make_env,
    resource_manifests,
    serialize_envelopes,
    signed_put,
    spec_update_manifest,
    unsigned_put,
)
from sigstore_tuf.build import (
    make_put_attestation,
    make_registered_self_target,
    make_signed_input,
    sign_manifests,
)
from sigstore_tuf.model import DerivedInput, RegisteredSelfTarget
from sigstore_tuf.sigstore_sign import Identity, Signer, SigstoreBundle
from sigstore_tuf.verify import VerificationError, verify_attestation_or_raise, verify_delivery


class ManagedResourceEquivTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env = make_env(
            tenant_constraints=False,
            user_anchor_id="fleet-users",
            user_subjects=["alice@tenant-a.example"],
            extra_addons=("cluster-mgmt-addon", "other-addon", "upgrade-planner"),
        )
        self.user = self.env.user_signer("alice@tenant-a.example")
        self.addon = self.env.addon_signer("cluster-mgmt-addon")
        self.other_addon = self.env.addon_signer("other-addon")
        self.relation = make_registered_self_target(self.addon, "clusters")
        self.relations = {"clusters-rel": self.relation}

    def _content(self, spec=None, resource_name="clusters/prod-us-east-1"):
        return ManagedResourceContent(
            resource_type="clusters",
            resource_name=resource_name,
            spec=spec or {"version": "1.29", "nodes": 3},
            addon_id=self.addon.identity.subject,
        )

    def _user_input(self, content=None, **kwargs):
        return make_signed_input(self.user, content or self._content(), **kwargs)

    def test_happy_path_registered_self_target(self) -> None:
        content = self._content()
        manifests = resource_manifests(content.spec)
        d = signed_put(
            self.env, "managed-resource-1", self.addon, self._user_input(content), manifests,
            fulfillment_relations=self.relations,
        )
        self.assertTrue(verify_delivery(d, target_identity={"id": "cluster-mgmt-addon"}).valid)

    def test_content_type_is_managed_resource(self) -> None:
        self.assertEqual(self._content().content_type(), "managed_resource")

    def test_content_id_is_resource_name(self) -> None:
        self.assertEqual(self._content(resource_name="clusters/my-cluster").content_id(), "clusters/my-cluster")

    def test_to_dict_includes_content_type(self) -> None:
        d = self._content().to_dict()
        self.assertEqual(d["content_type"], "managed_resource")
        self.assertEqual(d["resource_type"], "clusters")
        self.assertEqual(d["addon_id"], "cluster-mgmt-addon")
        self.assertNotIn("trust_anchor_id", d)
        self.assertNotIn("fulfillment_relation", d)

    def test_wrong_target_rejected(self) -> None:
        content = self._content()
        d = signed_put(
            self.env, "wrong-target", self.addon, self._user_input(content),
            resource_manifests(content.spec), fulfillment_relations=self.relations,
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity={"id": "some-other-target"})
        self.assertIn("placement targets addon", str(ctx.exception))

    def test_relation_not_found_in_bundle_rejected(self) -> None:
        content = self._content()
        d = signed_put(
            self.env, "no-relation", self.addon, self._user_input(content),
            resource_manifests(content.spec),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity={"id": "cluster-mgmt-addon"})
        self.assertIn("no fulfillment relation found", str(ctx.exception))

    def test_relation_signature_invalid_rejected(self) -> None:
        # Tamper DSSE signature bytes.
        rel = make_registered_self_target(self.addon, "clusters")
        env = dict(rel.bundle.dsse_envelope)
        import base64, copy
        env = copy.deepcopy(rel.bundle.dsse_envelope)
        env["signatures"][0]["sig"] = base64.b64encode(b"\x00" * 64).decode()
        bad = RegisteredSelfTarget(
            resource_type="clusters",
            bundle=SigstoreBundle(
                dsse_envelope=env,
                certificate_pem=rel.bundle.certificate_pem,
                timestamp=rel.bundle.timestamp,
                trust_anchor_id=rel.bundle.trust_anchor_id,
                identity=rel.bundle.identity,
            ),
        )
        content = self._content(resource_name="clusters/bad-sig")
        d = signed_put(
            self.env, "bad-relation-sig", self.addon, self._user_input(content),
            resource_manifests(content.spec),
            fulfillment_relations={"bad": bad},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity={"id": "cluster-mgmt-addon"})
        self.assertIn("relation signature invalid", str(ctx.exception))

    def test_relation_hash_mismatch_rejected(self) -> None:
        # Sign a different resource_type but claim clusters — digest won't match expected doc.
        rel = make_registered_self_target(self.addon, "monitoring-stacks")
        # Force resource_type field to clusters while keeping signature over monitoring-stacks
        bad = RegisteredSelfTarget(resource_type="clusters", bundle=rel.bundle)
        content = self._content(resource_name="clusters/bad-hash")
        d = signed_put(
            self.env, "bad-relation-hash", self.addon, self._user_input(content),
            resource_manifests(content.spec),
            fulfillment_relations={"bad": bad},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity={"id": "cluster-mgmt-addon"})
        self.assertIn("relation hash mismatch", str(ctx.exception))

    def test_wrong_addon_signs_relation_rejected(self) -> None:
        wrong = make_registered_self_target(self.other_addon, "clusters")
        content = ManagedResourceContent(
            resource_type="clusters",
            resource_name="clusters/wrong-signer",
            spec={"version": "1.29"},
            addon_id="cluster-mgmt-addon",
        )
        d = signed_put(
            self.env, "wrong-addon-signer", self.addon, self._user_input(content),
            resource_manifests(content.spec),
            fulfillment_relations={"wrong": wrong},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity={"id": "cluster-mgmt-addon"})
        self.assertIn("no fulfillment relation found", str(ctx.exception))

    def test_relation_resource_type_mismatch_rejected(self) -> None:
        wrong_type = make_registered_self_target(self.addon, "monitoring-stacks")
        content = ManagedResourceContent(
            resource_type="clusters",
            resource_name="clusters/type-mismatch",
            spec={"version": "1.29"},
            addon_id="cluster-mgmt-addon",
        )
        d = signed_put(
            self.env, "type-mismatch", self.addon, self._user_input(content),
            resource_manifests(content.spec),
            fulfillment_relations={"wrong-type": wrong_type},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity={"id": "cluster-mgmt-addon"})
        self.assertIn("no fulfillment relation found", str(ctx.exception))

    def test_relation_signer_not_in_trust_anchor_rejected(self) -> None:
        rogue = Signer(
            fulcio=self.env.workload_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer="https://oidc.addon.local", subject="rogue-addon"),
            trust_anchor_id="unknown-anchor",
        )
        rogue_relation = make_registered_self_target(rogue, "clusters")
        content = ManagedResourceContent(
            resource_type="clusters",
            resource_name="clusters/rogue",
            spec={"version": "1.29"},
            addon_id="rogue-addon",
        )
        d = unsigned_put(
            self.env, "rogue-relation", make_signed_input(self.user, content),
            resource_manifests(content.spec),
            fulfillment_relations={"rogue": rogue_relation},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity={"id": "rogue-addon"})
        self.assertIn("trust anchor not found for relation", str(ctx.exception))

    def test_unsigned_manifests_matching_spec_accepted(self) -> None:
        content = self._content()
        d = unsigned_put(
            self.env, "unsigned-manifests", self._user_input(content),
            resource_manifests(content.spec),
            fulfillment_relations=self.relations,
        )
        self.assertTrue(verify_delivery(d, target_identity={"id": "cluster-mgmt-addon"}).valid)

    def test_derived_managed_resource(self) -> None:
        original = self._content(spec={"version": "1.29", "nodes": 3})
        prior = self._user_input(original)
        planner = self.env.addon_signer("upgrade-planner")
        um = spec_update_manifest({"derive_input_expression": 'set_path(prior, "spec.version", "1.30")'})
        um, umb = sign_manifests(planner, um)
        update_att = make_put_attestation(
            "upgrade-1",
            make_signed_input(planner, addon_content("upgrade-planner", deployment_id="upgrade-managed-resources")),
            um, manifest_bundle=umb,
        )
        output = resource_manifests({"version": "1.30", "nodes": 3})
        d = signed_put(
            self.env, "managed-derived-1", self.addon,
            DerivedInput("clusters/prod-us-east-1", "managed_resource", "mr-v1", "upgrade-1"),
            output,
            prior_inputs={"mr-v1": prior},
            update_attestations={"upgrade-1": update_att},
            fulfillment_relations=self.relations,
        )
        self.assertTrue(verify_delivery(d, target_identity={"id": "cluster-mgmt-addon"}).valid)

    def test_deployment_and_managed_resource_coexist(self) -> None:
        serialized = [{"resource_type": "kubernetes", "content": {"kind": "ConfigMap"}}]
        deploy_content = DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(type="inline", attributes={"manifests": serialized}),
            placement_strategy=StrategySpec(type="predicate", attributes={"expression": 'target.labels.env == "prod"'}),
        )
        deploy_d = unsigned_put(
            self.env, "deploy-att", make_signed_input(self.user, deploy_content),
            (ManifestEnvelope(resource_type="kubernetes", content={"kind": "ConfigMap"}),),
        )
        self.assertTrue(verify_delivery(deploy_d, target_identity={"id": "target-1", "labels": {"env": "prod"}}).valid)

        mr = self._content()
        mr_d = signed_put(
            self.env, "managed-att", self.addon, self._user_input(mr),
            resource_manifests(mr.spec), fulfillment_relations=self.relations,
        )
        self.assertTrue(verify_delivery(mr_d, target_identity={"id": "cluster-mgmt-addon"}).valid)

    def test_manifests_spec_mismatch_rejected(self) -> None:
        content = self._content(spec={"version": "1.29", "nodes": 3})
        d = unsigned_put(
            self.env, "wrong-spec", self._user_input(content),
            resource_manifests({"version": "1.30", "nodes": 5}),
            fulfillment_relations=self.relations,
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(d, target_identity={"id": "cluster-mgmt-addon"})
        self.assertIn("manifests must match resource spec", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
