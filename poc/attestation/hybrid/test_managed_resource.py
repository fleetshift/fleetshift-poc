"""Tests for managed resource attestation with content-type polymorphism."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from typing import Any

from .build import (
    make_key_binding,
    make_managed_resource_input,
    make_registered_self_target,
    make_signed_input,
    sign_put_manifests,
)
from .crypto import KeyPair, content_hash, generate_keypair, sign
from .model import (
    Attestation,
    DeploymentContent,
    DerivedInput,
    KeyBinding,
    ManagedResourceContent,
    ManifestEnvelope,
    OutputConstraint,
    OutputSignature,
    PutManifests,
    RegisteredSelfTarget,
    Signature,
    StrategySpec,
    TrustAnchor,
)
from .verify import (
    TrustStore,
    VerificationBundle,
    VerificationError,
    verify_attestation,
)


# ---------------------------------------------------------------------------
# Helpers
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


def resource_manifests(*specs: dict) -> tuple[ManifestEnvelope, ...]:
    """Wrap resource specs as typed manifest envelopes."""
    return tuple(
        ManifestEnvelope(resource_type="managed_resource_spec", content=s)
        for s in specs
    )


def spec_update_manifest(directive: dict) -> tuple[ManifestEnvelope, ...]:
    return (ManifestEnvelope(resource_type="spec_update", content=directive),)


_NOOP_PLACEMENT = StrategySpec(type="predicate", attributes={"expression": "true"})


def _addon_content(
    addon_id: str,
    deployment_id: str = "update-request",
) -> DeploymentContent:
    return DeploymentContent(
        deployment_id=deployment_id,
        manifest_strategy=StrategySpec(
            type="addon",
            attributes={"addon_id": addon_id},
        ),
        placement_strategy=_NOOP_PLACEMENT,
    )


class ManagedResourceAttestationTests(unittest.TestCase):
    """Tests for managed resource attestation via RegisteredSelfTarget."""

    def setUp(self) -> None:
        self.user = make_identity("alice", "fleet-users")
        self.addon = make_identity("cluster-mgmt-addon", "fleet-addons")
        self.other_addon = make_identity("other-addon", "fleet-addons")

        self.trust_store = TrustStore()
        self.trust_store.add(TrustAnchor(
            anchor_id="fleet-users",
            known_keys={self.user.signer_id: self.user.keys.public_key_bytes},
        ))
        self.trust_store.add(TrustAnchor(
            anchor_id="fleet-addons",
            known_keys={
                self.addon.signer_id: self.addon.keys.public_key_bytes,
                self.other_addon.signer_id: self.other_addon.keys.public_key_bytes,
            },
        ))

        self.relation = make_registered_self_target(
            self.addon.keys,
            self.addon.signer_id,
            "fleet-addons",
            resource_type="clusters",
        )

        self.bundle = VerificationBundle(
            fulfillment_relations={"clusters-rel": self.relation},
        )

    def _make_content(
        self,
        spec: dict[str, Any] | None = None,
        resource_name: str = "clusters/prod-us-east-1",
    ) -> ManagedResourceContent:
        return ManagedResourceContent(
            resource_type="clusters",
            resource_name=resource_name,
            spec=spec or {"version": "1.29", "nodes": 3},
            addon_id=self.addon.signer_id,
        )

    def _user_input(
        self,
        content: ManagedResourceContent | None = None,
        **kwargs: Any,
    ) -> SignedInput:
        return make_signed_input(
            self.user.keys,
            self.user.key_binding,
            content or self._make_content(),
            **kwargs,
        )

    def _signed_output(
        self,
        identity: Identity,
        manifests: tuple[ManifestEnvelope, ...],
    ) -> PutManifests:
        return sign_put_manifests(
            identity.keys,
            identity.signer_id,
            identity.trust_anchor_id,
            manifests,
        )

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------

    def test_happy_path_registered_self_target(self) -> None:
        """User signs a managed resource, addon signs manifests, delivery
        to the addon verifies."""
        content = self._make_content()
        user_input = self._user_input(content)
        manifests = resource_manifests(content.spec)

        att = Attestation(
            attestation_id="managed-resource-1",
            input=user_input,
            output=self._signed_output(self.addon, manifests),
        )
        target = {"id": self.addon.signer_id}
        result = verify_attestation(
            att, self.bundle, self.trust_store, target_identity=target,
        )
        self.assertIsNotNone(result)

    def test_content_type_is_managed_resource(self) -> None:
        content = self._make_content()
        self.assertEqual(content.content_type(), "managed_resource")

    def test_content_id_is_resource_name(self) -> None:
        content = self._make_content(resource_name="clusters/my-cluster")
        self.assertEqual(content.content_id(), "clusters/my-cluster")

    def test_to_dict_includes_content_type(self) -> None:
        content = self._make_content()
        d = content.to_dict()
        self.assertEqual(d["content_type"], "managed_resource")
        self.assertEqual(d["resource_type"], "clusters")
        self.assertEqual(d["addon_id"], self.addon.signer_id)
        self.assertNotIn("trust_anchor_id", d)
        self.assertNotIn("fulfillment_relation", d)

    # ------------------------------------------------------------------
    # Wrong target -- delivery to a different target should fail
    # ------------------------------------------------------------------

    def test_wrong_target_rejected(self) -> None:
        """RegisteredSelfTarget implies placement to the addon; delivery
        to any other target fails."""
        content = self._make_content()
        user_input = self._user_input(content)
        manifests = resource_manifests(content.spec)

        att = Attestation(
            attestation_id="wrong-target",
            input=user_input,
            output=self._signed_output(self.addon, manifests),
        )
        wrong_target = {"id": "some-other-target"}
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.bundle, self.trust_store,
                target_identity=wrong_target,
            )
        self.assertIn("placement targets addon", str(ctx.exception))

    # ------------------------------------------------------------------
    # Missing relation in bundle
    # ------------------------------------------------------------------

    def test_relation_not_found_in_bundle_rejected(self) -> None:
        """When the bundle has no matching relation, verification fails closed."""
        content = self._make_content()
        user_input = self._user_input(content)
        manifests = resource_manifests(content.spec)

        att = Attestation(
            attestation_id="no-relation",
            input=user_input,
            output=self._signed_output(self.addon, manifests),
        )
        target = {"id": self.addon.signer_id}
        empty_bundle = VerificationBundle()
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, empty_bundle, self.trust_store,
                target_identity=target,
            )
        self.assertIn("no fulfillment relation found", str(ctx.exception))

    # ------------------------------------------------------------------
    # Invalid relation evidence in bundle
    # ------------------------------------------------------------------

    def test_relation_signature_invalid_rejected(self) -> None:
        """Tampered relation signature should fail constraint derivation."""
        tampered_sig = OutputSignature(
            signature=Signature(
                signer_id=self.addon.signer_id,
                public_key=self.addon.keys.public_key_bytes,
                content_hash=self.relation.signature.signature.content_hash,
                signature_bytes=b"tampered",
            ),
            trust_anchor_id="fleet-addons",
        )
        bad_relation = RegisteredSelfTarget(
            resource_type="clusters",
            signature=tampered_sig,
        )
        bundle = VerificationBundle(
            fulfillment_relations={"bad": bad_relation},
        )
        content = self._make_content(resource_name="clusters/bad-sig")
        user_input = self._user_input(content)
        manifests = resource_manifests(content.spec)

        att = Attestation(
            attestation_id="bad-relation-sig",
            input=user_input,
            output=self._signed_output(self.addon, manifests),
        )
        target = {"id": self.addon.signer_id}
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, bundle, self.trust_store,
                target_identity=target,
            )
        self.assertIn("relation signature invalid", str(ctx.exception))

    def test_relation_hash_mismatch_rejected(self) -> None:
        """Relation with wrong content hash fails."""
        wrong_hash_sig = OutputSignature(
            signature=Signature(
                signer_id=self.addon.signer_id,
                public_key=self.addon.keys.public_key_bytes,
                content_hash=b"wrong-hash",
                signature_bytes=self.relation.signature.signature.signature_bytes,
            ),
            trust_anchor_id="fleet-addons",
        )
        bad_relation = RegisteredSelfTarget(
            resource_type="clusters",
            signature=wrong_hash_sig,
        )
        bundle = VerificationBundle(
            fulfillment_relations={"bad": bad_relation},
        )
        content = self._make_content(resource_name="clusters/bad-hash")
        user_input = self._user_input(content)
        manifests = resource_manifests(content.spec)

        att = Attestation(
            attestation_id="bad-relation-hash",
            input=user_input,
            output=self._signed_output(self.addon, manifests),
        )
        target = {"id": self.addon.signer_id}
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, bundle, self.trust_store,
                target_identity=target,
            )
        self.assertIn("relation hash mismatch", str(ctx.exception))

    # ------------------------------------------------------------------
    # Wrong addon signs the relation
    # ------------------------------------------------------------------

    def test_wrong_addon_signs_relation_rejected(self) -> None:
        """Relation signed by a different addon than declared in the content."""
        wrong_relation = make_registered_self_target(
            self.other_addon.keys,
            self.other_addon.signer_id,
            "fleet-addons",
            resource_type="clusters",
        )
        bundle = VerificationBundle(
            fulfillment_relations={"wrong": wrong_relation},
        )
        content = ManagedResourceContent(
            resource_type="clusters",
            resource_name="clusters/wrong-signer",
            spec={"version": "1.29"},
            addon_id=self.addon.signer_id,
        )
        user_input = self._user_input(content)
        manifests = resource_manifests(content.spec)

        att = Attestation(
            attestation_id="wrong-addon-signer",
            input=user_input,
            output=self._signed_output(self.addon, manifests),
        )
        target = {"id": self.addon.signer_id}
        # The bundle lookup matches by (addon_id, resource_type) so
        # a relation signed by a different addon won't match at all.
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, bundle, self.trust_store,
                target_identity=target,
            )
        self.assertIn("no fulfillment relation found", str(ctx.exception))

    # ------------------------------------------------------------------
    # Resource type mismatch
    # ------------------------------------------------------------------

    def test_relation_resource_type_mismatch_rejected(self) -> None:
        """Relation claims a different resource type than the content."""
        wrong_type_relation = make_registered_self_target(
            self.addon.keys,
            self.addon.signer_id,
            "fleet-addons",
            resource_type="monitoring-stacks",
        )
        bundle = VerificationBundle(
            fulfillment_relations={"wrong-type": wrong_type_relation},
        )
        content = ManagedResourceContent(
            resource_type="clusters",
            resource_name="clusters/type-mismatch",
            spec={"version": "1.29"},
            addon_id=self.addon.signer_id,
        )
        user_input = self._user_input(content)
        manifests = resource_manifests(content.spec)

        att = Attestation(
            attestation_id="type-mismatch",
            input=user_input,
            output=self._signed_output(self.addon, manifests),
        )
        target = {"id": self.addon.signer_id}
        # The bundle lookup matches by (addon_id, resource_type) so
        # a relation for a different resource type won't match.
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, bundle, self.trust_store,
                target_identity=target,
            )
        self.assertIn("no fulfillment relation found", str(ctx.exception))

    # ------------------------------------------------------------------
    # Relation signer not trusted by anchor
    # ------------------------------------------------------------------

    def test_relation_signer_not_in_trust_anchor_rejected(self) -> None:
        """Relation signed by an addon whose key is not in the trust store."""
        rogue = make_identity("rogue-addon", "unknown-anchor")
        rogue_relation = make_registered_self_target(
            rogue.keys,
            rogue.signer_id,
            "unknown-anchor",
            resource_type="clusters",
        )
        bundle = VerificationBundle(
            fulfillment_relations={"rogue": rogue_relation},
        )
        content = ManagedResourceContent(
            resource_type="clusters",
            resource_name="clusters/rogue",
            spec={"version": "1.29"},
            addon_id=rogue.signer_id,
        )
        user_input = make_signed_input(
            self.user.keys, self.user.key_binding, content,
        )
        manifests = resource_manifests(content.spec)

        att = Attestation(
            attestation_id="rogue-relation",
            input=user_input,
            output=PutManifests(manifests=manifests),
        )
        target = {"id": rogue.signer_id}
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, bundle, self.trust_store,
                target_identity=target,
            )
        self.assertIn("trust anchor not found for relation", str(ctx.exception))

    # ------------------------------------------------------------------
    # Unsigned manifests rejected
    # ------------------------------------------------------------------

    def test_unsigned_manifests_matching_spec_accepted(self) -> None:
        """RegisteredSelfTarget manifests are deterministic from the user's
        spec -- no addon signature required on the output."""
        content = self._make_content()
        user_input = self._user_input(content)
        manifests = resource_manifests(content.spec)

        att = Attestation(
            attestation_id="unsigned-manifests",
            input=user_input,
            output=PutManifests(manifests=manifests),
        )
        target = {"id": self.addon.signer_id}
        result = verify_attestation(
            att, self.bundle, self.trust_store,
            target_identity=target,
        )
        self.assertIsNotNone(result)

    # ------------------------------------------------------------------
    # Derived managed resource
    # ------------------------------------------------------------------

    def test_derived_managed_resource(self) -> None:
        """A managed resource spec update through the derivation chain."""
        original_content = self._make_content(
            spec={"version": "1.29", "nodes": 3},
        )
        prior_input = self._user_input(original_content)

        update_identity = make_identity("upgrade-planner", "fleet-addons")
        self.trust_store.add(TrustAnchor(
            anchor_id="fleet-addons",
            known_keys={
                self.addon.signer_id: self.addon.keys.public_key_bytes,
                self.other_addon.signer_id: self.other_addon.keys.public_key_bytes,
                update_identity.signer_id: update_identity.keys.public_key_bytes,
            },
        ))

        update_input = make_signed_input(
            update_identity.keys,
            update_identity.key_binding,
            _addon_content(
                update_identity.signer_id,
                deployment_id="upgrade-managed-resources",
            ),
        )
        update_manifests = spec_update_manifest({
            "derive_input_expression": (
                'set_path(prior, "spec.version", "1.30")'
            ),
        })
        update_att = Attestation(
            attestation_id="upgrade-1",
            input=update_input,
            output=self._signed_output(update_identity, update_manifests),
        )

        output_manifests = resource_manifests({"version": "1.30", "nodes": 3})

        att = Attestation(
            attestation_id="managed-derived-1",
            input=DerivedInput(
                prior_content_id="clusters/prod-us-east-1",
                prior_content_type="managed_resource",
                prior_input_id="mr-v1",
                update_attestation_id="upgrade-1",
            ),
            output=self._signed_output(self.addon, output_manifests),
        )

        bundle = VerificationBundle(
            inputs={"mr-v1": prior_input},
            attestations={"upgrade-1": update_att},
            fulfillment_relations={"clusters-rel": self.relation},
        )
        target = {"id": self.addon.signer_id}
        result = verify_attestation(
            att, bundle, self.trust_store, target_identity=target,
        )
        self.assertIsNotNone(result)

    # ------------------------------------------------------------------
    # Coexistence: bundle with both content types
    # ------------------------------------------------------------------

    def test_deployment_and_managed_resource_coexist(self) -> None:
        """A verification bundle containing both a deployment and a managed
        resource attestation verifies each independently."""
        serialized_manifests = [
            {"resource_type": "kubernetes", "content": {"kind": "ConfigMap"}},
        ]
        deploy_content = DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="inline",
                attributes={"manifests": serialized_manifests},
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        )
        deploy_input = make_signed_input(
            self.user.keys, self.user.key_binding, deploy_content,
        )
        deploy_manifests = (
            ManifestEnvelope(resource_type="kubernetes", content={"kind": "ConfigMap"}),
        )
        deploy_att = Attestation(
            attestation_id="deploy-att",
            input=deploy_input,
            output=PutManifests(manifests=deploy_manifests),
        )

        mr_content = self._make_content()
        mr_input = self._user_input(mr_content)
        mr_manifests = resource_manifests(mr_content.spec)
        mr_att = Attestation(
            attestation_id="managed-att",
            input=mr_input,
            output=self._signed_output(self.addon, mr_manifests),
        )

        deploy_target = {"id": "target-1", "labels": {"env": "prod"}}
        result_deploy = verify_attestation(
            deploy_att, VerificationBundle(), self.trust_store,
            target_identity=deploy_target,
        )
        self.assertIsNotNone(result_deploy)

        addon_target = {"id": self.addon.signer_id}
        result_mr = verify_attestation(
            mr_att, self.bundle, self.trust_store,
            target_identity=addon_target,
        )
        self.assertIsNotNone(result_mr)

    # ------------------------------------------------------------------
    # Manifests signed by wrong addon
    # ------------------------------------------------------------------

    def test_manifests_spec_mismatch_rejected(self) -> None:
        """Manifests that don't match the user's signed spec are rejected,
        regardless of who signed them."""
        content = self._make_content(spec={"version": "1.29", "nodes": 3})
        user_input = self._user_input(content)
        wrong_manifests = resource_manifests({"version": "1.30", "nodes": 5})

        att = Attestation(
            attestation_id="wrong-spec",
            input=user_input,
            output=PutManifests(manifests=wrong_manifests),
        )
        target = {"id": self.addon.signer_id}
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, self.bundle, self.trust_store,
                target_identity=target,
            )
        self.assertIn("manifests must match resource spec", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
