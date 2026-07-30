"""Hybrid-equivalence tests for the Sigstore/TUF POC.

Ports every test from hybrid/test_hybrid.py. KeyBinding-specific attacks are
replaced by Fulcio/TSA/DSSE equivalent guarantees (see TEST_PORTING.md).
"""

from __future__ import annotations

import base64
import unittest

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec

from hybrid.model import DeploymentContent, ManifestEnvelope, OutputConstraint, StrategySpec
from hybrid.policy import constraint_to_document

from sigstore_tuf_bundle._test_helpers import (
    addon_content,
    addon_must_sign,
    all_details,
    allowed_gvks,
    assert_rejected_with,
    content,
    inline_content,
    k8s_manifests,
    make_env,
    namespace_constraint,
    no_cluster_admin,
    serialize_envelopes,
    signed_put,
    spec_update_manifest,
    unsigned_put,
)
from sigstore_tuf_bundle.build import (
    PoCEnvironment,
    make_put_attestation,
    make_signed_input,
    sign_manifests,
)
from sigstore_tuf_bundle.model import DerivedInput
from sigstore_tuf_bundle.sigstore import Signer
from sigstore_tuf_bundle.verify import VerificationError


class HybridParityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env = make_env()
        self.agent = self.env.delivery_agent()
        self.alice = self.env.user_signer("alice@tenant-a.example")
        self.bob = self.env.user_signer("bob@tenant-a.example")
        self.capi = self.env.addon_signer("capi-provisioner")
        self.lifecycle = self.env.addon_signer("cluster-lifecycle")
        self.planner = self.env.addon_signer("upgrade-planner")
        self.prod_target = {
            "id": "cluster-prod-1",
            "labels": {"env": "prod", "region": "us-east-1"},
        }

    def _si(self, signer, cont, **kwargs):
        return make_signed_input(signer, cont, **kwargs)

    def _update_att(self, att_id: str, version: str, signer=None, upd_signer=None, **si_kwargs):
        signer = signer or self.bob
        upd_signer = upd_signer or self.planner
        manifests = spec_update_manifest({
            "derive_input_expression": (
                f'set_path(prior, "manifest_strategy.config.version", "{version}")'
            ),
        })
        manifests, mb = sign_manifests(upd_signer, manifests)
        return make_put_attestation(
            att_id,
            self._si(signer, addon_content("upgrade-planner"), **si_kwargs),
            manifests,
            manifest_bundle=mb,
        )

    def _addon_prior(self, version: str = "1.29.5", deployment_id: str = "deploy-1", **si_kwargs):
        spec = DeploymentContent(
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
        )
        return self._si(self.alice, spec, **si_kwargs)

    # ------------------------------------------------------------------
    # Happy paths
    # ------------------------------------------------------------------

    def test_direct_attestation_with_signed_cel_constraints_verifies(self) -> None:
        manifests = k8s_manifests(
            {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"namespace": "prod", "name": "c"}}
        )
        delivery = unsigned_put(
            self.env,
            "direct-1",
            self._si(
                self.alice,
                inline_content(manifests),
                output_constraints=(namespace_constraint("prod"), no_cluster_admin()),
            ),
            manifests,
        )
        result = self.agent.verify(delivery, target_identity=self.prod_target)
        self.assertTrue(result.valid, result)

    def test_addon_signed_output_with_explicit_cel_policy_verifies(self) -> None:
        manifests = k8s_manifests({"kind": "Cluster", "metadata": {"namespace": "capi"}})
        delivery = signed_put(
            self.env,
            "addon-1",
            self.capi,
            self._si(
                self.alice,
                addon_content("capi-provisioner"),
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            manifests,
        )
        result = self.agent.verify(delivery, target_identity=self.prod_target)
        self.assertTrue(result.valid, result)
        self.assertIn("capi-provisioner", all_details(result))

    def test_derived_input_uses_signed_cel_update_and_constraints(self) -> None:
        prior = self._addon_prior(deployment_id="cluster-01", output_constraints=(addon_must_sign("capi-provisioner"),))
        update_directive = {
            "derive_input_expression": (
                'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
            ),
            "output_constraints": [
                constraint_to_document(namespace_constraint("capi-system")),
            ],
        }
        um = spec_update_manifest(update_directive)
        um, umb = sign_manifests(self.planner, um)
        update_att = make_put_attestation(
            "upgrade-1",
            self._si(
                self.bob,
                addon_content("upgrade-planner"),
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            um,
            manifest_bundle=umb,
        )
        final_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            }
        )
        delivery = signed_put(
            self.env,
            "d1-v2",
            self.capi,
            DerivedInput(
                prior_content_id="cluster-01",
                prior_content_type="deployment",
                prior_input_id="d1-v1",
                update_attestation_id="upgrade-1",
            ),
            final_manifests,
            prior_inputs={"d1-v1": prior},
            update_attestations={"upgrade-1": update_att},
        )
        result = self.agent.verify(delivery, target_identity=self.prod_target)
        self.assertTrue(result.valid, result)
        explanation = self.agent.explain(delivery, target_identity=self.prod_target)
        details = all_details(explanation)
        self.assertIn("derived from prior=d1-v1 + update=upgrade-1", details)
        self.assertIn("upgrade-planner", details)

    def test_derived_input_falls_back_to_spec_derived_constraints(self) -> None:
        prior = self._addon_prior()
        update_att = self._update_att("update", "1.30.2")
        delivery = signed_put(
            self.env,
            "final",
            self.capi,
            DerivedInput("deploy-1", "deployment", "prior", "update"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
            update_attestations={"update": update_att},
        )
        self.assertTrue(self.agent.verify(delivery, target_identity=self.prod_target).valid)

    def test_bad_update_expression_fails_derivation(self) -> None:
        prior = self._addon_prior()
        um = spec_update_manifest({"derive_input_expression": "1 + 2"})
        um, umb = sign_manifests(self.planner, um)
        update_att = make_put_attestation(
            "update-bad-expression",
            self._si(self.bob, addon_content("upgrade-planner")),
            um,
            manifest_bundle=umb,
        )
        delivery = signed_put(
            self.env,
            "final-bad-expression",
            self.capi,
            DerivedInput("deploy-1", "deployment", "prior-bad-update", "update-bad-expression"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior-bad-update": prior},
            update_attestations={"update-bad-expression": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            self.agent.verify_or_raise(delivery)
        self.assertIn("derive_input_expression must return an object", str(ctx.exception))

    def test_wrong_output_signer_fails_against_derived_constraint(self) -> None:
        prior = self._addon_prior(output_constraints=(addon_must_sign("capi-provisioner"),))
        update_att = self._update_att("update", "1.30.2")
        delivery = signed_put(
            self.env,
            "final",
            self.lifecycle,  # wrong addon
            DerivedInput("deploy-1", "deployment", "prior", "update"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
            update_attestations={"update": update_att},
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "manifests must be signed by capi-provisioner",
            target_identity=self.prod_target,
        )

    # ------------------------------------------------------------------
    # Expiry / tamper / CEL surface
    # ------------------------------------------------------------------

    def test_expired_signed_input_fails(self) -> None:
        delivery = unsigned_put(
            self.env,
            "expired",
            self._si(self.alice, content(), valid_duration_sec=-1),
            k8s_manifests({"x": 1}),
        )
        assert_rejected_with(self, self.agent, delivery, "expired")

    def test_tampered_signed_constraints_fail(self) -> None:
        signed = self._si(
            self.alice,
            inline_content(k8s_manifests({"kind": "ConfigMap", "metadata": {"namespace": "prod"}})),
            output_constraints=(namespace_constraint("prod"),),
        )
        object.__setattr__(
            signed,
            "output_constraints",
            (namespace_constraint("evil"),),
        )
        delivery = unsigned_put(self.env, "tampered-c", signed, k8s_manifests({"kind": "ConfigMap", "metadata": {"namespace": "prod"}}))
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "in-toto subject digest mismatch",
        )

    def test_failed_cel_constraint_surfaces_constraint_name(self) -> None:
        manifests = k8s_manifests(
            {
                "kind": "ClusterRoleBinding",
                "roleRef": {"name": "cluster-admin"},
            }
        )
        delivery = unsigned_put(
            self.env,
            "crb",
            self._si(
                self.alice,
                inline_content(manifests),
                output_constraints=(no_cluster_admin(),),
            ),
            manifests,
        )
        with self.assertRaises(VerificationError) as ctx:
            self.agent.verify_or_raise(delivery)
        self.assertIn("no ClusterRoleBinding may grant cluster-admin", str(ctx.exception))

    # ------------------------------------------------------------------
    # Tenant trust-anchor CEL
    # ------------------------------------------------------------------

    def test_tenant_scoped_trust_anchor_accepts_matching_tenant(self) -> None:
        env = PoCEnvironment(
            tenant_constraints=True,
            user_subjects=["alice-tenant-a@tenant-a.example"],
            user_anchor_id="tenant-a-idp",
            user_issuer="https://issuer.example/tenant-a",
        )
        alice = env.user_signer("alice-tenant-a@tenant-a.example")
        manifests = k8s_manifests({"kind": "ConfigMap"})
        delivery = unsigned_put(
            env,
            "tenant-match",
            make_signed_input(alice, inline_content(manifests, deployment_id="tenant-a/deploy-web")),
            manifests,
        )
        self.assertTrue(env.delivery_agent().verify(delivery).valid)

    def test_tenant_scoped_trust_anchor_rejects_other_tenant(self) -> None:
        env = PoCEnvironment(
            tenant_constraints=True,
            user_subjects=["alice-tenant-a@tenant-a.example"],
            user_anchor_id="tenant-a-idp",
        )
        alice = env.user_signer("alice-tenant-a@tenant-a.example")
        delivery = unsigned_put(
            env,
            "tenant-mismatch",
            make_signed_input(alice, content(deployment_id="tenant-b/deploy-web")),
            k8s_manifests({"kind": "ConfigMap"}),
        )
        with self.assertRaises(VerificationError) as ctx:
            env.delivery_agent().verify_or_raise(delivery)
        self.assertIn("input tenant must match anchor tenant", str(ctx.exception))

    def test_trust_anchor_constraint_cannot_use_attestation_identifier(self) -> None:
        env = PoCEnvironment(
            tenant_constraints=False,
            user_subjects=["alice-tenant-a@tenant-a.example"],
            user_anchor_id="tenant-a-idp",
            user_anchor_constraints=[
                {
                    "name": "attestation identifiers are not part of the authenticated subject",
                    "expression": 'subject.attestation_id.startsWith("tenant-a/")',
                }
            ],
        )
        alice = env.user_signer("alice-tenant-a@tenant-a.example")
        delivery = unsigned_put(
            env,
            "tenant-b/spoofed-reference",
            make_signed_input(alice, content(deployment_id="tenant-a/deploy-web")),
            k8s_manifests({"kind": "ConfigMap"}),
        )
        with self.assertRaises(VerificationError) as ctx:
            env.delivery_agent().verify_or_raise(delivery)
        self.assertIn("trust anchor constraint evaluation failed", str(ctx.exception))

    # ------------------------------------------------------------------
    # Constraint violations
    # ------------------------------------------------------------------

    def test_wrong_addon_signs_output(self) -> None:
        delivery = signed_put(
            self.env,
            "wrong-addon",
            self.lifecycle,
            self._si(
                self.alice,
                addon_content("capi-provisioner"),
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            k8s_manifests({"kind": "Cluster"}),
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "manifests must be signed by capi-provisioner",
        )

    def test_namespace_violation(self) -> None:
        manifests = k8s_manifests({"metadata": {"namespace": "wrong"}, "kind": "Pod"})
        delivery = unsigned_put(
            self.env,
            "ns",
            self._si(self.alice, inline_content(manifests), output_constraints=(namespace_constraint("prod"),)),
            manifests,
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "all manifests must be in namespace prod",
        )

    def test_multiple_constraints_all_pass(self) -> None:
        manifests = k8s_manifests(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"namespace": "prod", "name": "app"},
            }
        )
        delivery = unsigned_put(
            self.env,
            "multi",
            self._si(
                self.alice,
                inline_content(manifests),
                output_constraints=(
                    namespace_constraint("prod"),
                    allowed_gvks("apps/v1/Deployment"),
                    no_cluster_admin(),
                ),
            ),
            manifests,
        )
        self.assertTrue(self.agent.verify(delivery).valid)

    def test_gvk_violation(self) -> None:
        manifests = k8s_manifests(
            {"apiVersion": "v1", "kind": "Secret", "metadata": {"namespace": "prod"}}
        )
        delivery = unsigned_put(
            self.env,
            "gvk",
            self._si(
                self.alice,
                inline_content(manifests),
                output_constraints=(allowed_gvks("apps/v1/Deployment"),),
            ),
            manifests,
        )
        assert_rejected_with(self, self.agent, delivery, "only GVKs in")

    def test_unsigned_output_fails_addon_constraint(self) -> None:
        delivery = unsigned_put(
            self.env,
            "unsigned-addon",
            self._si(
                self.alice,
                addon_content("capi-provisioner"),
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            k8s_manifests({"kind": "Cluster"}),
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "manifests must be signed by capi-provisioner",
        )

    # ------------------------------------------------------------------
    # Forgery / trust
    # ------------------------------------------------------------------

    def test_forged_input_signature_wrong_key(self) -> None:
        """Equivalent: DSSE signature does not verify under Fulcio leaf pubkey."""
        signed = self._si(self.alice, content())
        # A TSA can timestamp arbitrary bytes. Obtain a matching timestamp for
        # the forged signature so this reaches DSSE verification rather than
        # being rejected earlier for an RFC 3161 imprint mismatch.
        forged_signature = b"\x00" * 64
        document = signed.bundle.to_dict()
        document["dsseEnvelope"]["signatures"][0]["sig"] = base64.b64encode(
            forged_signature
        ).decode()
        document["verificationMaterial"]["timestampVerificationData"][
            "rfc3161Timestamps"
        ][0]["signedTimestamp"] = base64.b64encode(
            self.env.tsa.timestamp(forged_signature)
        ).decode()
        object.__setattr__(signed, "bundle", signed.bundle.__class__(document))
        delivery = self.env.assemble(make_put_attestation("forged", signed, ()))
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "DSSE signature is invalid",
        )

    def test_untrusted_signer_empty_trust_store(self) -> None:
        """No tenant identity anchor in TUF policy → reject."""
        env = make_env()
        alice = env.user_signer("alice@tenant-a.example")
        env.anchors.pop(env.user_anchor_id)
        env.republish()
        delivery = unsigned_put(env, "empty", make_signed_input(alice, content()), ())
        assert_rejected_with(
            self,
            env.delivery_agent(),
            delivery,
            "unknown trust anchor",
        )

    def test_unknown_key_in_anchor(self) -> None:
        """Identity not on allowlist (hybrid: unknown pubkey in anchor)."""
        rogue = self.env.user_signer("eve@evil.example")
        delivery = unsigned_put(self.env, "unknown", make_signed_input(rogue, content()), ())
        assert_rejected_with(self, self.agent, delivery, "unknown trust anchor")

    def test_tampered_valid_until_breaks_envelope(self) -> None:
        signed = self._si(self.alice, content())
        object.__setattr__(signed, "valid_until", signed.valid_until + 99999)
        delivery = unsigned_put(self.env, "vu", signed, ())
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "in-toto subject digest mismatch",
        )

    def test_tampered_input_content_breaks_envelope(self) -> None:
        signed = self._si(self.alice, content(deployment_id="orig"))
        object.__setattr__(signed, "content", content(deployment_id="tampered"))
        delivery = unsigned_put(self.env, "tc", signed, ())
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "in-toto subject digest mismatch",
        )

    def test_forged_output_key_not_in_anchor(self) -> None:
        rogue = self.env.addon_signer("evil-addon")
        delivery = signed_put(
            self.env,
            "forged-out",
            rogue,
            self._si(self.alice, addon_content("capi-provisioner")),
            k8s_manifests({"kind": "Cluster"}),
        )
        assert_rejected_with(self, self.agent, delivery, "unknown trust anchor")

    def test_tampered_output_content(self) -> None:
        manifests = k8s_manifests({"kind": "Cluster", "x": 1})
        manifests, mb = sign_manifests(self.capi, manifests)
        # Tamper manifests after signing.
        tampered = k8s_manifests({"kind": "Cluster", "x": 2})
        att = make_put_attestation(
            "tampered-out",
            self._si(self.alice, addon_content("capi-provisioner")),
            tampered,
            manifest_bundle=mb,
        )
        assert_rejected_with(
            self,
            self.agent,
            self.env.assemble(att),
            "in-toto subject digest mismatch",
        )

    def test_output_trust_anchor_missing(self) -> None:
        rogue = self.env.addon_signer("rogue-addon")
        delivery = signed_put(
            self.env,
            "missing-anchor",
            rogue,
            self._si(
                self.alice,
                inline_content(k8s_manifests({"kind": "Cluster"})),
                output_constraints=(addon_must_sign("rogue-addon", "nonexistent-addon-ca"),),
            ),
            k8s_manifests({"kind": "Cluster"}),
        )
        with self.assertRaises(VerificationError) as ctx:
            self.agent.verify_or_raise(delivery)
        self.assertIn("trust anchor", str(ctx.exception).lower())

    # ------------------------------------------------------------------
    # KeyBinding → Fulcio/TSA/DSSE equivalents
    # ------------------------------------------------------------------

    def test_key_binding_signer_mismatch(self) -> None:
        """The certificate says Bob and cannot be confused with allowed Alice."""
        self.env.anchors[self.env.user_anchor_id]["allowed_identities"] = [
            {
                "issuer": self.env.user_issuer,
                "subject": "alice@tenant-a.example",
                "signer_id": "alice@tenant-a.example",
            }
        ]
        self.env.republish()
        delivery = unsigned_put(
            self.env,
            "kb-mismatch",
            self._si(self.bob, content()),
            (),
        )
        assert_rejected_with(self, self.agent, delivery, "unknown trust anchor")

    def test_key_binding_forged_proof(self) -> None:
        """Equiv: Fulcio refuses a forged proof of ephemeral-key possession."""
        identity = "alice@tenant-a.example"
        requested_key = ec.generate_private_key(ec.SECP256R1())
        attacker_key = ec.generate_private_key(ec.SECP256R1())
        forged_proof = attacker_key.sign(
            identity.encode(),
            ec.ECDSA(hashes.SHA256()),
        )
        with self.assertRaises(ValueError) as captured:
            self.env.user_fulcio.issue(
                identity_token=self.env.tenant_idp.mint(identity),
                public_key=requested_key.public_key(),
                proof_of_possession=forged_proof,
            )
        self.assertIn("Fulcio proof of possession is invalid", str(captured.exception))

    def test_key_binding_wrong_public_key(self) -> None:
        """Equiv: Fulcio leaf pubkey does not verify the DSSE signature."""
        signed = self._si(self.alice, content())
        other = self._si(self.bob, content())
        # Alice's envelope signed with Alice's key, but Bob's certificate attached.
        document = signed.bundle.to_dict()
        document["verificationMaterial"]["certificate"] = other.bundle.to_dict()[
            "verificationMaterial"
        ]["certificate"]
        object.__setattr__(signed, "bundle", signed.bundle.__class__(document))
        delivery = unsigned_put(self.env, "wrong-pubkey", signed, ())
        assert_rejected_with(self, self.agent, delivery, "DSSE signature is invalid")

    # ------------------------------------------------------------------
    # Cross-anchor / identity confusion
    # ------------------------------------------------------------------

    def test_user_key_cannot_satisfy_addon_constraint(self) -> None:
        # Permit user output evidence in this test so that the independent CEL
        # authorization check, rather than the stricter default kind policy,
        # proves that a tenant identity cannot satisfy an addon requirement.
        self.env.anchors[self.env.user_anchor_id]["kinds"].append("output")
        self.env.republish()
        delivery = signed_put(
            self.env,
            "user-as-addon",
            self.alice,
            self._si(
                self.alice,
                inline_content(k8s_manifests({"kind": "Cluster"})),
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            k8s_manifests({"kind": "Cluster"}),
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "output must be signed by capi-provisioner",
        )

    def test_user_registers_in_addon_anchor_fails(self) -> None:
        self.env.anchors.pop(self.env.user_anchor_id)
        self.env.republish()
        delivery = unsigned_put(
            self.env,
            "user-in-addon",
            make_signed_input(self.alice, content()),
            (),
        )
        # Only the addon anchor remains. A tenant Fulcio certificate cannot be
        # interpreted as an addon identity because CA, issuer, and kind differ.
        assert_rejected_with(self, self.agent, delivery, "unknown trust anchor")

    def test_addon_self_authorises_fails_constraint(self) -> None:
        # The production policy excludes platform identities from input
        # authority. Relax only that outer check here to retain the hybrid
        # test's defense-in-depth property at the signed CEL layer.
        self.env.anchors["fleet-addons"]["kinds"].append("input")
        self.env.republish()
        delivery = signed_put(
            self.env,
            "self-auth",
            self.capi,
            self._si(
                self.capi,
                inline_content(k8s_manifests({"kind": "Cluster"})),
                output_constraints=(addon_must_sign("cluster-lifecycle"),),
            ),
            k8s_manifests({"kind": "Cluster"}),
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "output must be signed by cluster-lifecycle",
        )

    def test_user_trusted_addon_not(self) -> None:
        """TUF policy has the user anchor but no addon anchor."""
        self.env.anchors.pop("fleet-addons")
        self.env.republish()
        delivery = signed_put(
            self.env,
            "addon-untrusted",
            self.capi,
            self._si(self.alice, addon_content("capi-provisioner")),
            k8s_manifests({"kind": "Cluster"}),
        )
        assert_rejected_with(self, self.agent, delivery, "unknown trust anchor")

    def test_addon_trusted_user_not(self) -> None:
        """TUF policy has the addon anchor but no user anchor."""
        self.env.anchors.pop(self.env.user_anchor_id)
        self.env.republish()
        delivery = signed_put(
            self.env,
            "user-untrusted",
            self.capi,
            make_signed_input(self.alice, addon_content("capi-provisioner")),
            k8s_manifests({"kind": "Cluster"}),
        )
        assert_rejected_with(self, self.agent, delivery, "unknown trust anchor")

    # ------------------------------------------------------------------
    # Derivation chains
    # ------------------------------------------------------------------

    def test_chained_three_version_updates(self) -> None:
        v1 = self._addon_prior("1.28")
        u1 = self._update_att("update-1", "1.29")
        u2 = self._update_att("update-2", "1.30")
        delivery = signed_put(
            self.env,
            "d1-v3",
            self.capi,
            DerivedInput("deploy-1", "deployment", "d1-v2", "update-2"),
            k8s_manifests({"kind": "Cluster", "spec": {"version": "1.30"}}),
            prior_inputs={
                "d1-v1": v1,
                "d1-v2": DerivedInput("deploy-1", "deployment", "d1-v1", "update-1"),
            },
            update_attestations={"update-1": u1, "update-2": u2},
        )
        self.assertTrue(self.agent.verify(delivery, target_identity=self.prod_target).valid)

    def test_input_and_attestation_ids_may_overlap_without_false_cycle(self) -> None:
        prior = self._addon_prior("1.29")
        shared_update = self._update_att("shared", "1.30")
        delivery = signed_put(
            self.env,
            "final-overlap",
            self.capi,
            DerivedInput("deploy-1", "deployment", "shared", "shared"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"shared": prior},
            update_attestations={"shared": shared_update},
        )
        self.assertTrue(self.agent.verify(delivery, target_identity=self.prod_target).valid)

    def test_missing_prior_input_in_bundle(self) -> None:
        update_att = self._update_att("update", "1.30")
        delivery = signed_put(
            self.env,
            "missing-prior",
            self.capi,
            DerivedInput("deploy-1", "deployment", "nonexistent", "update"),
            k8s_manifests({"kind": "Cluster"}),
            update_attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            self.agent.verify_or_raise(delivery)
        self.assertIn("prior input not found", str(ctx.exception))

    def test_missing_update_attestation_in_bundle(self) -> None:
        prior = self._si(self.alice, content(deployment_id="deploy-1"))
        delivery = signed_put(
            self.env,
            "missing-update",
            self.capi,
            DerivedInput("deploy-1", "deployment", "prior", "nonexistent"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
        )
        with self.assertRaises(VerificationError) as ctx:
            self.agent.verify_or_raise(delivery)
        self.assertIn("update attestation not found", str(ctx.exception))

    def test_derived_input_deployment_id_must_match_prior(self) -> None:
        prior = self._addon_prior()
        update_att = self._update_att("update", "1.30")
        delivery = signed_put(
            self.env,
            "deployment-id-mismatch",
            self.capi,
            DerivedInput("deploy-2", "deployment", "prior", "update"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
            update_attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            self.agent.verify_or_raise(delivery)
        self.assertIn("content_id mismatch", str(ctx.exception))

    def test_expired_prior_input_in_derivation(self) -> None:
        prior = self._addon_prior(valid_duration_sec=-1)
        update_att = self._update_att("update", "1.30")
        delivery = signed_put(
            self.env,
            "expired-prior",
            self.capi,
            DerivedInput("deploy-1", "deployment", "prior", "update"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
            update_attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            self.agent.verify_or_raise(delivery)
        self.assertIn("expired", str(ctx.exception))

    def test_expired_update_input_in_derivation(self) -> None:
        prior = self._addon_prior()
        update_att = self._update_att("update", "1.30", valid_duration_sec=-1)
        delivery = signed_put(
            self.env,
            "expired-update",
            self.capi,
            DerivedInput("deploy-1", "deployment", "prior", "update"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
            update_attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            self.agent.verify_or_raise(delivery)
        self.assertIn("expired", str(ctx.exception))

    def test_update_attestation_fails_own_constraints(self) -> None:
        prior = self._addon_prior()
        um = spec_update_manifest({
            "derive_input_expression": (
                'set_path(prior, "manifest_strategy.config.version", "1.30")'
            ),
        })
        um, umb = sign_manifests(self.lifecycle, um)  # wrong signer for update constraint
        update_att = make_put_attestation(
            "update",
            self._si(
                self.bob,
                addon_content("upgrade-planner"),
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            um,
            manifest_bundle=umb,
        )
        delivery = signed_put(
            self.env,
            "bad-update",
            self.capi,
            DerivedInput("deploy-1", "deployment", "prior", "update"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
            update_attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            self.agent.verify_or_raise(delivery)
        self.assertIn("must be signed by upgrade-planner", str(ctx.exception))

    def test_untrusted_prior_signer_deep_in_chain(self) -> None:
        eve = self.env.user_signer("eve@evil.example")
        v1 = make_signed_input(
            eve,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capi-provisioner",
                        "trust_anchor_id": "fleet-addons",
                        "config": {"version": "1.28"},
                    },
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": 'target.labels.env == "prod"'},
                ),
            ),
        )
        u1 = self._update_att("update-1", "1.29", signer=self.alice)
        delivery = signed_put(
            self.env,
            "d1-v2",
            self.capi,
            DerivedInput("deploy-1", "deployment", "d1-v1", "update-1"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"d1-v1": v1},
            update_attestations={"update-1": u1},
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "unknown trust anchor",
            target_identity=self.prod_target,
        )

    def test_untrusted_update_signer_in_derivation(self) -> None:
        prior = self._addon_prior()
        eve = self.env.user_signer("eve@evil.example")
        update_att = self._update_att("update", "1.30", signer=eve)
        delivery = signed_put(
            self.env,
            "final",
            self.capi,
            DerivedInput("deploy-1", "deployment", "prior", "update"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
            update_attestations={"update": update_att},
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "unknown trust anchor",
            target_identity=self.prod_target,
        )

    def test_replay_output_from_different_attestation(self) -> None:
        """A valid signed output is rejected under conflicting signed policy."""
        manifests = k8s_manifests(
            {"kind": "Cluster", "metadata": {"namespace": "prod"}}
        )
        manifests, bundle = sign_manifests(self.capi, manifests)
        delivery = self.env.assemble(
            make_put_attestation(
                "replay",
                self._si(
                    self.alice,
                    inline_content(manifests),
                    output_constraints=(
                        addon_must_sign("capi-provisioner"),
                        namespace_constraint("staging"),
                    ),
                ),
                manifests,
                manifest_bundle=bundle,
            )
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "all manifests must be in namespace staging",
        )

    def test_self_signed_bypass_untrusted(self) -> None:
        """An attacker signs both input and output under an untrusted Fulcio."""
        attacker_env = make_env(
            user_issuer="https://rogue.example/idp",
            user_subjects=["mallory@rogue.example"],
        )
        rogue_identity = attacker_env.user_signer("mallory@rogue.example")
        attacker = Signer(
            oidc_issuer=rogue_identity.oidc_issuer,
            fulcio=rogue_identity.fulcio,
            tsa=self.env.tsa,
            identity=rogue_identity.identity,
            signer_id=rogue_identity.signer_id,
        )
        manifests = k8s_manifests({"kind": "Backdoor"})
        delivery = signed_put(
            self.env,
            "self",
            attacker,
            make_signed_input(attacker, inline_content(manifests)),
            manifests,
        )
        assert_rejected_with(self, self.agent, delivery, "trusted authority")

    def test_valid_derivation_wrong_output_addon(self) -> None:
        prior = self._addon_prior(output_constraints=(addon_must_sign("capi-provisioner"),))
        update_att = self._update_att("update", "1.30")
        delivery = signed_put(
            self.env,
            "final",
            self.planner,
            DerivedInput("deploy-1", "deployment", "prior", "update"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
            update_attestations={"update": update_att},
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "manifests must be signed by capi-provisioner",
            target_identity=self.prod_target,
        )

    def test_mixed_trust_both_anchors_needed(self) -> None:
        """A derived chain fails when TUF policy omits the addon anchor."""
        prior = self._addon_prior(
            "1.29",
            output_constraints=(addon_must_sign("capi-provisioner"),),
        )
        update = self._update_att("update", "1.30")
        self.env.anchors.pop("fleet-addons")
        self.env.republish()
        delivery = signed_put(
            self.env,
            "mixed",
            self.capi,
            DerivedInput("deploy-1", "deployment", "prior", "update"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={"prior": prior},
            update_attestations={"update": update},
        )
        assert_rejected_with(self, self.agent, delivery, "unknown trust anchor")

    def test_chained_middle_update_untrusted(self) -> None:
        v1 = self._addon_prior("1.28")
        eve = self.env.user_signer("eve@evil.example")
        u1 = self._update_att("update-1", "1.29", signer=eve)
        u2 = self._update_att("update-2", "1.30")
        delivery = signed_put(
            self.env,
            "d1-v3",
            self.capi,
            DerivedInput("deploy-1", "deployment", "d1-v2", "update-2"),
            k8s_manifests({"kind": "Cluster"}),
            prior_inputs={
                "d1-v1": v1,
                "d1-v2": DerivedInput("deploy-1", "deployment", "d1-v1", "update-1"),
            },
            update_attestations={"update-1": u1, "update-2": u2},
        )
        assert_rejected_with(
            self,
            self.agent,
            delivery,
            "unknown trust anchor",
            target_identity=self.prod_target,
        )


if __name__ == "__main__":
    unittest.main()
