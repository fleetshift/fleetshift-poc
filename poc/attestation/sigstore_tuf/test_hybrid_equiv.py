"""Hybrid-equivalence tests for the Sigstore/TUF POC.

Ports every test from hybrid/test_hybrid.py. KeyBinding-specific attacks are
replaced by Fulcio/TSA/DSSE equivalent guarantees (see TEST_PORTING.md).
"""

from __future__ import annotations

import base64
import copy
import unittest

from hybrid.model import DeploymentContent, ManifestEnvelope, OutputConstraint, StrategySpec
from hybrid.policy import constraint_to_document

from sigstore_tuf._test_helpers import (
    addon_content,
    addon_must_sign,
    all_details,
    allowed_gvks,
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
from sigstore_tuf.build import (
    PoCEnvironment,
    make_put_attestation,
    make_signed_input,
    sign_manifests,
)
from sigstore_tuf.model import Attestation, DerivedInput, PutDelivery
from sigstore_tuf.sigstore_sign import Identity, Signer, verify_sigstore_bundle
from sigstore_tuf.verify import (
    VerificationError,
    explain_verification,
    verify_attestation_or_raise,
    verify_delivery,
)


class HybridEquivTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env = make_env()
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
        result = verify_delivery(delivery, target_identity=self.prod_target)
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
        result = verify_delivery(delivery, target_identity=self.prod_target)
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
        result = verify_delivery(delivery, target_identity=self.prod_target)
        self.assertTrue(result.valid, result)
        explanation = explain_verification(delivery, target_identity=self.prod_target)
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
        self.assertTrue(verify_delivery(delivery, target_identity=self.prod_target).valid)

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
            verify_attestation_or_raise(delivery)
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
        result = verify_delivery(delivery, target_identity=self.prod_target)
        self.assertFalse(result.valid)

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
        self.assertFalse(verify_delivery(delivery).valid)

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
        self.assertFalse(verify_delivery(delivery).valid)

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
            verify_attestation_or_raise(delivery)
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
        self.assertTrue(verify_delivery(delivery).valid)

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
            verify_attestation_or_raise(delivery)
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
            verify_attestation_or_raise(delivery)
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
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(delivery)
        self.assertIn("capi-provisioner", str(ctx.exception))

    def test_namespace_violation(self) -> None:
        manifests = k8s_manifests({"metadata": {"namespace": "wrong"}, "kind": "Pod"})
        delivery = unsigned_put(
            self.env,
            "ns",
            self._si(self.alice, inline_content(manifests), output_constraints=(namespace_constraint("prod"),)),
            manifests,
        )
        self.assertFalse(verify_delivery(delivery).valid)

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
        self.assertTrue(verify_delivery(delivery).valid)

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
        self.assertFalse(verify_delivery(delivery).valid)

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
        self.assertFalse(verify_delivery(delivery).valid)

    # ------------------------------------------------------------------
    # Forgery / trust
    # ------------------------------------------------------------------

    def test_forged_input_signature_wrong_key(self) -> None:
        """Equivalent: DSSE signature does not verify under Fulcio leaf pubkey."""
        signed = self._si(self.alice, content())
        env_bad = make_env()  # different Fulcio root
        # Replace signature with garbage while keeping cert.
        env_copy = copy.deepcopy(signed.bundle.dsse_envelope)
        env_copy["signatures"][0]["sig"] = base64.b64encode(b"\x00" * 64).decode()
        object.__setattr__(signed, "bundle", signed.bundle.__class__(
            dsse_envelope=env_copy,
            certificate_pem=signed.bundle.certificate_pem,
            timestamp=signed.bundle.timestamp,
            trust_anchor_id=signed.bundle.trust_anchor_id,
            identity=signed.bundle.identity,
        ))
        delivery = self.env.assemble(make_put_attestation("forged", signed, ()))
        self.assertFalse(verify_delivery(delivery).valid)

    def test_untrusted_signer_empty_trust_store(self) -> None:
        """No allowed identities in TUF anchors → reject."""
        env = make_env(user_subjects=[])  # empty allowlist
        # Still need to publish with empty list — user_subjects=[] means empty allowlist
        alice = Signer(
            fulcio=env.user_fulcio,
            tsa=env.tsa,
            identity=Identity(issuer=env.user_issuer, subject="alice@tenant-a.example"),
            trust_anchor_id=env.user_anchor_id,
        )
        delivery = unsigned_put(env, "empty", make_signed_input(alice, content()), ())
        self.assertFalse(verify_delivery(delivery).valid)

    def test_unknown_key_in_anchor(self) -> None:
        """Identity not on allowlist (hybrid: unknown pubkey in anchor)."""
        rogue = Signer(
            fulcio=self.env.user_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer=self.env.user_issuer, subject="eve@evil.example"),
            trust_anchor_id=self.env.user_anchor_id,
        )
        delivery = unsigned_put(self.env, "unknown", make_signed_input(rogue, content()), ())
        self.assertFalse(verify_delivery(delivery).valid)

    def test_tampered_valid_until_breaks_envelope(self) -> None:
        signed = self._si(self.alice, content())
        object.__setattr__(signed, "valid_until", signed.valid_until + 99999)
        delivery = unsigned_put(self.env, "vu", signed, ())
        self.assertFalse(verify_delivery(delivery).valid)

    def test_tampered_input_content_breaks_envelope(self) -> None:
        signed = self._si(self.alice, content(deployment_id="orig"))
        object.__setattr__(signed, "content", content(deployment_id="tampered"))
        delivery = unsigned_put(self.env, "tc", signed, ())
        self.assertFalse(verify_delivery(delivery).valid)

    def test_forged_output_key_not_in_anchor(self) -> None:
        rogue = Signer(
            fulcio=self.env.workload_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer="https://oidc.addon.local", subject="evil-addon"),
            trust_anchor_id="fleet-addons",
        )
        delivery = signed_put(
            self.env,
            "forged-out",
            rogue,
            self._si(self.alice, addon_content("capi-provisioner")),
            k8s_manifests({"kind": "Cluster"}),
        )
        self.assertFalse(verify_delivery(delivery).valid)

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
        self.assertFalse(verify_delivery(self.env.assemble(att)).valid)

    def test_output_trust_anchor_missing(self) -> None:
        rogue = Signer(
            fulcio=self.env.workload_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer="https://oidc.addon.local", subject="rogue-addon"),
            trust_anchor_id="nonexistent-addon-ca",
        )
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
            verify_attestation_or_raise(delivery)
        self.assertIn("trust anchor", str(ctx.exception).lower())

    # ------------------------------------------------------------------
    # KeyBinding → Fulcio/TSA/DSSE equivalents
    # ------------------------------------------------------------------

    def test_key_binding_signer_mismatch(self) -> None:
        """Equiv: bundle.identity subject disagrees with Fulcio cert extensions."""
        signed = self._si(self.alice, content())
        from sigstore_tuf.sigstore_sign import SigstoreBundle

        bad = SigstoreBundle(
            dsse_envelope=signed.bundle.dsse_envelope,
            certificate_pem=signed.bundle.certificate_pem,
            timestamp=signed.bundle.timestamp,
            trust_anchor_id=signed.bundle.trust_anchor_id,
            identity=Identity(issuer=signed.bundle.identity.issuer, subject="bob@tenant-a.example"),
        )
        object.__setattr__(signed, "bundle", bad)
        delivery = unsigned_put(self.env, "kb-mismatch", signed, ())
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(delivery)
        self.assertIn("identity", str(ctx.exception).lower())

    def test_key_binding_forged_proof(self) -> None:
        """Equiv: TSA token forged / does not bind to DSSE PAE digest."""
        signed = self._si(self.alice, content())
        from sigstore_tuf.sigstore_sign import SigstoreBundle
        from sigstore_tuf.tsa import TimestampToken

        ts = signed.bundle.timestamp
        forged_ts = TimestampToken(
            digest_hex="11" * 32,
            gen_time=ts.gen_time,
            signature_b64=ts.signature_b64,
            leaf_pem=ts.leaf_pem,
            root_pem=ts.root_pem,
        )
        bad = SigstoreBundle(
            dsse_envelope=signed.bundle.dsse_envelope,
            certificate_pem=signed.bundle.certificate_pem,
            timestamp=forged_ts,
            trust_anchor_id=signed.bundle.trust_anchor_id,
            identity=signed.bundle.identity,
        )
        object.__setattr__(signed, "bundle", bad)
        delivery = unsigned_put(self.env, "forged-proof", signed, ())
        self.assertFalse(verify_delivery(delivery).valid)

    def test_key_binding_wrong_public_key(self) -> None:
        """Equiv: Fulcio leaf pubkey does not verify the DSSE signature."""
        signed = self._si(self.alice, content())
        other = self._si(self.bob, content())
        from sigstore_tuf.sigstore_sign import SigstoreBundle

        # Alice's envelope signed with Alice's key, but Bob's certificate attached.
        bad = SigstoreBundle(
            dsse_envelope=signed.bundle.dsse_envelope,
            certificate_pem=other.bundle.certificate_pem,
            timestamp=signed.bundle.timestamp,
            trust_anchor_id=signed.bundle.trust_anchor_id,
            identity=signed.bundle.identity,
        )
        object.__setattr__(signed, "bundle", bad)
        delivery = unsigned_put(self.env, "wrong-pubkey", signed, ())
        self.assertFalse(verify_delivery(delivery).valid)

    # ------------------------------------------------------------------
    # Cross-anchor / identity confusion
    # ------------------------------------------------------------------

    def test_user_key_cannot_satisfy_addon_constraint(self) -> None:
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
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(delivery)
        self.assertIn("output must be signed by capi-provisioner", str(ctx.exception))

    def test_user_registers_in_addon_anchor_fails(self) -> None:
        rogue = Signer(
            fulcio=self.env.user_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer=self.env.user_issuer, subject="evil-user@tenant-a.example"),
            trust_anchor_id="fleet-addons",
        )
        delivery = unsigned_put(self.env, "user-in-addon", make_signed_input(rogue, content()), ())
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(delivery)
        self.assertFalse(verify_delivery(delivery).valid)

    def test_addon_self_authorises_fails_constraint(self) -> None:
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
        # Addon signing input: need addon identity allowed on user anchor? capi uses workload fulcio + fleet-addons.
        # Input signed by capi against fleet-addons — allowed. Output also by capi but constraint wants lifecycle.
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation_or_raise(delivery)
        self.assertIn("cluster-lifecycle", str(ctx.exception))

    def test_user_trusted_addon_not(self) -> None:
        """User OK; addon output signer not in fleet-addons allowlist."""
        delivery = signed_put(
            self.env,
            "addon-untrusted",
            Signer(
                fulcio=self.env.workload_fulcio,
                tsa=self.env.tsa,
                identity=Identity(issuer="https://oidc.addon.local", subject="not-registered"),
                trust_anchor_id="fleet-addons",
            ),
            self._si(self.alice, addon_content("capi-provisioner")),
            k8s_manifests({"kind": "Cluster"}),
        )
        self.assertFalse(verify_delivery(delivery).valid)

    def test_addon_trusted_user_not(self) -> None:
        """Input from untrusted user; addon output would be fine."""
        eve = Signer(
            fulcio=self.env.user_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer=self.env.user_issuer, subject="eve@other.example"),
            trust_anchor_id=self.env.user_anchor_id,
        )
        delivery = signed_put(
            self.env,
            "user-untrusted",
            self.capi,
            make_signed_input(eve, addon_content("capi-provisioner")),
            k8s_manifests({"kind": "Cluster"}),
        )
        self.assertFalse(verify_delivery(delivery).valid)

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
        self.assertTrue(verify_delivery(delivery, target_identity=self.prod_target).valid)

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
        self.assertTrue(verify_delivery(delivery, target_identity=self.prod_target).valid)

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
            verify_attestation_or_raise(delivery)
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
            verify_attestation_or_raise(delivery)
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
            verify_attestation_or_raise(delivery)
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
            verify_attestation_or_raise(delivery)
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
            verify_attestation_or_raise(delivery)
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
            verify_attestation_or_raise(delivery)
        self.assertIn("must be signed by upgrade-planner", str(ctx.exception))

    def test_untrusted_prior_signer_deep_in_chain(self) -> None:
        eve = Signer(
            fulcio=self.env.user_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer=self.env.user_issuer, subject="eve@evil.example"),
            trust_anchor_id=self.env.user_anchor_id,
        )
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
        self.assertFalse(verify_delivery(delivery, target_identity=self.prod_target).valid)

    def test_untrusted_update_signer_in_derivation(self) -> None:
        prior = self._addon_prior()
        eve = Signer(
            fulcio=self.env.user_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer=self.env.user_issuer, subject="eve@evil.example"),
            trust_anchor_id=self.env.user_anchor_id,
        )
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
        self.assertFalse(verify_delivery(delivery, target_identity=self.prod_target).valid)

    def test_replay_output_from_different_attestation(self) -> None:
        """Manifests signed for one payload reused with different input constraints."""
        m1 = k8s_manifests({"kind": "Cluster", "v": 1})
        m1, mb = sign_manifests(self.capi, m1)
        # Attach the signature from m1 to different manifest content already covered;
        # here: reuse mb with matching content but wrong input (inline vs addon).
        delivery = self.env.assemble(
            make_put_attestation(
                "replay",
                self._si(
                    self.alice,
                    inline_content(m1),
                    output_constraints=(addon_must_sign("capi-provisioner"),),
                ),
                m1,
                manifest_bundle=mb,
            )
        )
        # Inline strategy requires manifests match; they do. Explicit addon_must_sign also passes.
        # Hybrid test swaps output between attestations — simulate by signing for A then using with B's input.
        other = k8s_manifests({"kind": "Cluster", "v": 2})
        att = make_put_attestation(
            "replay2",
            self._si(self.alice, addon_content("capi-provisioner")),
            other,
            manifest_bundle=mb,  # signature over m1, not other
        )
        self.assertFalse(verify_delivery(self.env.assemble(att)).valid)

    def test_self_signed_bypass_untrusted(self) -> None:
        """Untrusted identity cannot self-sign around trust store."""
        eve = Signer(
            fulcio=self.env.user_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer=self.env.user_issuer, subject="eve@evil.example"),
            trust_anchor_id=self.env.user_anchor_id,
        )
        delivery = unsigned_put(self.env, "self", make_signed_input(eve, content()), ())
        self.assertFalse(verify_delivery(delivery).valid)

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
        self.assertFalse(verify_delivery(delivery, target_identity=self.prod_target).valid)

    def test_mixed_trust_both_anchors_needed(self) -> None:
        """User input + addon output both required."""
        delivery = signed_put(
            self.env,
            "mixed",
            self.capi,
            self._si(self.alice, addon_content("capi-provisioner")),
            k8s_manifests({"kind": "Cluster"}),
        )
        self.assertTrue(verify_delivery(delivery).valid)

    def test_chained_middle_update_untrusted(self) -> None:
        v1 = self._addon_prior("1.28")
        eve = Signer(
            fulcio=self.env.user_fulcio,
            tsa=self.env.tsa,
            identity=Identity(issuer=self.env.user_issuer, subject="eve@evil.example"),
            trust_anchor_id=self.env.user_anchor_id,
        )
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
        self.assertFalse(verify_delivery(delivery, target_identity=self.prod_target).valid)


if __name__ == "__main__":
    unittest.main()
