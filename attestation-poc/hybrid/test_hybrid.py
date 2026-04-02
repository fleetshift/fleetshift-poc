"""Tests for the hybrid attestation prototype."""

from __future__ import annotations

import time
from dataclasses import dataclass
import json
import unittest

from .build import (
    make_key_binding,
    make_put_manifests,
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
    PutManifests,
    Signature,
    SignedInput,
    StrategySpec,
    TrustAnchor,
    TrustAnchorConstraint,
)
from .policy import constraint_to_document
from .verify import (
    TrustStore,
    VerificationBundle,
    VerificationError,
    VerificationResult,
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


# ---------------------------------------------------------------------------
# Reusable constraint builders
# ---------------------------------------------------------------------------


def addon_must_sign(
    addon_id: str,
    trust_anchor_id: str = "fleet-addons",
) -> OutputConstraint:
    return OutputConstraint(
        name=f"output must be signed by {addon_id} via {trust_anchor_id}",
        expression=(
            f'output.has_signature && '
            f'output.signature.trust_anchor_id == "{trust_anchor_id}" && '
            f'output.signer_id == "{addon_id}"'
        ),
    )


def namespace_constraint(namespace: str) -> OutputConstraint:
    return OutputConstraint(
        name=f"all manifests must be in namespace {namespace}",
        expression=f'output.manifests.all(m, m.content.metadata.namespace == "{namespace}")',
    )


def allowed_gvks(*gvks: str) -> OutputConstraint:
    allowed_literal = json.dumps(list(gvks))
    return OutputConstraint(
        name=f"only GVKs in {list(gvks)}",
        expression=(
            f'output.manifests.all(m, ((m.content.apiVersion + "/" + m.content.kind) in {allowed_literal}))'
        ),
    )


def no_cluster_admin() -> OutputConstraint:
    return OutputConstraint(
        name="no ClusterRoleBinding may grant cluster-admin",
        expression=(
            'output.manifests.all('
            'm, !(m.content.kind == "ClusterRoleBinding" && m.content.roleRef.name == "cluster-admin")'
            ')'
        ),
    )


def tenant_subject_must_match_anchor_tenant() -> TrustAnchorConstraint:
    return TrustAnchorConstraint(
        name="input tenant must match anchor tenant",
        expression=(
            'subject.kind != "input" || '
            '("tenant" in subject.content && '
            'subject.content.tenant == anchor.attributes.tenant)'
        ),
    )


class HybridAttestationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.alice = make_identity("alice", "tenant-idp")
        self.bob = make_identity("bob", "tenant-idp")
        self.capi_addon = make_identity("capi-provisioner", "fleet-addons")
        self.lifecycle_addon = make_identity("cluster-lifecycle", "fleet-addons")
        self.planner_addon = make_identity("upgrade-planner", "fleet-addons")

        self.empty_bundle = VerificationBundle()
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
                    "capi-provisioner": self.capi_addon.keys.public_key_bytes,
                    "cluster-lifecycle": self.lifecycle_addon.keys.public_key_bytes,
                    "upgrade-planner": self.planner_addon.keys.public_key_bytes,
                },
            )
        )

        self.prod_target = {
            "id": "cluster-prod-1",
            "labels": {"env": "prod", "region": "us-east-1"},
        }

    def test_direct_attestation_with_signed_cel_constraints_verifies(self) -> None:
        manifests = k8s_manifests(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "web", "namespace": "prod"},
            }
        )
        attestation = Attestation(
            attestation_id="direct",
            input=self._input(
                self.alice,
                {"intent": "deploy-web"},
                output_constraints=(
                    namespace_constraint("prod"),
                    allowed_gvks("apps/v1/Deployment"),
                ),
            ),
            output=make_put_manifests(manifests),
        )

        verified = verify_attestation(
            attestation,
            self.empty_bundle,
            self.trust_store,
        )

        self.assertEqual(verified.content, serialize_envelopes(manifests))
        self.assertIsNone(verified.signer_id)

    def test_addon_signed_output_with_explicit_cel_policy_verifies(self) -> None:
        spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "trust_anchor_id": "fleet-addons",
            }
        }
        manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
            }
        )
        attestation = Attestation(
            attestation_id="addon",
            input=self._input(
                self.alice,
                spec,
                output_constraints=(
                    addon_must_sign("capi-provisioner"),
                    no_cluster_admin(),
                ),
            ),
            output=self._signed_output(self.capi_addon, manifests),
        )

        verified = verify_attestation(
            attestation,
            self.empty_bundle,
            self.trust_store,
        )

        self.assertEqual(verified.signer_id, "capi-provisioner")

    def test_derived_input_uses_signed_cel_update_and_constraints(self) -> None:
        prior_spec = DeploymentContent(
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
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        )
        prior_input = self._input(
            self.alice,
            prior_spec,
            output_constraints=(addon_must_sign("capi-provisioner"),),
        )

        update_request = {"type": "request", "capability": "upgrade-planner"}
        update_directive = {
            "derive_input_expression": (
                'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
            ),
            "output_constraints": [
                constraint_to_document(namespace_constraint("capi-system")),
            ],
        }
        update_attestation = Attestation(
            attestation_id="upgrade-1",
            input=self._input(
                self.bob,
                update_request,
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            output=self._signed_output(
                self.planner_addon, spec_update_manifest(update_directive),
            ),
        )

        final_manifests = k8s_manifests(
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            }
        )
        final_attestation = Attestation(
            attestation_id="d1-v2",
            input=DerivedInput(
                deployment_id="cluster-01",
                prior_input_id="d1-v1",
                update_attestation_id="upgrade-1",
            ),
            output=self._signed_output(self.capi_addon, final_manifests),
        )

        bundle = VerificationBundle(
            inputs={"d1-v1": prior_input},
            attestations={"upgrade-1": update_attestation},
        )

        verified = verify_attestation(
            final_attestation, bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        explanation = explain_verification(
            final_attestation, bundle, self.trust_store,
            target_identity=self.prod_target,
        )

        self.assertEqual(verified.signer_id, "capi-provisioner")
        serialized = serialize_envelopes(final_manifests)
        self.assertEqual(
            serialized[0]["content"]["spec"]["topology"]["version"],
            "1.30.2",
        )
        self.assertIn(
            "derived from prior=d1-v1 + update=upgrade-1",
            self._all_details(explanation),
        )
        self.assertIn("upgrade-planner", self._all_details(explanation))

    def test_derived_input_falls_back_to_spec_derived_constraints(self) -> None:
        prior_spec = DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "capi-provisioner",
                    "trust_anchor_id": "fleet-addons",
                    "config": {"version": "1.29.5"},
                },
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        )
        prior_input = self._input(self.alice, prior_spec)
        update_attestation = Attestation(
            attestation_id="update",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
                    ),
                }),
            ),
        )
        final_attestation = Attestation(
            attestation_id="final",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior",
                update_attestation_id="update",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )

        bundle = VerificationBundle(
            inputs={"prior": prior_input},
            attestations={"update": update_attestation},
        )

        verified = verify_attestation(
            final_attestation, bundle, self.trust_store,
            target_identity=self.prod_target,
        )

        self.assertEqual(verified.signer_id, "capi-provisioner")

    def test_bad_update_expression_fails_derivation(self) -> None:
        prior_spec = DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "capi-provisioner",
                    "trust_anchor_id": "fleet-addons",
                },
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        )
        prior_input = self._input(self.alice, prior_spec)
        update_attestation = Attestation(
            attestation_id="update-bad-expression",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": "1 + 2",
                }),
            ),
        )
        final_attestation = Attestation(
            attestation_id="final-bad-expression",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior-bad-update",
                update_attestation_id="update-bad-expression",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )

        bundle = VerificationBundle(
            inputs={"prior-bad-update": prior_input},
            attestations={"update-bad-expression": update_attestation},
        )

        with self.assertRaises(VerificationError) as context:
            verify_attestation(final_attestation, bundle, self.trust_store)

        self.assertIn("derive_input_expression must return an object", str(context.exception))

    def test_wrong_output_signer_fails_against_derived_constraint(self) -> None:
        prior_spec = DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "capi-provisioner",
                    "trust_anchor_id": "fleet-addons",
                    "config": {"version": "1.29.5"},
                },
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        )
        prior_input = self._input(self.alice, prior_spec)
        update_attestation = Attestation(
            attestation_id="update-wrong",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
                    ),
                }),
            ),
        )
        final_attestation = Attestation(
            attestation_id="final-wrong",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior-wrong",
                update_attestation_id="update-wrong",
            ),
            output=self._signed_output(
                self.lifecycle_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )

        bundle = VerificationBundle(
            inputs={"prior-wrong": prior_input},
            attestations={"update-wrong": update_attestation},
        )

        with self.assertRaises(VerificationError) as context:
            verify_attestation(
                final_attestation, bundle, self.trust_store,
                target_identity=self.prod_target,
            )

        self.assertIn("manifests must be signed by capi-provisioner", str(context.exception))

    def test_expired_signed_input_fails(self) -> None:
        attestation = Attestation(
            attestation_id="expired",
            input=self._input(
                self.alice,
                {"intent": "too-late"},
                valid_duration_sec=-1,
            ),
            output=make_put_manifests(k8s_manifests({"ignored": True})),
        )

        with self.assertRaises(VerificationError) as context:
            verify_attestation(attestation, self.empty_bundle, self.trust_store)

        self.assertIn("expired", str(context.exception))

    def test_tampered_signed_constraints_fail(self) -> None:
        signed_input = self._input(
            self.alice,
            {"intent": "tamper-check"},
            output_constraints=(namespace_constraint("prod"),),
        )
        tampered_input = SignedInput(
            content=signed_input.content,
            signature=signed_input.signature,
            key_binding=signed_input.key_binding,
            valid_until=signed_input.valid_until,
            output_constraints=(),
        )
        attestation = Attestation(
            attestation_id="tampered-constraints",
            input=tampered_input,
            output=make_put_manifests(k8s_manifests({"kind": "ConfigMap"})),
        )

        with self.assertRaises(VerificationError) as context:
            verify_attestation(attestation, self.empty_bundle, self.trust_store)

        self.assertIn("signed input hash mismatch", str(context.exception))

    def test_failed_cel_constraint_surfaces_constraint_name(self) -> None:
        manifests = k8s_manifests(
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRoleBinding",
                "metadata": {"name": "evil", "namespace": "prod"},
                "roleRef": {"name": "cluster-admin"},
            }
        )
        attestation = Attestation(
            attestation_id="bad-rbac",
            input=self._input(
                self.alice,
                {"intent": "deploy-rbac"},
                output_constraints=(no_cluster_admin(),),
            ),
            output=make_put_manifests(manifests),
        )

        with self.assertRaises(VerificationError) as context:
            verify_attestation(attestation, self.empty_bundle, self.trust_store)

        self.assertIn("no ClusterRoleBinding may grant cluster-admin", str(context.exception))

    def test_tenant_scoped_trust_anchor_accepts_matching_tenant(self) -> None:
        tenant_alice = make_identity("alice-tenant-a", "tenant-a-idp")
        trust_store = TrustStore()
        trust_store.add(
            TrustAnchor(
                anchor_id="tenant-a-idp",
                known_keys={"alice-tenant-a": tenant_alice.keys.public_key_bytes},
                attributes={"tenant": "tenant-a"},
                constraints=(tenant_subject_must_match_anchor_tenant(),),
            )
        )

        manifests = k8s_manifests({"kind": "ConfigMap"})
        attestation = Attestation(
            attestation_id="tenant-match",
            input=self._input(
                tenant_alice,
                {"tenant": "tenant-a", "intent": "deploy-web"},
            ),
            output=make_put_manifests(manifests),
        )

        verified = verify_attestation(attestation, self.empty_bundle, trust_store)

        self.assertEqual(verified.content, serialize_envelopes(manifests))

    def test_tenant_scoped_trust_anchor_rejects_other_tenant(self) -> None:
        tenant_alice = make_identity("alice-tenant-a", "tenant-a-idp")
        trust_store = TrustStore()
        trust_store.add(
            TrustAnchor(
                anchor_id="tenant-a-idp",
                known_keys={"alice-tenant-a": tenant_alice.keys.public_key_bytes},
                attributes={"tenant": "tenant-a"},
                constraints=(tenant_subject_must_match_anchor_tenant(),),
            )
        )

        attestation = Attestation(
            attestation_id="tenant-mismatch",
            input=self._input(
                tenant_alice,
                {"tenant": "tenant-b", "intent": "deploy-web"},
            ),
            output=make_put_manifests(k8s_manifests({"kind": "ConfigMap"})),
        )

        with self.assertRaises(VerificationError) as context:
            verify_attestation(attestation, self.empty_bundle, trust_store)

        self.assertIn("input tenant must match anchor tenant", str(context.exception))

    def test_trust_anchor_constraint_cannot_use_attestation_identifier(self) -> None:
        tenant_alice = make_identity("alice-tenant-a", "tenant-a-idp")
        trust_store = TrustStore()
        trust_store.add(
            TrustAnchor(
                anchor_id="tenant-a-idp",
                known_keys={"alice-tenant-a": tenant_alice.keys.public_key_bytes},
                constraints=(
                    TrustAnchorConstraint(
                        name="attestation identifiers are not part of the authenticated subject",
                        expression='subject.attestation_id.startsWith("tenant-a/")',
                    ),
                ),
            )
        )

        attestation = Attestation(
            attestation_id="tenant-b/spoofed-reference",
            input=self._input(
                tenant_alice,
                {"tenant": "tenant-a", "intent": "deploy-web"},
            ),
            output=make_put_manifests(k8s_manifests({"kind": "ConfigMap"})),
        )

        with self.assertRaises(VerificationError) as context:
            verify_attestation(attestation, self.empty_bundle, trust_store)

        self.assertIn("trust anchor constraint evaluation failed", str(context.exception))

    # ------------------------------------------------------------------
    # Constraint violations (non-derived)
    # ------------------------------------------------------------------

    def test_wrong_addon_signs_output(self) -> None:
        att = Attestation(
            attestation_id="wrong-addon",
            input=self._input(
                self.alice,
                {"manifest_strategy": {"type": "addon", "addon_id": "capi-provisioner"}},
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            output=self._signed_output(
                self.lifecycle_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn(
            "manifests must be signed by capi-provisioner via fleet-addons",
            str(ctx.exception),
        )

    def test_namespace_violation(self) -> None:
        att = Attestation(
            attestation_id="ns-viol",
            input=self._input(
                self.alice,
                {"intent": "deploy"},
                output_constraints=(namespace_constraint("prod"),),
            ),
            output=make_put_manifests(k8s_manifests(
                {"kind": "Deployment", "metadata": {"name": "web", "namespace": "kube-system"}},
            )),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("all manifests must be in namespace prod", str(ctx.exception))

    def test_multiple_constraints_all_pass(self) -> None:
        manifests = k8s_manifests(
            {"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "web", "namespace": "prod"}},
        )
        att = Attestation(
            attestation_id="multi-ok",
            input=self._input(
                self.alice,
                {"intent": "deploy"},
                output_constraints=(
                    namespace_constraint("prod"),
                    allowed_gvks("apps/v1/Deployment", "v1/Service"),
                ),
            ),
            output=make_put_manifests(manifests),
        )
        verified = verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertEqual(verified.content, serialize_envelopes(manifests))

    def test_gvk_violation(self) -> None:
        manifests = k8s_manifests(
            {"apiVersion": "apps/v1", "kind": "DaemonSet",
             "metadata": {"name": "agent", "namespace": "prod"}},
        )
        att = Attestation(
            attestation_id="gvk-viol",
            input=self._input(
                self.alice,
                {"intent": "deploy"},
                output_constraints=(allowed_gvks("apps/v1/Deployment", "v1/Service"),),
            ),
            output=make_put_manifests(manifests),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("only GVKs in", str(ctx.exception))

    def test_unsigned_output_fails_addon_constraint(self) -> None:
        att = Attestation(
            attestation_id="unsigned",
            input=self._input(
                self.alice,
                {"manifest_strategy": {"type": "addon"}},
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            output=make_put_manifests(k8s_manifests({"kind": "Cluster"})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("output must be signed by capi-provisioner", str(ctx.exception))

    # ------------------------------------------------------------------
    # Input signature integrity
    # ------------------------------------------------------------------

    def test_forged_input_signature_wrong_key(self) -> None:
        """Forger signs the same content with their key, keeps alice's key binding."""
        real = self._input(self.alice, {"x": 1})
        forger = generate_keypair()
        forger_kb = make_key_binding(forger, "alice", "tenant-idp")
        forged = make_signed_input(forger, forger_kb, {"x": 1})
        tampered = SignedInput(
            content=real.content,
            signature=forged.signature,
            key_binding=real.key_binding,
            valid_until=real.valid_until,
            output_constraints=real.output_constraints,
        )
        att = Attestation(
            attestation_id="forged-sig",
            input=tampered,
            output=make_put_manifests(k8s_manifests({"x": 1})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("signature key does not match key binding", str(ctx.exception))

    def test_untrusted_signer_empty_trust_store(self) -> None:
        empty_ts = TrustStore()
        att = Attestation(
            attestation_id="untrusted",
            input=self._input(self.alice, {"x": 1}),
            output=make_put_manifests(k8s_manifests({"x": 1})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, empty_ts)
        self.assertIn("trust anchor not found", str(ctx.exception))

    def test_unknown_key_in_anchor(self) -> None:
        """Anchor exists but signer is not registered in it."""
        carol = make_identity("carol", "tenant-idp")
        att = Attestation(
            attestation_id="unknown-key",
            input=self._input(carol, {"x": 1}),
            output=make_put_manifests(k8s_manifests({"x": 1})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("key not recognised", str(ctx.exception))

    def test_tampered_valid_until_breaks_envelope(self) -> None:
        """Extend valid_until after signing. Envelope hash changes."""
        si = self._input(self.alice, {"x": 1}, valid_duration_sec=-1)
        tampered = SignedInput(
            content=si.content,
            signature=si.signature,
            key_binding=si.key_binding,
            valid_until=time.time() + 86400,
            output_constraints=si.output_constraints,
        )
        att = Attestation(
            attestation_id="tampered-expiry",
            input=tampered,
            output=make_put_manifests(k8s_manifests({"x": 1})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("signed input hash mismatch", str(ctx.exception))

    def test_tampered_input_content_breaks_envelope(self) -> None:
        si = self._input(self.alice, {"original": True})
        tampered = SignedInput(
            content={"malicious": True},
            signature=si.signature,
            key_binding=si.key_binding,
            valid_until=si.valid_until,
            output_constraints=si.output_constraints,
        )
        att = Attestation(
            attestation_id="tampered-content",
            input=tampered,
            output=make_put_manifests(k8s_manifests({"malicious": True})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("signed input hash mismatch", str(ctx.exception))

    # ------------------------------------------------------------------
    # Output signature integrity
    # ------------------------------------------------------------------

    def test_forged_output_key_not_in_anchor(self) -> None:
        """New key signs output claiming to be capi-provisioner. Anchor rejects."""
        forger = make_identity("capi-provisioner", "fleet-addons")
        att = Attestation(
            attestation_id="forged-output",
            input=self._input(
                self.alice,
                {"manifest_strategy": {"type": "addon"}},
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            output=self._signed_output(
                forger, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("key not recognised", str(ctx.exception))

    def test_tampered_output_content(self) -> None:
        """Change output content after addon signed it. Hash mismatch."""
        legit = self._signed_output(
            self.capi_addon, k8s_manifests({"kind": "Cluster"}),
        )
        tampered = PutManifests(
            manifests=k8s_manifests({"kind": "DaemonSet", "spec": {"evil": True}}),
            signature=legit.signature,
        )
        att = Attestation(
            attestation_id="tampered-output",
            input=self._input(
                self.alice,
                {"manifest_strategy": {"type": "addon"}},
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            output=tampered,
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("hash mismatch", str(ctx.exception))

    def test_output_trust_anchor_missing(self) -> None:
        """Output signature references an anchor not in the trust store."""
        rogue = make_identity("rogue-addon", "nonexistent-addon-ca")
        att = Attestation(
            attestation_id="missing-anchor",
            input=self._input(
                self.alice,
                {"manifest_strategy": {"type": "addon"}},
                output_constraints=(
                    addon_must_sign("rogue-addon", "nonexistent-addon-ca"),
                ),
            ),
            output=self._signed_output(
                rogue, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("trust anchor not found", str(ctx.exception))

    # ------------------------------------------------------------------
    # Key binding attacks
    # ------------------------------------------------------------------

    def test_key_binding_signer_mismatch(self) -> None:
        """Signature claims bob, key binding is for alice."""
        si = self._input(self.alice, {"x": 1})
        tampered_sig = Signature(
            signer_id="bob",
            public_key=si.signature.public_key,
            content_hash=si.signature.content_hash,
            signature_bytes=si.signature.signature_bytes,
        )
        tampered = SignedInput(
            content=si.content,
            signature=tampered_sig,
            key_binding=si.key_binding,
            valid_until=si.valid_until,
            output_constraints=si.output_constraints,
        )
        att = Attestation(
            attestation_id="kb-mismatch",
            input=tampered,
            output=make_put_manifests(k8s_manifests({"x": 1})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("signature signer does not match key binding", str(ctx.exception))

    def test_key_binding_forged_proof(self) -> None:
        """Key binding has alice's real key but proof signed by a different key."""
        forger = generate_keypair()
        binding_doc = {
            "public_key": self.alice.keys.public_key_bytes.hex(),
            "signer_id": "alice",
            "trust_anchor_id": "tenant-idp",
        }
        forged_proof = sign(forger.private_key, content_hash(binding_doc))
        forged_kb = KeyBinding(
            signer_id="alice",
            public_key=self.alice.keys.public_key_bytes,
            trust_anchor_id="tenant-idp",
            binding_proof=forged_proof,
        )
        si = make_signed_input(self.alice.keys, forged_kb, {"x": 1})
        att = Attestation(
            attestation_id="forged-proof",
            input=si,
            output=make_put_manifests(k8s_manifests({"x": 1})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("proof-of-possession failed", str(ctx.exception))

    def test_key_binding_wrong_public_key(self) -> None:
        """Signature uses a different public key than the key binding."""
        si = self._input(self.alice, {"x": 1})
        other_keys = generate_keypair()
        tampered_sig = Signature(
            signer_id="alice",
            public_key=other_keys.public_key_bytes,
            content_hash=si.signature.content_hash,
            signature_bytes=si.signature.signature_bytes,
        )
        tampered = SignedInput(
            content=si.content,
            signature=tampered_sig,
            key_binding=si.key_binding,
            valid_until=si.valid_until,
            output_constraints=si.output_constraints,
        )
        att = Attestation(
            attestation_id="wrong-pubkey",
            input=tampered,
            output=make_put_manifests(k8s_manifests({"x": 1})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("signature key does not match key binding", str(ctx.exception))

    # ------------------------------------------------------------------
    # Cross-anchor and identity confusion
    # ------------------------------------------------------------------

    def test_user_key_cannot_satisfy_addon_constraint(self) -> None:
        """User signs output directly. addon_must_sign rejects (wrong anchor + signer)."""
        att = Attestation(
            attestation_id="user-as-addon",
            input=self._input(
                self.alice,
                {"manifest_strategy": {"type": "addon"}},
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            output=self._signed_output(
                self.alice, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("output must be signed by capi-provisioner", str(ctx.exception))

    def test_user_registers_in_addon_anchor_fails(self) -> None:
        """User creates a key binding against fleet-addons. Anchor doesn't know them."""
        rogue = make_identity("evil-user", "fleet-addons")
        att = Attestation(
            attestation_id="user-in-addon",
            input=self._input(rogue, {"x": 1}),
            output=make_put_manifests(k8s_manifests({"x": 1})),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("key not recognised", str(ctx.exception))

    def test_addon_self_authorises_fails_constraint(self) -> None:
        """Addon is both authority and producer, but constraint expects a different addon."""
        att = Attestation(
            attestation_id="self-auth",
            input=self._input(
                self.capi_addon,
                {"manifest_strategy": {"type": "addon"}},
                output_constraints=(addon_must_sign("cluster-lifecycle"),),
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("output must be signed by cluster-lifecycle", str(ctx.exception))

    # ------------------------------------------------------------------
    # Trust store gaps
    # ------------------------------------------------------------------

    def test_user_trusted_addon_not(self) -> None:
        """Trust store has user anchor but no addon anchor."""
        ts = TrustStore()
        ts.add(TrustAnchor(
            anchor_id="tenant-idp",
            known_keys={"alice": self.alice.keys.public_key_bytes},
        ))
        att = Attestation(
            attestation_id="no-addon-anchor",
            input=self._input(
                self.alice,
                {"manifest_strategy": {"type": "addon"}},
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, ts)
        self.assertIn("trust anchor not found", str(ctx.exception))

    def test_addon_trusted_user_not(self) -> None:
        """Trust store has addon anchor but no user anchor."""
        ts = TrustStore()
        ts.add(TrustAnchor(
            anchor_id="fleet-addons",
            known_keys={"capi-provisioner": self.capi_addon.keys.public_key_bytes},
        ))
        att = Attestation(
            attestation_id="no-user-anchor",
            input=self._input(
                self.alice,
                {"manifest_strategy": {"type": "addon"}},
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, ts)
        self.assertIn("trust anchor not found", str(ctx.exception))

    # ------------------------------------------------------------------
    # Derived input: chained updates, missing refs, propagated failures
    # ------------------------------------------------------------------

    def test_chained_three_version_updates(self) -> None:
        """v1 -> v2 -> v3 through bundle references."""
        v1_spec = DeploymentContent(
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
        )
        v1_input = self._input(self.alice, v1_spec)

        def _update_att(version: str, att_id: str) -> Attestation:
            return Attestation(
                attestation_id=att_id,
                input=self._input(
                    self.bob,
                    {"type": "request", "version": version},
                ),
                output=self._signed_output(
                    self.planner_addon,
                    spec_update_manifest({
                        "derive_input_expression": (
                            f'set_path(prior, "manifest_strategy.config.version", "{version}")'
                        ),
                    }),
                ),
            )

        bundle = VerificationBundle(
            inputs={
                "d1-v1": v1_input,
                "d1-v2": DerivedInput(
                    deployment_id="deploy-1",
                    prior_input_id="d1-v1",
                    update_attestation_id="update-1",
                ),
            },
            attestations={
                "update-1": _update_att("1.29", "update-1"),
                "update-2": _update_att("1.30", "update-2"),
            },
        )

        att = Attestation(
            attestation_id="d1-v3",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="d1-v2",
                update_attestation_id="update-2",
            ),
            output=self._signed_output(
                self.capi_addon,
                k8s_manifests({"kind": "Cluster", "spec": {"version": "1.30"}}),
            ),
        )

        verified = verify_attestation(
            att, bundle, self.trust_store,
            target_identity=self.prod_target,
        )
        self.assertEqual(verified.signer_id, "capi-provisioner")

    def test_input_and_attestation_ids_may_overlap_without_false_cycle(self) -> None:
        prior_input = self._input(self.alice, DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "capi-provisioner",
                    "trust_anchor_id": "fleet-addons",
                    "config": {"version": "1.29"},
                },
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        ))
        shared_update = Attestation(
            attestation_id="shared",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30")'
                    ),
                }),
            ),
        )
        att = Attestation(
            attestation_id="final-overlap",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="shared",
                update_attestation_id="shared",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        bundle = VerificationBundle(
            inputs={"shared": prior_input},
            attestations={"shared": shared_update},
        )

        verified = verify_attestation(
            att, bundle, self.trust_store,
            target_identity=self.prod_target,
        )

        self.assertEqual(verified.signer_id, "capi-provisioner")

    def test_missing_prior_input_in_bundle(self) -> None:
        update_att = Attestation(
            attestation_id="update",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": 'set_path(prior, "x", "y")',
                }),
            ),
        )
        att = Attestation(
            attestation_id="missing-prior",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="nonexistent",
                update_attestation_id="update",
            ),
            output=make_put_manifests(k8s_manifests({"kind": "Cluster"})),
        )
        bundle = VerificationBundle(attestations={"update": update_att})
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, self.trust_store)
        self.assertIn("prior input not found", str(ctx.exception))

    def test_missing_update_attestation_in_bundle(self) -> None:
        prior_input = self._input(self.alice, {"x": 1})
        att = Attestation(
            attestation_id="missing-update",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior",
                update_attestation_id="nonexistent",
            ),
            output=make_put_manifests(k8s_manifests({"kind": "Cluster"})),
        )
        bundle = VerificationBundle(inputs={"prior": prior_input})
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, self.trust_store)
        self.assertIn("update attestation not found", str(ctx.exception))

    def test_derived_input_deployment_id_must_match_prior(self) -> None:
        prior_input = self._input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capi-provisioner",
                        "trust_anchor_id": "fleet-addons",
                        "config": {"version": "1.29"},
                    },
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": 'target.labels.env == "prod"'},
                ),
            ),
        )
        update_att = Attestation(
            attestation_id="update",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30")'
                    ),
                }),
            ),
        )
        att = Attestation(
            attestation_id="deployment-id-mismatch",
            input=DerivedInput(
                deployment_id="deploy-2",
                prior_input_id="prior",
                update_attestation_id="update",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        bundle = VerificationBundle(
            inputs={"prior": prior_input},
            attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, self.trust_store)
        self.assertIn("deployment_id mismatch", str(ctx.exception))
        self.assertIn("input declares 'deploy-2'", str(ctx.exception))

    def test_expired_prior_input_in_derivation(self) -> None:
        expired_input = self._input(
            self.alice,
            DeploymentContent(
                deployment_id="deploy-1",
                manifest_strategy=StrategySpec(
                    type="addon",
                    attributes={
                        "addon_id": "capi-provisioner",
                        "trust_anchor_id": "fleet-addons",
                        "config": {"version": "1.29"},
                    },
                ),
                placement_strategy=StrategySpec(
                    type="predicate",
                    attributes={"expression": 'target.labels.env == "prod"'},
                ),
            ),
            valid_duration_sec=-1,
        )
        update_att = Attestation(
            attestation_id="update",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30")'
                    ),
                }),
            ),
        )
        att = Attestation(
            attestation_id="expired-prior",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior",
                update_attestation_id="update",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        bundle = VerificationBundle(
            inputs={"prior": expired_input},
            attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, self.trust_store)
        self.assertIn("expired", str(ctx.exception))

    def test_expired_update_input_in_derivation(self) -> None:
        prior_input = self._input(self.alice, DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "capi-provisioner",
                    "trust_anchor_id": "fleet-addons",
                    "config": {"version": "1.29"},
                },
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        ))
        update_att = Attestation(
            attestation_id="update",
            input=self._input(
                self.bob, {"type": "request"}, valid_duration_sec=-1,
            ),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30")'
                    ),
                }),
            ),
        )
        att = Attestation(
            attestation_id="expired-update",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior",
                update_attestation_id="update",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        bundle = VerificationBundle(
            inputs={"prior": prior_input},
            attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, self.trust_store)
        self.assertIn("expired", str(ctx.exception))

    def test_update_attestation_fails_own_constraints(self) -> None:
        """Update's output is signed by the wrong addon for its own input constraints."""
        prior_input = self._input(self.alice, DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "capi-provisioner",
                    "trust_anchor_id": "fleet-addons",
                    "config": {"version": "1.29"},
                },
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        ))
        update_att = Attestation(
            attestation_id="update",
            input=self._input(
                self.bob,
                {"type": "request"},
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            output=self._signed_output(
                self.lifecycle_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30")'
                    ),
                }),
            ),
        )
        att = Attestation(
            attestation_id="bad-update",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior",
                update_attestation_id="update",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        bundle = VerificationBundle(
            inputs={"prior": prior_input},
            attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, self.trust_store)
        self.assertIn("output must be signed by upgrade-planner", str(ctx.exception))

    def test_untrusted_prior_signer_deep_in_chain(self) -> None:
        """Chain rooted in an untrusted signer (eve / rogue-idp)."""
        eve = make_identity("eve", "rogue-idp")
        v1_input = self._input(eve, DeploymentContent(
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
        ))

        def _update_att(version: str, att_id: str) -> Attestation:
            return Attestation(
                attestation_id=att_id,
                input=self._input(self.alice, {"type": "request"}),
                output=self._signed_output(
                    self.planner_addon,
                    spec_update_manifest({
                        "derive_input_expression": (
                            f'set_path(prior, "manifest_strategy.config.version", "{version}")'
                        ),
                    }),
                ),
            )

        bundle = VerificationBundle(
            inputs={
                "d1-v1": v1_input,
                "d1-v2": DerivedInput(
                    deployment_id="deploy-1",
                    prior_input_id="d1-v1",
                    update_attestation_id="update-1",
                ),
            },
            attestations={
                "update-1": _update_att("1.29", "update-1"),
                "update-2": _update_att("1.30", "update-2"),
            },
        )

        att = Attestation(
            attestation_id="d1-v3",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="d1-v2",
                update_attestation_id="update-2",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, self.trust_store)
        self.assertIn("trust anchor not found", str(ctx.exception))

    def test_untrusted_update_signer_in_derivation(self) -> None:
        """Update signed by unknown user (eve / rogue-idp)."""
        prior_input = self._input(self.alice, DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "capi-provisioner",
                    "trust_anchor_id": "fleet-addons",
                    "config": {"version": "1.29"},
                },
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        ))
        eve = make_identity("eve", "rogue-idp")
        update_att = Attestation(
            attestation_id="update",
            input=self._input(eve, {"type": "request"}),
            output=make_put_manifests(spec_update_manifest({
                "derive_input_expression": (
                    'set_path(prior, "manifest_strategy.config.version", "9.9.9")'
                ),
            })),
        )
        att = Attestation(
            attestation_id="untrusted-update",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior",
                update_attestation_id="update",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        bundle = VerificationBundle(
            inputs={"prior": prior_input},
            attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, self.trust_store)
        self.assertIn("trust anchor not found", str(ctx.exception))

    # ------------------------------------------------------------------
    # Replay, bypass, and complex graph attacks
    # ------------------------------------------------------------------

    def test_replay_output_from_different_attestation(self) -> None:
        """Addon signed manifests for deployment A. Replayed in B (different constraints)."""
        legit_output = self._signed_output(
            self.capi_addon,
            k8s_manifests({"kind": "Cluster", "metadata": {"namespace": "prod"}}),
        )
        att = Attestation(
            attestation_id="replay",
            input=self._input(
                self.alice,
                {"intent": "staging"},
                output_constraints=(
                    addon_must_sign("capi-provisioner"),
                    namespace_constraint("staging"),
                ),
            ),
            output=legit_output,
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("all manifests must be in namespace staging", str(ctx.exception))

    def test_self_signed_bypass_untrusted(self) -> None:
        """Attacker creates both input and output. Not in any trust store."""
        attacker = make_identity("mallory", "rogue-idp")
        att = Attestation(
            attestation_id="bypass",
            input=self._input(attacker, {"evil": True}),
            output=self._signed_output(
                attacker, k8s_manifests({"kind": "Backdoor"}),
            ),
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, self.empty_bundle, self.trust_store)
        self.assertIn("trust anchor not found", str(ctx.exception))

    def test_valid_derivation_wrong_output_addon(self) -> None:
        """Derivation is valid but final output signed by the wrong addon."""
        prior_input = self._input(self.alice, DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "capi-provisioner",
                    "trust_anchor_id": "fleet-addons",
                    "config": {"version": "1.29"},
                },
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        ))
        update_att = Attestation(
            attestation_id="update",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30")'
                    ),
                }),
            ),
        )
        att = Attestation(
            attestation_id="wrong-output",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior",
                update_attestation_id="update",
            ),
            output=self._signed_output(
                self.lifecycle_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        bundle = VerificationBundle(
            inputs={"prior": prior_input},
            attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(
                att, bundle, self.trust_store,
                target_identity=self.prod_target,
            )
        self.assertIn("manifests must be signed by capi-provisioner", str(ctx.exception))

    def test_mixed_trust_both_anchors_needed(self) -> None:
        """Trust store has users but no addon anchor. Addon signatures fail."""
        ts = TrustStore()
        ts.add(TrustAnchor(
            anchor_id="tenant-idp",
            known_keys={
                "alice": self.alice.keys.public_key_bytes,
                "bob": self.bob.keys.public_key_bytes,
            },
        ))
        prior_input = self._input(self.alice, DeploymentContent(
            deployment_id="deploy-1",
            manifest_strategy=StrategySpec(
                type="addon",
                attributes={
                    "addon_id": "capi-provisioner",
                    "trust_anchor_id": "fleet-addons",
                    "config": {"version": "1.29"},
                },
            ),
            placement_strategy=StrategySpec(
                type="predicate",
                attributes={"expression": 'target.labels.env == "prod"'},
            ),
        ))
        update_att = Attestation(
            attestation_id="update",
            input=self._input(
                self.bob,
                {"type": "request"},
                output_constraints=(addon_must_sign("upgrade-planner"),),
            ),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30")'
                    ),
                }),
            ),
        )
        att = Attestation(
            attestation_id="mixed",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="prior",
                update_attestation_id="update",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        bundle = VerificationBundle(
            inputs={"prior": prior_input},
            attestations={"update": update_att},
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, ts)
        self.assertIn("trust anchor not found", str(ctx.exception))

    def test_chained_middle_update_untrusted(self) -> None:
        """Three-step chain: the second update is signed by an untrusted signer."""
        v1_spec = DeploymentContent(
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
        )
        v1_input = self._input(self.alice, v1_spec)
        update_1 = Attestation(
            attestation_id="update-1",
            input=self._input(self.alice, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                spec_update_manifest({
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.29")'
                    ),
                }),
            ),
        )
        eve = make_identity("eve", "rogue-idp")
        update_2 = Attestation(
            attestation_id="update-2",
            input=self._input(eve, {"type": "request"}),
            output=make_put_manifests(spec_update_manifest({
                "derive_input_expression": (
                    'set_path(prior, "manifest_strategy.config.version", "1.30")'
                ),
            })),
        )
        att = Attestation(
            attestation_id="d1-v3",
            input=DerivedInput(
                deployment_id="deploy-1",
                prior_input_id="d1-v2",
                update_attestation_id="update-2",
            ),
            output=self._signed_output(
                self.capi_addon, k8s_manifests({"kind": "Cluster"}),
            ),
        )
        bundle = VerificationBundle(
            inputs={
                "d1-v1": v1_input,
                "d1-v2": DerivedInput(
                    deployment_id="deploy-1",
                    prior_input_id="d1-v1",
                    update_attestation_id="update-1",
                ),
            },
            attestations={
                "update-1": update_1,
                "update-2": update_2,
            },
        )
        with self.assertRaises(VerificationError) as ctx:
            verify_attestation(att, bundle, self.trust_store)
        self.assertIn("trust anchor not found", str(ctx.exception))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _input(
        self,
        identity: Identity,
        content: object,
        *,
        output_constraints: tuple[OutputConstraint, ...] = (),
        valid_duration_sec: float = 86400,
    ) -> SignedInput:
        return make_signed_input(
            identity.keys,
            identity.key_binding,
            content,
            output_constraints=output_constraints,
            valid_duration_sec=valid_duration_sec,
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

    def _all_details(self, result: VerificationResult) -> str:
        return " | ".join(
            node.detail for node in self._all_nodes(result) if node.detail
        )

    def _all_nodes(self, result: VerificationResult) -> list[VerificationResult]:
        nodes = [result]
        for child in result.children:
            nodes.extend(self._all_nodes(child))
        return nodes


if __name__ == "__main__":
    unittest.main()
