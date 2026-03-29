"""Tests for the hybrid attestation prototype."""

from __future__ import annotations

from dataclasses import dataclass
import json
import unittest

from .build import make_key_binding, make_output, make_signed_input, sign_output
from .crypto import KeyPair, generate_keypair
from .model import (
    Attestation,
    DerivedInput,
    KeyBinding,
    OutputConstraint,
    SignedInput,
    TrustAnchor,
    TrustAnchorConstraint,
)
from .policy import constraint_to_document
from .verify import (
    AttestationStore,
    TrustStore,
    VerificationError,
    VerificationResult,
    explain_verification,
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
        expression=f'output.content.all(m, m.metadata.namespace == "{namespace}")',
    )


def allowed_gvks(*allowed_gvks: str) -> OutputConstraint:
    allowed_literal = json.dumps(list(allowed_gvks))
    return OutputConstraint(
        name=f"only GVKs in {list(allowed_gvks)}",
        expression=(
            f'output.content.all(m, ((m.apiVersion + "/" + m.kind) in {allowed_literal}))'
        ),
    )


def no_cluster_admin() -> OutputConstraint:
    return OutputConstraint(
        name="no ClusterRoleBinding may grant cluster-admin",
        expression=(
            'output.content.all('
            'm, !(m.kind == "ClusterRoleBinding" && m.roleRef.name == "cluster-admin")'
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

        self.attestation_store = AttestationStore()
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

    def test_direct_attestation_with_signed_cel_constraints_verifies(self) -> None:
        manifests = [
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "web", "namespace": "prod"},
            }
        ]
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
            output=make_output(manifests),
        )
        self.attestation_store.add(attestation)

        verified = verify_attestation(
            attestation,
            self.attestation_store,
            self.trust_store,
        )

        self.assertEqual(verified.content, manifests)
        self.assertIsNone(verified.signer_id)

    def test_addon_signed_output_with_explicit_cel_policy_verifies(self) -> None:
        spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "trust_anchor": "fleet-addons",
            }
        }
        manifests = [
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
            }
        ]
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
        self.attestation_store.add(attestation)

        verified = verify_attestation(
            attestation,
            self.attestation_store,
            self.trust_store,
        )

        self.assertEqual(verified.signer_id, "capi-provisioner")

    def test_derived_input_uses_signed_cel_update_and_constraints(self) -> None:
        prior_spec = {
            "deployment_id": "cluster-01",
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "trust_anchor": "fleet-addons",
                "config": {"version": "1.29.5"},
            },
        }
        prior_output = [
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.29.5"}},
            }
        ]
        prior_attestation = Attestation(
            attestation_id="d1-v1",
            input=self._input(
                self.alice,
                prior_spec,
                output_constraints=(addon_must_sign("capi-provisioner"),),
            ),
            output=self._signed_output(self.capi_addon, prior_output),
        )

        update_request = {"type": "request", "capability": "upgrade-planner"}
        update_directive = {
            "type": "spec_update",
            "derive_input_expression": (
                'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
            ),
            "output_constraints": [
                constraint_to_document(addon_must_sign("capi-provisioner")),
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
            output=self._signed_output(self.planner_addon, update_directive),
        )

        final_output = [
            {
                "apiVersion": "cluster.x-k8s.io/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": "workload-01", "namespace": "capi-system"},
                "spec": {"topology": {"version": "1.30.2"}},
            }
        ]
        final_attestation = Attestation(
            attestation_id="d1-v2",
            input=DerivedInput(
                prior_attestation_id="d1-v1",
                update_attestation_id="upgrade-1",
            ),
            output=self._signed_output(self.capi_addon, final_output),
        )

        for attestation in (prior_attestation, update_attestation, final_attestation):
            self.attestation_store.add(attestation)

        verified = verify_attestation(
            final_attestation,
            self.attestation_store,
            self.trust_store,
        )
        explanation = explain_verification(
            final_attestation,
            self.attestation_store,
            self.trust_store,
        )

        self.assertEqual(verified.signer_id, "capi-provisioner")
        self.assertEqual(
            final_output[0]["spec"]["topology"]["version"],
            "1.30.2",
        )
        self.assertIn(
            "derived from prior=d1-v1 + update=upgrade-1",
            self._all_details(explanation),
        )
        self.assertIn("upgrade-planner", self._all_details(explanation))

    def test_derived_input_falls_back_to_spec_derived_constraints(self) -> None:
        prior_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "trust_anchor": "fleet-addons",
                "config": {"version": "1.29.5"},
            }
        }
        prior_attestation = Attestation(
            attestation_id="prior",
            input=self._input(self.alice, prior_spec),
            output=self._signed_output(self.capi_addon, [{"kind": "Cluster"}]),
        )
        update_attestation = Attestation(
            attestation_id="update",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                {
                    "type": "spec_update",
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
                    ),
                },
            ),
        )
        final_attestation = Attestation(
            attestation_id="final",
            input=DerivedInput(
                prior_attestation_id="prior",
                update_attestation_id="update",
            ),
            output=self._signed_output(self.capi_addon, [{"kind": "Cluster"}]),
        )

        for attestation in (prior_attestation, update_attestation, final_attestation):
            self.attestation_store.add(attestation)

        verified = verify_attestation(
            final_attestation,
            self.attestation_store,
            self.trust_store,
        )

        self.assertEqual(verified.signer_id, "capi-provisioner")

    def test_bad_update_expression_fails_derivation(self) -> None:
        prior_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "trust_anchor": "fleet-addons",
            }
        }
        prior_attestation = Attestation(
            attestation_id="prior-bad-update",
            input=self._input(self.alice, prior_spec),
            output=self._signed_output(self.capi_addon, [{"kind": "Cluster"}]),
        )
        update_attestation = Attestation(
            attestation_id="update-bad-expression",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                {
                    "type": "spec_update",
                    "derive_input_expression": "1 + 2",
                },
            ),
        )
        final_attestation = Attestation(
            attestation_id="final-bad-expression",
            input=DerivedInput(
                prior_attestation_id="prior-bad-update",
                update_attestation_id="update-bad-expression",
            ),
            output=self._signed_output(self.capi_addon, [{"kind": "Cluster"}]),
        )

        for attestation in (prior_attestation, update_attestation, final_attestation):
            self.attestation_store.add(attestation)

        with self.assertRaises(VerificationError) as context:
            verify_attestation(
                final_attestation,
                self.attestation_store,
                self.trust_store,
            )

        self.assertIn("derive_input_expression must return an object", str(context.exception))

    def test_wrong_output_signer_fails_against_derived_constraint(self) -> None:
        prior_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "trust_anchor": "fleet-addons",
                "config": {"version": "1.29.5"},
            }
        }
        prior_attestation = Attestation(
            attestation_id="prior-wrong",
            input=self._input(self.alice, prior_spec),
            output=self._signed_output(self.capi_addon, [{"kind": "Cluster"}]),
        )
        update_attestation = Attestation(
            attestation_id="update-wrong",
            input=self._input(self.bob, {"type": "request"}),
            output=self._signed_output(
                self.planner_addon,
                {
                    "type": "spec_update",
                    "derive_input_expression": (
                        'set_path(prior, "manifest_strategy.config.version", "1.30.2")'
                    ),
                },
            ),
        )
        final_attestation = Attestation(
            attestation_id="final-wrong",
            input=DerivedInput(
                prior_attestation_id="prior-wrong",
                update_attestation_id="update-wrong",
            ),
            output=self._signed_output(self.lifecycle_addon, [{"kind": "Cluster"}]),
        )

        for attestation in (prior_attestation, update_attestation, final_attestation):
            self.attestation_store.add(attestation)

        with self.assertRaises(VerificationError) as context:
            verify_attestation(
                final_attestation,
                self.attestation_store,
                self.trust_store,
            )

        self.assertIn("output must be signed by capi-provisioner", str(context.exception))

    def test_expired_signed_input_fails(self) -> None:
        attestation = Attestation(
            attestation_id="expired",
            input=self._input(
                self.alice,
                {"intent": "too-late"},
                valid_duration_sec=-1,
            ),
            output=make_output({"ignored": True}),
        )
        self.attestation_store.add(attestation)

        with self.assertRaises(VerificationError) as context:
            verify_attestation(
                attestation,
                self.attestation_store,
                self.trust_store,
            )

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
            output=make_output([{"kind": "ConfigMap"}]),
        )
        self.attestation_store.add(attestation)

        with self.assertRaises(VerificationError) as context:
            verify_attestation(
                attestation,
                self.attestation_store,
                self.trust_store,
            )

        self.assertIn("signed input hash mismatch", str(context.exception))

    def test_failed_cel_constraint_surfaces_constraint_name(self) -> None:
        manifests = [
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRoleBinding",
                "metadata": {"name": "evil", "namespace": "prod"},
                "roleRef": {"name": "cluster-admin"},
            }
        ]
        attestation = Attestation(
            attestation_id="bad-rbac",
            input=self._input(
                self.alice,
                {"intent": "deploy-rbac"},
                output_constraints=(no_cluster_admin(),),
            ),
            output=make_output(manifests),
        )
        self.attestation_store.add(attestation)

        with self.assertRaises(VerificationError) as context:
            verify_attestation(
                attestation,
                self.attestation_store,
                self.trust_store,
            )

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

        attestation = Attestation(
            attestation_id="tenant-match",
            input=self._input(
                tenant_alice,
                {"tenant": "tenant-a", "intent": "deploy-web"},
            ),
            output=make_output([{"kind": "ConfigMap"}]),
        )
        self.attestation_store.add(attestation)

        verified = verify_attestation(
            attestation,
            self.attestation_store,
            trust_store,
        )

        self.assertEqual(verified.content, [{"kind": "ConfigMap"}])

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
            output=make_output([{"kind": "ConfigMap"}]),
        )
        self.attestation_store.add(attestation)

        with self.assertRaises(VerificationError) as context:
            verify_attestation(
                attestation,
                self.attestation_store,
                trust_store,
            )

        self.assertIn("input tenant must match anchor tenant", str(context.exception))

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

    def _signed_output(self, identity: Identity, content: object):
        return sign_output(
            identity.keys,
            identity.signer_id,
            identity.trust_anchor_id,
            content,
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
