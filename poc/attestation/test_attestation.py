"""
Tests for the Attestation (input + output) model.

Every attestation is the same shape: input + output.  Inputs carry
constraints; outputs don't.  DerivedInput is a kind of input, not
a kind of attestation.

Scenarios:
  1.  SignedInput verification (standalone)
  2.  Direct signing (user signs manifests, unsigned output)
  3.  Addon signing (user spec, addon-signed output)
  4.  Constraint violations (wrong addon, namespace, cluster-admin)
  5.  CEL update inline (DerivedInput, unsigned output)
  6.  CEL update tampered (derivation fails)
  7.  CEL update + addon (DerivedInput, addon-signed output)
  8.  Addon produces patch (planner addon as update source)
  9.  Constraint propagation through derivation
  10. Hard case: D1v1 + D2 -> D1v2 (cross-deployment update)
  11. Chained updates (D1v1 -> D1v2 -> D1v3)
  12. Sub-attestation failures propagate
  13. Signature failures (expired, forged, untrusted, tampered)
  14. Output signature failures
  15. Multiple constraints (GVK + namespace)

Run: pytest test_attestation.py -v
"""

from __future__ import annotations

import copy
import time
from dataclasses import dataclass

import pytest

from attestation import (
    Attestation,
    DerivedInput,
    KeyBinding,
    KeyPair,
    Output,
    OutputConstraint,
    SignedInput,
    TrustAnchor,
    TrustStore,
    VerificationError,
    VerifiedOutput,
    content_hash,
    generate_keypair,
    make_key_binding,
    make_output,
    make_signed_input,
    sign,
    sign_output,
)


# ---------------------------------------------------------------------------
# Test identity helper
# ---------------------------------------------------------------------------

@dataclass
class Identity:
    signer_id: str
    keys: KeyPair
    key_binding: KeyBinding


def make_identity(signer_id: str, trust_anchor_id: str) -> Identity:
    keys = generate_keypair()
    kb = make_key_binding(keys, signer_id, trust_anchor_id)
    return Identity(signer_id=signer_id, keys=keys, key_binding=kb)


def _input(identity: Identity, content, **kwargs) -> SignedInput:
    return make_signed_input(identity.keys, identity.key_binding, content, **kwargs)


def _output(identity: Identity, content) -> Output:
    return sign_output(identity.keys, identity.key_binding, content)


# ---------------------------------------------------------------------------
# Output constraint helpers
# ---------------------------------------------------------------------------

def addon_must_sign(expected_addon_id: str) -> OutputConstraint:
    return OutputConstraint(
        description=f"output must be signed by {expected_addon_id}",
        check=lambda _auth, out: out.signer_id == expected_addon_id,
    )


def namespace_constraint(ns: str) -> OutputConstraint:
    return OutputConstraint(
        description=f"all manifests must be in namespace '{ns}'",
        check=lambda _auth, out: all(
            m.get("metadata", {}).get("namespace") == ns
            for m in out.content
        ),
    )


def no_cluster_admin() -> OutputConstraint:
    return OutputConstraint(
        description="no ClusterRoleBinding granting cluster-admin",
        check=lambda _auth, out: all(
            not (
                m.get("kind") == "ClusterRoleBinding"
                and m.get("roleRef", {}).get("name") == "cluster-admin"
            )
            for m in out.content
        ),
    )


def allowed_gvks(allowed: set[str]) -> OutputConstraint:
    def check(_auth, out: VerifiedOutput) -> bool:
        for m in out.content:
            gvk = f"{m.get('apiVersion', '')}/{m.get('kind', '')}"
            if gvk not in allowed:
                return False
        return True

    return OutputConstraint(description=f"only GVKs in {allowed}", check=check)


# ---------------------------------------------------------------------------
# Derivation helpers
# ---------------------------------------------------------------------------

def cel_set_version(version: str):
    """Return a function that sets manifest_strategy.config.version."""
    def transform(input_spec: dict) -> dict:
        out = copy.deepcopy(input_spec)
        out.setdefault("manifest_strategy", {}).setdefault("config", {})["version"] = version
        return out
    return transform


def cel_upgrade_apply(prior_content, update_content):
    """Apply a CEL-like mutation: update_content specifies the new version."""
    version = update_content.get("new_version")
    if not version:
        raise ValueError("update_content must specify new_version")
    new_spec = cel_set_version(version)(prior_content)
    constraints = _derive_constraints(new_spec)
    return new_spec, constraints


def _derive_constraints(spec: dict) -> list[OutputConstraint]:
    """Domain logic: derive output constraints implied by a spec."""
    constraints: list[OutputConstraint] = []
    ms = spec.get("manifest_strategy", {})
    if ms.get("type") == "addon":
        constraints.append(addon_must_sign(ms["addon_id"]))
    return constraints


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def alice():
    return make_identity("alice", "tenant-idp")


@pytest.fixture
def bob():
    return make_identity("bob", "tenant-idp")


@pytest.fixture
def capi_addon():
    return make_identity("capi-provisioner", "fleet-addons")


@pytest.fixture
def lifecycle_addon():
    return make_identity("cluster-lifecycle", "fleet-addons")


@pytest.fixture
def planner_addon():
    return make_identity("upgrade-planner", "fleet-addons")


@pytest.fixture
def trust_store(alice, bob, capi_addon, lifecycle_addon, planner_addon):
    ts = TrustStore()
    ts.add(TrustAnchor(
        anchor_id="tenant-idp",
        known_keys={
            "alice": alice.keys.public_key_bytes,
            "bob": bob.keys.public_key_bytes,
        },
    ))
    ts.add(TrustAnchor(
        anchor_id="fleet-addons",
        known_keys={
            "capi-provisioner": capi_addon.keys.public_key_bytes,
            "cluster-lifecycle": lifecycle_addon.keys.public_key_bytes,
            "upgrade-planner": planner_addon.keys.public_key_bytes,
        },
    ))
    return ts


# ===================================================================
# 1. SignedInput verification
# ===================================================================

class TestSignedInput:
    def test_valid(self, alice, trust_store):
        si = _input(alice, {"version": "1.29"})
        result = si.verify(trust_store)
        assert result.content == {"version": "1.29"}
        assert result.signer_id == "alice"
        assert result.output_constraints == []

    def test_with_constraints(self, alice, trust_store):
        si = _input(
            alice, {"manifest_strategy": {"type": "addon"}},
            output_constraints=[addon_must_sign("capi-provisioner")],
        )
        result = si.verify(trust_store)
        assert len(result.output_constraints) == 1


# ===================================================================
# 2. Direct signing (unsigned output)
# ===================================================================

class TestDirectSigning:
    def test_valid(self, alice, trust_store):
        manifests = [
            {"apiVersion": "v1", "kind": "Secret",
             "metadata": {"name": "db-creds", "namespace": "production"},
             "data": {"password": "c2VjcmV0"}},
        ]
        att = Attestation(
            input=_input(alice, manifests),
            output=make_output(manifests),
        )
        result = att.verify(trust_store)
        assert result.content == manifests
        assert result.signer_id is None

    def test_with_self_constraints(self, alice, trust_store):
        manifests = [
            {"kind": "Deployment",
             "metadata": {"name": "web", "namespace": "prod"}},
        ]
        att = Attestation(
            input=_input(
                alice, manifests,
                output_constraints=[namespace_constraint("prod")],
            ),
            output=make_output(manifests),
        )
        result = att.verify(trust_store)
        assert result.content == manifests


# ===================================================================
# 3. Addon signing (signed output)
# ===================================================================

class TestAddonSigning:
    def test_valid(self, alice, capi_addon, trust_store):
        spec = {
            "manifest_strategy": {
                "type": "addon", "addon_id": "capi-provisioner",
            },
            "placement": {"selector": {"pool": "management"}},
        }
        manifests = [
            {"apiVersion": "cluster.x-k8s.io/v1beta1", "kind": "Cluster",
             "metadata": {"name": "workload-01", "namespace": "capi-system"},
             "spec": {"topology": {"version": "v1.29.5"}}},
        ]

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[
                    addon_must_sign("capi-provisioner"),
                    no_cluster_admin(),
                ],
            ),
            output=_output(capi_addon, manifests),
        )
        result = att.verify(trust_store)
        assert result.content == manifests
        assert result.signer_id == "capi-provisioner"


# ===================================================================
# 4. Constraint violations
# ===================================================================

class TestConstraintViolations:
    def test_unauthorized_addon(self, alice, lifecycle_addon, trust_store):
        spec = {
            "manifest_strategy": {
                "type": "addon", "addon_id": "capi-provisioner",
            },
        }
        manifests = [{"kind": "ConfigMap", "metadata": {"name": "x"}}]

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=_output(lifecycle_addon, manifests),
        )
        with pytest.raises(
            VerificationError,
            match="output constraint failed.*signed by capi-provisioner",
        ):
            att.verify(trust_store)

    def test_namespace_violation(self, alice, capi_addon, trust_store):
        spec = {"manifest_strategy": {"type": "addon"}}
        wrong_ns = [
            {"kind": "Deployment",
             "metadata": {"name": "web", "namespace": "kube-system"}},
        ]

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[
                    addon_must_sign("capi-provisioner"),
                    namespace_constraint("production"),
                ],
            ),
            output=_output(capi_addon, wrong_ns),
        )
        with pytest.raises(VerificationError, match="namespace"):
            att.verify(trust_store)

    def test_cluster_admin_blocked(self, alice, capi_addon, trust_store):
        spec = {"manifest_strategy": {"type": "addon"}}
        evil = [
            {"apiVersion": "rbac.authorization.k8s.io/v1",
             "kind": "ClusterRoleBinding",
             "metadata": {"name": "evil"},
             "roleRef": {"name": "cluster-admin"}},
        ]

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[
                    addon_must_sign("capi-provisioner"),
                    no_cluster_admin(),
                ],
            ),
            output=_output(capi_addon, evil),
        )
        with pytest.raises(VerificationError, match="cluster-admin"):
            att.verify(trust_store)


# ===================================================================
# 5. CEL update inline (DerivedInput, unsigned output)
# ===================================================================

class TestCelUpdateInline:
    def test_valid(self, alice, trust_store):
        old_spec = {
            "deployment_id": "cluster-deploy-007",
            "manifest_strategy": {
                "type": "inline",
                "config": {"version": "1.29.5", "region": "us-east-1"},
            },
        }
        update_content = {
            "type": "update",
            "cel_expression": 'spec.manifest_strategy.config.version = "1.30.2"',
            "new_version": "1.30.2",
        }

        def inline_apply(prior, update):
            return cel_set_version(update["new_version"])(prior), []

        new_spec = cel_set_version("1.30.2")(old_spec)

        att = Attestation(
            input=DerivedInput(
                prior=_input(alice, old_spec),
                update=Attestation(
                    input=_input(alice, update_content),
                    output=make_output(update_content),
                ),
                apply=inline_apply,
            ),
            output=make_output(new_spec),
        )
        result = att.verify(trust_store)
        assert result.content["manifest_strategy"]["config"]["version"] == "1.30.2"
        assert result.content["manifest_strategy"]["config"]["region"] == "us-east-1"


# ===================================================================
# 6. CEL update tampered
# ===================================================================

class TestCelUpdateTampered:
    def test_derivation_mismatch_detected(self, alice, trust_store):
        old_spec = {
            "manifest_strategy": {
                "type": "inline",
                "config": {"version": "1.29.5"},
            },
        }
        update_content = {"type": "update", "new_version": "1.30.2"}

        def tampered_apply(prior, update):
            raise ValueError("mutation does not match signed expression")

        att = Attestation(
            input=DerivedInput(
                prior=_input(alice, old_spec),
                update=Attestation(
                    input=_input(alice, update_content),
                    output=make_output(update_content),
                ),
                apply=tampered_apply,
            ),
            output=make_output({"whatever": True}),
        )
        with pytest.raises(VerificationError, match="derivation failed"):
            att.verify(trust_store)


# ===================================================================
# 7. CEL update + addon (DerivedInput, addon-signed output)
# ===================================================================

class TestCelUpdateWithAddon:
    def test_valid(self, alice, capi_addon, trust_store):
        old_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        update_content = {
            "type": "update",
            "cel_expression": 'spec.config.version = "1.30.2"',
            "new_version": "1.30.2",
        }
        manifests_v2 = [
            {"apiVersion": "cluster.x-k8s.io/v1beta1", "kind": "Cluster",
             "metadata": {"name": "workload-010", "namespace": "capi-system"},
             "spec": {"topology": {"version": "v1.30.2"}}},
        ]

        att = Attestation(
            input=DerivedInput(
                prior=_input(
                    alice, old_spec,
                    output_constraints=[addon_must_sign("capi-provisioner")],
                ),
                update=Attestation(
                    input=_input(alice, update_content),
                    output=make_output(update_content),
                ),
                apply=cel_upgrade_apply,
            ),
            output=_output(capi_addon, manifests_v2),
        )

        result = att.verify(trust_store)
        assert result.content == manifests_v2
        assert result.signer_id == "capi-provisioner"


# ===================================================================
# 8. Addon produces patch (planner addon as update source)
# ===================================================================

class TestAddonProducesPatch:
    def test_valid(self, alice, planner_addon, trust_store):
        old_spec = {
            "deployment_id": "cluster-deploy-020",
            "manifest_strategy": {
                "type": "inline",
                "config": {"version": "1.29.5", "region": "us-east-1"},
            },
        }
        intent = {
            "type": "update",
            "capability": "upgrade-planner",
        }
        patch = {
            "type": "version_patch",
            "target_field": "manifest_strategy.config.version",
            "new_version": "1.30.2",
        }

        def apply_patch(prior, update):
            new = cel_set_version(update["new_version"])(prior)
            return new, []

        new_spec = cel_set_version("1.30.2")(old_spec)

        att = Attestation(
            input=DerivedInput(
                prior=_input(alice, old_spec),
                update=Attestation(
                    input=_input(
                        alice, intent,
                        output_constraints=[addon_must_sign("upgrade-planner")],
                    ),
                    output=_output(planner_addon, patch),
                ),
                apply=apply_patch,
            ),
            output=make_output(new_spec),
        )
        result = att.verify(trust_store)
        assert result.content["manifest_strategy"]["config"]["version"] == "1.30.2"
        assert result.content["manifest_strategy"]["config"]["region"] == "us-east-1"

    def test_wrong_planner(self, alice, lifecycle_addon, trust_store):
        """User expects the upgrade-planner but a different addon signs."""
        old_spec = {"manifest_strategy": {"config": {"version": "1.29.5"}}}
        intent = {"type": "update", "capability": "upgrade-planner"}
        patch = {"type": "version_patch", "new_version": "1.30.2"}

        def apply_patch(prior, update):
            return cel_set_version(update["new_version"])(prior), []

        att = Attestation(
            input=DerivedInput(
                prior=_input(alice, old_spec),
                update=Attestation(
                    input=_input(
                        alice, intent,
                        output_constraints=[addon_must_sign("upgrade-planner")],
                    ),
                    output=_output(lifecycle_addon, patch),
                ),
                apply=apply_patch,
            ),
            output=make_output({"whatever": True}),
        )
        with pytest.raises(
            VerificationError,
            match="output constraint failed.*upgrade-planner",
        ):
            att.verify(trust_store)


# ===================================================================
# 9. Constraint propagation through derivation
# ===================================================================

class TestConstraintPropagation:
    def test_update_changes_addon_constraint(
        self, alice, capi_addon, lifecycle_addon, trust_store,
    ):
        """Update changes the addon; the derived constraint must follow."""
        old_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }

        update_content = {"type": "update", "new_version": "1.30.2"}

        def upgrade_and_switch_addon(prior, update):
            new = cel_set_version(update["new_version"])(prior)
            new["manifest_strategy"]["addon_id"] = "cluster-lifecycle"
            return new, _derive_constraints(new)

        derived_input = DerivedInput(
            prior=_input(
                alice, old_spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            update=Attestation(
                input=_input(alice, update_content),
                output=make_output(update_content),
            ),
            apply=upgrade_and_switch_addon,
        )

        manifests_v2 = [{"kind": "SomeNewResource"}]

        wrong = Attestation(input=derived_input, output=_output(capi_addon, manifests_v2))
        with pytest.raises(
            VerificationError,
            match="output constraint failed.*cluster-lifecycle",
        ):
            wrong.verify(trust_store)

        right = Attestation(input=derived_input, output=_output(lifecycle_addon, manifests_v2))
        result = right.verify(trust_store)
        assert result.content == manifests_v2
        assert result.signer_id == "cluster-lifecycle"


# ===================================================================
# 10. Hard case: D1v1 + D2 -> D1v2 (cross-deployment update)
# ===================================================================

class TestCrossDeploymentUpdate:
    """
    D1v1: UserA spec -> AddonX manifests_v1
    D2:   UserB spec -> AddonY update_instruction
    D1v2: DerivedInput(prior=D1_input, update=D2) -> AddonX manifests_v2

    D2 is a full Attestation used as the update.  Its verified
    output (the update instruction) feeds into the derivation.
    """

    def test_full_scenario(
        self, alice, bob, capi_addon, lifecycle_addon, trust_store,
    ):
        d1_spec = {
            "deployment_id": "dep-001",
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
            "placement": {"selector": {"pool": "production"}},
        }
        manifests_v1 = [
            {"apiVersion": "cluster.x-k8s.io/v1beta1", "kind": "Cluster",
             "metadata": {"name": "prod-001", "namespace": "capi-system"},
             "spec": {"topology": {"version": "v1.29.5"}}},
        ]

        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )
        d1v1 = Attestation(input=d1_input, output=_output(capi_addon, manifests_v1))

        d2_spec = {
            "type": "fleet-update",
            "description": "upgrade all production clusters to 1.30.2",
            "target_selector": {"labels": {"pool": "production"}},
        }
        update_instruction = {
            "type": "deployment-update",
            "new_version": "1.30.2",
            "target_selector": {"labels": {"pool": "production"}},
        }

        d2 = Attestation(
            input=_input(
                bob, d2_spec,
                output_constraints=[addon_must_sign("cluster-lifecycle")],
            ),
            output=_output(lifecycle_addon, update_instruction),
        )

        manifests_v2 = [
            {"apiVersion": "cluster.x-k8s.io/v1beta1", "kind": "Cluster",
             "metadata": {"name": "prod-001", "namespace": "capi-system"},
             "spec": {"topology": {"version": "v1.30.2"}}},
        ]

        d1v2 = Attestation(
            input=DerivedInput(
                prior=d1_input,
                update=d2,
                apply=cel_upgrade_apply,
            ),
            output=_output(capi_addon, manifests_v2),
        )

        result = d1v2.verify(trust_store)
        assert result.content == manifests_v2
        assert result.signer_id == "capi-provisioner"

    def test_d1v1_verifies_independently(
        self, alice, capi_addon, trust_store,
    ):
        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        manifests_v1 = [{"kind": "Cluster"}]

        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )
        d1v1 = Attestation(input=d1_input, output=_output(capi_addon, manifests_v1))

        result = d1v1.verify(trust_store)
        assert result.content == manifests_v1
        assert result.signer_id == "capi-provisioner"


# ===================================================================
# 11. Chained updates (D1v1 -> D1v2 -> D1v3)
# ===================================================================

class TestChainedUpdates:
    def test_three_version_chain(self, alice, capi_addon, trust_store):
        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.28"},
            },
        }

        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )

        def make_update_att(version):
            content = {"type": "update", "new_version": version}
            return Attestation(
                input=_input(alice, content),
                output=make_output(content),
            )

        # v2: 1.28 -> 1.29
        d1v2_input = DerivedInput(
            prior=d1_input,
            update=make_update_att("1.29"),
            apply=cel_upgrade_apply,
        )

        # v3: 1.29 -> 1.30 (prior is the previous DerivedInput)
        d1v3_input = DerivedInput(
            prior=d1v2_input,
            update=make_update_att("1.30"),
            apply=cel_upgrade_apply,
        )

        manifests_v3 = [{"kind": "Cluster", "spec": {"version": "1.30"}}]
        d1v3 = Attestation(
            input=d1v3_input,
            output=_output(capi_addon, manifests_v3),
        )

        result = d1v3.verify(trust_store)
        assert result.content == manifests_v3
        assert result.signer_id == "capi-provisioner"


# ===================================================================
# 12. Sub-attestation failures propagate
# ===================================================================

class TestSubAttestationFailures:
    def test_forged_addon_in_update(
        self, alice, bob, capi_addon, lifecycle_addon, trust_store,
    ):
        """Forged addon signature in D2 causes D1v2 verification to fail."""
        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )

        forger = make_identity("cluster-lifecycle", "fleet-addons")
        update = {"type": "deployment-update", "new_version": "1.30.2"}
        d2 = Attestation(
            input=_input(
                bob, {"type": "fleet-update"},
                output_constraints=[addon_must_sign("cluster-lifecycle")],
            ),
            output=_output(forger, update),
        )

        manifests_v2 = [{"kind": "Cluster"}]
        d1v2 = Attestation(
            input=DerivedInput(prior=d1_input, update=d2, apply=cel_upgrade_apply),
            output=_output(capi_addon, manifests_v2),
        )

        with pytest.raises(VerificationError, match="key not recognised"):
            d1v2.verify(trust_store)

    def test_untrusted_update_signer(self, alice, capi_addon, trust_store):
        """Update signed by unknown user -> fails."""
        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )

        eve = make_identity("eve", "rogue-idp")
        update = {"type": "deployment-update", "new_version": "9.9.9"}

        d1v2 = Attestation(
            input=DerivedInput(
                prior=d1_input,
                update=Attestation(
                    input=_input(eve, update),
                    output=make_output(update),
                ),
                apply=cel_upgrade_apply,
            ),
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )

        with pytest.raises(VerificationError, match="trust anchor not found"):
            d1v2.verify(trust_store)


# ===================================================================
# 13. Signature failures
# ===================================================================

class TestSignatureFailures:
    def test_expired(self, alice, trust_store):
        si = _input(alice, {"x": 1}, valid_duration_sec=-1)
        with pytest.raises(VerificationError, match="expired"):
            si.verify(trust_store)

    def test_forged(self, alice, trust_store):
        si = _input(alice, {"x": 1})
        forger_keys = generate_keypair()
        forger_kb = make_key_binding(forger_keys, "alice", "tenant-idp")
        forged = make_signed_input(forger_keys, forger_kb, {"x": 1})

        tampered = SignedInput(
            content=si.content,
            signer_id=si.signer_id,
            public_key=si.public_key,
            signature=forged.signature,
            valid_until=si.valid_until,
            key_binding=si.key_binding,
            output_constraints=si.output_constraints,
        )
        with pytest.raises(VerificationError, match="signature verification failed"):
            tampered.verify(trust_store)

    def test_untrusted_signer(self, alice):
        empty_store = TrustStore()
        si = _input(alice, {"x": 1})
        with pytest.raises(VerificationError, match="trust anchor not found"):
            si.verify(empty_store)

    def test_key_not_in_anchor(self, trust_store):
        unknown = make_identity("eve", "tenant-idp")
        si = _input(unknown, {"x": 1})
        with pytest.raises(VerificationError, match="key not recognised"):
            si.verify(trust_store)

    def test_tampered_valid_until(self, alice, trust_store):
        si = _input(alice, {"x": 1}, valid_duration_sec=-1)
        tampered = SignedInput(
            content=si.content,
            signer_id=si.signer_id,
            public_key=si.public_key,
            signature=si.signature,
            valid_until=time.time() + 86400,
            key_binding=si.key_binding,
            output_constraints=si.output_constraints,
        )
        with pytest.raises(VerificationError, match="signature verification failed"):
            tampered.verify(trust_store)

    def test_tampered_constraints(self, alice, trust_store):
        si = _input(
            alice, {"x": 1},
            output_constraints=[namespace_constraint("prod")],
        )
        tampered = SignedInput(
            content=si.content,
            signer_id=si.signer_id,
            public_key=si.public_key,
            signature=si.signature,
            valid_until=si.valid_until,
            key_binding=si.key_binding,
            output_constraints=[],
        )
        with pytest.raises(VerificationError, match="signature verification failed"):
            tampered.verify(trust_store)

    def test_expiry_tampered_in_attestation(self, alice, trust_store):
        """Platform extends valid_until after signing. Signature breaks."""
        si = _input(alice, [{"kind": "ConfigMap"}], valid_duration_sec=-1)
        tampered = SignedInput(
            content=si.content,
            signer_id=si.signer_id,
            public_key=si.public_key,
            signature=si.signature,
            valid_until=time.time() + 86400,
            key_binding=si.key_binding,
            output_constraints=si.output_constraints,
        )
        att = Attestation(
            input=tampered,
            output=make_output([{"kind": "ConfigMap"}]),
        )
        with pytest.raises(VerificationError, match="signature verification failed"):
            att.verify(trust_store)


# ===================================================================
# 14. Output signature failures
# ===================================================================

class TestOutputSignatureFailures:
    def test_forged_output(self, alice, capi_addon, trust_store):
        spec = {"manifest_strategy": {"type": "addon"}}
        manifests = [{"kind": "Cluster"}]

        forger = make_identity("capi-provisioner", "fleet-addons")
        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=_output(forger, manifests),
        )
        with pytest.raises(VerificationError, match="key not recognised"):
            att.verify(trust_store)

    def test_tampered_output_content(self, alice, capi_addon, trust_store):
        spec = {"manifest_strategy": {"type": "addon"}}
        signed = _output(capi_addon, [{"kind": "Cluster"}])

        tampered_output = Output(
            content=[{"kind": "ClusterRoleBinding",
                      "roleRef": {"name": "cluster-admin"}}],
            signer_id=signed.signer_id,
            public_key=signed.public_key,
            signature=signed.signature,
            key_binding=signed.key_binding,
        )
        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=tampered_output,
        )
        with pytest.raises(VerificationError, match="output signature verification failed"):
            att.verify(trust_store)


# ===================================================================
# 15. Multiple constraints (GVK + namespace)
# ===================================================================

class TestMultipleConstraints:
    def test_all_constraints_pass(self, alice, trust_store):
        manifests = [
            {"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "web", "namespace": "prod"}},
        ]

        att = Attestation(
            input=_input(
                alice, manifests,
                output_constraints=[
                    namespace_constraint("prod"),
                    allowed_gvks({"apps/v1/Deployment", "v1/Service"}),
                ],
            ),
            output=make_output(manifests),
        )
        result = att.verify(trust_store)
        assert result.content == manifests

    def test_gvk_violation(self, alice, trust_store):
        manifests = [
            {"apiVersion": "apps/v1", "kind": "DaemonSet",
             "metadata": {"name": "agent", "namespace": "prod"}},
        ]

        att = Attestation(
            input=_input(
                alice, manifests,
                output_constraints=[
                    allowed_gvks({"apps/v1/Deployment", "v1/Service"}),
                ],
            ),
            output=make_output(manifests),
        )
        with pytest.raises(VerificationError, match="output constraint failed"):
            att.verify(trust_store)


# ===================================================================
# 16. Adversarial: untrusted output through tampering / spoofing
# ===================================================================

class TestAdversarialOutputSpoofing:
    """Attacks that try to produce trusted-looking output without authority."""

    def test_unsigned_output_with_spoofed_signer_id(
        self, alice, trust_store,
    ):
        """Output claims a signer_id but has no signature.

        Without the fix, signer_id passes through to VerifiedOutput
        and addon_must_sign would accept it.
        """
        spec = {"manifest_strategy": {"type": "addon"}}
        evil = [{"kind": "Cluster", "spec": {"backdoor": True}}]

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=Output(content=evil, signer_id="capi-provisioner"),
        )
        with pytest.raises(VerificationError, match="output constraint failed"):
            att.verify(trust_store)

    def test_signed_output_without_key_binding(
        self, alice, trust_store,
    ):
        """Attacker signs output with own key, claims addon signer_id,
        but provides no key binding.  Signature is valid against the
        attacker's key, but identity is unproven.
        """
        spec = {"manifest_strategy": {"type": "addon"}}
        evil = [{"kind": "Cluster"}]

        attacker = generate_keypair()
        output_hash = content_hash(evil)
        sig = sign(attacker.private_key, output_hash)

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=Output(
                content=evil,
                signer_id="capi-provisioner",
                public_key=attacker.public_key_bytes,
                signature=sig,
                key_binding=None,
            ),
        )
        with pytest.raises(VerificationError, match="output constraint failed"):
            att.verify(trust_store)


class TestAdversarialKeyBindingAttacks:
    """Attacks that forge or misuse key bindings."""

    def test_key_binding_signer_mismatch(self, trust_store):
        """Alice's key binding used to sign as bob."""
        alice = make_identity("alice", "tenant-idp")
        si = _input(alice, {"x": 1})

        tampered = SignedInput(
            content=si.content,
            signer_id="bob",
            public_key=si.public_key,
            signature=si.signature,
            valid_until=si.valid_until,
            key_binding=si.key_binding,
            output_constraints=si.output_constraints,
        )
        with pytest.raises(VerificationError, match="key binding signer"):
            tampered.verify(trust_store)

    def test_key_binding_wrong_anchor(self, trust_store):
        """Key binding points to an anchor that doesn't exist."""
        keys = generate_keypair()
        kb = make_key_binding(keys, "alice", "nonexistent-idp")
        si = make_signed_input(keys, kb, {"x": 1})
        with pytest.raises(VerificationError, match="trust anchor not found"):
            si.verify(trust_store)

    def test_key_binding_wrong_public_key(self, alice, trust_store):
        """Swapping the public_key breaks signature verification
        (the signature was made with alice's key, not other_keys).
        """
        other_keys = generate_keypair()

        si = _input(alice, {"x": 1})
        tampered = SignedInput(
            content=si.content,
            signer_id=si.signer_id,
            public_key=other_keys.public_key_bytes,
            signature=si.signature,
            valid_until=si.valid_until,
            key_binding=si.key_binding,
            output_constraints=si.output_constraints,
        )
        with pytest.raises(VerificationError, match="signature verification failed"):
            tampered.verify(trust_store)

    def test_key_binding_proof_forged(self, alice, trust_store):
        """Key binding uses alice's real key (recognised by anchor)
        but the proof-of-possession is signed by a different key.
        """
        forger = generate_keypair()

        binding_doc = {
            "public_key": alice.keys.public_key_bytes.hex(),
            "signer_id": "alice",
            "trust_anchor_id": "tenant-idp",
        }
        forged_proof = sign(forger.private_key, content_hash(binding_doc))

        kb = KeyBinding(
            signer_id="alice",
            public_key=alice.keys.public_key_bytes,
            trust_anchor_id="tenant-idp",
            binding_proof=forged_proof,
        )
        si = make_signed_input(alice.keys, kb, {"x": 1})
        with pytest.raises(VerificationError, match="proof-of-possession failed"):
            si.verify(trust_store)

    def test_reuse_key_binding_across_identities(self, trust_store):
        """Bob tries to use alice's key binding (different key)."""
        alice = make_identity("alice", "tenant-idp")
        bob = make_identity("bob", "tenant-idp")

        si = make_signed_input(bob.keys, alice.key_binding, {"x": 1})
        with pytest.raises(VerificationError, match="signature key does not match"):
            si.verify(trust_store)

    def test_output_key_binding_wrong_anchor(self, alice, trust_store):
        """Output's key binding points to nonexistent anchor."""
        spec = {"manifest_strategy": {"type": "addon"}}

        addon_keys = generate_keypair()
        kb = make_key_binding(addon_keys, "capi-provisioner", "nonexistent-addon-ca")
        output = sign_output(addon_keys, kb, [{"kind": "Cluster"}])

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=output,
        )
        with pytest.raises(VerificationError, match="trust anchor not found"):
            att.verify(trust_store)

    def test_output_key_binding_key_not_known(self, alice, trust_store):
        """Output signer's key is not in the anchor's known_keys."""
        spec = {"manifest_strategy": {"type": "addon"}}

        rogue = generate_keypair()
        kb = make_key_binding(rogue, "rogue-addon", "fleet-addons")
        output = sign_output(rogue, kb, [{"kind": "Cluster"}])

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("rogue-addon")],
            ),
            output=output,
        )
        with pytest.raises(VerificationError, match="key not recognised"):
            att.verify(trust_store)


class TestAdversarialCrossAnchorKind:
    """Attacks mixing user and addon trust anchors."""

    def test_user_key_cannot_satisfy_addon_constraint(
        self, alice, trust_store,
    ):
        """User signs the output directly.  addon_must_sign rejects
        because alice is not the expected addon.
        """
        spec = {"manifest_strategy": {"type": "addon"}}
        manifests = [{"kind": "Cluster"}]

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=_output(alice, manifests),
        )
        with pytest.raises(VerificationError, match="output constraint failed"):
            att.verify(trust_store)

    def test_addon_as_input_signer_wrong_anchor(self, capi_addon, trust_store):
        """Addon identity tries to sign an input (spec).  It has a valid
        key binding in fleet-addons, but if the constraint expects a
        user, this is just an addon acting as authority.
        """
        spec = {"manifest_strategy": {"type": "inline"}}
        manifests = [{"kind": "ConfigMap"}]

        att = Attestation(
            input=_input(capi_addon, spec),
            output=make_output(manifests),
        )
        result = att.verify(trust_store)
        assert result.signer_id is None

    def test_user_registers_in_addon_anchor_fails(self, trust_store):
        """A user tries to create a key binding against the addon
        anchor.  The anchor doesn't know them.
        """
        rogue = generate_keypair()
        kb = make_key_binding(rogue, "evil-user", "fleet-addons")
        si = make_signed_input(rogue, kb, {"x": 1})
        with pytest.raises(VerificationError, match="key not recognised"):
            si.verify(trust_store)

    def test_addon_tries_to_impersonate_another_addon(
        self, alice, lifecycle_addon, trust_store,
    ):
        """lifecycle addon signs output claiming to be capi-provisioner."""
        spec = {"manifest_strategy": {"type": "addon"}}
        manifests = [{"kind": "Cluster"}]

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=_output(lifecycle_addon, manifests),
        )
        with pytest.raises(VerificationError, match="output constraint failed"):
            att.verify(trust_store)


class TestAdversarialTrustStoreMissing:
    """Attacks where trust anchors are partially or fully absent."""

    def test_empty_trust_store_rejects_everything(self):
        empty = TrustStore()
        alice = make_identity("alice", "tenant-idp")
        att = Attestation(
            input=_input(alice, {"x": 1}),
            output=make_output({"x": 1}),
        )
        with pytest.raises(VerificationError, match="trust anchor not found"):
            att.verify(empty)

    def test_user_trusted_addon_not(self, alice):
        """Trust store has user anchor but not addon anchor."""
        ts = TrustStore()
        ts.add(TrustAnchor(
            anchor_id="tenant-idp",
            known_keys={"alice": alice.keys.public_key_bytes},
        ))

        addon = make_identity("capi-provisioner", "fleet-addons")
        att = Attestation(
            input=_input(
                alice, {"manifest_strategy": {"type": "addon"}},
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=_output(addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="trust anchor not found"):
            att.verify(ts)

    def test_addon_trusted_user_not(self, capi_addon):
        """Trust store has addon anchor but not user anchor."""
        ts = TrustStore()
        ts.add(TrustAnchor(
            anchor_id="fleet-addons",
            known_keys={"capi-provisioner": capi_addon.keys.public_key_bytes},
        ))

        alice = make_identity("alice", "tenant-idp")
        att = Attestation(
            input=_input(
                alice, {"manifest_strategy": {"type": "addon"}},
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="trust anchor not found"):
            att.verify(ts)

    def test_anchor_exists_but_signer_not_registered(self, trust_store):
        """Trust anchor 'tenant-idp' exists but 'carol' is not in it."""
        carol = make_identity("carol", "tenant-idp")
        att = Attestation(
            input=_input(carol, {"x": 1}),
            output=make_output({"x": 1}),
        )
        with pytest.raises(VerificationError, match="key not recognised"):
            att.verify(trust_store)


class TestAdversarialContentTampering:
    """Attacks that tamper with content at various points in the graph."""

    def test_input_content_swapped(self, alice, trust_store):
        """Attacker replaces input content after signing."""
        si = _input(alice, {"original": True})
        tampered = SignedInput(
            content={"malicious": True},
            signer_id=si.signer_id,
            public_key=si.public_key,
            signature=si.signature,
            valid_until=si.valid_until,
            key_binding=si.key_binding,
            output_constraints=si.output_constraints,
        )
        with pytest.raises(VerificationError, match="signature verification failed"):
            tampered.verify(trust_store)

    def test_constraints_injected_after_signing(self, alice, trust_store):
        """Attacker adds constraints that weren't in the signed envelope."""
        si = _input(alice, {"x": 1})
        tampered = SignedInput(
            content=si.content,
            signer_id=si.signer_id,
            public_key=si.public_key,
            signature=si.signature,
            valid_until=si.valid_until,
            key_binding=si.key_binding,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )
        with pytest.raises(VerificationError, match="signature verification failed"):
            tampered.verify(trust_store)

    def test_output_content_swapped_unsigned(self, alice, trust_store):
        """For unsigned output, content can be swapped freely.
        This is only safe when constraints validate content.
        """
        manifests = [{"kind": "Deployment",
                      "metadata": {"name": "web", "namespace": "prod"}}]
        evil = [{"kind": "Deployment",
                 "metadata": {"name": "web", "namespace": "kube-system"}}]

        att = Attestation(
            input=_input(
                alice, manifests,
                output_constraints=[namespace_constraint("prod")],
            ),
            output=make_output(evil),
        )
        with pytest.raises(VerificationError, match="namespace"):
            att.verify(trust_store)

    def test_output_content_swapped_signed(self, alice, capi_addon, trust_store):
        """Signed output with swapped content — signature breaks."""
        spec = {"manifest_strategy": {"type": "addon"}}
        legit = _output(capi_addon, [{"kind": "Cluster"}])

        att = Attestation(
            input=_input(
                alice, spec,
                output_constraints=[addon_must_sign("capi-provisioner")],
            ),
            output=Output(
                content=[{"kind": "DaemonSet", "spec": {"evil": True}}],
                signer_id=legit.signer_id,
                public_key=legit.public_key,
                signature=legit.signature,
                key_binding=legit.key_binding,
            ),
        )
        with pytest.raises(VerificationError, match="output signature verification"):
            att.verify(trust_store)


class TestAdversarialDerivedInputAttacks:
    """Attacks targeting DerivedInput and its recursive verification."""

    def test_expired_prior_in_derivation(self, alice, capi_addon, trust_store):
        """Prior input is expired — derivation must fail."""
        old_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }

        expired_prior = _input(alice, old_spec, valid_duration_sec=-1)
        update = {"type": "update", "new_version": "1.30.2"}

        att = Attestation(
            input=DerivedInput(
                prior=expired_prior,
                update=Attestation(
                    input=_input(alice, update),
                    output=make_output(update),
                ),
                apply=cel_upgrade_apply,
            ),
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="expired"):
            att.verify(trust_store)

    def test_expired_update_input_in_derivation(
        self, alice, capi_addon, trust_store,
    ):
        """Update attestation's input is expired."""
        old_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        update = {"type": "update", "new_version": "1.30.2"}

        att = Attestation(
            input=DerivedInput(
                prior=_input(
                    alice, old_spec,
                    output_constraints=[addon_must_sign("capi-provisioner")],
                ),
                update=Attestation(
                    input=_input(alice, update, valid_duration_sec=-1),
                    output=make_output(update),
                ),
                apply=cel_upgrade_apply,
            ),
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="expired"):
            att.verify(trust_store)

    def test_update_attestation_fails_own_constraints(
        self, alice, lifecycle_addon, trust_store,
    ):
        """Update attestation's output doesn't satisfy its own input's
        constraints.  The update itself is invalid.
        """
        old_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        update = {"type": "update", "new_version": "1.30.2"}

        att = Attestation(
            input=DerivedInput(
                prior=_input(
                    alice, old_spec,
                    output_constraints=[addon_must_sign("capi-provisioner")],
                ),
                update=Attestation(
                    input=_input(
                        alice, {"type": "fleet-update"},
                        output_constraints=[addon_must_sign("upgrade-planner")],
                    ),
                    output=_output(lifecycle_addon, update),
                ),
                apply=cel_upgrade_apply,
            ),
            output=make_output([{"kind": "Cluster"}]),
        )
        with pytest.raises(
            VerificationError,
            match="output constraint failed.*upgrade-planner",
        ):
            att.verify(trust_store)

    def test_untrusted_prior_signer_in_deep_chain(
        self, alice, capi_addon, trust_store,
    ):
        """Chained derivation where the original prior was signed by
        an untrusted identity.
        """
        eve = make_identity("eve", "rogue-idp")
        old_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.28"},
            },
        }

        def make_update_att(version):
            content = {"type": "update", "new_version": version}
            return Attestation(
                input=_input(alice, content),
                output=make_output(content),
            )

        d1v2_input = DerivedInput(
            prior=_input(eve, old_spec),
            update=make_update_att("1.29"),
            apply=cel_upgrade_apply,
        )
        d1v3_input = DerivedInput(
            prior=d1v2_input,
            update=make_update_att("1.30"),
            apply=cel_upgrade_apply,
        )

        att = Attestation(
            input=d1v3_input,
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="trust anchor not found"):
            att.verify(trust_store)

    def test_untrusted_update_signer_in_cross_deployment(
        self, alice, capi_addon, trust_store,
    ):
        """Cross-deployment: D2 is signed by untrusted user.
        D1v2's derivation fails when verifying D2.
        """
        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )

        eve = make_identity("eve", "rogue-idp")
        d2 = Attestation(
            input=_input(eve, {"type": "fleet-update"}),
            output=make_output({"type": "update", "new_version": "9.9.9"}),
        )

        att = Attestation(
            input=DerivedInput(prior=d1_input, update=d2, apply=cel_upgrade_apply),
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="trust anchor not found"):
            att.verify(trust_store)

    def test_update_output_addon_key_not_registered(
        self, alice, bob, trust_store,
    ):
        """Update attestation's output is signed by an addon whose
        key is not in the fleet-addons anchor.
        """
        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )

        rogue_addon = make_identity("rogue-lifecycle", "fleet-addons")
        update = {"type": "deployment-update", "new_version": "1.30.2"}

        d2 = Attestation(
            input=_input(
                bob, {"type": "fleet-update"},
                output_constraints=[addon_must_sign("rogue-lifecycle")],
            ),
            output=_output(rogue_addon, update),
        )

        att = Attestation(
            input=DerivedInput(prior=d1_input, update=d2, apply=cel_upgrade_apply),
            output=make_output([{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="key not recognised"):
            att.verify(trust_store)


class TestAdversarialReplayAndIdentityConfusion:
    """Replay attacks and identity confusion."""

    def test_replay_input_with_different_identity(self, trust_store):
        """Take alice's valid signature and try to claim it's bob's."""
        alice = make_identity("alice", "tenant-idp")
        si = _input(alice, {"x": 1})

        replayed = SignedInput(
            content=si.content,
            signer_id="bob",
            public_key=si.public_key,
            signature=si.signature,
            valid_until=si.valid_until,
            key_binding=si.key_binding,
            output_constraints=si.output_constraints,
        )
        with pytest.raises(VerificationError, match="key binding signer"):
            replayed.verify(trust_store)

    def test_replay_output_from_different_attestation(
        self, alice, capi_addon, trust_store,
    ):
        """Addon signed manifests for deployment A.  Attacker replays
        that output in deployment B (different spec).  The content
        is valid but the spec constraints differ.
        """
        spec_a = {"manifest_strategy": {"type": "addon"},
                   "namespace": "prod"}
        spec_b = {"manifest_strategy": {"type": "addon"},
                   "namespace": "staging"}
        manifests = [{"kind": "Cluster",
                      "metadata": {"namespace": "prod"}}]

        legit_output = _output(capi_addon, manifests)

        att_b = Attestation(
            input=_input(
                alice, spec_b,
                output_constraints=[
                    addon_must_sign("capi-provisioner"),
                    namespace_constraint("staging"),
                ],
            ),
            output=legit_output,
        )
        with pytest.raises(VerificationError, match="namespace"):
            att_b.verify(trust_store)

    def test_self_signed_input_and_output_bypass_attempt(self, trust_store):
        """Attacker creates both input and output with own keys,
        registered in the user anchor.  This passes IFF the
        attacker is actually in the trust store.  Here they are not.
        """
        attacker = make_identity("mallory", "rogue-idp")
        att = Attestation(
            input=_input(attacker, {"evil": True}),
            output=_output(attacker, [{"kind": "Backdoor"}]),
        )
        with pytest.raises(VerificationError, match="trust anchor not found"):
            att.verify(trust_store)

    def test_addon_self_authorises_without_user_input(
        self, capi_addon, trust_store,
    ):
        """Addon tries to be both authority (input) and producer
        (output).  The addon IS in the trust store, so this
        technically passes — but the output signer differs from
        what addon_must_sign expects because no user constraint
        demanded this addon.

        This tests that an addon acting unilaterally does NOT
        satisfy a constraint expecting a specific addon, when the
        signer_ids don't match the constraint.
        """
        att = Attestation(
            input=_input(
                capi_addon, {"manifest_strategy": {"type": "addon"}},
                output_constraints=[addon_must_sign("cluster-lifecycle")],
            ),
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="output constraint failed"):
            att.verify(trust_store)


class TestAdversarialComplexGraphAttacks:
    """Multi-level attacks across the attestation graph."""

    def test_d1v2_with_forged_d2_addon_output(
        self, alice, bob, capi_addon, trust_store,
    ):
        """D2's addon output is signed by a forger (new key, same
        signer_id).  The key is not in the trust store.
        """
        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )

        forger = make_identity("cluster-lifecycle", "fleet-addons")
        d2 = Attestation(
            input=_input(
                bob, {"type": "fleet-update"},
                output_constraints=[addon_must_sign("cluster-lifecycle")],
            ),
            output=_output(forger, {"type": "update", "new_version": "1.30.2"}),
        )

        att = Attestation(
            input=DerivedInput(prior=d1_input, update=d2, apply=cel_upgrade_apply),
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="key not recognised"):
            att.verify(trust_store)

    def test_d1v2_output_from_wrong_addon_after_valid_derivation(
        self, alice, bob, capi_addon, lifecycle_addon, trust_store,
    ):
        """Derivation succeeds and produces constraints requiring
        capi-provisioner.  But the output is signed by lifecycle.
        """
        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )

        update = {"type": "update", "new_version": "1.30.2"}
        d2 = Attestation(
            input=_input(bob, {"type": "fleet-update"}),
            output=make_output(update),
        )

        att = Attestation(
            input=DerivedInput(prior=d1_input, update=d2, apply=cel_upgrade_apply),
            output=_output(lifecycle_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(
            VerificationError,
            match="output constraint failed.*capi-provisioner",
        ):
            att.verify(trust_store)

    def test_mixed_trust_both_anchors_needed(
        self, alice, bob, capi_addon, lifecycle_addon,
    ):
        """Full cross-deployment scenario.  Trust store has users
        but is missing the addon anchor entirely.  Every addon
        signature fails.
        """
        ts = TrustStore()
        ts.add(TrustAnchor(
            anchor_id="tenant-idp",
            known_keys={
                "alice": alice.keys.public_key_bytes,
                "bob": bob.keys.public_key_bytes,
            },
        ))

        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }
        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )

        d2 = Attestation(
            input=_input(
                bob, {"type": "fleet-update"},
                output_constraints=[addon_must_sign("cluster-lifecycle")],
            ),
            output=_output(lifecycle_addon, {"type": "update", "new_version": "1.30.2"}),
        )

        att = Attestation(
            input=DerivedInput(prior=d1_input, update=d2, apply=cel_upgrade_apply),
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="trust anchor not found"):
            att.verify(ts)

    def test_chained_derivation_middle_update_untrusted(
        self, alice, capi_addon, trust_store,
    ):
        """Three-step chain: v1 -> v2 -> v3.  The second update
        (v2->v3) is signed by an untrusted identity.
        """
        d1_spec = {
            "manifest_strategy": {
                "type": "addon",
                "addon_id": "capi-provisioner",
                "config": {"version": "1.28"},
            },
        }
        d1_input = _input(
            alice, d1_spec,
            output_constraints=[addon_must_sign("capi-provisioner")],
        )

        u1 = {"type": "update", "new_version": "1.29"}
        d1v2_input = DerivedInput(
            prior=d1_input,
            update=Attestation(
                input=_input(alice, u1),
                output=make_output(u1),
            ),
            apply=cel_upgrade_apply,
        )

        eve = make_identity("eve", "rogue-idp")
        u2 = {"type": "update", "new_version": "1.30"}
        d1v3_input = DerivedInput(
            prior=d1v2_input,
            update=Attestation(
                input=_input(eve, u2),
                output=make_output(u2),
            ),
            apply=cel_upgrade_apply,
        )

        att = Attestation(
            input=d1v3_input,
            output=_output(capi_addon, [{"kind": "Cluster"}]),
        )
        with pytest.raises(VerificationError, match="trust anchor not found"):
            att.verify(trust_store)
