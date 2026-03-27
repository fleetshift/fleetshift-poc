"""
Tests that exercise every attestation scenario through the same
verify_chain algorithm.

Scenarios:
  1. User intent signing with structural constraints (inline strategy)
  2. User output signing (zero derivation trust)
  3. User intent + addon co-signing (opaque derivation)
  4. Update with CEL derivation + inline manifests
  5. Update with CEL derivation + addon co-signing
  6. Deep chain: Update → CEL → addon manifest generation
  7. Failure cases: tampered manifests, expired signature, unauthorized
     addon, constraint violation, CEL derivation mismatch

Run: pytest test_attestation.py -v
"""

import copy
import time

import pytest

from attestation import (
    AddonIdentity,
    AttestationChain,
    Constraint,
    DerivationStep,
    SignatureStep,
    UserIdentity,
    VerificationError,
    canonical_json,
    content_hash,
    derive_step,
    generate_keypair,
    sign_step,
    verify_chain,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def user_keys():
    priv, pub = generate_keypair()
    identity = UserIdentity(subject="alice", issuer="https://idp.acme.com", public_key=pub)
    return priv, identity


@pytest.fixture
def addon_keys():
    priv, pub = generate_keypair()
    identity = AddonIdentity(addon_id="spiffe://fleet-addons/capi-provisioner", public_key=pub)
    return priv, identity


@pytest.fixture
def second_addon_keys():
    priv, pub = generate_keypair()
    identity = AddonIdentity(addon_id="spiffe://fleet-addons/observability", public_key=pub)
    return priv, identity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def namespace_constraint(ns: str) -> Constraint:
    return Constraint(
        description=f"all manifests must be in namespace '{ns}'",
        predicate=lambda intent, manifests: all(
            m.get("metadata", {}).get("namespace") == ns for m in manifests
        ),
    )


def no_cluster_admin_constraint() -> Constraint:
    return Constraint(
        description="no ClusterRoleBinding granting cluster-admin",
        predicate=lambda intent, manifests: all(
            not (
                m.get("kind") == "ClusterRoleBinding"
                and m.get("roleRef", {}).get("name") == "cluster-admin"
            )
            for m in manifests
        ),
    )


def allowed_gvks_constraint(allowed: set[str]) -> Constraint:
    def check(intent: dict, manifests: list[dict]) -> bool:
        for m in manifests:
            gvk = f"{m.get('apiVersion', '')}/{m.get('kind', '')}"
            if gvk not in allowed:
                return False
        return True

    return Constraint(description=f"only GVKs in {allowed}", predicate=check)


# ---------------------------------------------------------------------------
# Scenario 1: User intent signing + structural constraints (constraining chain)
# ---------------------------------------------------------------------------

class TestUserIntentConstraining:
    """
    Chain: [Signed(user, intent, constraints)]
    The signed intent is NOT the manifests — it's a high-level
    authorization. Constraints are the sole binding between the chain
    and the applied content.
    """

    def test_valid(self, user_keys):
        priv, identity = user_keys

        intent = {
            "type": "deployment",
            "deployment_id": "deploy-001",
            "manifest_strategy": {"type": "inline"},
            "placement": {"namespace": "production"},
        }

        manifests = [
            {"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "web", "namespace": "production"}},
            {"apiVersion": "v1", "kind": "Service",
             "metadata": {"name": "web-svc", "namespace": "production"}},
        ]

        step = sign_step(
            priv, identity, content=intent,
            constraints=[
                namespace_constraint("production"),
                no_cluster_admin_constraint(),
            ],
        )

        chain = AttestationChain(steps=[step], deployment_id="deploy-001")
        verify_chain(chain, manifests)

    def test_constraint_violation(self, user_keys):
        priv, identity = user_keys

        intent = {
            "type": "deployment",
            "deployment_id": "deploy-001",
            "placement": {"namespace": "production"},
        }

        manifests = [
            {"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "web", "namespace": "kube-system"}},
        ]

        step = sign_step(
            priv, identity, content=intent,
            constraints=[namespace_constraint("production")],
        )

        chain = AttestationChain(steps=[step])
        with pytest.raises(VerificationError, match="constraint failed"):
            verify_chain(chain, manifests)

    def test_manifests_satisfy_constraints_but_different_content(self, user_keys):
        """
        Constraining chains allow any content that satisfies the
        constraints — this is by design. The trust model accepts that
        the platform derives specifics within constraint bounds.
        """
        priv, identity = user_keys

        intent = {
            "type": "deployment",
            "deployment_id": "deploy-001",
            "placement": {"namespace": "production"},
        }

        # Any manifests in the right namespace pass, even though the
        # user didn't specify these exact manifests.
        manifests_v1 = [
            {"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "web-v1", "namespace": "production"}},
        ]
        manifests_v2 = [
            {"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "web-v2", "namespace": "production"}},
            {"apiVersion": "v1", "kind": "ConfigMap",
             "metadata": {"name": "web-v2-config", "namespace": "production"}},
        ]

        step = sign_step(
            priv, identity, content=intent,
            constraints=[namespace_constraint("production")],
        )

        chain = AttestationChain(steps=[step])
        verify_chain(chain, manifests_v1)
        verify_chain(chain, manifests_v2)

    def test_unbound_chain_rejected(self, user_keys):
        """
        A chain with no terminal match AND no constraints is rejected.
        The chain is cryptographically valid but proves nothing about
        the applied content.
        """
        priv, identity = user_keys

        intent = {
            "type": "deployment",
            "deployment_id": "deploy-001",
            "placement": {"namespace": "production"},
        }
        manifests = [
            {"apiVersion": "v1", "kind": "ConfigMap",
             "metadata": {"name": "x", "namespace": "production"}},
        ]

        step = sign_step(priv, identity, content=intent)  # no constraints
        chain = AttestationChain(steps=[step])
        with pytest.raises(VerificationError, match="unbound chain"):
            verify_chain(chain, manifests)


# ---------------------------------------------------------------------------
# Scenario 1b: Output signing (terminating chain, tamper detection)
# ---------------------------------------------------------------------------

class TestTerminatingChainTamperDetection:
    """
    When the chain terminates at the applied content (output signing
    or addon signing), tampering is detected by hash mismatch even
    without constraints.
    """

    def test_tampered_manifests(self, user_keys):
        priv, identity = user_keys

        original = [{"apiVersion": "v1", "kind": "ConfigMap",
                      "metadata": {"name": "cfg", "namespace": "production"}}]
        tampered = [{"apiVersion": "v1", "kind": "ConfigMap",
                      "metadata": {"name": "cfg", "namespace": "production"},
                      "data": {"injected": "true"}}]

        step = sign_step(priv, identity, content=original)
        chain = AttestationChain(steps=[step])
        # Terminates: original hash != tampered hash.
        # No constraints, but the chain IS terminating (original matches
        # itself), so the applied tampered content fails the binding.
        with pytest.raises(VerificationError, match="unbound chain"):
            verify_chain(chain, tampered)


# ---------------------------------------------------------------------------
# Scenario 2: User output signing (zero derivation trust)
# ---------------------------------------------------------------------------

class TestUserOutputSigning:
    """
    Chain: [Signed(user, manifests)] → terminal = manifests
    Single step; signed content IS the applied content.
    """

    def test_valid(self, user_keys):
        priv, identity = user_keys
        manifests = [
            {"apiVersion": "v1", "kind": "Secret",
             "metadata": {"name": "db-creds", "namespace": "production"},
             "data": {"password": "c2VjcmV0"}},
        ]

        step = sign_step(priv, identity, content=manifests)
        chain = AttestationChain(steps=[step])
        verify_chain(chain, manifests)


# ---------------------------------------------------------------------------
# Scenario 3: User intent + addon co-signing
# ---------------------------------------------------------------------------

class TestAddonCoSigning:
    """
    Chain:
      [Signed(user, intent, authorized_next=[addon_X])]
      [Signed(addon_X, manifests)]
    → terminal = manifests
    """

    def test_valid(self, user_keys, addon_keys):
        user_priv, user_id = user_keys
        addon_priv, addon_id = addon_keys

        intent = {
            "type": "deployment",
            "deployment_id": "deploy-002",
            "manifest_strategy": {"type": "addon", "capability": "capi-provisioner"},
            "placement": {"selector": {"pool": "management"}},
        }

        manifests = [
            {"apiVersion": "cluster.x-k8s.io/v1beta1", "kind": "Cluster",
             "metadata": {"name": "workload-01", "namespace": "capi-system"},
             "spec": {"topology": {"version": "v1.29.5"}}},
        ]

        user_step = sign_step(
            user_priv, user_id, content=intent,
            authorized_next_signers=[f"addon:{addon_id.addon_id}"],
            constraints=[no_cluster_admin_constraint()],
        )
        addon_step = sign_step(addon_priv, addon_id, content=manifests)

        chain = AttestationChain(steps=[user_step, addon_step])
        verify_chain(chain, manifests)

    def test_unauthorized_addon(self, user_keys, addon_keys, second_addon_keys):
        """User authorizes addon_X but addon_Y signs the manifests."""
        user_priv, user_id = user_keys
        _, authorized_addon_id = addon_keys
        rogue_priv, rogue_addon_id = second_addon_keys

        intent = {
            "type": "deployment",
            "deployment_id": "deploy-003",
            "manifest_strategy": {"type": "addon", "capability": "capi-provisioner"},
        }
        manifests = [{"apiVersion": "v1", "kind": "ConfigMap",
                       "metadata": {"name": "rogue", "namespace": "default"}}]

        user_step = sign_step(
            user_priv, user_id, content=intent,
            authorized_next_signers=[f"addon:{authorized_addon_id.addon_id}"],
        )
        rogue_step = sign_step(rogue_priv, rogue_addon_id, content=manifests)

        chain = AttestationChain(steps=[user_step, rogue_step])
        with pytest.raises(VerificationError, match="not in authorized list"):
            verify_chain(chain, manifests)


# ---------------------------------------------------------------------------
# Scenario 4: Update with CEL derivation + inline manifests
# ---------------------------------------------------------------------------

class TestUpdateCelInline:
    """
    Chain:
      [Signed(user, update_intent with CEL)]
      [Derived(CEL, old_spec → new_spec)]
    → terminal = new_spec (which contains inline manifests)

    The CEL expression is modeled as a Python function for this prototype.
    """

    @staticmethod
    def cel_set_version(version: str):
        """Simulates: spec.manifest_strategy.config.version = version"""
        def transform(input_spec: dict) -> dict:
            out = copy.deepcopy(input_spec)
            out.setdefault("manifest_strategy", {}).setdefault("config", {})["version"] = version
            return out
        return transform

    def test_valid(self, user_keys):
        priv, identity = user_keys

        old_spec = {
            "deployment_id": "cluster-deploy-007",
            "manifest_strategy": {
                "type": "inline",
                "config": {"version": "1.29.5", "region": "us-east-1"},
            },
        }

        cel_fn = self.cel_set_version("1.30.2")
        new_spec = cel_fn(old_spec)

        update_intent = {
            "type": "update",
            "update_id": "upgrade-k8s-1.30",
            "cel_expression": 'spec.manifest_strategy.config.version = "1.30.2"',
            "deployment_selector": {"labels": {"type": "cluster-provisioning"}},
        }

        user_step = sign_step(priv, identity, content=update_intent)
        derivation = derive_step(
            cel_fn,
            input_content=old_spec,
            description='spec.manifest_strategy.config.version = "1.30.2"',
        )

        chain = AttestationChain(steps=[user_step, derivation])
        verify_chain(chain, new_spec)

    def test_cel_derivation_tampered(self, user_keys):
        """Platform applies a different mutation than the signed CEL expression."""
        priv, identity = user_keys

        old_spec = {
            "deployment_id": "cluster-deploy-008",
            "manifest_strategy": {"type": "inline", "config": {"version": "1.29.5"}},
        }

        cel_fn = self.cel_set_version("1.30.2")

        update_intent = {
            "type": "update",
            "update_id": "upgrade-k8s-1.30",
            "cel_expression": 'spec.manifest_strategy.config.version = "1.30.2"',
        }

        user_step = sign_step(priv, identity, content=update_intent)

        # The platform sneaks in a different version
        def tampered_fn(input_spec):
            out = copy.deepcopy(input_spec)
            out["manifest_strategy"]["config"]["version"] = "1.31.0-rc1"
            return out

        tampered_output = tampered_fn(old_spec)
        derivation = DerivationStep(
            content=tampered_output,
            derivation_fn=cel_fn,  # but the real CEL fn produces 1.30.2, not 1.31.0-rc1
            input_content=old_spec,
        )

        chain = AttestationChain(steps=[user_step, derivation])
        with pytest.raises(VerificationError, match="derivation re-execution produced different output"):
            verify_chain(chain, tampered_output)


# ---------------------------------------------------------------------------
# Scenario 5: Update with CEL derivation + addon co-signing
# ---------------------------------------------------------------------------

class TestUpdateCelAddon:
    """
    Chain:
      [Signed(user, update_intent, authorized_next=[addon_X])]
      [Derived(CEL, old_spec → new_spec)]
      [Signed(addon_X, manifests)]
    → terminal = manifests
    """

    def test_valid(self, user_keys, addon_keys):
        user_priv, user_id = user_keys
        addon_priv, addon_id = addon_keys

        old_spec = {
            "deployment_id": "cluster-deploy-010",
            "manifest_strategy": {
                "type": "addon",
                "capability": "capi-provisioner",
                "config": {"version": "1.29.5"},
            },
        }

        def cel_fn(spec):
            out = copy.deepcopy(spec)
            out["manifest_strategy"]["config"]["version"] = "1.30.2"
            return out

        new_spec = cel_fn(old_spec)

        manifests = [
            {"apiVersion": "cluster.x-k8s.io/v1beta1", "kind": "Cluster",
             "metadata": {"name": "workload-010", "namespace": "capi-system"},
             "spec": {"topology": {"version": "v1.30.2"}}},
            {"apiVersion": "cluster.x-k8s.io/v1beta1", "kind": "MachineDeployment",
             "metadata": {"name": "workload-010-md-0", "namespace": "capi-system"},
             "spec": {"template": {"spec": {"version": "v1.30.2"}}}},
        ]

        update_intent = {
            "type": "update",
            "update_id": "upgrade-k8s-1.30",
            "cel_expression": 'spec.manifest_strategy.config.version = "1.30.2"',
            "deployment_selector": {"labels": {"type": "cluster-provisioning"}},
        }

        user_step = sign_step(
            user_priv, user_id, content=update_intent,
            authorized_next_signers=[f"addon:{addon_id.addon_id}"],
        )
        derivation = derive_step(cel_fn, input_content=old_spec)
        addon_step = sign_step(
            addon_priv, addon_id, content=manifests,
            constraints=[no_cluster_admin_constraint()],
        )

        chain = AttestationChain(steps=[user_step, derivation, addon_step])
        verify_chain(chain, manifests)


# ---------------------------------------------------------------------------
# Scenario 6: Placement enforcement (label change provenance)
# ---------------------------------------------------------------------------

class TestPlacementEnforcement:
    """
    Chain:
      [Signed(admin, label_change)]
      [Derived(placement_rules, label_change → removal_decision)]
    → terminal = removal_decision
    """

    def test_valid(self, user_keys):
        priv, identity = user_keys

        label_change = {
            "type": "label_change",
            "target_id": "cluster-abc",
            "removed_labels": {"pool": "production"},
            "added_labels": {"pool": "decommissioned"},
        }

        def placement_derivation(label_change: dict) -> dict:
            if label_change.get("removed_labels", {}).get("pool") == "production":
                return {
                    "action": "remove",
                    "target_id": label_change["target_id"],
                    "reason": "target no longer matches pool=production selector",
                }
            return {"action": "none"}

        removal = placement_derivation(label_change)

        admin_step = sign_step(priv, identity, content=label_change)
        derivation = derive_step(
            placement_derivation,
            input_content=label_change,
            description="placement selector no longer matches after label removal",
        )

        chain = AttestationChain(steps=[admin_step, derivation])
        verify_chain(chain, removal)


# ---------------------------------------------------------------------------
# Scenario 7: Expired signature
# ---------------------------------------------------------------------------

class TestExpiredSignature:
    def test_expired(self, user_keys):
        priv, identity = user_keys
        manifests = [{"apiVersion": "v1", "kind": "ConfigMap",
                       "metadata": {"name": "x", "namespace": "default"}}]

        step = sign_step(priv, identity, content=manifests, valid_duration_sec=-1)
        chain = AttestationChain(steps=[step])
        with pytest.raises(VerificationError, match="expired"):
            verify_chain(chain, manifests)

    def test_expiry_tampered(self, user_keys):
        """Platform extends valid_until after signing. Signature breaks."""
        priv, identity = user_keys
        manifests = [{"apiVersion": "v1", "kind": "ConfigMap",
                       "metadata": {"name": "x", "namespace": "default"}}]

        step = sign_step(priv, identity, content=manifests, valid_duration_sec=-1)
        step.valid_until = time.time() + 86400  # try to extend expiry
        chain = AttestationChain(steps=[step])
        with pytest.raises(VerificationError, match="signature verification failed"):
            verify_chain(chain, manifests)


# ---------------------------------------------------------------------------
# Scenario 8: Forged signature (wrong key)
# ---------------------------------------------------------------------------

class TestForgedSignature:
    def test_wrong_key(self, user_keys):
        priv, identity = user_keys
        attacker_priv, _ = generate_keypair()

        manifests = [{"apiVersion": "v1", "kind": "ConfigMap",
                       "metadata": {"name": "x", "namespace": "default"}}]

        step = sign_step(priv, identity, content=manifests)
        # Tamper: re-sign with attacker's key but keep the user's identity
        step.signature = sign_step(attacker_priv, identity, content=manifests).signature

        chain = AttestationChain(steps=[step])
        with pytest.raises(VerificationError, match="signature verification failed"):
            verify_chain(chain, manifests)


# ---------------------------------------------------------------------------
# Scenario 9: Constraint on addon output (structural schema defense-in-depth)
# ---------------------------------------------------------------------------

class TestAddonConstraintDefenseInDepth:
    """
    The user authorizes the addon but constrains the output:
    no cluster-admin bindings allowed.
    """

    def test_addon_produces_forbidden_manifest(self, user_keys, addon_keys):
        user_priv, user_id = user_keys
        addon_priv, addon_id = addon_keys

        intent = {
            "type": "deployment",
            "manifest_strategy": {"type": "addon", "capability": "observability"},
        }

        malicious_manifests = [
            {"apiVersion": "rbac.authorization.k8s.io/v1",
             "kind": "ClusterRoleBinding",
             "metadata": {"name": "evil"},
             "roleRef": {"name": "cluster-admin"}},
        ]

        user_step = sign_step(
            user_priv, user_id, content=intent,
            authorized_next_signers=[f"addon:{addon_id.addon_id}"],
            constraints=[no_cluster_admin_constraint()],
        )
        addon_step = sign_step(addon_priv, addon_id, content=malicious_manifests)

        chain = AttestationChain(steps=[user_step, addon_step])
        with pytest.raises(VerificationError, match="constraint failed.*cluster-admin"):
            verify_chain(chain, malicious_manifests)


# ---------------------------------------------------------------------------
# Scenario 10: Multi-constraint (GVK allowlist + namespace)
# ---------------------------------------------------------------------------

class TestMultipleConstraints:
    def test_all_constraints_pass(self, user_keys):
        priv, identity = user_keys

        manifests = [
            {"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "web", "namespace": "prod"}},
        ]

        step = sign_step(
            priv, identity, content=manifests,
            constraints=[
                namespace_constraint("prod"),
                allowed_gvks_constraint({"apps/v1/Deployment", "v1/Service"}),
            ],
        )

        chain = AttestationChain(steps=[step])
        verify_chain(chain, manifests)

    def test_gvk_violation(self, user_keys):
        priv, identity = user_keys

        manifests = [
            {"apiVersion": "apps/v1", "kind": "DaemonSet",
             "metadata": {"name": "agent", "namespace": "prod"}},
        ]

        step = sign_step(
            priv, identity, content=manifests,
            constraints=[
                allowed_gvks_constraint({"apps/v1/Deployment", "v1/Service"}),
            ],
        )

        chain = AttestationChain(steps=[step])
        with pytest.raises(VerificationError, match="constraint failed"):
            verify_chain(chain, manifests)
