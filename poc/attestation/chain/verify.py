"""
Provenance chain verification algorithm.

The verifier walks the attestation graph recursively:

  1. Start at the terminal attestation.
  2. Examine its input to determine output constraints.
  3. Verify input provenance:
       - User-signed → check signature, key binding, trust anchor.
       - Update-derived → verify prior attestation's INPUT (not output),
         verify update attestation FULLY (input AND output), re-execute
         the mutation expression and confirm the result matches.
  4. Once the input is verified, derive output constraints from the
     (now-trusted) input spec and verify the output against them.

Each step produces a VerificationResult node; together they form a
tree that shows the full verification path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import crypto, mutation
from .model import (
    AddonSignature,
    Attestation,
    KeyBinding,
    Signature,
    TrustAnchor,
    UpdateDerivedInput,
    UserSignedInput,
)


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------

class AttestationStore:
    """Registry of attestations for graph traversal."""

    def __init__(self) -> None:
        self._store: dict[str, Attestation] = {}

    def add(self, att: Attestation) -> None:
        self._store[att.attestation_id] = att

    def get(self, attestation_id: str) -> Attestation | None:
        return self._store.get(attestation_id)


class TrustStore:
    """Registry of trust anchors."""

    def __init__(self) -> None:
        self._store: dict[str, TrustAnchor] = {}

    def add(self, anchor: TrustAnchor) -> None:
        self._store[anchor.anchor_id] = anchor

    def get(self, anchor_id: str) -> TrustAnchor | None:
        return self._store.get(anchor_id)


# ---------------------------------------------------------------------------
# Verification result tree
# ---------------------------------------------------------------------------

@dataclass
class VerificationResult:
    valid: bool
    label: str
    detail: str = ""
    children: list[VerificationResult] = field(default_factory=list)

    def pretty(self, indent: int = 0) -> str:
        icon = "✓" if self.valid else "✗"
        header = f"{'  ' * indent}{icon} {self.label}"
        if self.detail:
            header += f": {self.detail}"
        lines = [header]
        for child in self.children:
            lines.append(child.pretty(indent + 1))
        return "\n".join(lines)

    def __str__(self) -> str:
        return self.pretty()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def verify_attestation(
    attestation: Attestation,
    attestation_store: AttestationStore,
    trust_store: TrustStore,
    *,
    _visited: frozenset[str] = frozenset(),
) -> VerificationResult:
    att_id = attestation.attestation_id

    if att_id in _visited:
        return _fail(att_id, "cycle detected in attestation graph")
    visited = _visited | {att_id}

    input_result = _verify_input(
        attestation, attestation_store, trust_store, visited
    )
    if not input_result.valid:
        return _fail(att_id, "input verification failed", [input_result])

    constraints = _derive_output_constraints(attestation.input_spec)
    output_result = _verify_output(attestation, constraints, trust_store)
    if not output_result.valid:
        return _fail(
            att_id, "output verification failed", [input_result, output_result]
        )

    return VerificationResult(
        valid=True,
        label=att_id,
        detail="fully verified",
        children=[input_result, output_result],
    )


# ---------------------------------------------------------------------------
# Input verification
# ---------------------------------------------------------------------------

def _verify_input(
    att: Attestation,
    att_store: AttestationStore,
    trust_store: TrustStore,
    visited: frozenset[str],
) -> VerificationResult:
    match att.input_provenance:
        case UserSignedInput() as prov:
            return _verify_user_signed(att, prov, trust_store)
        case UpdateDerivedInput() as prov:
            return _verify_update_derived(att, prov, att_store, trust_store, visited)
        case _:  # pragma: no cover
            return _fail(att.attestation_id, f"unknown input provenance type")


def _verify_user_signed(
    att: Attestation,
    prov: UserSignedInput,
    trust_store: TrustStore,
) -> VerificationResult:
    label = f"{att.attestation_id} input"
    kb = prov.key_binding

    # 1. Trust anchor exists
    anchor = trust_store.get(kb.trust_anchor_id)
    if anchor is None:
        return _fail(label, f"trust anchor not found: {kb.trust_anchor_id}")

    # 2. Trust anchor recognises this user's public key
    known = anchor.known_keys.get(kb.user_id)
    if known is None or known != kb.public_key:
        return _fail(label, f"key not recognised by anchor {anchor.anchor_id}")

    # 3. Proof of possession on the key binding
    binding_hash = crypto.content_hash(
        {
            "user_id": kb.user_id,
            "public_key": kb.public_key.hex(),
            "trust_anchor_id": kb.trust_anchor_id,
        }
    )
    if not crypto.verify(kb.public_key, binding_hash, kb.binding_proof):
        return _fail(label, "key binding proof-of-possession failed")

    # 4. Signature key must match the key binding (continuity)
    sig = prov.signature
    if sig.public_key != kb.public_key:
        return _fail(label, "signature key does not match key binding")

    # 5. Signature over the input spec
    input_hash = crypto.content_hash(att.input_spec)
    if sig.content_hash != input_hash:
        return _fail(label, "input spec hash mismatch")
    if not crypto.verify(sig.public_key, input_hash, sig.signature_bytes):
        return _fail(label, f"signature verification failed for {sig.signer_id}")

    return VerificationResult(
        valid=True,
        label=label,
        detail=f"signed by {kb.user_id}, verified against {kb.trust_anchor_id}",
    )


def _verify_update_derived(
    att: Attestation,
    prov: UpdateDerivedInput,
    att_store: AttestationStore,
    trust_store: TrustStore,
    visited: frozenset[str],
) -> VerificationResult:
    label = f"{att.attestation_id} input"
    children: list[VerificationResult] = []

    # --- Resolve referenced attestations --------------------------------
    prior = att_store.get(prov.prior_attestation_id)
    if prior is None:
        return _fail(label, f"prior attestation not found: {prov.prior_attestation_id}")

    update = att_store.get(prov.update_attestation_id)
    if update is None:
        return _fail(label, f"update attestation not found: {prov.update_attestation_id}")

    # --- (a) Verify the prior attestation's INPUT only ------------------
    # The update replaces the prior's output; we only need to know the
    # prior's spec was legitimately authorized.
    prior_input = _verify_input(prior, att_store, trust_store, visited)
    children.append(prior_input)
    if not prior_input.valid:
        return _fail(label, "prior attestation input failed", children)

    # --- (b) Verify the update attestation FULLY ------------------------
    # We are consuming the update's *output*, so both its input and
    # output must be verified.
    update_result = verify_attestation(update, att_store, trust_store, _visited=visited)
    children.append(update_result)
    if not update_result.valid:
        return _fail(label, "update attestation failed", children)

    # --- (c) Mutation must match the update's output --------------------
    update_mutation = update.output.get("mutation")
    if update_mutation != prov.mutation_expr:
        return _fail(
            label,
            "mutation expression does not match update attestation output",
            children,
        )

    # --- (d) Re-execute the mutation and confirm the result -------------
    expected = mutation.apply_mutation(prov.mutation_expr, prior.input_spec)
    if expected != att.input_spec:
        return _fail(
            label,
            "mutation re-execution produced a different spec",
            children,
        )

    return VerificationResult(
        valid=True,
        label=label,
        detail=(
            f"derived from prior={prov.prior_attestation_id}"
            f" + update={prov.update_attestation_id}, mutation verified"
        ),
        children=children,
    )


# ---------------------------------------------------------------------------
# Output constraint derivation
# ---------------------------------------------------------------------------

def _derive_output_constraints(input_spec: dict[str, Any]) -> list[dict[str, Any]]:
    """
    The input spec *implies* what the output must satisfy.

    manifest_strategy.type == "addon"  →  addon must sign the output
    manifest_strategy.type == "inline" →  no additional signature needed
    """
    constraints: list[dict[str, Any]] = []
    strategy = input_spec.get("manifest_strategy", {})
    if strategy.get("type") == "addon":
        constraints.append(
            {
                "type": "addon_signed",
                "addon_id": strategy["addon"],
                "trust_anchor_id": strategy.get("trust_anchor", strategy["addon"]),
            }
        )
    return constraints


# ---------------------------------------------------------------------------
# Output verification
# ---------------------------------------------------------------------------

def _verify_output(
    att: Attestation,
    constraints: list[dict[str, Any]],
    trust_store: TrustStore,
) -> VerificationResult:
    label = f"{att.attestation_id} output"
    children: list[VerificationResult] = []

    for c in constraints:
        if c["type"] == "addon_signed":
            r = _verify_addon_signed(att, c, trust_store)
            children.append(r)
            if not r.valid:
                return _fail(label, "addon signature constraint failed", children)

    if not constraints:
        children.append(
            VerificationResult(
                valid=True, label=label, detail="no output constraints"
            )
        )

    return VerificationResult(
        valid=True, label=label, detail="satisfies all constraints", children=children
    )


def _verify_addon_signed(
    att: Attestation,
    constraint: dict[str, Any],
    trust_store: TrustStore,
) -> VerificationResult:
    label = f"{att.attestation_id} output addon"
    required_addon = constraint["addon_id"]
    required_anchor = constraint["trust_anchor_id"]

    if not isinstance(att.output_provenance, AddonSignature):
        return _fail(label, f"not addon-signed (expected {required_addon})")

    prov = att.output_provenance
    if prov.addon_id != required_addon:
        return _fail(
            label,
            f"signed by {prov.addon_id}, expected {required_addon}",
        )

    # Signature over the output content
    output_hash = crypto.content_hash(att.output)
    sig = prov.signature
    if sig.content_hash != output_hash:
        return _fail(label, "output hash mismatch")
    if not crypto.verify(sig.public_key, output_hash, sig.signature_bytes):
        return _fail(label, f"addon signature verification failed")

    # Addon key recognised by its trust anchor
    anchor = trust_store.get(required_anchor)
    if anchor is None:
        return _fail(label, f"trust anchor not found: {required_anchor}")
    known = anchor.known_keys.get(prov.addon_id)
    if known is None or known != sig.public_key:
        return _fail(label, f"addon key not recognised by {anchor.anchor_id}")

    return VerificationResult(
        valid=True,
        label=label,
        detail=f"signed by {prov.addon_id}, verified against {required_anchor}",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fail(
    label: str,
    detail: str,
    children: list[VerificationResult] | None = None,
) -> VerificationResult:
    return VerificationResult(
        valid=False,
        label=label,
        detail=detail,
        children=children or [],
    )
