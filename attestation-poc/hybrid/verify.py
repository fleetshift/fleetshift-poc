"""Verification for the hybrid attestation prototype."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .crypto import content_hash, verify
from .mutation import apply_update, derive_constraints
from .model import (
    AddonSignedConstraint,
    AllowedGVKsConstraint,
    Attestation,
    DerivedInput,
    KeyBinding,
    NamespaceConstraint,
    NoClusterAdminConstraint,
    Output,
    OutputSignature,
    SignedInput,
    TrustAnchor,
    VerifiedOutput,
)
from .policy import describe_constraint, signed_input_envelope


class VerificationError(Exception):
    """Raised when attestation verification fails."""

    def __init__(self, message: str, result: VerificationResult) -> None:
        super().__init__(message)
        self.result = result


class AttestationStore:
    """Registry of attestations for graph traversal."""

    def __init__(self) -> None:
        self._store: dict[str, Attestation] = {}

    def add(self, attestation: Attestation) -> None:
        self._store[attestation.attestation_id] = attestation

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


@dataclass(frozen=True)
class _VerifiedInput:
    content: Any
    content_hash: bytes
    output_constraints: tuple[
        AddonSignedConstraint
        | NamespaceConstraint
        | AllowedGVKsConstraint
        | NoClusterAdminConstraint,
        ...,
    ]
    signer_id: str | None = None


def verify_attestation(
    attestation: Attestation,
    attestation_store: AttestationStore,
    trust_store: TrustStore,
) -> VerifiedOutput:
    result, _, verified_output = _verify_attestation(
        attestation,
        attestation_store,
        trust_store,
        frozenset(),
    )
    if not result.valid or verified_output is None:
        raise VerificationError(result.pretty(), result)
    return verified_output


def explain_verification(
    attestation: Attestation,
    attestation_store: AttestationStore,
    trust_store: TrustStore,
) -> VerificationResult:
    result, _, _ = _verify_attestation(
        attestation,
        attestation_store,
        trust_store,
        frozenset(),
    )
    return result


def _verify_attestation(
    attestation: Attestation,
    attestation_store: AttestationStore,
    trust_store: TrustStore,
    visited: frozenset[str],
) -> tuple[VerificationResult, _VerifiedInput | None, VerifiedOutput | None]:
    if attestation.attestation_id in visited:
        return _fail(attestation.attestation_id, "cycle detected in attestation graph"), None, None

    next_visited = visited | {attestation.attestation_id}
    input_result, verified_input = _verify_input(
        attestation,
        attestation_store,
        trust_store,
        next_visited,
    )
    if not input_result.valid or verified_input is None:
        return _fail(attestation.attestation_id, "input verification failed", [input_result]), None, None

    output_result, verified_output = _verify_output(
        attestation.attestation_id,
        attestation.output,
        verified_input,
        trust_store,
    )
    if not output_result.valid or verified_output is None:
        return (
            _fail(attestation.attestation_id, "output verification failed", [input_result, output_result]),
            verified_input,
            None,
        )

    return (
        VerificationResult(
            valid=True,
            label=attestation.attestation_id,
            detail="fully verified",
            children=[input_result, output_result],
        ),
        verified_input,
        verified_output,
    )


def _verify_attestation_input_only(
    attestation: Attestation,
    attestation_store: AttestationStore,
    trust_store: TrustStore,
    visited: frozenset[str],
) -> tuple[VerificationResult, _VerifiedInput | None]:
    if attestation.attestation_id in visited:
        return _fail(
            f"{attestation.attestation_id} input",
            "cycle detected in attestation graph",
        ), None

    next_visited = visited | {attestation.attestation_id}
    return _verify_input(attestation, attestation_store, trust_store, next_visited)


def _verify_input(
    attestation: Attestation,
    attestation_store: AttestationStore,
    trust_store: TrustStore,
    visited: frozenset[str],
) -> tuple[VerificationResult, _VerifiedInput | None]:
    input_data = attestation.input
    match input_data:
        case SignedInput():
            return _verify_signed_input(attestation.attestation_id, input_data, trust_store)
        case DerivedInput():
            return _verify_derived_input(
                attestation.attestation_id,
                input_data,
                attestation_store,
                trust_store,
                visited,
            )
        case _:
            return _fail(
                f"{attestation.attestation_id} input",
                f"unknown input type: {type(input_data).__name__}",
            ), None


def _verify_signed_input(
    attestation_id: str,
    signed_input: SignedInput,
    trust_store: TrustStore,
) -> tuple[VerificationResult, _VerifiedInput | None]:
    label = f"{attestation_id} input"
    key_binding = signed_input.key_binding
    signature = signed_input.signature

    if key_binding.signer_id != signature.signer_id:
        return _fail(label, "signature signer does not match key binding"), None

    anchor = trust_store.get(key_binding.trust_anchor_id)
    if anchor is None:
        return _fail(label, f"trust anchor not found: {key_binding.trust_anchor_id}"), None

    known_key = anchor.known_keys.get(key_binding.signer_id)
    if known_key is None or known_key != key_binding.public_key:
        return _fail(label, f"key not recognised by anchor {anchor.anchor_id}"), None

    binding_result = _verify_key_binding(key_binding)
    if not binding_result.valid:
        return _fail(label, "key binding failed", [binding_result]), None

    if signature.public_key != key_binding.public_key:
        return _fail(label, "signature key does not match key binding"), None

    envelope = signed_input_envelope(
        signed_input.content,
        signed_input.valid_until,
        signed_input.output_constraints,
    )
    envelope_hash = content_hash(envelope)
    if signature.content_hash != envelope_hash:
        return _fail(label, "signed input hash mismatch"), None
    if not verify(signature.public_key, envelope_hash, signature.signature_bytes):
        return _fail(label, f"signature verification failed for {signature.signer_id}"), None
    if time.time() > signed_input.valid_until:
        return _fail(label, f"expired: {signature.signer_id}"), None

    detail = (
        f"signed by {signature.signer_id}, verified against {key_binding.trust_anchor_id}, "
        f"{len(signed_input.output_constraints)} constraints"
    )
    return (
        VerificationResult(valid=True, label=label, detail=detail),
        _VerifiedInput(
            content=signed_input.content,
            content_hash=content_hash(signed_input.content),
            output_constraints=signed_input.output_constraints,
            signer_id=signature.signer_id,
        ),
    )


def _verify_derived_input(
    attestation_id: str,
    derived_input: DerivedInput,
    attestation_store: AttestationStore,
    trust_store: TrustStore,
    visited: frozenset[str],
) -> tuple[VerificationResult, _VerifiedInput | None]:
    label = f"{attestation_id} input"
    children: list[VerificationResult] = []

    prior_attestation = attestation_store.get(derived_input.prior_attestation_id)
    if prior_attestation is None:
        return _fail(label, f"prior attestation not found: {derived_input.prior_attestation_id}"), None

    update_attestation = attestation_store.get(derived_input.update_attestation_id)
    if update_attestation is None:
        return _fail(label, f"update attestation not found: {derived_input.update_attestation_id}"), None

    prior_result, verified_prior = _verify_attestation_input_only(
        prior_attestation,
        attestation_store,
        trust_store,
        visited,
    )
    children.append(prior_result)
    if not prior_result.valid or verified_prior is None:
        return _fail(label, "prior input verification failed", children), None

    update_result, _, verified_update_output = _verify_attestation(
        update_attestation,
        attestation_store,
        trust_store,
        visited,
    )
    children.append(update_result)
    if not update_result.valid or verified_update_output is None:
        return _fail(label, "update attestation verification failed", children), None

    try:
        derived_content = apply_update(verified_prior.content, verified_update_output.content)
        output_constraints = derive_constraints(verified_update_output.content, derived_content)
    except Exception as exc:
        return _fail(label, f"derivation failed: {exc}", children), None

    return (
        VerificationResult(
            valid=True,
            label=label,
            detail=(
                f"derived from prior={derived_input.prior_attestation_id} "
                f"+ update={derived_input.update_attestation_id}"
            ),
            children=children,
        ),
        _VerifiedInput(
            content=derived_content,
            content_hash=content_hash(derived_content),
            output_constraints=output_constraints,
        ),
    )


def _verify_output(
    attestation_id: str,
    output: Output,
    verified_input: _VerifiedInput,
    trust_store: TrustStore,
) -> tuple[VerificationResult, VerifiedOutput | None]:
    label = f"{attestation_id} output"
    children: list[VerificationResult] = []

    signature_result, verified_output = _verify_output_signature(attestation_id, output, trust_store)
    children.append(signature_result)
    if not signature_result.valid or verified_output is None:
        return _fail(label, "output signature invalid", children), None

    if not verified_input.output_constraints:
        children.append(
            VerificationResult(
                valid=True,
                label=f"{attestation_id} output constraints",
                detail="no output constraints",
            )
        )
        return VerificationResult(valid=True, label=label, detail="satisfies all constraints", children=children), verified_output

    for constraint in verified_input.output_constraints:
        constraint_result = _verify_constraint(
            attestation_id,
            constraint,
            output,
            verified_output,
        )
        children.append(constraint_result)
        if not constraint_result.valid:
            return _fail(label, f"constraint failed: {describe_constraint(constraint)}", children), None

    return VerificationResult(valid=True, label=label, detail="satisfies all constraints", children=children), verified_output


def _verify_output_signature(
    attestation_id: str,
    output: Output,
    trust_store: TrustStore,
) -> tuple[VerificationResult, VerifiedOutput | None]:
    label = f"{attestation_id} output signature"
    output_hash = content_hash(output.content)

    if output.signature is None:
        return (
            VerificationResult(valid=True, label=label, detail="unsigned output"),
            VerifiedOutput(content=output.content, content_hash=output_hash),
        )

    signed_output = output.signature
    signature = signed_output.signature

    if signature.content_hash != output_hash:
        return _fail(label, "output hash mismatch"), None
    if not verify(signature.public_key, output_hash, signature.signature_bytes):
        return _fail(label, f"output signature verification failed for {signature.signer_id}"), None

    anchor = trust_store.get(signed_output.trust_anchor_id)
    if anchor is None:
        return _fail(label, f"trust anchor not found: {signed_output.trust_anchor_id}"), None

    known_key = anchor.known_keys.get(signature.signer_id)
    if known_key is None or known_key != signature.public_key:
        return _fail(label, f"key not recognised by anchor {anchor.anchor_id}"), None

    return (
        VerificationResult(
            valid=True,
            label=label,
            detail=f"signed by {signature.signer_id}, verified against {signed_output.trust_anchor_id}",
        ),
        VerifiedOutput(
            content=output.content,
            content_hash=output_hash,
            signer_id=signature.signer_id,
        ),
    )


def _verify_key_binding(key_binding: KeyBinding) -> VerificationResult:
    label = f"{key_binding.signer_id} key binding"
    binding_doc = {
        "public_key": key_binding.public_key.hex(),
        "signer_id": key_binding.signer_id,
        "trust_anchor_id": key_binding.trust_anchor_id,
    }
    binding_hash = content_hash(binding_doc)
    if not verify(key_binding.public_key, binding_hash, key_binding.binding_proof):
        return _fail(label, "proof-of-possession failed")
    return VerificationResult(valid=True, label=label, detail="proof-of-possession verified")


def _verify_constraint(
    attestation_id: str,
    constraint: AddonSignedConstraint | NamespaceConstraint | AllowedGVKsConstraint | NoClusterAdminConstraint,
    output: Output,
    verified_output: VerifiedOutput,
) -> VerificationResult:
    label = f"{attestation_id} constraint"

    match constraint:
        case AddonSignedConstraint(addon_id=addon_id, trust_anchor_id=trust_anchor_id):
            if output.signature is None:
                return _fail(label, f"unsigned output, expected {addon_id}")
            if output.signature.trust_anchor_id != trust_anchor_id:
                return _fail(
                    label,
                    f"signed via {output.signature.trust_anchor_id}, expected {trust_anchor_id}",
                )
            if verified_output.signer_id != addon_id:
                return _fail(label, f"signed by {verified_output.signer_id}, expected {addon_id}")
            return VerificationResult(
                valid=True,
                label=label,
                detail=f"addon signature matches {addon_id}",
            )

        case NamespaceConstraint(namespace=namespace):
            manifests = _extract_manifests(verified_output.content)
            if manifests is None:
                return _fail(label, "output is not a manifest collection")
            for manifest in manifests:
                if manifest.get("metadata", {}).get("namespace") != namespace:
                    return _fail(label, f"manifest outside namespace {namespace}")
            return VerificationResult(
                valid=True,
                label=label,
                detail=f"all manifests are in namespace {namespace}",
            )

        case AllowedGVKsConstraint(allowed_gvks=allowed_gvks):
            manifests = _extract_manifests(verified_output.content)
            if manifests is None:
                return _fail(label, "output is not a manifest collection")
            allowed = set(allowed_gvks)
            for manifest in manifests:
                gvk = f"{manifest.get('apiVersion', '')}/{manifest.get('kind', '')}"
                if gvk not in allowed:
                    return _fail(label, f"disallowed GVK {gvk}")
            return VerificationResult(
                valid=True,
                label=label,
                detail=f"GVKs limited to {sorted(allowed)}",
            )

        case NoClusterAdminConstraint():
            manifests = _extract_manifests(verified_output.content)
            if manifests is None:
                return _fail(label, "output is not a manifest collection")
            for manifest in manifests:
                if (
                    manifest.get("kind") == "ClusterRoleBinding"
                    and manifest.get("roleRef", {}).get("name") == "cluster-admin"
                ):
                    return _fail(label, "cluster-admin grant detected")
            return VerificationResult(
                valid=True,
                label=label,
                detail="no cluster-admin grant detected",
            )

    return _fail(label, f"unsupported constraint type: {type(constraint).__name__}")


def _extract_manifests(content: Any) -> list[dict[str, Any]] | None:
    if isinstance(content, list) and all(isinstance(item, dict) for item in content):
        return content

    if isinstance(content, dict):
        manifests = content.get("manifests")
        if isinstance(manifests, list) and all(isinstance(item, dict) for item in manifests):
            return manifests

    return None


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
