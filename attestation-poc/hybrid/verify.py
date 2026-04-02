"""Verification for the hybrid attestation prototype."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .model import Attestation, Input, TrustAnchor, VerifiedOutput


class VerificationError(Exception):
    """Raised when attestation verification fails."""

    def __init__(self, message: str, result: VerificationResult) -> None:
        super().__init__(message)
        self.result = result


class TrustStore:
    """Registry of trust anchors (out-of-band trust roots)."""

    def __init__(self) -> None:
        self._store: dict[str, TrustAnchor] = {}

    def add(self, anchor: TrustAnchor) -> None:
        self._store[anchor.anchor_id] = anchor

    def get(self, anchor_id: str) -> TrustAnchor | None:
        return self._store.get(anchor_id)


VerificationRef = tuple[str, str]


@dataclass(frozen=True)
class VerificationBundle:
    """Self-contained material for verifying an attestation graph.

    Carried on the verification request, not fetched from a service.
    All referenced inputs and attestations are here; only trust
    anchors come from the out-of-band [TrustStore].

    Two typed maps enforce the structural distinction:
      - inputs: prior state references (SignedInput | DerivedInput)
      - attestations: full attestations whose output is consumed
    """

    inputs: dict[str, Input] = field(default_factory=dict)
    attestations: dict[str, Attestation] = field(default_factory=dict)

    def get_input(self, input_id: str) -> Input | None:
        return self.inputs.get(input_id)

    def get_attestation(self, attestation_id: str) -> Attestation | None:
        return self.attestations.get(attestation_id)


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
class VerificationContext:
    bundle: VerificationBundle
    trust_store: TrustStore
    target_identity: dict[str, Any] = field(default_factory=dict)

    def input_ref(self, input_id: str) -> VerificationRef:
        return ("input", input_id)

    def attestation_ref(self, attestation_id: str) -> VerificationRef:
        return ("attestation", attestation_id)

    def ok(
        self,
        label: str,
        detail: str = "",
        children: list[VerificationResult] | None = None,
    ) -> VerificationResult:
        return VerificationResult(
            valid=True,
            label=label,
            detail=detail,
            children=children or [],
        )

    def fail(
        self,
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

    def with_target_identity(self, target_identity: dict[str, Any]) -> VerificationContext:
        return VerificationContext(
            bundle=self.bundle,
            trust_store=self.trust_store,
            target_identity=target_identity,
        )

    def now(self) -> float:
        return time.time()


def verify_attestation(
    attestation: Attestation,
    bundle: VerificationBundle,
    trust_store: TrustStore,
    *,
    target_identity: dict[str, Any] | None = None,
) -> VerifiedOutput:
    context = VerificationContext(
        bundle=bundle,
        trust_store=trust_store,
        target_identity=target_identity or {},
    )
    result, _, verified_output = attestation.verify(context, frozenset())
    if not result.valid or verified_output is None:
        raise VerificationError(result.pretty(), result)
    return verified_output


def explain_verification(
    attestation: Attestation,
    bundle: VerificationBundle,
    trust_store: TrustStore,
    *,
    target_identity: dict[str, Any] | None = None,
) -> VerificationResult:
    context = VerificationContext(
        bundle=bundle,
        trust_store=trust_store,
        target_identity=target_identity or {},
    )
    result, _, _ = attestation.verify(context, frozenset())
    return result
