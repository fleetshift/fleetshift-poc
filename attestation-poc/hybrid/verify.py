"""Verification for the hybrid attestation prototype."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .model import Attestation, TrustAnchor, VerifiedOutput


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
class VerificationContext:
    attestation_store: AttestationStore
    trust_store: TrustStore

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

    def now(self) -> float:
        return time.time()


def verify_attestation(
    attestation: Attestation,
    attestation_store: AttestationStore,
    trust_store: TrustStore,
) -> VerifiedOutput:
    context = VerificationContext(
        attestation_store=attestation_store,
        trust_store=trust_store,
    )
    result, _, verified_output = attestation.verify(context, frozenset())
    if not result.valid or verified_output is None:
        raise VerificationError(result.pretty(), result)
    return verified_output


def explain_verification(
    attestation: Attestation,
    attestation_store: AttestationStore,
    trust_store: TrustStore,
) -> VerificationResult:
    context = VerificationContext(
        attestation_store=attestation_store,
        trust_store=trust_store,
    )
    result, _, _ = attestation.verify(context, frozenset())
    return result
