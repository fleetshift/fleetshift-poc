"""Core data model for the hybrid attestation prototype."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TrustAnchor:
    """An external trust root that maps identities to public keys."""

    anchor_id: str
    known_keys: dict[str, bytes]


@dataclass(frozen=True)
class Signature:
    """A detached signature over a canonical content hash."""

    signer_id: str
    public_key: bytes
    content_hash: bytes
    signature_bytes: bytes


@dataclass(frozen=True)
class OutputSignature:
    """Verification material for a signed output."""

    signature: Signature
    trust_anchor_id: str


@dataclass(frozen=True)
class KeyBinding:
    """Binds a signer's key to an identity via a trust anchor."""

    signer_id: str
    public_key: bytes
    trust_anchor_id: str
    binding_proof: bytes


@dataclass(frozen=True)
class AddonSignedConstraint:
    addon_id: str
    trust_anchor_id: str = "fleet-addons"


@dataclass(frozen=True)
class NamespaceConstraint:
    namespace: str


@dataclass(frozen=True)
class AllowedGVKsConstraint:
    allowed_gvks: tuple[str, ...]


@dataclass(frozen=True)
class NoClusterAdminConstraint:
    pass


OutputConstraint = (
    AddonSignedConstraint
    | NamespaceConstraint
    | AllowedGVKsConstraint
    | NoClusterAdminConstraint
)


@dataclass(frozen=True)
class VerifiedOutput:
    content: Any
    content_hash: bytes
    signer_id: str | None = None


@dataclass(frozen=True)
class Output:
    """Produced content, optionally signed."""

    content: Any
    signature: OutputSignature | None = None


@dataclass(frozen=True)
class SignedInput:
    """A signer's direct authorization of input content and constraints."""

    content: Any
    signature: Signature
    key_binding: KeyBinding
    valid_until: float
    output_constraints: tuple[OutputConstraint, ...] = ()


@dataclass(frozen=True)
class DerivedInput:
    """Input derived from a prior attestation and a verified update."""

    prior_attestation_id: str
    update_attestation_id: str


Input = SignedInput | DerivedInput


@dataclass(frozen=True)
class Attestation:
    """A single attested deployment: input plus output."""

    attestation_id: str
    input: Input
    output: Output
