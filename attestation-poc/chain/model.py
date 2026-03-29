"""
Core data model for provenance chain attestations.

An Attestation is a node in a directed graph with two halves:

  - INPUT: what was authorized (the spec / intent).
  - OUTPUT: what was produced (manifests, an update instruction, …).

The input determines how the output must be verified.  Verification
starts from the input: its provenance tells us who authorized it,
and the spec's content implies what constraints the output must
satisfy.  Only after the input is verified do we check the output
against those constraints.

Attestations reference other attestations through their provenance,
forming a graph.  The verifier walks this graph recursively.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# Trust anchors
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TrustAnchor:
    """
    An external trust root: an OIDC issuer, SPIFFE trust domain, or
    static key registry.

    *known_keys* maps an identity (user id or addon id) to a public
    key.  In a real system this would be JWKS / trust-bundle resolution;
    here we inline the mapping for prototype simplicity.
    """

    anchor_id: str
    known_keys: dict[str, bytes]  # identity → Ed25519 public key bytes


# ---------------------------------------------------------------------------
# Cryptographic primitives carried inside attestations
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Signature:
    signer_id: str
    public_key: bytes
    content_hash: bytes
    signature_bytes: bytes


@dataclass(frozen=True)
class KeyBinding:
    """
    Ties a user's signing key to their identity via an external trust
    anchor.  Simplified from the full model (JWT + proof-of-possession
    bundle) to: the user signed {user_id, public_key, trust_anchor_id}
    with their own private key, and the trust anchor independently
    recognises the public key.
    """

    user_id: str
    public_key: bytes
    trust_anchor_id: str
    binding_proof: bytes  # PoP: sign(private_key, hash(binding_doc))


# ---------------------------------------------------------------------------
# Input provenance variants
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class UserSignedInput:
    """A user directly signed this input spec."""

    signature: Signature
    key_binding: KeyBinding


@dataclass(frozen=True)
class UpdateDerivedInput:
    """
    This input was derived by applying a deterministic mutation
    expression to a prior attestation's input spec, where the
    mutation came from another (fully verified) attestation's output.
    """

    prior_attestation_id: str
    update_attestation_id: str
    mutation_expr: str  # deterministic expression (CEL-like); re-executable


InputProvenance = UserSignedInput | UpdateDerivedInput


# ---------------------------------------------------------------------------
# Output provenance variants
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AddonSignature:
    """An addon generated and signed this output."""

    addon_id: str
    signature: Signature
    trust_anchor_id: str


OutputProvenance = AddonSignature | None
# None → output needs no independent signature (e.g. inline manifests
# where the user signed the spec and the output IS the spec content).


# ---------------------------------------------------------------------------
# The attestation node
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Attestation:
    """
    A node in the provenance graph.

    *input_spec*       – what was authorized.
    *input_provenance*  – how the input is authenticated.
    *output*           – what was produced.
    *output_provenance* – how the output is authenticated (may be None).
    """

    attestation_id: str
    input_spec: dict[str, Any]
    input_provenance: InputProvenance
    output: dict[str, Any]
    output_provenance: OutputProvenance
