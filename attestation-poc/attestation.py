"""
Attestation chain prototype.

Proves out the generalized attestation model where every verification
scenario (user intent signing, addon co-signing, CEL-based update
derivations, structural constraints) is handled by one algorithm
walking a uniform chain of steps.

Two step types:
  - Signature: a principal signed content. Verified cryptographically.
  - Derivation: content was deterministically produced from a prior
    step's content. Verified by re-execution.

Signature steps can carry constraints (predicates over the terminal
applied content). Constraints are CEL-like expressions evaluated in
phase 3 of verification.

Run: python attestation.py
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ec import (
    ECDSA,
    SECP256R1,
    EllipticCurvePrivateKey,
    EllipticCurvePublicKey,
    generate_private_key,
)
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
    load_der_public_key,
)


# ---------------------------------------------------------------------------
# Crypto helpers
# ---------------------------------------------------------------------------

def generate_keypair() -> tuple[EllipticCurvePrivateKey, bytes]:
    """Returns (private_key, der_encoded_public_key)."""
    private = generate_private_key(SECP256R1())
    pub_der = private.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    return private, pub_der


def sign(private_key: EllipticCurvePrivateKey, data: bytes) -> bytes:
    return private_key.sign(data, ECDSA(SHA256()))


def verify_sig(public_key_der: bytes, signature: bytes, data: bytes) -> bool:
    pubkey = load_der_public_key(public_key_der)
    if not isinstance(pubkey, EllipticCurvePublicKey):
        return False
    try:
        pubkey.verify(signature, data, ECDSA(SHA256()))
        return True
    except InvalidSignature:
        return False


def content_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


# ---------------------------------------------------------------------------
# Identity types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class UserIdentity:
    subject: str
    issuer: str
    public_key: bytes  # raw EC public key bytes


@dataclass(frozen=True)
class AddonIdentity:
    addon_id: str  # e.g. "spiffe://fleet-addons/capi-provisioner"
    public_key: bytes


Identity = UserIdentity | AddonIdentity


# ---------------------------------------------------------------------------
# Constraint: a predicate over (signed_content, applied_content)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Constraint:
    description: str
    # (signed_content_dict, applied_manifests_list) -> bool
    predicate: Callable[[dict, list[dict]], bool]


# ---------------------------------------------------------------------------
# Chain step types
# ---------------------------------------------------------------------------

class StepType(Enum):
    SIGNATURE = "signature"
    DERIVATION = "derivation"


@dataclass
class SignatureStep:
    """A principal signed some content."""
    type: StepType = field(default=StepType.SIGNATURE, init=False)
    content: dict                        # the structured content that was signed
    content_hash: str = ""               # sha256 hex of canonical JSON
    signature: bytes = b""               # ECDSA signature over the hash
    signer: Identity | None = None
    valid_until: float = 0.0             # unix timestamp
    authorized_next_signers: list[str] = field(default_factory=list)
    constraints: list[Constraint] = field(default_factory=list)

    def __post_init__(self):
        if not self.content_hash:
            self.content_hash = content_hash(canonical_json(self.content))


@dataclass
class DerivationStep:
    """Content deterministically derived from a prior step."""
    type: StepType = field(default=StepType.DERIVATION, init=False)
    content: dict                         # the derived output
    content_hash: str = ""
    # f(input_dict) -> output_dict; the deterministic derivation rule
    derivation_fn: Callable[[dict], dict] | None = None
    derivation_description: str = ""      # human-readable (e.g. the CEL expression)
    input_content: dict = field(default_factory=dict)
    input_hash: str = ""

    def __post_init__(self):
        if not self.content_hash:
            self.content_hash = content_hash(canonical_json(self.content))
        if self.input_content and not self.input_hash:
            self.input_hash = content_hash(canonical_json(self.input_content))


Step = SignatureStep | DerivationStep


# ---------------------------------------------------------------------------
# Attestation chain
# ---------------------------------------------------------------------------

@dataclass
class AttestationChain:
    steps: list[Step]
    deployment_id: str = ""


# ---------------------------------------------------------------------------
# Chain construction helpers
# ---------------------------------------------------------------------------

def _signable_envelope(content: dict, valid_until: float) -> bytes:
    """The canonical bytes that get signed: content + temporal bound."""
    envelope = {"content": content, "valid_until": valid_until}
    return canonical_json(envelope)


def sign_step(
    private_key: EllipticCurvePrivateKey,
    identity: Identity,
    content: dict,
    valid_duration_sec: float = 86400,
    authorized_next_signers: list[str] | None = None,
    constraints: list[Constraint] | None = None,
) -> SignatureStep:
    """Build a signed step: compute canonical hash and sign it."""
    valid_until = time.time() + valid_duration_sec
    envelope_bytes = _signable_envelope(content, valid_until)
    c_hash = content_hash(canonical_json(content))
    sig = sign(private_key, envelope_bytes)
    return SignatureStep(
        content=content,
        content_hash=c_hash,
        signature=sig,
        signer=identity,
        valid_until=valid_until,
        authorized_next_signers=authorized_next_signers or [],
        constraints=constraints or [],
    )


def derive_step(
    derivation_fn: Callable[[dict], dict],
    input_content: dict,
    description: str = "",
) -> DerivationStep:
    """Build a derivation step: apply the function and record input/output."""
    output = derivation_fn(input_content)
    return DerivationStep(
        content=output,
        derivation_fn=derivation_fn,
        derivation_description=description,
        input_content=input_content,
    )


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

class VerificationError(Exception):
    pass


def _signer_id(identity: Identity) -> str:
    if isinstance(identity, UserIdentity):
        return f"user:{identity.subject}@{identity.issuer}"
    return f"addon:{identity.addon_id}"


def _next_signer_step(chain: list[Step], after: int) -> SignatureStep | None:
    for s in chain[after + 1 :]:
        if isinstance(s, SignatureStep):
            return s
    return None


def _has_constraints(steps: list[Step]) -> bool:
    return any(
        isinstance(s, SignatureStep) and s.constraints for s in steps
    )


def verify_chain(chain: AttestationChain, applied_manifests: list[dict]) -> None:
    """
    Verify an attestation chain against applied manifests.
    Raises VerificationError on failure.

    Two phases:
      1. Walk chain, verify each step (signatures and derivations).
      2. Binding check: the chain must be bound to the applied content
         via either terminal hash match or constraints (or both).

    Binding modes:
      - Terminating: the last step's content hash matches the applied
        content hash. This is the strongest binding — the chain proves
        exactly what should be applied. Constraints are defense-in-depth.
      - Constraining: the chain does not terminate at the applied content,
        but signature steps carry constraints that validate it. This is
        the binding for intent signing where the intent is structurally
        different from the manifests.
      - Unbound: neither terminal match nor constraints. Rejected — the
        chain floats in space with no connection to what's being applied.
    """
    steps = chain.steps
    if not steps:
        raise VerificationError("empty chain")

    applied_hash = content_hash(canonical_json(applied_manifests))

    # Phase 1: verify each step
    for i, step in enumerate(steps):
        if isinstance(step, SignatureStep):
            envelope_bytes = _signable_envelope(step.content, step.valid_until)
            if not verify_sig(step.signer.public_key, step.signature, envelope_bytes):
                raise VerificationError(
                    f"step {i}: signature verification failed for {_signer_id(step.signer)}"
                )

            c_bytes = canonical_json(step.content)
            if content_hash(c_bytes) != step.content_hash:
                raise VerificationError(f"step {i}: content hash mismatch")

            if time.time() > step.valid_until:
                raise VerificationError(f"step {i}: expired")

            if step.authorized_next_signers:
                nxt = _next_signer_step(steps, i)
                if nxt is None:
                    raise VerificationError(
                        f"step {i}: authorizes next signers but no subsequent signature step"
                    )
                nxt_id = _signer_id(nxt.signer)
                if nxt_id not in step.authorized_next_signers:
                    raise VerificationError(
                        f"step {i}: next signer {nxt_id} not in authorized list"
                    )

        elif isinstance(step, DerivationStep):
            if step.derivation_fn is None:
                raise VerificationError(f"step {i}: no derivation function")
            computed = step.derivation_fn(step.input_content)
            computed_hash = content_hash(canonical_json(computed))
            if computed_hash != step.content_hash:
                raise VerificationError(
                    f"step {i}: derivation re-execution produced different output"
                )

            if step.input_hash:
                actual_input_hash = content_hash(canonical_json(step.input_content))
                if actual_input_hash != step.input_hash:
                    raise VerificationError(f"step {i}: input hash mismatch")

    # Phase 2: binding check
    last = steps[-1]
    terminates = last.content_hash == applied_hash
    has_constraints = _has_constraints(steps)

    if not terminates and not has_constraints:
        raise VerificationError(
            "unbound chain: does not terminate at applied content "
            "and carries no constraints"
        )

    # Evaluate all constraints against applied content.
    # For terminating chains this is defense-in-depth.
    # For constraining chains this is the sole binding.
    for i, step in enumerate(steps):
        if isinstance(step, SignatureStep):
            for c in step.constraints:
                if not c.predicate(step.content, applied_manifests):
                    raise VerificationError(
                        f"step {i}: constraint failed: {c.description}"
                    )
