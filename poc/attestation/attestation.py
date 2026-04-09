"""
Attestation model.

An attestation is always input + output -- one shape.

  - Input describes the intent and constrains the output.
  - Output is the produced content (manifests, placement, etc.).

Two kinds of input:

  - SignedInput: a principal signs a spec with output constraints.
  - DerivedInput: computed from a prior input and an update
    attestation's output.  The result is a new spec -- a new input
    -- whose constraints are derived from that computed content.

Constraints live on inputs, never on outputs.  An output only has
constraints when it is itself an input to something else (i.e.
when it is part of an attestation used as an update).

Recursion comes from DerivedInput.update being a full Attestation:
updates are deployments too, with their own inputs and outputs.

Trust anchors (TrustStore) are the only out-of-band input.

Run: pytest test_attestation.py -v
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
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

@dataclass
class KeyPair:
    private_key: EllipticCurvePrivateKey
    public_key_bytes: bytes


def generate_keypair() -> KeyPair:
    """Returns a KeyPair with DER-encoded public key."""
    private = generate_private_key(SECP256R1())
    pub_der = private.public_key().public_bytes(
        Encoding.DER, PublicFormat.SubjectPublicKeyInfo,
    )
    return KeyPair(private, pub_der)


def sign(private_key: EllipticCurvePrivateKey, data: bytes) -> bytes:
    return private_key.sign(data, ECDSA(SHA256()))


def verify_sig(public_key_der: bytes, data: bytes, signature: bytes) -> bool:
    pubkey = load_der_public_key(public_key_der)
    if not isinstance(pubkey, EllipticCurvePublicKey):
        return False
    try:
        pubkey.verify(signature, data, ECDSA(SHA256()))
        return True
    except InvalidSignature:
        return False


def content_hash(obj: Any) -> bytes:
    """Deterministic SHA-256 hash of arbitrary content via canonical JSON."""
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).digest()


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class VerificationError(Exception):
    pass


# ---------------------------------------------------------------------------
# Trust anchors
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TrustAnchor:
    anchor_id: str
    known_keys: dict[str, bytes]  # signer_id -> DER-encoded public key


class TrustStore:
    def __init__(self) -> None:
        self._anchors: dict[str, TrustAnchor] = {}

    def add(self, anchor: TrustAnchor) -> None:
        self._anchors[anchor.anchor_id] = anchor

    def get(self, anchor_id: str) -> TrustAnchor | None:
        return self._anchors.get(anchor_id)


# ---------------------------------------------------------------------------
# Key binding
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KeyBinding:
    """Ties a signing key to an identity via a trust anchor.

    binding_proof is a proof-of-possession: the private key signs
    the canonical hash of {signer_id, public_key, trust_anchor_id}.
    """

    signer_id: str
    public_key: bytes
    trust_anchor_id: str
    binding_proof: bytes


# ---------------------------------------------------------------------------
# Verification result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class VerifiedOutput:
    """Public result of attestation verification.

    This is what the delivery agent sees: content to apply, plus
    the output signer if the output was signed.  No constraints --
    those are consumed internally during verification.
    """

    content: Any
    content_hash: bytes
    signer_id: str | None = None


@dataclass(frozen=True)
class OutputConstraint:
    """A predicate the authority places on the output.

    check receives (input_content, verified_output).
    """

    description: str
    check: Callable[[Any, VerifiedOutput], bool]


@dataclass(frozen=True)
class _VerifiedInput:
    """Internal result of input verification.  Carries constraints."""

    content: Any
    content_hash: bytes
    output_constraints: list[OutputConstraint]
    signer_id: str | None = None


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Output:
    """Produced content, optionally signed.

    Whether a signature is required is determined by the input's
    constraints, not by the Output itself.
    """

    content: Any
    signer_id: str | None = None
    public_key: bytes | None = None
    signature: bytes | None = None
    key_binding: KeyBinding | None = None


# ---------------------------------------------------------------------------
# SignedInput
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SignedInput:
    """A principal's signed content with explicit output constraints.

    The fundamental authority unit.  Constraints are part of the
    signed envelope: they are the authority's assertion about what
    the output must look like.
    """

    content: Any
    signer_id: str
    public_key: bytes
    signature: bytes
    valid_until: float
    key_binding: KeyBinding
    output_constraints: list[OutputConstraint] = field(default_factory=list)

    def verify(
        self,
        trust_store: TrustStore,
        _visited: frozenset[int] = frozenset(),
    ) -> _VerifiedInput:
        _check_cycle(self, _visited)

        envelope = _signed_envelope(
            self.content, self.valid_until, self.output_constraints,
        )
        envelope_hash = content_hash(envelope)
        if not verify_sig(self.public_key, envelope_hash, self.signature):
            raise VerificationError(
                f"signature verification failed for {self.signer_id}"
            )

        if time.time() > self.valid_until:
            raise VerificationError(f"expired: {self.signer_id}")

        _verify_key_binding(
            self.key_binding, self.signer_id, self.public_key, trust_store,
        )

        return _VerifiedInput(
            content=self.content,
            content_hash=content_hash(self.content),
            output_constraints=self.output_constraints,
            signer_id=self.signer_id,
        )


# ---------------------------------------------------------------------------
# DerivedInput
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DerivedInput:
    """Input computed from a prior input and an update attestation.

    prior is SignedInput | DerivedInput (never a full Attestation --
    the prior's output is structurally irrelevant).

    update is a full Attestation -- we verify it and consume its
    output.  Updates are deployments targeting other deployments;
    this is where recursion enters.

    apply takes (prior_content, update_output) and returns
    (new_content, output_constraints).  The result is a new spec
    whose constraints are derived from that content.

    TODO: The apply callable should ideally come from the update
    attestation's content rather than being a separate function.
    """

    prior: SignedInput | DerivedInput
    update: Attestation
    apply: Callable[[Any, Any], tuple[Any, list[OutputConstraint]]]

    def verify(
        self,
        trust_store: TrustStore,
        _visited: frozenset[int] = frozenset(),
    ) -> _VerifiedInput:
        _check_cycle(self, _visited)
        visited = _visited | {id(self)}

        prior_result = self.prior.verify(trust_store, visited)
        update_result = self.update.verify(trust_store, visited)

        try:
            derived_content, output_constraints = self.apply(
                prior_result.content, update_result.content,
            )
        except VerificationError:
            raise
        except Exception as exc:
            raise VerificationError(f"derivation failed: {exc}") from exc

        return _VerifiedInput(
            content=derived_content,
            content_hash=content_hash(derived_content),
            output_constraints=output_constraints,
        )


# ---------------------------------------------------------------------------
# Attestation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Attestation:
    """An attested deployment: input + output.

    The input (SignedInput or DerivedInput) describes the intent and
    carries constraints.  The output is verified against those
    constraints.  This is the only attestation shape.

    verify() returns the verified output -- what the delivery agent
    applies.  Constraint enforcement is internal.
    """

    input: SignedInput | DerivedInput
    output: Output

    def verify(
        self,
        trust_store: TrustStore,
        _visited: frozenset[int] = frozenset(),
    ) -> VerifiedOutput:
        _check_cycle(self, _visited)
        visited = _visited | {id(self)}

        input_result = self.input.verify(trust_store, visited)
        output_result = _verify_output(self.output, trust_store)

        for c in input_result.output_constraints:
            if not c.check(input_result.content, output_result):
                raise VerificationError(
                    f"output constraint failed: {c.description}"
                )

        return output_result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_cycle(obj: object, visited: frozenset[int]) -> None:
    if id(obj) in visited:
        raise VerificationError("cycle detected in attestation graph")


def _signed_envelope(
    content: Any,
    valid_until: float,
    constraints: list[OutputConstraint],
) -> dict:
    """Canonical dict that gets hashed and signed."""
    return {
        "content": content,
        "output_constraints": sorted(c.description for c in constraints),
        "valid_until": valid_until,
    }


def _verify_key_binding(
    kb: KeyBinding,
    expected_signer: str,
    expected_key: bytes,
    trust_store: TrustStore,
) -> None:
    if kb.signer_id != expected_signer:
        raise VerificationError(
            f"key binding signer {kb.signer_id} != expected {expected_signer}"
        )
    if kb.public_key != expected_key:
        raise VerificationError("signature key does not match key binding")

    anchor = trust_store.get(kb.trust_anchor_id)
    if anchor is None:
        raise VerificationError(
            f"trust anchor not found: {kb.trust_anchor_id}"
        )

    known = anchor.known_keys.get(kb.signer_id)
    if known is None or known != kb.public_key:
        raise VerificationError(
            f"key not recognised by anchor {kb.trust_anchor_id}"
        )

    binding_doc = {
        "public_key": kb.public_key.hex(),
        "signer_id": kb.signer_id,
        "trust_anchor_id": kb.trust_anchor_id,
    }
    if not verify_sig(
        kb.public_key,
        content_hash(binding_doc),
        kb.binding_proof,
    ):
        raise VerificationError("key binding proof-of-possession failed")


def _verify_output(output: Output, trust_store: TrustStore) -> VerifiedOutput:
    """Verify an output's optional signature and trust anchor.

    signer_id is only propagated to VerifiedOutput when backed by
    a verified signature AND a verified key binding.  Without both,
    the identity is unproven and treated as unsigned.
    """
    verified_signer: str | None = None

    if output.signature is not None:
        output_hash = content_hash(output.content)
        if not verify_sig(output.public_key, output_hash, output.signature):
            raise VerificationError(
                f"output signature verification failed for {output.signer_id}"
            )
        if output.key_binding is not None:
            _verify_key_binding(
                output.key_binding, output.signer_id,
                output.public_key, trust_store,
            )
            verified_signer = output.signer_id

    return VerifiedOutput(
        content=output.content,
        content_hash=content_hash(output.content),
        signer_id=verified_signer,
    )


# ---------------------------------------------------------------------------
# Construction helpers
# ---------------------------------------------------------------------------

def make_key_binding(
    keys: KeyPair,
    signer_id: str,
    trust_anchor_id: str,
) -> KeyBinding:
    """Create a [KeyBinding] with proof-of-possession."""
    binding_doc = {
        "public_key": keys.public_key_bytes.hex(),
        "signer_id": signer_id,
        "trust_anchor_id": trust_anchor_id,
    }
    binding_proof = sign(
        keys.private_key, content_hash(binding_doc),
    )
    return KeyBinding(
        signer_id=signer_id,
        public_key=keys.public_key_bytes,
        trust_anchor_id=trust_anchor_id,
        binding_proof=binding_proof,
    )


def make_signed_input(
    keys: KeyPair,
    key_binding: KeyBinding,
    content: Any,
    output_constraints: list[OutputConstraint] | None = None,
    valid_duration_sec: float = 86400,
) -> SignedInput:
    """Create a [SignedInput]."""
    constraints = output_constraints or []
    valid_until = time.time() + valid_duration_sec

    envelope = _signed_envelope(content, valid_until, constraints)
    envelope_hash = content_hash(envelope)
    sig = sign(keys.private_key, envelope_hash)

    return SignedInput(
        content=content,
        signer_id=key_binding.signer_id,
        public_key=keys.public_key_bytes,
        signature=sig,
        valid_until=valid_until,
        key_binding=key_binding,
        output_constraints=constraints,
    )


def make_output(content: Any) -> Output:
    """Create an unsigned [Output]."""
    return Output(content=content)


def sign_output(
    keys: KeyPair,
    key_binding: KeyBinding,
    content: Any,
) -> Output:
    """Create a signed [Output]."""
    output_hash = content_hash(content)
    sig = sign(keys.private_key, output_hash)
    return Output(
        content=content,
        signer_id=key_binding.signer_id,
        public_key=keys.public_key_bytes,
        signature=sig,
        key_binding=key_binding,
    )
