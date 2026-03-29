"""Construction helpers for the hybrid prototype."""

from __future__ import annotations

import time
from typing import Any, Iterable

from .crypto import KeyPair, content_hash, sign
from .model import (
    KeyBinding,
    Output,
    OutputConstraint,
    OutputSignature,
    Signature,
    SignedInput,
)
from .policy import signed_input_envelope


def make_key_binding(
    keys: KeyPair,
    signer_id: str,
    trust_anchor_id: str,
) -> KeyBinding:
    binding_doc = {
        "public_key": keys.public_key_bytes.hex(),
        "signer_id": signer_id,
        "trust_anchor_id": trust_anchor_id,
    }
    binding_hash = content_hash(binding_doc)
    return KeyBinding(
        signer_id=signer_id,
        public_key=keys.public_key_bytes,
        trust_anchor_id=trust_anchor_id,
        binding_proof=sign(keys.private_key, binding_hash),
    )


def make_signed_input(
    keys: KeyPair,
    key_binding: KeyBinding,
    content: Any,
    *,
    output_constraints: Iterable[OutputConstraint] = (),
    valid_duration_sec: float = 86400,
) -> SignedInput:
    constraints = tuple(output_constraints)
    valid_until = time.time() + valid_duration_sec
    envelope = signed_input_envelope(content, valid_until, constraints)
    envelope_hash = content_hash(envelope)
    return SignedInput(
        content=content,
        signature=Signature(
            signer_id=key_binding.signer_id,
            public_key=keys.public_key_bytes,
            content_hash=envelope_hash,
            signature_bytes=sign(keys.private_key, envelope_hash),
        ),
        key_binding=key_binding,
        valid_until=valid_until,
        output_constraints=constraints,
    )


def make_output(content: Any) -> Output:
    return Output(content=content)


def sign_output(
    keys: KeyPair,
    signer_id: str,
    trust_anchor_id: str,
    content: Any,
) -> Output:
    output_hash = content_hash(content)
    return Output(
        content=content,
        signature=OutputSignature(
            signature=Signature(
                signer_id=signer_id,
                public_key=keys.public_key_bytes,
                content_hash=output_hash,
                signature_bytes=sign(keys.private_key, output_hash),
            ),
            trust_anchor_id=trust_anchor_id,
        ),
    )
