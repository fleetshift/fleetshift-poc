"""Construction helpers for the hybrid prototype."""

from __future__ import annotations

import time
from typing import Any, Iterable

from .crypto import KeyPair, content_hash, sign
from .model import (
    KeyBinding,
    ManifestEnvelope,
    OutputConstraint,
    OutputSignature,
    PlacementEvidence,
    PutManifests,
    RemoveByDeploymentId,
    Signature,
    SignedInput,
    _serialize_manifests,
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
    expected_generation: int | None = None,
) -> SignedInput:
    constraints = tuple(output_constraints)
    valid_until = time.time() + valid_duration_sec
    envelope = signed_input_envelope(
        content, valid_until, constraints, expected_generation,
    )
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
        expected_generation=expected_generation,
    )


# ---------------------------------------------------------------------------
# Delivery output helpers
# ---------------------------------------------------------------------------


def make_put_manifests(
    manifests: tuple[ManifestEnvelope, ...],
    *,
    placement: PlacementEvidence | None = None,
) -> PutManifests:
    return PutManifests(manifests=manifests, placement=placement)


def sign_put_manifests(
    keys: KeyPair,
    signer_id: str,
    trust_anchor_id: str,
    manifests: tuple[ManifestEnvelope, ...],
    *,
    placement: PlacementEvidence | None = None,
) -> PutManifests:
    serialized = _serialize_manifests(manifests)
    manifest_hash = content_hash(serialized)
    return PutManifests(
        manifests=manifests,
        signature=OutputSignature(
            signature=Signature(
                signer_id=signer_id,
                public_key=keys.public_key_bytes,
                content_hash=manifest_hash,
                signature_bytes=sign(keys.private_key, manifest_hash),
            ),
            trust_anchor_id=trust_anchor_id,
        ),
        placement=placement,
    )


def make_remove_by_deployment_id(
    deployment_id: str,
    *,
    placement: PlacementEvidence | None = None,
) -> RemoveByDeploymentId:
    return RemoveByDeploymentId(deployment_id=deployment_id, placement=placement)


def make_placement_evidence(
    keys: KeyPair,
    signer_id: str,
    trust_anchor_id: str,
    targets: tuple[str, ...],
    *,
    deployment_id: str,
) -> PlacementEvidence:
    evidence_doc = {"deployment_id": deployment_id, "targets": list(targets)}
    doc_hash = content_hash(evidence_doc)
    return PlacementEvidence(
        deployment_id=deployment_id,
        targets=targets,
        signature=OutputSignature(
            signature=Signature(
                signer_id=signer_id,
                public_key=keys.public_key_bytes,
                content_hash=doc_hash,
                signature_bytes=sign(keys.private_key, doc_hash),
            ),
            trust_anchor_id=trust_anchor_id,
        ),
    )
