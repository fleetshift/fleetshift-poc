"""DSSE + in-toto statement helpers (cosign attestation shape)."""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.types import (
    CertificateIssuerPrivateKeyTypes,
    CertificatePublicKeyTypes,
)

INTOTO_STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
DSSE_PAYLOAD_TYPE = "application/vnd.in-toto+json"
PREDICATE_TYPE = "https://fleetshift.io/attestation-envelope/v1"


def sha256_bytes(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


def build_statement(
    *,
    subject_name: str,
    subject_digest: bytes,
    predicate: dict[str, Any],
) -> dict[str, Any]:
    return {
        "_type": INTOTO_STATEMENT_TYPE,
        "subject": [
            {
                "name": subject_name,
                "digest": {"sha256": subject_digest.hex()},
            }
        ],
        "predicateType": PREDICATE_TYPE,
        "predicate": predicate,
    }


def dsse_pae(payload_type: str, payload: bytes) -> bytes:
    """DSSE Pre-Authentication Encoding (PAE)."""
    return (
        f"DSSEv1 {len(payload_type)} {payload_type} {len(payload)} ".encode()
        + payload
    )


def sign_dsse(
    private_key: CertificateIssuerPrivateKeyTypes,
    statement: dict[str, Any],
) -> dict[str, Any]:
    payload = canonical_json(statement)
    pae = dsse_pae(DSSE_PAYLOAD_TYPE, payload)
    signature = private_key.sign(pae, ec.ECDSA(hashes.SHA256()))  # type: ignore[union-attr]
    return {
        "payloadType": DSSE_PAYLOAD_TYPE,
        "payload": base64.b64encode(payload).decode("ascii"),
        "signatures": [
            {"sig": base64.b64encode(signature).decode("ascii")},
        ],
    }


def verify_dsse(
    public_key: CertificatePublicKeyTypes,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    if envelope.get("payloadType") != DSSE_PAYLOAD_TYPE:
        raise ValueError("unexpected DSSE payloadType")
    payload = base64.b64decode(envelope["payload"])
    sigs = envelope.get("signatures") or []
    if not sigs:
        raise ValueError("DSSE envelope has no signatures")
    signature = base64.b64decode(sigs[0]["sig"])
    pae = dsse_pae(DSSE_PAYLOAD_TYPE, payload)
    public_key.verify(signature, pae, ec.ECDSA(hashes.SHA256()))  # type: ignore[union-attr]
    statement = json.loads(payload)
    if statement.get("_type") != INTOTO_STATEMENT_TYPE:
        raise ValueError("not an in-toto statement")
    return statement
