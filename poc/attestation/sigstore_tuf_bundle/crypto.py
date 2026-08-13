"""Small protocol helpers used by the POC.

The helpers are deliberately limited to canonical JSON, DSSE PAE, ES256 JWS,
and Fulcio's DER-encoded UTF8String extensions.  Certificate and timestamp
verification live in their respective modules.
"""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
    encode_dss_signature,
)

DSSE_PAYLOAD_TYPE = "application/vnd.in-toto+json"
INTOTO_STATEMENT_TYPE = "https://in-toto.io/Statement/v1"


def canonical_json(value: Any) -> bytes:
    """Return deterministic UTF-8 JSON bytes for signed POC documents."""

    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def dsse_pae(payload_type: str, payload: bytes) -> bytes:
    """DSSE v1 pre-authentication encoding."""

    return (
        f"DSSEv1 {len(payload_type.encode('utf-8'))} {payload_type} "
        f"{len(payload)} ".encode("utf-8")
        + payload
    )


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def make_es256_jwt(
    private_key: ec.EllipticCurvePrivateKey,
    claims: dict[str, Any],
    *,
    key_id: str,
) -> str:
    """Create a compact ES256 JWS using the raw ``R || S`` JWT encoding."""

    header = {"alg": "ES256", "kid": key_id, "typ": "JWT"}
    signing_input = (
        b64url_encode(canonical_json(header))
        + "."
        + b64url_encode(canonical_json(claims))
    ).encode("ascii")
    der_signature = private_key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der_signature)
    raw_signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    return signing_input.decode("ascii") + "." + b64url_encode(raw_signature)


def verify_es256_jwt(
    token: str,
    public_key: ec.EllipticCurvePublicKey,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Verify an ES256 compact JWS and return ``(header, claims)``."""

    try:
        header_part, claims_part, signature_part = token.split(".")
    except ValueError as exc:
        raise ValueError("OIDC token is not a compact JWS") from exc
    header = json.loads(b64url_decode(header_part))
    claims = json.loads(b64url_decode(claims_part))
    if header.get("alg") != "ES256":
        raise ValueError("OIDC token must use ES256")
    raw_signature = b64url_decode(signature_part)
    if len(raw_signature) != 64:
        raise ValueError("invalid ES256 JWS signature length")
    r = int.from_bytes(raw_signature[:32], "big")
    s = int.from_bytes(raw_signature[32:], "big")
    try:
        public_key.verify(
            encode_dss_signature(r, s),
            f"{header_part}.{claims_part}".encode("ascii"),
            ec.ECDSA(hashes.SHA256()),
        )
    except InvalidSignature as exc:
        raise ValueError("OIDC token signature is invalid") from exc
    return header, claims


def der_utf8_string(value: str) -> bytes:
    """Encode a DER UTF8String as required for Fulcio OIDs .1.8-.1.24."""

    encoded = value.encode("utf-8")
    if len(encoded) < 128:
        length = bytes([len(encoded)])
    else:
        length_bytes = len(encoded).to_bytes((len(encoded).bit_length() + 7) // 8, "big")
        length = bytes([0x80 | len(length_bytes)]) + length_bytes
    return b"\x0c" + length + encoded


def parse_der_utf8_string(value: bytes) -> str:
    """Decode the strict DER UTF8String representation used by Fulcio."""

    if len(value) < 2 or value[0] != 0x0C:
        raise ValueError("Fulcio extension is not a DER UTF8String")
    first_length = value[1]
    if first_length & 0x80:
        count = first_length & 0x7F
        if count == 0 or len(value) < 2 + count:
            raise ValueError("invalid DER UTF8String length")
        length = int.from_bytes(value[2 : 2 + count], "big")
        offset = 2 + count
    else:
        length = first_length
        offset = 2
    if offset + length != len(value):
        raise ValueError("invalid DER UTF8String payload length")
    return value[offset:].decode("utf-8")
