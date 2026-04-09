"""Minimal real cryptography for the prototype (Ed25519 + SHA-256)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
)


@dataclass
class KeyPair:
    private_key: Ed25519PrivateKey
    public_key_bytes: bytes


def generate_keypair() -> KeyPair:
    private_key = Ed25519PrivateKey.generate()
    public_key_bytes = private_key.public_key().public_bytes(
        Encoding.Raw, PublicFormat.Raw
    )
    return KeyPair(private_key, public_key_bytes)


def sign(private_key: Ed25519PrivateKey, data: bytes) -> bytes:
    return private_key.sign(data)


def verify(public_key_bytes: bytes, data: bytes, signature: bytes) -> bool:
    try:
        Ed25519PublicKey.from_public_bytes(public_key_bytes).verify(signature, data)
        return True
    except Exception:
        return False


def content_hash(content: Any) -> bytes:
    """Deterministic SHA-256 hash of arbitrary content via canonical JSON."""
    canonical = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).digest()
