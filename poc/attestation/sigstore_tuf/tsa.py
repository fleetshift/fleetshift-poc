"""In-process RFC 3161-style timestamp authority for the POC.

Produces signed timestamps over a message digest. Production would run
sigstore/timestamp-authority (or another RFC 3161 TSA). This implementation
uses a compact signed note rather than full ASN.1 TimeStampToken so the POC
stays dependency-light while preserving the security property verifiers need:
trusted time proving a Fulcio leaf was valid when the signature was created.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
from dataclasses import dataclass

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


@dataclass(frozen=True)
class TimestampToken:
    """Signed timestamp over a SHA-256 digest."""

    digest_hex: str
    gen_time: str  # RFC3339 UTC
    signature_b64: str
    leaf_pem: bytes
    root_pem: bytes

    def to_dict(self) -> dict[str, str]:
        return {
            "digest_hex": self.digest_hex,
            "gen_time": self.gen_time,
            "signature_b64": self.signature_b64,
            "leaf_pem": self.leaf_pem.decode("ascii"),
            "root_pem": self.root_pem.decode("ascii"),
        }

    @staticmethod
    def from_dict(d: dict[str, str]) -> TimestampToken:
        return TimestampToken(
            digest_hex=d["digest_hex"],
            gen_time=d["gen_time"],
            signature_b64=d["signature_b64"],
            leaf_pem=d["leaf_pem"].encode("ascii"),
            root_pem=d["root_pem"].encode("ascii"),
        )


class LocalTSA:
    """Memory-backed timestamp authority with a dedicated timestamping cert."""

    def __init__(self, *, common_name: str = "poc-tsa") -> None:
        self._key = ec.generate_private_key(ec.SECP256R1())
        now = dt.datetime.now(dt.timezone.utc)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
        self._root = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(self._key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=1))
            .not_valid_after(now + dt.timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.TIME_STAMPING]),
                critical=True,
            )
            .sign(self._key, hashes.SHA256())
        )
        # Leaf shares the same key for POC simplicity (single-cert TSA).
        self._leaf = self._root

    @property
    def root_pem(self) -> bytes:
        return self._root.public_bytes(serialization.Encoding.PEM)

    @property
    def chain_pem(self) -> bytes:
        return self.root_pem

    def timestamp(self, digest: bytes) -> TimestampToken:
        if len(digest) != 32:
            raise ValueError("digest must be SHA-256 (32 bytes)")
        gen_time = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = json.dumps(
            {"digest_hex": digest.hex(), "gen_time": gen_time},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        # Sign the timestamp payload (not prehashed digest alone) so gen_time is bound.
        signature = self._key.sign(payload, ec.ECDSA(hashes.SHA256()))
        return TimestampToken(
            digest_hex=digest.hex(),
            gen_time=gen_time,
            signature_b64=base64.b64encode(signature).decode("ascii"),
            leaf_pem=self._leaf.public_bytes(serialization.Encoding.PEM),
            root_pem=self.root_pem,
        )


def verify_timestamp(
    token: TimestampToken,
    *,
    expected_digest: bytes,
    trusted_roots: list[bytes],
) -> dt.datetime:
    """Verify TSA token against trusted roots; return generation time (UTC)."""
    if token.digest_hex != expected_digest.hex():
        raise ValueError("timestamp digest mismatch")

    leaf = x509.load_pem_x509_certificate(token.leaf_pem)
    root = x509.load_pem_x509_certificate(token.root_pem)
    if root.public_bytes(serialization.Encoding.PEM) not in trusted_roots:
        # Also accept if leaf itself is the trusted root (single-cert TSA).
        if token.leaf_pem not in trusted_roots and token.root_pem not in trusted_roots:
            raise ValueError("timestamp authority root is not trusted")

    # Basic chain: leaf issued by root (or self-signed root).
    if leaf.issuer != root.subject:
        raise ValueError("timestamp leaf issuer mismatch")

    payload = json.dumps(
        {"digest_hex": token.digest_hex, "gen_time": token.gen_time},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    leaf.public_key().verify(  # type: ignore[union-attr]
        base64.b64decode(token.signature_b64),
        payload,
        ec.ECDSA(hashes.SHA256()),
    )
    return dt.datetime.strptime(token.gen_time, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=dt.timezone.utc
    )
