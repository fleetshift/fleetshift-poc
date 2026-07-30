"""In-process Fulcio-compatible CA for the POC.

Issues short-lived code-signing certificates that bind an OIDC-style identity
(issuer + subject) to an ephemeral public key, using Fulcio's OID extensions.
Production would run real Fulcio; this keeps tests free of extra processes while
exercising the same certificate shape verifiers expect.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.types import (
    CertificateIssuerPrivateKeyTypes,
    CertificatePublicKeyTypes,
)
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID, ObjectIdentifier

# Fulcio certificate extensions (https://github.com/sigstore/fulcio/blob/main/docs/oid-info.md)
OID_ISSUER_V2 = ObjectIdentifier("1.3.6.1.4.1.57264.1.8")
OID_SUBJECT = ObjectIdentifier("1.3.6.1.4.1.57264.1.24")

DEFAULT_TTL = dt.timedelta(minutes=10)


@dataclass(frozen=True)
class IssuedCertificate:
    certificate: x509.Certificate
    chain_pem: bytes  # leaf + root PEM concatenated


class LocalFulcio:
    """Ephemeral Fulcio-like CA (root key held in memory)."""

    def __init__(self, *, common_name: str = "poc-fulcio-root") -> None:
        self._key: CertificateIssuerPrivateKeyTypes = ec.generate_private_key(
            ec.SECP256R1()
        )
        now = dt.datetime.now(dt.timezone.utc)
        subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
        self._root = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(self._key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=1))
            .not_valid_after(now + dt.timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    key_cert_sign=True,
                    crl_sign=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .sign(self._key, hashes.SHA256())
        )

    @property
    def root_certificate(self) -> x509.Certificate:
        return self._root

    @property
    def root_pem(self) -> bytes:
        return self._root.public_bytes(serialization.Encoding.PEM)

    def issue(
        self,
        *,
        public_key: CertificatePublicKeyTypes,
        issuer: str,
        subject: str,
        ttl: dt.timedelta = DEFAULT_TTL,
    ) -> IssuedCertificate:
        """Issue a short-lived leaf binding ``issuer``/``subject`` to ``public_key``."""
        now = dt.datetime.now(dt.timezone.utc)
        # SAN mirrors common Fulcio encodings: email for users, URI for workloads.
        if subject.startswith("spiffe://") or subject.startswith("https://"):
            san = x509.SubjectAlternativeName([x509.UniformResourceIdentifier(subject)])
        else:
            san = x509.SubjectAlternativeName([x509.RFC822Name(subject)])

        leaf = (
            x509.CertificateBuilder()
            .subject_name(
                x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject[:64])])
            )
            .issuer_name(self._root.subject)
            .public_key(public_key)
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(seconds=30))
            .not_valid_after(now + ttl)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    key_cert_sign=False,
                    crl_sign=False,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CODE_SIGNING]),
                critical=True,
            )
            .add_extension(san, critical=False)
            .add_extension(
                x509.UnrecognizedExtension(OID_ISSUER_V2, issuer.encode("utf-8")),
                critical=False,
            )
            .add_extension(
                x509.UnrecognizedExtension(OID_SUBJECT, subject.encode("utf-8")),
                critical=False,
            )
            .sign(self._key, hashes.SHA256())
        )
        leaf_pem = leaf.public_bytes(serialization.Encoding.PEM)
        return IssuedCertificate(certificate=leaf, chain_pem=leaf_pem + self.root_pem)


def extract_identity(cert: x509.Certificate) -> tuple[str, str]:
    """Return ``(issuer, subject)`` from Fulcio OID extensions."""
    issuer = subject = None
    for ext in cert.extensions:
        if ext.oid == OID_ISSUER_V2:
            issuer = ext.value.value.decode("utf-8")  # type: ignore[attr-defined]
        elif ext.oid == OID_SUBJECT:
            subject = ext.value.value.decode("utf-8")  # type: ignore[attr-defined]
    if not issuer or not subject:
        raise ValueError("certificate missing Fulcio issuer/subject extensions")
    return issuer, subject
