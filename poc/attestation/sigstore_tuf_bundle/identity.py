"""OIDC-authenticated, proof-of-possession Fulcio test doubles.

The services are in-process so the POC is deterministic, but the issuance
ceremony is not skipped: Fulcio verifies an ES256 OIDC token, its audience and
time claims, and a proof-of-possession signature over the token subject before
issuing a ten-minute code-signing certificate.
"""

from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID, ObjectIdentifier

from .crypto import (
    der_utf8_string,
    make_es256_jwt,
    parse_der_utf8_string,
    verify_es256_jwt,
)

OID_ISSUER_V2 = ObjectIdentifier("1.3.6.1.4.1.57264.1.8")
OID_TOKEN_SUBJECT = ObjectIdentifier("1.3.6.1.4.1.57264.1.24")
SIGSTORE_AUDIENCE = "sigstore"


@dataclass(frozen=True)
class OIDCIdentity:
    issuer: str
    subject: str


class OIDCIssuer:
    """Minimal tenant or platform IdP used to exercise Fulcio authentication."""

    def __init__(self, issuer: str) -> None:
        self.issuer = issuer
        self._key = ec.generate_private_key(ec.SECP256R1())
        self.key_id = "poc-idp-es256"

    @property
    def public_key(self) -> ec.EllipticCurvePublicKey:
        return self._key.public_key()

    def mint(
        self,
        subject: str,
        *,
        audience: str = SIGSTORE_AUDIENCE,
        lifetime: dt.timedelta = dt.timedelta(minutes=5),
        now: float | None = None,
    ) -> str:
        issued_at = int(time.time() if now is None else now)
        return make_es256_jwt(
            self._key,
            {
                "aud": audience,
                "exp": issued_at + int(lifetime.total_seconds()),
                "iat": issued_at,
                "iss": self.issuer,
                "sub": subject,
            },
            key_id=self.key_id,
        )


@dataclass(frozen=True)
class IssuedCertificate:
    leaf: x509.Certificate
    intermediate: x509.Certificate


class FulcioCA:
    """Private Fulcio-shaped CA configured for one OIDC issuer."""

    def __init__(self, name: str, oidc_issuer: OIDCIssuer) -> None:
        self.name = name
        self.oidc_issuer = oidc_issuer
        self._root_key = ec.generate_private_key(ec.SECP256R1())
        self._intermediate_key = ec.generate_private_key(ec.SECP256R1())
        now = dt.datetime.now(dt.timezone.utc)
        root_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"{name} root")])
        self.root = (
            x509.CertificateBuilder()
            .subject_name(root_name)
            .issuer_name(root_name)
            .public_key(self._root_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=5))
            .not_valid_after(now + dt.timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=True, path_length=1), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=True,
                    crl_sign=True,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .sign(self._root_key, hashes.SHA256())
        )
        intermediate_name = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, f"{name} intermediate")]
        )
        self.intermediate = (
            x509.CertificateBuilder()
            .subject_name(intermediate_name)
            .issuer_name(self.root.subject)
            .public_key(self._intermediate_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=5))
            .not_valid_after(now + dt.timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=True,
                    crl_sign=True,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(
                    self._root_key.public_key()
                ),
                critical=False,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(
                    self._intermediate_key.public_key()
                ),
                critical=False,
            )
            .sign(self._root_key, hashes.SHA256())
        )

    @property
    def root_pem(self) -> bytes:
        return self.root.public_bytes(serialization.Encoding.PEM)

    @property
    def intermediate_pem(self) -> bytes:
        return self.intermediate.public_bytes(serialization.Encoding.PEM)

    def issue(
        self,
        *,
        identity_token: str,
        public_key: ec.EllipticCurvePublicKey,
        proof_of_possession: bytes,
        now: float | None = None,
        lifetime: dt.timedelta = dt.timedelta(minutes=10),
    ) -> IssuedCertificate:
        """Verify the Fulcio request and issue a short-lived leaf."""

        header, claims = verify_es256_jwt(
            identity_token,
            self.oidc_issuer.public_key,
        )
        current = time.time() if now is None else now
        if header.get("kid") != self.oidc_issuer.key_id:
            raise ValueError("OIDC token uses an unknown signing key")
        if claims.get("iss") != self.oidc_issuer.issuer:
            raise ValueError("OIDC issuer is not configured in this Fulcio")
        if claims.get("aud") != SIGSTORE_AUDIENCE:
            raise ValueError("OIDC token has the wrong audience")
        if not isinstance(claims.get("sub"), str) or not claims["sub"]:
            raise ValueError("OIDC token has no subject")
        if current < float(claims.get("iat", 0)) - 30:
            raise ValueError("OIDC token is not yet valid")
        if current >= float(claims.get("exp", 0)):
            raise ValueError("OIDC token is expired")
        try:
            public_key.verify(
                proof_of_possession,
                claims["sub"].encode("utf-8"),
                ec.ECDSA(hashes.SHA256()),
            )
        except InvalidSignature as exc:
            raise ValueError("Fulcio proof of possession is invalid") from exc

        subject = claims["sub"]
        if "@" in subject and not subject.startswith(("https://", "spiffe://")):
            san = x509.SubjectAlternativeName([x509.RFC822Name(subject)])
        else:
            san = x509.SubjectAlternativeName(
                [x509.UniformResourceIdentifier(subject)]
            )
        issued_at = dt.datetime.fromtimestamp(current, tz=dt.timezone.utc)
        leaf = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([]))
            .issuer_name(self.intermediate.subject)
            .public_key(public_key)
            .serial_number(x509.random_serial_number())
            .not_valid_before(issued_at - dt.timedelta(seconds=30))
            .not_valid_after(issued_at + lifetime)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CODE_SIGNING]),
                critical=True,
            )
            .add_extension(san, critical=True)
            .add_extension(
                x509.UnrecognizedExtension(
                    OID_ISSUER_V2,
                    der_utf8_string(claims["iss"]),
                ),
                critical=False,
            )
            .add_extension(
                x509.UnrecognizedExtension(
                    OID_TOKEN_SUBJECT,
                    der_utf8_string(subject),
                ),
                critical=False,
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(
                    self._intermediate_key.public_key()
                ),
                critical=False,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(public_key),
                critical=False,
            )
            .sign(self._intermediate_key, hashes.SHA256())
        )
        return IssuedCertificate(leaf=leaf, intermediate=self.intermediate)


def extract_fulcio_identity(certificate: x509.Certificate) -> OIDCIdentity:
    """Extract the canonical ``(issuer, token subject)`` identity tuple."""

    try:
        issuer = certificate.extensions.get_extension_for_oid(OID_ISSUER_V2)
        subject = certificate.extensions.get_extension_for_oid(OID_TOKEN_SUBJECT)
    except x509.ExtensionNotFound as exc:
        raise ValueError("certificate lacks required Fulcio identity extensions") from exc
    return OIDCIdentity(
        issuer=parse_der_utf8_string(issuer.value.value),  # type: ignore[attr-defined]
        subject=parse_der_utf8_string(subject.value.value),  # type: ignore[attr-defined]
    )
