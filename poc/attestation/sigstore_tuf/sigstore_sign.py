"""Keyless signing: Fulcio leaf + DSSE/in-toto + TSA (Mode A cosign shape)."""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from . import dsse
from .fulcio import LocalFulcio, extract_identity
from .tsa import LocalTSA, TimestampToken, verify_timestamp
from .tuf_store import TrustedMaterial


@dataclass(frozen=True)
class Identity:
    """OIDC-style identity presented to Fulcio."""

    issuer: str
    subject: str


@dataclass(frozen=True)
class SigstoreBundle:
    """Cosign-compatible attestation bundle (Mode A: cert + DSSE + TSA)."""

    dsse_envelope: dict[str, Any]
    certificate_pem: bytes
    timestamp: TimestampToken
    trust_anchor_id: str
    identity: Identity

    def to_dict(self) -> dict[str, Any]:
        return {
            "mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json",
            "dsseEnvelope": self.dsse_envelope,
            "verificationMaterial": {
                "certificate": {
                    "rawBytes": base64.b64encode(self.certificate_pem).decode("ascii"),
                },
                "timestampVerificationData": {
                    "rfc3161Timestamps": [self.timestamp.to_dict()],
                },
            },
            "trustAnchorId": self.trust_anchor_id,
            "identity": {
                "issuer": self.identity.issuer,
                "subject": self.identity.subject,
            },
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> SigstoreBundle:
        cert_b64 = d["verificationMaterial"]["certificate"]["rawBytes"]
        ts = d["verificationMaterial"]["timestampVerificationData"]["rfc3161Timestamps"][0]
        ident = d["identity"]
        return SigstoreBundle(
            dsse_envelope=d["dsseEnvelope"],
            certificate_pem=base64.b64decode(cert_b64),
            timestamp=TimestampToken.from_dict(ts),
            trust_anchor_id=d["trustAnchorId"],
            identity=Identity(issuer=ident["issuer"], subject=ident["subject"]),
        )


@dataclass
class Signer:
    """Performs keyless signing through local Fulcio + TSA."""

    fulcio: LocalFulcio
    tsa: LocalTSA
    identity: Identity
    trust_anchor_id: str

    def sign_predicate(
        self,
        *,
        subject_name: str,
        subject_digest: bytes,
        predicate: dict[str, Any],
    ) -> SigstoreBundle:
        key = ec.generate_private_key(ec.SECP256R1())
        issued = self.fulcio.issue(
            public_key=key.public_key(),
            issuer=self.identity.issuer,
            subject=self.identity.subject,
        )
        statement = dsse.build_statement(
            subject_name=subject_name,
            subject_digest=subject_digest,
            predicate=predicate,
        )
        envelope = dsse.sign_dsse(key, statement)
        # Timestamp the PAE hash (what was signed), binding time to the signature event.
        pae = dsse.dsse_pae(dsse.DSSE_PAYLOAD_TYPE, dsse.canonical_json(statement))
        token = self.tsa.timestamp(dsse.sha256_bytes(pae))
        return SigstoreBundle(
            dsse_envelope=envelope,
            certificate_pem=issued.certificate.public_bytes(Encoding.PEM),
            timestamp=token,
            trust_anchor_id=self.trust_anchor_id,
            identity=self.identity,
        )


def _identity_allowed(identity: Identity, allowlist: list[dict[str, str]]) -> bool:
    for entry in allowlist:
        issuer_ok = entry.get("issuer") == identity.issuer
        if not issuer_ok and entry.get("issuer_regexp"):
            issuer_ok = bool(re.fullmatch(entry["issuer_regexp"], identity.issuer))
        if not issuer_ok:
            continue
        if entry.get("subject") == identity.subject:
            return True
        if entry.get("subject_regexp") and re.fullmatch(
            entry["subject_regexp"], identity.subject
        ):
            return True
    return False


def verify_sigstore_bundle(
    bundle: SigstoreBundle,
    *,
    trusted: TrustedMaterial,
    expected_subject_digest: bytes | None = None,
) -> dict[str, Any]:
    """Verify Mode A bundle; return the in-toto statement on success."""
    leaf = x509.load_pem_x509_certificate(bundle.certificate_pem)
    # Chain to one of the trusted Fulcio roots (user and/or workload).
    chained = False
    for root_pem in trusted.fulcio_roots_pem:
        fulcio_root = x509.load_pem_x509_certificate(root_pem)
        if leaf.issuer != fulcio_root.subject:
            continue
        try:
            fulcio_root.public_key().verify(  # type: ignore[union-attr]
                leaf.signature,
                leaf.tbs_certificate_bytes,
                ec.ECDSA(leaf.signature_hash_algorithm),  # type: ignore[arg-type]
            )
            chained = True
            break
        except Exception:  # noqa: BLE001
            continue
    if not chained:
        raise ValueError("Fulcio leaf does not chain to a trusted root")


    issuer, subject = extract_identity(leaf)
    if (issuer, subject) != (bundle.identity.issuer, bundle.identity.subject):
        raise ValueError("bundle identity does not match certificate extensions")

    anchor = trusted.anchors.get(bundle.trust_anchor_id)
    if anchor is None:
        raise ValueError(f"unknown trust anchor: {bundle.trust_anchor_id}")
    if not _identity_allowed(
        bundle.identity, anchor.get("allowed_identities", [])
    ):
        raise ValueError(
            f"identity {bundle.identity.subject} not allowed by {bundle.trust_anchor_id}"
        )

    statement = dsse.verify_dsse(leaf.public_key(), bundle.dsse_envelope)
    pae = dsse.dsse_pae(
        dsse.DSSE_PAYLOAD_TYPE,
        dsse.canonical_json(statement),
    )
    gen_time = verify_timestamp(
        bundle.timestamp,
        expected_digest=dsse.sha256_bytes(pae),
        trusted_roots=[trusted.tsa_root_pem],
    )

    # Trusted time must fall within the Fulcio leaf validity window.
    not_before = leaf.not_valid_before_utc
    not_after = leaf.not_valid_after_utc
    if not (not_before <= gen_time <= not_after):
        raise ValueError("timestamp outside Fulcio certificate validity window")

    subjects = statement.get("subject") or []
    if expected_subject_digest is not None:
        if not subjects:
            raise ValueError("statement missing subject")
        got = subjects[0]["digest"]["sha256"]
        if got != expected_subject_digest.hex():
            raise ValueError("in-toto subject digest mismatch")

    return statement
