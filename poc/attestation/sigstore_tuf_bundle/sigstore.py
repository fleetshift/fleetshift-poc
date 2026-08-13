"""Sigstore v0.3 DSSE/in-toto bundles with external RFC 3161 time.

There are deliberately no Rekor entries and no SCTs in this profile.  The
external timestamp countersigns the raw DSSE signature, which is the exact
binding required by the Sigstore bundle protobuf and timestamp-authority
guidance.
"""

from __future__ import annotations

import base64
import copy
import datetime as dt
import json
from dataclasses import dataclass
from typing import Any, Iterable

import betterproto
from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, ExtensionOID, NameOID
from sigstore_protobuf_specs.dev.sigstore.bundle.v1 import (
    Bundle,
    TimestampVerificationData,
    VerificationMaterial,
)
from sigstore_protobuf_specs.dev.sigstore.common.v1 import (
    Rfc3161SignedTimestamp,
    X509Certificate,
)
from sigstore_protobuf_specs.io.intoto import Envelope, Signature

from .crypto import (
    DSSE_PAYLOAD_TYPE,
    INTOTO_STATEMENT_TYPE,
    canonical_json,
    dsse_pae,
)
from .identity import FulcioCA, OIDCIdentity, OIDCIssuer, extract_fulcio_identity
from .tsa import RFC3161TimestampAuthority, verify_rfc3161_timestamp
from .tuf import TrustedMaterial

BUNDLE_MEDIA_TYPE = "application/vnd.dev.sigstore.bundle.v0.3+json"

PREDICATE_DELIVERY_AUTHORIZATION = (
    "https://fleetshift.io/attestation/delivery-authorization/v1"
)
PREDICATE_MANIFEST_SET = "https://fleetshift.io/attestation/manifest-set/v1"
PREDICATE_PLACEMENT = "https://fleetshift.io/attestation/placement/v1"
PREDICATE_FULFILLMENT_RELATION = (
    "https://fleetshift.io/attestation/fulfillment-relation/v1"
)


@dataclass(frozen=True)
class SigstoreBundle:
    """JSON representation of the official Sigstore ``Bundle`` protobuf."""

    document: dict[str, Any]

    @classmethod
    def from_proto(cls, bundle: Bundle) -> SigstoreBundle:
        return cls(bundle.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return copy.deepcopy(self.document)

    def to_json(self) -> str:
        return json.dumps(self.document, sort_keys=True, separators=(",", ":"))

    def to_proto(self) -> Bundle:
        # Parsing through the maintained bindings catches bundle-shape errors.
        return Bundle.from_dict(copy.deepcopy(self.document))

    @property
    def dsse_envelope(self) -> dict[str, Any]:
        return copy.deepcopy(self.document["dsseEnvelope"])

    @property
    def certificate_der(self) -> bytes:
        return base64.b64decode(
            self.document["verificationMaterial"]["certificate"]["rawBytes"]
        )

    @property
    def timestamp_der(self) -> bytes:
        timestamps = self.document["verificationMaterial"][
            "timestampVerificationData"
        ]["rfc3161Timestamps"]
        return base64.b64decode(timestamps[0]["signedTimestamp"])


@dataclass(frozen=True)
class VerifiedSigstore:
    statement: dict[str, Any]
    identity: OIDCIdentity
    certificate_authority_uri: str
    timestamp_authority_uri: str
    integrated_time: dt.datetime


@dataclass(frozen=True)
class Signer:
    """A keyless signer using OIDC, the appropriate Fulcio, and an external TSA."""

    oidc_issuer: OIDCIssuer
    fulcio: FulcioCA
    tsa: RFC3161TimestampAuthority
    identity: OIDCIdentity
    # ``signer_id`` is FleetShift's stable logical workload/user identity.  It
    # is policy data, not self-authenticating Sigstore material: verification
    # derives it from the TUF-authenticated mapping for the certificate
    # identity.  Keeping it distinct lets a platform IdP use SPIFFE subjects
    # while delivery policy continues to name addons by addon ID.
    signer_id: str
    def __post_init__(self) -> None:
        if self.identity.issuer != self.oidc_issuer.issuer:
            raise ValueError("signer identity issuer does not match its OIDC issuer")
        if self.fulcio.oidc_issuer is not self.oidc_issuer:
            raise ValueError("signer OIDC issuer is not configured for this Fulcio")
        if not self.signer_id:
            raise ValueError("FleetShift signer ID must not be empty")

    def sign_predicate(
        self,
        *,
        subject_name: str,
        subject_digest: bytes,
        predicate_type: str,
        predicate: dict[str, Any],
    ) -> SigstoreBundle:
        if len(subject_digest) != 32:
            raise ValueError("in-toto subjects must use a SHA-256 digest")
        ephemeral_key = ec.generate_private_key(ec.SECP256R1())
        identity_token = self.oidc_issuer.mint(self.identity.subject)
        proof = ephemeral_key.sign(
            self.identity.subject.encode("utf-8"),
            ec.ECDSA(hashes.SHA256()),
        )
        issued = self.fulcio.issue(
            identity_token=identity_token,
            public_key=ephemeral_key.public_key(),
            proof_of_possession=proof,
        )
        statement = {
            "_type": INTOTO_STATEMENT_TYPE,
            "subject": [
                {
                    "name": subject_name,
                    "digest": {"sha256": subject_digest.hex()},
                }
            ],
            "predicateType": predicate_type,
            "predicate": predicate,
        }
        payload = canonical_json(statement)
        signature_bytes = ephemeral_key.sign(
            dsse_pae(DSSE_PAYLOAD_TYPE, payload),
            ec.ECDSA(hashes.SHA256()),
        )
        timestamp = self.tsa.timestamp(signature_bytes)
        proto = Bundle(
            media_type=BUNDLE_MEDIA_TYPE,
            verification_material=VerificationMaterial(
                certificate=X509Certificate(
                    raw_bytes=issued.leaf.public_bytes(serialization.Encoding.DER)
                ),
                timestamp_verification_data=TimestampVerificationData(
                    rfc3161_timestamps=[
                        Rfc3161SignedTimestamp(signed_timestamp=timestamp)
                    ]
                ),
            ),
            dsse_envelope=Envelope(
                payload=payload,
                payload_type=DSSE_PAYLOAD_TYPE,
                signatures=[Signature(sig=signature_bytes)],
            ),
        )
        return SigstoreBundle.from_proto(proto)


def _verify_ec_signature(
    certificate: x509.Certificate,
    issuer: x509.Certificate,
) -> None:
    key = issuer.public_key()
    if not isinstance(key, ec.EllipticCurvePublicKey):
        raise ValueError("POC trust roots must use ECDSA")
    try:
        key.verify(
            certificate.signature,
            certificate.tbs_certificate_bytes,
            ec.ECDSA(certificate.signature_hash_algorithm),
        )
    except InvalidSignature as exc:
        raise ValueError("certificate signature is invalid") from exc


def _time_in_range(moment: dt.datetime, start: dt.datetime, end: dt.datetime | None) -> bool:
    return moment >= start and (end is None or moment <= end)


def _authority_subject_matches(authority: Any, root: x509.Certificate) -> bool:
    common_names = root.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    organizations = root.subject.get_attributes_for_oid(NameOID.ORGANIZATION_NAME)
    return (
        authority.subject.common_name == (common_names[0].value if common_names else "")
        and authority.subject.organization
        == (organizations[0].value if organizations else "")
    )


def _verify_fulcio_chain(
    leaf: x509.Certificate,
    *,
    trusted: TrustedMaterial,
    integrated_time: dt.datetime,
) -> str:
    """Verify the fixed leaf -> intermediate -> root private-Fulcio chain."""

    for authority in trusted.sigstore_root.certificate_authorities:
        certificates = [
            x509.load_der_x509_certificate(item.raw_bytes)
            for item in authority.cert_chain.certificates
        ]
        if len(certificates) != 2:
            continue
        intermediate, root = certificates
        try:
            if leaf.issuer != intermediate.subject or intermediate.issuer != root.subject:
                continue
            if not _authority_subject_matches(authority, root):
                raise ValueError("Fulcio authority subject does not match its trust anchor")
            _verify_ec_signature(leaf, intermediate)
            _verify_ec_signature(intermediate, root)
            _verify_ec_signature(root, root)
            if not _time_in_range(
                integrated_time,
                authority.valid_for.start,
                authority.valid_for.end,
            ):
                raise ValueError("Fulcio authority was not trusted at signing time")
            for certificate in (leaf, intermediate, root):
                if not (
                    certificate.not_valid_before_utc
                    <= integrated_time
                    <= certificate.not_valid_after_utc
                ):
                    raise ValueError("certificate was not valid at signing time")
            for certificate in (intermediate, root):
                basic = certificate.extensions.get_extension_for_class(
                    x509.BasicConstraints
                ).value
                usage = certificate.extensions.get_extension_for_class(
                    x509.KeyUsage
                ).value
                if not basic.ca or not usage.key_cert_sign:
                    raise ValueError("Fulcio CA certificate lacks CA constraints")
            leaf_basic = leaf.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            leaf_usage = leaf.extensions.get_extension_for_class(x509.KeyUsage).value
            leaf_eku = leaf.extensions.get_extension_for_class(
                x509.ExtendedKeyUsage
            ).value
            if leaf_basic.ca or not leaf_usage.digital_signature:
                raise ValueError("Fulcio leaf is not a signing certificate")
            if ExtendedKeyUsageOID.CODE_SIGNING not in leaf_eku:
                raise ValueError("Fulcio leaf lacks code-signing EKU")
            try:
                leaf.extensions.get_extension_for_oid(
                    ExtensionOID.PRECERT_SIGNED_CERTIFICATE_TIMESTAMPS
                )
            except x509.ExtensionNotFound:
                pass
            else:
                raise ValueError("CT-omitted profile rejects certificates containing SCTs")
            return authority.uri
        except (InvalidSignature, x509.ExtensionNotFound, ValueError):
            continue
    raise ValueError("Fulcio leaf does not chain to a trusted authority")


def _verify_timestamp(
    token: bytes,
    signature_bytes: bytes,
    trusted: TrustedMaterial,
) -> tuple[dt.datetime, str]:
    errors: list[str] = []
    for authority in trusted.sigstore_root.timestamp_authorities:
        certificates = authority.cert_chain.certificates
        if len(certificates) != 2:
            errors.append(f"{authority.uri}: expected TSA leaf and root")
            continue
        leaf = x509.load_der_x509_certificate(certificates[0].raw_bytes)
        root = x509.load_der_x509_certificate(certificates[1].raw_bytes)
        try:
            integrated_time = verify_rfc3161_timestamp(
                token,
                signature=signature_bytes,
                tsa_root_pem=root.public_bytes(serialization.Encoding.PEM),
                tsa_leaf_pem=leaf.public_bytes(serialization.Encoding.PEM),
            )
            if not _time_in_range(
                integrated_time,
                authority.valid_for.start,
                authority.valid_for.end,
            ):
                raise ValueError("TSA was not trusted at the timestamp time")
            if not _authority_subject_matches(authority, root):
                raise ValueError("TSA authority subject does not match its trust anchor")
            for certificate in (leaf, root):
                if not (
                    certificate.not_valid_before_utc
                    <= integrated_time
                    <= certificate.not_valid_after_utc
                ):
                    raise ValueError("TSA certificate was not valid at timestamp time")
            root_basic = root.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            leaf_basic = leaf.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            leaf_eku = leaf.extensions.get_extension_for_class(
                x509.ExtendedKeyUsage
            ).value
            if not root_basic.ca or leaf_basic.ca:
                raise ValueError("TSA chain has invalid basic constraints")
            if ExtendedKeyUsageOID.TIME_STAMPING not in leaf_eku:
                raise ValueError("TSA leaf lacks time-stamping EKU")
            return integrated_time, authority.uri
        except (ValueError, x509.ExtensionNotFound) as exc:
            errors.append(f"{authority.uri}: {exc}")
    raise ValueError("no trusted RFC 3161 timestamp: " + "; ".join(errors))


def verify_sigstore_bundle(
    bundle: SigstoreBundle,
    *,
    trusted: TrustedMaterial,
    expected_subject_digest: bytes,
    expected_predicate_types: Iterable[str],
    expected_subject_name: str | None = None,
) -> VerifiedSigstore:
    """Verify the complete logless Sigstore bundle and return authenticated data."""

    proto = bundle.to_proto()
    if proto.media_type != BUNDLE_MEDIA_TYPE:
        raise ValueError("unsupported Sigstore bundle media type")
    content_field, content_value = betterproto.which_one_of(proto, "content")
    if content_field != "dsse_envelope":
        raise ValueError("FleetShift attestations require DSSE bundle content")
    envelope = content_value
    if envelope.payload_type != DSSE_PAYLOAD_TYPE:
        raise ValueError("unexpected DSSE payload type")
    if len(envelope.signatures) != 1:
        raise ValueError("Sigstore v0.3 DSSE bundles require exactly one signature")
    material = proto.verification_material
    material_field, material_value = betterproto.which_one_of(material, "content")
    if material_field != "certificate":
        raise ValueError("Sigstore v0.3 keyless bundle must contain one leaf certificate")
    if material.tlog_entries:
        raise ValueError("this profile does not accept transparency-log entries")
    timestamps = material.timestamp_verification_data.rfc3161_timestamps
    if len(timestamps) != 1:
        raise ValueError("exactly one external RFC 3161 timestamp is required")

    leaf = x509.load_der_x509_certificate(material_value.raw_bytes)
    signature_bytes = envelope.signatures[0].sig
    integrated_time, tsa_uri = _verify_timestamp(
        timestamps[0].signed_timestamp,
        signature_bytes,
        trusted,
    )
    ca_uri = _verify_fulcio_chain(
        leaf,
        trusted=trusted,
        integrated_time=integrated_time,
    )
    identity = extract_fulcio_identity(leaf)
    san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    projected_identities = set(san.get_values_for_type(x509.RFC822Name)) | set(
        san.get_values_for_type(x509.UniformResourceIdentifier)
    )
    if identity.subject not in projected_identities:
        raise ValueError("Fulcio token subject is inconsistent with certificate SAN")

    key = leaf.public_key()
    if not isinstance(key, ec.EllipticCurvePublicKey):
        raise ValueError("POC leaf signing key must use ECDSA")
    try:
        key.verify(
            signature_bytes,
            dsse_pae(envelope.payload_type, envelope.payload),
            ec.ECDSA(hashes.SHA256()),
        )
    except InvalidSignature as exc:
        raise ValueError("DSSE signature is invalid") from exc

    try:
        statement = json.loads(envelope.payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("DSSE payload is not an in-toto JSON statement") from exc
    if statement.get("_type") != INTOTO_STATEMENT_TYPE:
        raise ValueError("DSSE payload is not an in-toto Statement/v1")
    allowed_predicates = set(expected_predicate_types)
    if statement.get("predicateType") not in allowed_predicates:
        raise ValueError("unexpected FleetShift in-toto predicate type")
    subjects = statement.get("subject")
    if not isinstance(subjects, list) or len(subjects) != 1:
        raise ValueError("FleetShift statement requires exactly one subject")
    subject = subjects[0]
    digest = subject.get("digest")
    if not isinstance(digest, dict) or set(digest) != {"sha256"}:
        raise ValueError("in-toto subject must contain only a SHA-256 digest")
    if digest["sha256"] != expected_subject_digest.hex():
        raise ValueError("in-toto subject digest mismatch")
    if expected_subject_name is not None and subject.get("name") != expected_subject_name:
        raise ValueError("in-toto subject name mismatch")
    if not isinstance(statement.get("predicate"), dict):
        raise ValueError("FleetShift predicate must be an object")
    return VerifiedSigstore(
        statement=statement,
        identity=identity,
        certificate_authority_uri=ca_uri,
        timestamp_authority_uri=tsa_uri,
        integrated_time=integrated_time,
    )
