"""A real RFC 3161 timestamp-authority test double.

The implementation shells out to OpenSSL 3's RFC 3161 implementation.  This
keeps the timestamp token interoperable DER instead of inventing a signed JSON
lookalike.  Production uses an external ``sigstore/timestamp-authority``
service; the in-process class merely avoids a network daemon in unit tests.
"""

from __future__ import annotations

import datetime as dt
import subprocess
import tempfile
import threading
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID, ObjectIdentifier

SIGSTORE_TSA_POLICY_OID = ObjectIdentifier("1.3.6.1.4.1.57264.2")


def _run_openssl(*args: str) -> bytes:
    result = subprocess.run(
        ("openssl", *args),
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise ValueError(f"OpenSSL timestamp operation failed: {detail}")
    return result.stdout


class RFC3161TimestampAuthority:
    """Memory-scoped external-TSA analogue backed by OpenSSL RFC 3161."""

    def __init__(self, name: str = "fleetshift external TSA") -> None:
        self._tempdir = tempfile.TemporaryDirectory(prefix="fleetshift-tsa-")
        self._path = Path(self._tempdir.name)
        self._lock = threading.Lock()
        root_key = ec.generate_private_key(ec.SECP256R1())
        leaf_key = ec.generate_private_key(ec.SECP256R1())
        now = dt.datetime.now(dt.timezone.utc)
        root_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"{name} root")])
        self.root = (
            x509.CertificateBuilder()
            .subject_name(root_name)
            .issuer_name(root_name)
            .public_key(root_key.public_key())
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
            .sign(root_key, hashes.SHA256())
        )
        leaf_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        self.leaf = (
            x509.CertificateBuilder()
            .subject_name(leaf_name)
            .issuer_name(root_name)
            .public_key(leaf_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=5))
            .not_valid_after(now + dt.timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=True,
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
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.TIME_STAMPING]),
                critical=True,
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(root_key.public_key()),
                critical=False,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(leaf_key.public_key()),
                critical=False,
            )
            .sign(root_key, hashes.SHA256())
        )

        self._root_path = self._path / "root.pem"
        self._leaf_path = self._path / "tsa.pem"
        self._key_path = self._path / "tsa-key.pem"
        self._serial_path = self._path / "serial"
        self._config_path = self._path / "openssl-tsa.cnf"
        self._root_path.write_bytes(self.root_pem)
        self._leaf_path.write_bytes(self.leaf_pem)
        self._key_path.write_bytes(
            leaf_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        self._serial_path.write_text("01\n", encoding="ascii")
        self._config_path.write_text(
            "\n".join(
                (
                    "[ tsa ]",
                    "default_tsa = tsa_config",
                    "[ tsa_config ]",
                    f"serial = {self._serial_path}",
                    f"signer_cert = {self._leaf_path}",
                    f"certs = {self._root_path}",
                    f"signer_key = {self._key_path}",
                    "signer_digest = sha256",
                    f"default_policy = {SIGSTORE_TSA_POLICY_OID.dotted_string}",
                    f"other_policies = {SIGSTORE_TSA_POLICY_OID.dotted_string}",
                    "digests = sha256",
                    "accuracy = secs:1",
                    "ordering = yes",
                    "tsa_name = yes",
                    "ess_cert_id_chain = yes",
                    "ess_cert_id_alg = sha256",
                    "",
                )
            ),
            encoding="utf-8",
        )

    @property
    def root_pem(self) -> bytes:
        return self.root.public_bytes(serialization.Encoding.PEM)

    @property
    def leaf_pem(self) -> bytes:
        return self.leaf.public_bytes(serialization.Encoding.PEM)

    def timestamp(self, signature: bytes) -> bytes:
        """Return a DER TimeStampResp countersigning raw signature bytes."""

        if not signature:
            raise ValueError("cannot timestamp an empty signature")
        with self._lock, tempfile.TemporaryDirectory(
            prefix="request-", dir=self._path
        ) as request_dir:
            directory = Path(request_dir)
            signature_path = directory / "signature.bin"
            request_path = directory / "request.tsq"
            response_path = directory / "response.tsr"
            signature_path.write_bytes(signature)
            _run_openssl(
                "ts",
                "-query",
                "-data",
                str(signature_path),
                "-sha256",
                "-cert",
                "-out",
                str(request_path),
            )
            _run_openssl(
                "ts",
                "-reply",
                "-queryfile",
                str(request_path),
                "-config",
                str(self._config_path),
                "-section",
                "tsa_config",
                "-out",
                str(response_path),
            )
            return response_path.read_bytes()


def verify_rfc3161_timestamp(
    token: bytes,
    *,
    signature: bytes,
    tsa_root_pem: bytes,
    tsa_leaf_pem: bytes,
) -> dt.datetime:
    """Verify the token, its PKI, and message imprint; return trusted time."""

    with tempfile.TemporaryDirectory(prefix="fleetshift-ts-verify-") as tempdir:
        directory = Path(tempdir)
        token_path = directory / "response.tsr"
        signature_path = directory / "signature.bin"
        root_path = directory / "root.pem"
        leaf_path = directory / "tsa.pem"
        token_path.write_bytes(token)
        signature_path.write_bytes(signature)
        root_path.write_bytes(tsa_root_pem)
        leaf_path.write_bytes(tsa_leaf_pem)
        _run_openssl(
            "ts",
            "-verify",
            "-in",
            str(token_path),
            "-data",
            str(signature_path),
            "-CAfile",
            str(root_path),
            "-untrusted",
            str(leaf_path),
        )
        description = _run_openssl(
            "ts",
            "-reply",
            "-in",
            str(token_path),
            "-text",
        ).decode("utf-8", errors="strict")
    prefix = "Time stamp: "
    line = next((line for line in description.splitlines() if line.startswith(prefix)), None)
    if line is None:
        raise ValueError("RFC 3161 response has no generation time")
    timestamp = dt.datetime.strptime(line[len(prefix) :], "%b %d %H:%M:%S %Y GMT")
    return timestamp.replace(tzinfo=dt.timezone.utc)
