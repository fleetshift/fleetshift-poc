import json
import base64
import hashlib
from typing import NamedTuple

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


class KeypairResult(NamedTuple):
    private_key_pem: str
    private_key_pem_base64: str
    jwks_json: str
    kid: str


def generate_cluster_keypair() -> KeypairResult:
    private_key, public_key = _generate_keypair()
    pem = _private_key_to_pem(private_key)
    pem_base64 = base64.b64encode(pem.encode("utf-8")).decode("utf-8")
    jwks_json, kid = _public_key_to_jwks(public_key)
    return KeypairResult(
        private_key_pem=pem,
        private_key_pem_base64=pem_base64,
        jwks_json=jwks_json,
        kid=kid,
    )


def _generate_keypair():
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=default_backend(),
    )
    return private_key, private_key.public_key()


def _private_key_to_pem(private_key) -> str:
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")


def _public_key_to_jwks(public_key):
    public_numbers = public_key.public_numbers()
    public_der = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    kid = base64.urlsafe_b64encode(
        hashlib.sha256(public_der).digest()
    ).decode("utf-8").rstrip("=")

    def int_to_base64url(n):
        n_bytes = n.to_bytes((n.bit_length() + 7) // 8, byteorder="big")
        return base64.urlsafe_b64encode(n_bytes).decode("utf-8").rstrip("=")

    jwks = {
        "keys": [
            {
                "use": "sig",
                "kty": "RSA",
                "kid": kid,
                "alg": "RS256",
                "n": int_to_base64url(public_numbers.n),
                "e": int_to_base64url(public_numbers.e),
            }
        ]
    }
    return json.dumps(jwks, indent=2), kid
